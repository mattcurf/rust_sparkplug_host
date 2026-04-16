// SPDX-FileCopyrightText: 2026 Matt Curfman
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use rumqttc::{
    AsyncClient, Event, Outgoing, Packet, Publish, QoS, SubscribeFilter, SubscribeReasonCode,
};
use serde::Serialize;
use sparkplug_rs::protobuf::{Enum, Message};
use sparkplug_rs::{DataType, DeviceMessageType, NodeMessageType, Payload, payload};

use crate::config::AppConfig;
use crate::decode::{DecodedPayload, decode_payload, decode_payload_with_metric_datatypes};
use crate::model::{BirthOutcome, DataOutcome, DeathOutcome, HostRuntimeModel, MetricUpdate};
use crate::mqtt::{MqttRuntimePlan, build_runtime_plan};
use crate::reorder::{ReorderEvent, ReorderKey, ReorderRequest, ReorderTracker};
use crate::state;
use crate::topic::{SparkplugTopic, parse_topic};

pub struct SparkplugPrimaryHost {
    config: AppConfig,
    model: HostRuntimeModel,
}

impl SparkplugPrimaryHost {
    pub fn new(config: AppConfig) -> Self {
        let model = HostRuntimeModel::new(config.reorder_timeout);
        Self { config, model }
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            let connect_timestamp = current_timestamp_millis();
            let runtime = build_runtime_plan(&self.config, connect_timestamp)?;
            match self.run_session(runtime).await? {
                SessionOutcome::Reconnect => {
                    println!(
                        "HOST event=session_restart reconnect_delay_ms={} tracked_nodes={}",
                        RECONNECT_DELAY.as_millis(),
                        self.model.edge_node_count(),
                    );
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
                SessionOutcome::Shutdown => return Ok(()),
            }
        }
    }

    async fn run_session(&mut self, runtime: MqttRuntimePlan) -> Result<SessionOutcome> {
        let MqttRuntimePlan {
            mqtt_options,
            connect_timestamp,
            state_topic,
            will_payload,
            birth_payload,
            subscription_filters,
        } = runtime;

        println!(
            "HOST event=session_start broker={} host_id={} client_id={} tls={} auth={} connect_timestamp={} reorder_timeout_ms={}",
            self.config.broker,
            self.config.host_id,
            self.config.client_id,
            self.config.broker.scheme.is_tls(),
            self.config.username.is_some(),
            connect_timestamp,
            self.config.reorder_timeout.as_millis(),
        );

        let session = SessionRuntime {
            state_topic,
            birth_payload,
            death_payload: will_payload,
            subscription_filters,
        };

        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
        let mut state = SessionState::default();
        let mut reorder = ReorderTracker::new(self.config.reorder_timeout);

        loop {
            let reorder_deadline = reorder.next_deadline();
            tokio::select! {
                _ = tokio::signal::ctrl_c(), if matches!(state.shutdown, ShutdownState::Running) => {
                    println!("HOST action=shutdown_requested connected={} birth_published={}", state.connected, state.birth_published);
                    if state.connected {
                        self.publish_state_death(&client, &session).await?;
                        state.shutdown = ShutdownState::WaitingForDeathSend;
                    } else {
                        println!("HOST event=shutdown_complete connected=false");
                        return Ok(SessionOutcome::Shutdown);
                    }
                }
                _ = async {
                    tokio::time::sleep_until(reorder_deadline.expect("reorder timer is guarded")).await;
                }, if reorder_deadline.is_some() && matches!(state.shutdown, ShutdownState::Running) => {
                    self.handle_reorder_timeout(&client, &mut reorder).await?;
                }
                event = eventloop.poll() => {
                    match event {
                        Ok(event) => {
                            if self.handle_event(&client, &session, &mut state, &mut reorder, event).await? {
                                return Ok(SessionOutcome::Shutdown);
                            }
                        }
                        Err(error) => {
                            println!("MQTT event=connection_error error={error}");
                            if state.shutdown.is_active() {
                                println!("HOST event=shutdown_complete reason=connection_error_during_shutdown");
                                return Ok(SessionOutcome::Shutdown);
                            }
                            if state.connected {
                                let stale_metric_count = self
                                    .model
                                    .mark_metrics_stale_for_broker_disconnect(
                                        current_timestamp_millis(),
                                    );
                                println!(
                                    "HOST event=broker_disconnect tracked_nodes={} stale_metric_count={} action=mark_metrics_stale",
                                    self.model.edge_node_count(),
                                    stale_metric_count,
                                );
                            }
                            return Ok(SessionOutcome::Reconnect);
                        }
                    }
                }
            }
        }
    }

    async fn handle_event(
        &mut self,
        client: &AsyncClient,
        session: &SessionRuntime,
        state: &mut SessionState,
        reorder: &mut ReorderTracker,
        event: Event,
    ) -> Result<bool> {
        match event {
            Event::Incoming(packet) => {
                self.handle_incoming(client, session, state, reorder, packet)
                    .await
            }
            Event::Outgoing(outgoing) => self.handle_outgoing(state, outgoing),
        }
    }

    async fn handle_incoming(
        &mut self,
        client: &AsyncClient,
        session: &SessionRuntime,
        state: &mut SessionState,
        reorder: &mut ReorderTracker,
        packet: Packet,
    ) -> Result<bool> {
        match packet {
            Packet::ConnAck(connack) => {
                println!(
                    "MQTT event=connected session_present={} subscription_count={} tracked_nodes={}",
                    connack.session_present,
                    session.subscription_filters.len(),
                    self.model.edge_node_count(),
                );
                state.connected = true;
                state.initial_subscribe_pending = true;
                self.subscribe_all(client, session).await?;
                Ok(false)
            }
            Packet::SubAck(suback) => {
                println!(
                    "MQTT event=suback pkid={} return_codes={}",
                    suback.pkid,
                    format!("{:?}", suback.return_codes),
                );

                if state.initial_subscribe_pending && !state.birth_published {
                    if suback.return_codes.len() != session.subscription_filters.len() {
                        bail!(
                            "expected {} subscription acks but broker returned {}",
                            session.subscription_filters.len(),
                            suback.return_codes.len()
                        );
                    }

                    if suback
                        .return_codes
                        .iter()
                        .any(|code| matches!(code, SubscribeReasonCode::Failure))
                    {
                        bail!("broker rejected one or more required subscriptions")
                    }

                    state.initial_subscribe_pending = false;
                    state.birth_published = true;
                    self.publish_state_birth(client, session).await?;
                }

                Ok(false)
            }
            Packet::Publish(publish) => {
                self.handle_publish(client, session, state, reorder, publish)
                    .await?;
                Ok(false)
            }
            Packet::PubAck(puback) => {
                println!("MQTT event=puback pkid={}", puback.pkid);
                if let ShutdownState::WaitingForDeathAck { pkid } = state.shutdown {
                    if puback.pkid == pkid {
                        println!("STATE event=death_delivery_confirmed pkid={}", puback.pkid);
                        client
                            .disconnect()
                            .await
                            .context("failed to request MQTT disconnect")?;
                        state.shutdown = ShutdownState::DisconnectRequested;
                    }
                }

                Ok(false)
            }
            Packet::PingResp => {
                println!("MQTT event=ping_response");
                Ok(false)
            }
            other => {
                println!("MQTT event=incoming packet={other:?}");
                Ok(false)
            }
        }
    }

    fn handle_outgoing(&self, state: &mut SessionState, outgoing: Outgoing) -> Result<bool> {
        match outgoing {
            Outgoing::Publish(pkid) => {
                println!("MQTT event=publish_sent pkid={pkid}");
                if matches!(state.shutdown, ShutdownState::WaitingForDeathSend) {
                    state.shutdown = ShutdownState::WaitingForDeathAck { pkid };
                }
                Ok(false)
            }
            Outgoing::Subscribe(pkid) => {
                println!("MQTT event=subscribe_sent pkid={pkid}");
                Ok(false)
            }
            Outgoing::Disconnect => {
                println!("MQTT event=disconnect_sent");
                Ok(matches!(state.shutdown, ShutdownState::DisconnectRequested))
            }
            other => {
                println!("MQTT event=outgoing packet={other:?}");
                Ok(false)
            }
        }
    }

    async fn handle_publish(
        &mut self,
        client: &AsyncClient,
        session: &SessionRuntime,
        session_state: &SessionState,
        reorder: &mut ReorderTracker,
        publish: Publish,
    ) -> Result<()> {
        println!(
            "MQTT event=publish_received topic={} qos={:?} retain={} bytes={}",
            publish.topic,
            publish.qos,
            publish.retain,
            publish.payload.len(),
        );

        match parse_topic(&publish.topic) {
            Ok(SparkplugTopic::State { host_id }) => {
                let payload = state::decode_payload(publish.payload.as_ref())
                    .context("failed to decode STATE payload")?;
                println!(
                    "STATE event=received host_id={} online={} timestamp={} retain={}",
                    host_id, payload.online, payload.timestamp, publish.retain,
                );

                if host_id == self.config.host_id
                    && !payload.online
                    && session_state.birth_published
                    && !session_state.shutdown.is_active()
                {
                    println!(
                        "STATE event=offline_echo_detected host_id={} action=republish_online_birth",
                        host_id,
                    );
                    self.publish_state_birth(client, session).await?;
                }
            }
            Ok(SparkplugTopic::Node {
                group_id,
                message_type,
                edge_node_id,
            }) => match message_type {
                NodeMessageType::NBIRTH => {
                    let Some(payload) =
                        decode_sparkplug_publish_payload(&publish.topic, publish.payload.as_ref())
                    else {
                        return Ok(());
                    };
                    let outcome =
                        self.model
                            .apply_node_birth(&group_id, &edge_node_id, &payload)?;
                    self.log_birth(
                        "node_birth",
                        &group_id,
                        &edge_node_id,
                        None,
                        &payload,
                        &outcome,
                    );
                    self.reset_reorder_tracking(
                        reorder,
                        &group_id,
                        &edge_node_id,
                        outcome.sequence,
                    );
                }
                NodeMessageType::NDATA => {
                    let payload = match decode_payload_with_metric_datatypes(
                        publish.payload.as_ref(),
                        |name, alias| {
                            self.model
                                .node_metric_datatype(&group_id, &edge_node_id, name, alias)
                        },
                    ) {
                        Ok(payload) => payload,
                        Err(error) => {
                            log_payload_decode_error(
                                &publish.topic,
                                publish.payload.as_ref(),
                                &error,
                            );
                            self.handle_malformed_edge_node_message(
                                client,
                                "NDATA",
                                &group_id,
                                &edge_node_id,
                                None,
                                &publish.topic,
                                &error,
                            )
                            .await?;
                            return Ok(());
                        }
                    };
                    let outcome =
                        match self
                            .model
                            .apply_node_data(&group_id, &edge_node_id, &payload)
                        {
                            Ok(outcome) => outcome,
                            Err(error) => {
                                self.handle_malformed_edge_node_message(
                                    client,
                                    "NDATA",
                                    &group_id,
                                    &edge_node_id,
                                    None,
                                    &publish.topic,
                                    &error,
                                )
                                .await?;
                                return Ok(());
                            }
                        };
                    self.observe_reorder_sequence(
                        reorder,
                        "node",
                        &group_id,
                        &edge_node_id,
                        None,
                        outcome.sequence,
                    );
                    self.log_data_updates("node_data", &group_id, &edge_node_id, None, &outcome);
                }
                NodeMessageType::NDEATH => {
                    let Some(payload) =
                        decode_sparkplug_publish_payload(&publish.topic, publish.payload.as_ref())
                    else {
                        return Ok(());
                    };
                    let outcome = self.model.apply_node_death(
                        &group_id,
                        &edge_node_id,
                        &payload,
                        current_timestamp_millis(),
                    )?;
                    self.log_death(
                        "node_death",
                        &group_id,
                        &edge_node_id,
                        None,
                        &payload,
                        &outcome,
                    );
                    reorder.clear(&reorder_key(&group_id, &edge_node_id));
                }
                NodeMessageType::NCMD => {
                    let Some(payload) =
                        decode_sparkplug_publish_payload(&publish.topic, publish.payload.as_ref())
                    else {
                        return Ok(());
                    };
                    println!(
                        "SPARKPLUG event=ignored_node_command group_id={} edge_node_id={} payload={}",
                        group_id,
                        edge_node_id,
                        json(&payload),
                    );
                }
            },
            Ok(SparkplugTopic::Device {
                group_id,
                message_type,
                edge_node_id,
                device_id,
            }) => match message_type {
                DeviceMessageType::DBIRTH => {
                    let Some(payload) =
                        decode_sparkplug_publish_payload(&publish.topic, publish.payload.as_ref())
                    else {
                        return Ok(());
                    };
                    let outcome = match self.model.apply_device_birth(
                        &group_id,
                        &edge_node_id,
                        &device_id,
                        &payload,
                    ) {
                        Ok(outcome) => outcome,
                        Err(error) => {
                            self.handle_malformed_edge_node_message(
                                client,
                                "DBIRTH",
                                &group_id,
                                &edge_node_id,
                                Some(&device_id),
                                &publish.topic,
                                &error,
                            )
                            .await?;
                            return Ok(());
                        }
                    };
                    self.log_birth(
                        "device_birth",
                        &group_id,
                        &edge_node_id,
                        Some(&device_id),
                        &payload,
                        &outcome,
                    );
                    self.observe_reorder_sequence(
                        reorder,
                        "device",
                        &group_id,
                        &edge_node_id,
                        Some(&device_id),
                        outcome.sequence,
                    );
                }
                DeviceMessageType::DDATA => {
                    let payload = match decode_payload_with_metric_datatypes(
                        publish.payload.as_ref(),
                        |name, alias| {
                            self.model.device_metric_datatype(
                                &group_id,
                                &edge_node_id,
                                &device_id,
                                name,
                                alias,
                            )
                        },
                    ) {
                        Ok(payload) => payload,
                        Err(error) => {
                            log_payload_decode_error(
                                &publish.topic,
                                publish.payload.as_ref(),
                                &error,
                            );
                            self.handle_malformed_edge_node_message(
                                client,
                                "DDATA",
                                &group_id,
                                &edge_node_id,
                                Some(&device_id),
                                &publish.topic,
                                &error,
                            )
                            .await?;
                            return Ok(());
                        }
                    };
                    let outcome = match self.model.apply_device_data(
                        &group_id,
                        &edge_node_id,
                        &device_id,
                        &payload,
                    ) {
                        Ok(outcome) => outcome,
                        Err(error) => {
                            self.handle_malformed_edge_node_message(
                                client,
                                "DDATA",
                                &group_id,
                                &edge_node_id,
                                Some(&device_id),
                                &publish.topic,
                                &error,
                            )
                            .await?;
                            return Ok(());
                        }
                    };
                    self.log_data_updates(
                        "device_data",
                        &group_id,
                        &edge_node_id,
                        Some(&device_id),
                        &outcome,
                    );
                    self.observe_reorder_sequence(
                        reorder,
                        "device",
                        &group_id,
                        &edge_node_id,
                        Some(&device_id),
                        outcome.sequence,
                    );
                }
                DeviceMessageType::DDEATH => {
                    let Some(payload) =
                        decode_sparkplug_publish_payload(&publish.topic, publish.payload.as_ref())
                    else {
                        return Ok(());
                    };
                    let outcome = match self.model.apply_device_death(
                        &group_id,
                        &edge_node_id,
                        &device_id,
                        &payload,
                    ) {
                        Ok(outcome) => outcome,
                        Err(error) => {
                            self.handle_malformed_edge_node_message(
                                client,
                                "DDEATH",
                                &group_id,
                                &edge_node_id,
                                Some(&device_id),
                                &publish.topic,
                                &error,
                            )
                            .await?;
                            return Ok(());
                        }
                    };
                    self.log_death(
                        "device_death",
                        &group_id,
                        &edge_node_id,
                        Some(&device_id),
                        &payload,
                        &outcome,
                    );
                    if let Some(sequence) = outcome.sequence {
                        self.observe_reorder_sequence(
                            reorder,
                            "device",
                            &group_id,
                            &edge_node_id,
                            Some(&device_id),
                            sequence,
                        );
                    }
                }
                other => {
                    let Some(payload) =
                        decode_sparkplug_publish_payload(&publish.topic, publish.payload.as_ref())
                    else {
                        return Ok(());
                    };
                    println!(
                        "SPARKPLUG event=ignored_device_message message_type={} group_id={} edge_node_id={} device_id={} payload={}",
                        other.to_string(),
                        group_id,
                        edge_node_id,
                        device_id,
                        json(&payload),
                    );
                }
            },
            Err(error) => {
                println!(
                    "SPARKPLUG event=topic_parse_error topic={} error={error}",
                    publish.topic,
                );
            }
        }

        Ok(())
    }

    async fn subscribe_all(&self, client: &AsyncClient, session: &SessionRuntime) -> Result<()> {
        let filters = session
            .subscription_filters
            .iter()
            .map(|topic| SubscribeFilter::new(topic.clone(), QoS::AtLeastOnce))
            .collect::<Vec<_>>();

        println!(
            "MQTT action=subscribe qos=1 topics={}",
            json(&session.subscription_filters),
        );

        client
            .subscribe_many(filters)
            .await
            .context("failed to send MQTT subscribe request")
    }

    async fn publish_state_birth(
        &self,
        client: &AsyncClient,
        session: &SessionRuntime,
    ) -> Result<()> {
        println!(
            "STATE action=publish_online_birth topic={} retain=true qos=1 payload={}",
            session.state_topic,
            String::from_utf8_lossy(&session.birth_payload),
        );

        client
            .publish(
                session.state_topic.clone(),
                QoS::AtLeastOnce,
                true,
                session.birth_payload.clone(),
            )
            .await
            .context("failed to publish STATE birth")
    }

    async fn publish_state_death(
        &self,
        client: &AsyncClient,
        session: &SessionRuntime,
    ) -> Result<()> {
        println!(
            "STATE action=publish_offline_death topic={} retain=true qos=1 payload={}",
            session.state_topic,
            String::from_utf8_lossy(&session.death_payload),
        );

        client
            .publish(
                session.state_topic.clone(),
                QoS::AtLeastOnce,
                true,
                session.death_payload.clone(),
            )
            .await
            .context("failed to publish STATE death")
    }

    async fn handle_reorder_timeout(
        &self,
        client: &AsyncClient,
        reorder: &mut ReorderTracker,
    ) -> Result<()> {
        for request in reorder.due_rebirth_requests(tokio::time::Instant::now()) {
            println!(
                "REORDER event=timeout_elapsed group_id={} edge_node_id={} expected={} observed={} timeout_ms={} action=publish_rebirth_request",
                request.key.group_id,
                request.key.edge_node_id,
                request.gap.expected,
                request.gap.observed,
                self.config.reorder_timeout.as_millis(),
            );
            self.publish_node_rebirth_request(client, &request).await?;
        }

        Ok(())
    }

    async fn publish_node_rebirth_request(
        &self,
        client: &AsyncClient,
        request: &ReorderRequest,
    ) -> Result<()> {
        println!(
            "SPARKPLUG action=publish_node_rebirth_request group_id={} edge_node_id={} topic={} qos=0 retain=false timestamp={} expected={} observed={}",
            request.key.group_id,
            request.key.edge_node_id,
            node_rebirth_topic(&request.key.group_id, &request.key.edge_node_id),
            current_timestamp_millis(),
            request.gap.expected,
            request.gap.observed,
        );

        self.publish_node_rebirth_request_for_edge_node(
            client,
            &request.key.group_id,
            &request.key.edge_node_id,
        )
        .await
    }

    async fn handle_malformed_edge_node_message(
        &self,
        client: &AsyncClient,
        message_type: &str,
        group_id: &str,
        edge_node_id: &str,
        device_id: Option<&str>,
        topic: &str,
        error: &anyhow::Error,
    ) -> Result<()> {
        println!(
            "SPARKPLUG event=malformed_message message_type={} group_id={} edge_node_id={} device_id={} topic={} error={} action=publish_rebirth_request",
            message_type,
            group_id,
            edge_node_id,
            device_id.unwrap_or("-"),
            topic,
            error,
        );

        self.publish_node_rebirth_request_for_edge_node(client, group_id, edge_node_id)
            .await
    }

    async fn publish_node_rebirth_request_for_edge_node(
        &self,
        client: &AsyncClient,
        group_id: &str,
        edge_node_id: &str,
    ) -> Result<()> {
        let timestamp = current_timestamp_millis();
        let topic = node_rebirth_topic(group_id, edge_node_id);
        let payload = build_node_rebirth_payload(timestamp)?;

        client
            .publish(topic, QoS::AtMostOnce, false, payload)
            .await
            .context("failed to publish NCMD rebirth request")
    }

    fn reset_reorder_tracking(
        &self,
        reorder: &mut ReorderTracker,
        group_id: &str,
        edge_node_id: &str,
        sequence: u8,
    ) {
        reorder.reset(reorder_key(group_id, edge_node_id), sequence);
    }

    fn observe_reorder_sequence(
        &self,
        reorder: &mut ReorderTracker,
        scope: &str,
        group_id: &str,
        edge_node_id: &str,
        device_id: Option<&str>,
        sequence: u8,
    ) {
        match reorder.observe(
            reorder_key(group_id, edge_node_id),
            sequence,
            tokio::time::Instant::now(),
        ) {
            Some(ReorderEvent::GapDetected(gap)) => {
                self.log_gap(scope, group_id, edge_node_id, device_id, Some(&gap))
            }
            Some(ReorderEvent::GapResolved(gap)) => {
                println!(
                    "REORDER event=gap_resolved scope={} group_id={} edge_node_id={} device_id={} expected={} observed={} action=continue_normal_operation",
                    scope,
                    group_id,
                    edge_node_id,
                    device_id.unwrap_or("-"),
                    gap.expected,
                    gap.observed,
                );
            }
            None => {}
        }
    }

    fn log_birth(
        &self,
        event_name: &str,
        group_id: &str,
        edge_node_id: &str,
        device_id: Option<&str>,
        payload: &impl Serialize,
        outcome: &BirthOutcome,
    ) {
        println!(
            "SPARKPLUG event={} group_id={} edge_node_id={} device_id={} seq={} bd_seq={} metric_count={} payload={}",
            event_name,
            group_id,
            edge_node_id,
            device_id.unwrap_or("-"),
            outcome.sequence,
            outcome
                .bd_seq
                .map_or_else(|| "-".to_string(), |value| value.to_string()),
            outcome.metric_count,
            json(payload),
        );
    }

    fn log_death(
        &self,
        event_name: &str,
        group_id: &str,
        edge_node_id: &str,
        device_id: Option<&str>,
        payload: &impl Serialize,
        outcome: &DeathOutcome,
    ) {
        println!(
            "SPARKPLUG event={} group_id={} edge_node_id={} device_id={} seq={} bd_seq={} stale_metric_count={} ignored={} reason={} payload={}",
            event_name,
            group_id,
            edge_node_id,
            device_id.unwrap_or("-"),
            outcome
                .sequence
                .map_or_else(|| "-".to_string(), |value| value.to_string()),
            outcome
                .bd_seq
                .map_or_else(|| "-".to_string(), |value| value.to_string()),
            outcome.stale_metric_count,
            outcome.ignored,
            outcome.reason.as_deref().unwrap_or("-"),
            json(payload),
        );
    }

    fn log_data_updates(
        &self,
        event_name: &str,
        group_id: &str,
        edge_node_id: &str,
        device_id: Option<&str>,
        outcome: &DataOutcome,
    ) {
        println!(
            "SPARKPLUG event={} group_id={} edge_node_id={} device_id={} seq={} changed_metric_count={}",
            event_name,
            group_id,
            edge_node_id,
            device_id.unwrap_or("-"),
            outcome.sequence,
            outcome.changes.len(),
        );

        if outcome.changes.is_empty() {
            println!(
                "SPARKPLUG event={} group_id={} edge_node_id={} device_id={} detail=no_effective_metric_changes",
                event_name,
                group_id,
                edge_node_id,
                device_id.unwrap_or("-"),
            );
        }

        for update in &outcome.changes {
            self.log_metric_update(event_name, group_id, edge_node_id, device_id, update);
        }
    }

    fn log_metric_update(
        &self,
        event_name: &str,
        group_id: &str,
        edge_node_id: &str,
        device_id: Option<&str>,
        update: &MetricUpdate,
    ) {
        println!(
            "SPARKPLUG event={} group_id={} edge_node_id={} device_id={} metric={}",
            event_name,
            group_id,
            edge_node_id,
            device_id.unwrap_or("-"),
            json(update),
        );
    }

    fn log_gap(
        &self,
        scope: &str,
        group_id: &str,
        edge_node_id: &str,
        device_id: Option<&str>,
        gap: Option<&crate::reorder::SequenceGap>,
    ) {
        if let Some(gap) = gap {
            println!(
                "REORDER event=gap_detected scope={} group_id={} edge_node_id={} device_id={} expected={} observed={} timeout_ms={} action=await_missing_message",
                scope,
                group_id,
                edge_node_id,
                device_id.unwrap_or("-"),
                gap.expected,
                gap.observed,
                self.config.reorder_timeout.as_millis(),
            );
        }
    }
}

const RECONNECT_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct SessionRuntime {
    state_topic: String,
    death_payload: Vec<u8>,
    birth_payload: Vec<u8>,
    subscription_filters: Vec<String>,
}

#[derive(Debug, Default)]
struct SessionState {
    connected: bool,
    initial_subscribe_pending: bool,
    birth_published: bool,
    shutdown: ShutdownState,
}

#[derive(Debug, Default)]
enum ShutdownState {
    #[default]
    Running,
    WaitingForDeathSend,
    WaitingForDeathAck {
        pkid: u16,
    },
    DisconnectRequested,
}

impl ShutdownState {
    fn is_active(&self) -> bool {
        !matches!(self, Self::Running)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionOutcome {
    Reconnect,
    Shutdown,
}

fn reorder_key(group_id: &str, edge_node_id: &str) -> ReorderKey {
    ReorderKey::new(group_id, edge_node_id)
}

fn node_rebirth_topic(group_id: &str, edge_node_id: &str) -> String {
    format!("spBv1.0/{group_id}/NCMD/{edge_node_id}")
}

fn build_node_rebirth_payload(timestamp: u64) -> Result<Vec<u8>> {
    let mut payload = Payload::new();
    payload.set_timestamp(timestamp);

    let mut metric = payload::Metric::new();
    metric.set_name("Node Control/Rebirth".to_string());
    metric.set_datatype(DataType::Boolean.value() as u32);
    metric.set_boolean_value(true);
    payload.metrics.push(metric);

    payload
        .write_to_bytes()
        .context("failed to encode NCMD rebirth payload")
}

fn decode_sparkplug_publish_payload(topic: &str, payload: &[u8]) -> Option<DecodedPayload> {
    match decode_payload(payload) {
        Ok(payload) => Some(payload),
        Err(error) => {
            log_payload_decode_error(topic, payload, &error);
            None
        }
    }
}

#[cfg(test)]
fn decode_sparkplug_publish_payload_with_datatypes<F>(
    topic: &str,
    payload: &[u8],
    resolve_datatype: F,
) -> Option<DecodedPayload>
where
    F: Fn(Option<&str>, Option<u64>) -> Option<u32>,
{
    match decode_payload_with_metric_datatypes(payload, resolve_datatype) {
        Ok(payload) => Some(payload),
        Err(error) => {
            log_payload_decode_error(topic, payload, &error);
            None
        }
    }
}

fn log_payload_decode_error(topic: &str, payload: &[u8], error: &anyhow::Error) {
    println!(
        "SPARKPLUG event=payload_decode_error topic={} bytes={} preview={} error={error}",
        topic,
        payload.len(),
        payload_preview(payload),
    );
}

fn json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("serializing a log value should succeed")
}

fn payload_preview(payload: &[u8]) -> String {
    const PREVIEW_LIMIT: usize = 64;

    let preview = &payload[..payload.len().min(PREVIEW_LIMIT)];
    if preview
        .iter()
        .all(|byte| byte.is_ascii_graphic() || byte.is_ascii_whitespace())
    {
        let mut text = String::from_utf8_lossy(preview).into_owned();
        text = text.replace('\n', "\\n").replace('\r', "\\r");
        if preview.len() < payload.len() {
            text.push_str("...");
        }
        return format!("{text:?}");
    }

    let mut hex = preview
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if preview.len() < payload.len() {
        hex.push_str("...");
    }
    format!("0x{hex}")
}

fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after Unix epoch")
        .as_millis()
        .try_into()
        .expect("timestamp must fit into u64")
}

#[cfg(test)]
mod tests {
    use sparkplug_rs::protobuf::{Enum, Message};
    use sparkplug_rs::{DataType, Payload, payload};

    use crate::decode::{DecodedMetric, DecodedMetricValue, DecodedPayload, decode_payload};
    use crate::model::HostRuntimeModel;

    use super::{
        build_node_rebirth_payload, decode_sparkplug_publish_payload,
        decode_sparkplug_publish_payload_with_datatypes, node_rebirth_topic,
    };

    #[test]
    fn ignores_invalid_sparkplug_publish_payloads() {
        let payload = br#"{"online":true,"timestamp":1776226056310}"#;

        assert!(decode_sparkplug_publish_payload("spBv1.0/home/NDATA/sensor", payload).is_none());
    }

    #[test]
    fn decodes_alias_only_node_data_using_birth_datatypes() {
        let mut model = HostRuntimeModel::new(std::time::Duration::from_millis(2000));
        model
            .apply_node_birth(
                "home",
                "sensor",
                &DecodedPayload {
                    timestamp: Some(100),
                    seq: Some(0),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("temperature_c".to_string()),
                        alias: Some(2),
                        timestamp: Some(100),
                        datatype: DataType::Float.value() as u32,
                        datatype_name: "Float".to_string(),
                        quality: None,
                        is_historical: false,
                        is_transient: false,
                        is_null: false,
                        metadata: None,
                        properties: None,
                        value: DecodedMetricValue::Float(25.0),
                    }],
                },
            )
            .unwrap();

        let mut payload = Payload::new();
        payload.set_timestamp(101);
        payload.set_seq(1);
        let mut metric = payload::Metric::new();
        metric.set_alias(2);
        metric.set_timestamp(101);
        metric.set_float_value(26.5);
        payload.metrics.push(metric);

        let bytes = payload.write_to_bytes().unwrap();
        let decoded = decode_sparkplug_publish_payload_with_datatypes(
            "spBv1.0/home/NDATA/sensor",
            &bytes,
            |name, alias| model.node_metric_datatype("home", "sensor", name, alias),
        )
        .unwrap();

        assert_eq!(decoded.metrics[0].datatype_name, "Float");
        assert_eq!(decoded.metrics[0].value, DecodedMetricValue::Float(26.5));
    }

    #[test]
    fn decodes_logged_alias_only_node_data_bytes_using_birth_datatypes() {
        let mut model = HostRuntimeModel::new(std::time::Duration::from_millis(2000));
        model
            .apply_node_birth(
                "home",
                "sensor",
                &DecodedPayload {
                    timestamp: Some(1776226270410),
                    seq: Some(0),
                    uuid: None,
                    body: None,
                    metrics: vec![
                        DecodedMetric {
                            name: Some("temperature_c".to_string()),
                            alias: Some(2),
                            timestamp: Some(1776226270410),
                            datatype: DataType::Float.value() as u32,
                            datatype_name: "Float".to_string(),
                            quality: None,
                            is_historical: false,
                            is_transient: false,
                            is_null: false,
                            metadata: None,
                            properties: None,
                            value: DecodedMetricValue::Float(-35.8),
                        },
                        DecodedMetric {
                            name: Some("synthetic_sinewave".to_string()),
                            alias: Some(3),
                            timestamp: Some(1776226270410),
                            datatype: DataType::Float.value() as u32,
                            datatype_name: "Float".to_string(),
                            quality: None,
                            is_historical: false,
                            is_transient: false,
                            is_null: false,
                            metadata: None,
                            properties: None,
                            value: DecodedMetricValue::Float(43.57752),
                        },
                    ],
                },
            )
            .unwrap();

        let bytes = [
            0x08, 0xb1, 0xf9, 0x9f, 0xfb, 0xd8, 0x33, 0x12, 0x0e, 0x10, 0x02, 0x18, 0xb1, 0xf9,
            0x9f, 0xfb, 0xd8, 0x33, 0x65, 0x33, 0x33, 0x0f, 0xc2, 0x12, 0x0e, 0x10, 0x03, 0x18,
            0xb1, 0xf9, 0x9f, 0xfb, 0xd8, 0x33, 0x65, 0x35, 0xed, 0x45, 0x42, 0x18, 0x12,
        ];

        let decoded = decode_sparkplug_publish_payload_with_datatypes(
            "spBv1.0/home/NDATA/sensor",
            &bytes,
            |name, alias| model.node_metric_datatype("home", "sensor", name, alias),
        )
        .unwrap();

        assert_eq!(decoded.seq, Some(18));
        assert_eq!(decoded.metrics.len(), 2);
        assert_eq!(decoded.metrics[0].alias, Some(2));
        assert_eq!(decoded.metrics[0].datatype_name, "Float");
        assert_eq!(decoded.metrics[1].alias, Some(3));
        assert_eq!(decoded.metrics[1].datatype_name, "Float");
    }

    #[test]
    fn builds_node_rebirth_request_payload() {
        let bytes = build_node_rebirth_payload(123).unwrap();
        let decoded = decode_payload(&bytes).unwrap();

        assert_eq!(
            node_rebirth_topic("home", "sensor"),
            "spBv1.0/home/NCMD/sensor"
        );
        assert_eq!(decoded.timestamp, Some(123));
        assert_eq!(decoded.seq, None);
        assert_eq!(decoded.metrics.len(), 1);
        assert_eq!(
            decoded.metrics[0].name.as_deref(),
            Some("Node Control/Rebirth")
        );
        assert_eq!(decoded.metrics[0].datatype_name, "Boolean");
        assert_eq!(decoded.metrics[0].value, DecodedMetricValue::Boolean(true));
    }
}
