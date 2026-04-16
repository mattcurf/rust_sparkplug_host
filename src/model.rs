// SPDX-FileCopyrightText: 2026 Matt Curfman
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::decode::{DecodedMetric, DecodedMetricValue, DecodedPayload};
use crate::reorder::SequenceGap;

#[derive(Debug)]
pub struct HostRuntimeModel {
    pub reorder_timeout: Duration,
    pub edge_nodes: BTreeMap<EdgeNodeKey, EdgeNodeState>,
}

impl HostRuntimeModel {
    pub fn new(reorder_timeout: Duration) -> Self {
        Self {
            reorder_timeout,
            edge_nodes: BTreeMap::new(),
        }
    }

    pub fn edge_node_count(&self) -> usize {
        self.edge_nodes.len()
    }

    pub fn mark_metrics_stale_for_broker_disconnect(&mut self, timestamp: u64) -> usize {
        let mut stale_metric_count = 0;

        for node_state in self.edge_nodes.values_mut() {
            stale_metric_count += mark_stale(&mut node_state.metrics, timestamp);
            for device_state in node_state.devices.values_mut() {
                stale_metric_count += mark_stale(&mut device_state.metrics, timestamp);
            }
        }

        stale_metric_count
    }

    pub fn node_metric_datatype(
        &self,
        group_id: &str,
        edge_node_id: &str,
        name: Option<&str>,
        alias: Option<u64>,
    ) -> Option<u32> {
        self.edge_nodes
            .get(&edge_node_key(group_id, edge_node_id))
            .and_then(|node_state| {
                resolve_metric_datatype(
                    &node_state.alias_map,
                    &node_state.metric_datatypes,
                    name,
                    alias,
                )
            })
    }

    pub fn device_metric_datatype(
        &self,
        group_id: &str,
        edge_node_id: &str,
        device_id: &str,
        name: Option<&str>,
        alias: Option<u64>,
    ) -> Option<u32> {
        self.edge_nodes
            .get(&edge_node_key(group_id, edge_node_id))
            .and_then(|node_state| node_state.devices.get(device_id))
            .and_then(|device_state| {
                resolve_metric_datatype(
                    &device_state.alias_map,
                    &device_state.metric_datatypes,
                    name,
                    alias,
                )
            })
    }

    pub fn apply_node_birth(
        &mut self,
        group_id: &str,
        edge_node_id: &str,
        payload: &DecodedPayload,
    ) -> Result<BirthOutcome> {
        let sequence = require_sequence(payload, "NBIRTH")?;
        let bd_seq = extract_bd_seq(&payload.metrics, None)?;
        let mut node_state = EdgeNodeState {
            online: true,
            last_seq: Some(sequence),
            bd_seq,
            ..Default::default()
        };

        for metric in &payload.metrics {
            let name = metric
                .name
                .clone()
                .context("NBIRTH metrics must include names")?;

            if let Some(alias) = metric.alias {
                node_state.alias_map.insert(alias, name.clone());
            }
            node_state
                .metric_datatypes
                .insert(name.clone(), metric.datatype);

            node_state
                .metrics
                .insert(name, snapshot_from_metric(metric, None, false));
        }

        self.edge_nodes
            .insert(edge_node_key(group_id, edge_node_id), node_state);

        Ok(BirthOutcome {
            sequence,
            gap: None,
            metric_count: payload.metrics.len(),
            bd_seq,
        })
    }

    pub fn apply_device_birth(
        &mut self,
        group_id: &str,
        edge_node_id: &str,
        device_id: &str,
        payload: &DecodedPayload,
    ) -> Result<BirthOutcome> {
        let sequence = require_sequence(payload, "DBIRTH")?;
        let key = edge_node_key(group_id, edge_node_id);
        let node_state = self.edge_nodes.get_mut(&key).with_context(|| {
            format!("received DBIRTH before NBIRTH for edge node {group_id}/{edge_node_id}")
        })?;
        let gap = track_sequence(self.reorder_timeout, &mut node_state.last_seq, sequence);
        node_state.online = true;

        let mut device_state = DeviceState {
            online: true,
            ..Default::default()
        };

        for metric in &payload.metrics {
            let name = metric
                .name
                .clone()
                .context("DBIRTH metrics must include names")?;

            if let Some(alias) = metric.alias {
                device_state.alias_map.insert(alias, name.clone());
            }
            device_state
                .metric_datatypes
                .insert(name.clone(), metric.datatype);

            device_state
                .metrics
                .insert(name, snapshot_from_metric(metric, None, false));
        }

        node_state
            .devices
            .insert(device_id.to_string(), device_state);

        Ok(BirthOutcome {
            sequence,
            gap,
            metric_count: payload.metrics.len(),
            bd_seq: None,
        })
    }

    pub fn apply_node_data(
        &mut self,
        group_id: &str,
        edge_node_id: &str,
        payload: &DecodedPayload,
    ) -> Result<DataOutcome> {
        let sequence = require_sequence(payload, "NDATA")?;
        let key = edge_node_key(group_id, edge_node_id);
        let node_state = self.edge_nodes.get_mut(&key).with_context(|| {
            format!("received NDATA before NBIRTH for edge node {group_id}/{edge_node_id}")
        })?;
        let gap = track_sequence(self.reorder_timeout, &mut node_state.last_seq, sequence);
        node_state.online = true;

        let mut changes = Vec::new();
        for metric in &payload.metrics {
            let name = resolve_metric_name(&node_state.alias_map, metric, "NDATA")?;
            ensure_declared_metric(&node_state.metric_datatypes, &name, "NDATA")?;
            if let Some(alias) = metric.alias {
                node_state.alias_map.insert(alias, name.clone());
            }
            node_state
                .metric_datatypes
                .insert(name.clone(), metric.datatype);

            let snapshot = snapshot_from_metric(metric, None, false);
            if is_effective_change(node_state.metrics.get(&name), &snapshot) {
                changes.push(MetricUpdate {
                    name: name.clone(),
                    snapshot: snapshot.clone(),
                });
            }

            node_state.metrics.insert(name, snapshot);
        }

        Ok(DataOutcome {
            sequence,
            gap,
            changes,
        })
    }

    pub fn apply_device_data(
        &mut self,
        group_id: &str,
        edge_node_id: &str,
        device_id: &str,
        payload: &DecodedPayload,
    ) -> Result<DataOutcome> {
        let sequence = require_sequence(payload, "DDATA")?;
        let key = edge_node_key(group_id, edge_node_id);
        let node_state = self.edge_nodes.get_mut(&key).with_context(|| {
            format!("received DDATA before NBIRTH for edge node {group_id}/{edge_node_id}")
        })?;
        let gap = track_sequence(self.reorder_timeout, &mut node_state.last_seq, sequence);
        node_state.online = true;

        let device_state = node_state.devices.get_mut(device_id).with_context(|| {
            format!("received DDATA before DBIRTH for device {group_id}/{edge_node_id}/{device_id}")
        })?;
        device_state.online = true;

        let mut changes = Vec::new();
        for metric in &payload.metrics {
            let name = resolve_metric_name(&device_state.alias_map, metric, "DDATA")?;
            ensure_declared_metric(&device_state.metric_datatypes, &name, "DDATA")?;
            if let Some(alias) = metric.alias {
                device_state.alias_map.insert(alias, name.clone());
            }
            device_state
                .metric_datatypes
                .insert(name.clone(), metric.datatype);

            let snapshot = snapshot_from_metric(metric, None, false);
            if is_effective_change(device_state.metrics.get(&name), &snapshot) {
                changes.push(MetricUpdate {
                    name: name.clone(),
                    snapshot: snapshot.clone(),
                });
            }

            device_state.metrics.insert(name, snapshot);
        }

        Ok(DataOutcome {
            sequence,
            gap,
            changes,
        })
    }

    pub fn apply_node_death(
        &mut self,
        group_id: &str,
        edge_node_id: &str,
        payload: &DecodedPayload,
        receipt_timestamp: u64,
    ) -> Result<DeathOutcome> {
        let key = edge_node_key(group_id, edge_node_id);
        let Some(node_state) = self.edge_nodes.get_mut(&key) else {
            return Ok(DeathOutcome {
                sequence: None,
                gap: None,
                stale_metric_count: 0,
                bd_seq: None,
                ignored: true,
                reason: Some("received NDEATH for an unknown edge node".to_string()),
            });
        };

        let bd_seq = extract_bd_seq(&payload.metrics, Some(&node_state.alias_map))?;
        if let (Some(expected), Some(observed)) = (node_state.bd_seq, bd_seq) {
            if expected != observed {
                return Ok(DeathOutcome {
                    sequence: None,
                    gap: None,
                    stale_metric_count: 0,
                    bd_seq,
                    ignored: true,
                    reason: Some(format!(
                        "ignored NDEATH because bdSeq {observed} did not match current session bdSeq {expected}"
                    )),
                });
            }
        }

        node_state.online = false;
        node_state.last_seq = None;
        let mut stale_metric_count = mark_stale(&mut node_state.metrics, receipt_timestamp);
        for device_state in node_state.devices.values_mut() {
            device_state.online = false;
            stale_metric_count += mark_stale(&mut device_state.metrics, receipt_timestamp);
        }

        Ok(DeathOutcome {
            sequence: None,
            gap: None,
            stale_metric_count,
            bd_seq,
            ignored: false,
            reason: None,
        })
    }

    pub fn apply_device_death(
        &mut self,
        group_id: &str,
        edge_node_id: &str,
        device_id: &str,
        payload: &DecodedPayload,
    ) -> Result<DeathOutcome> {
        let sequence = require_sequence(payload, "DDEATH")?;
        let timestamp = payload
            .timestamp
            .context("DDEATH missing payload timestamp")?;
        let key = edge_node_key(group_id, edge_node_id);
        let node_state = self.edge_nodes.get_mut(&key).with_context(|| {
            format!("received DDEATH before NBIRTH for edge node {group_id}/{edge_node_id}")
        })?;

        let gap = track_sequence(self.reorder_timeout, &mut node_state.last_seq, sequence);
        let device_state = node_state.devices.get_mut(device_id).with_context(|| {
            format!(
                "received DDEATH before DBIRTH for device {group_id}/{edge_node_id}/{device_id}"
            )
        })?;

        device_state.online = false;
        let stale_metric_count = mark_stale(&mut device_state.metrics, timestamp);

        Ok(DeathOutcome {
            sequence: Some(sequence),
            gap,
            stale_metric_count,
            bd_seq: None,
            ignored: false,
            reason: None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EdgeNodeKey {
    pub group_id: String,
    pub edge_node_id: String,
}

#[derive(Debug, Default)]
pub struct EdgeNodeState {
    pub online: bool,
    pub last_seq: Option<u8>,
    pub bd_seq: Option<u64>,
    pub alias_map: BTreeMap<u64, String>,
    pub metric_datatypes: BTreeMap<String, u32>,
    pub metrics: BTreeMap<String, MetricSnapshot>,
    pub devices: BTreeMap<String, DeviceState>,
}

#[derive(Debug, Default)]
pub struct DeviceState {
    pub online: bool,
    pub alias_map: BTreeMap<u64, String>,
    pub metric_datatypes: BTreeMap<String, u32>,
    pub metrics: BTreeMap<String, MetricSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct MetricSnapshot {
    pub timestamp: Option<u64>,
    pub quality: Option<i32>,
    pub value: Option<DecodedMetricValue>,
    pub stale: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MetricUpdate {
    pub name: String,
    pub snapshot: MetricSnapshot,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BirthOutcome {
    pub sequence: u8,
    pub gap: Option<SequenceGap>,
    pub metric_count: usize,
    pub bd_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataOutcome {
    pub sequence: u8,
    pub gap: Option<SequenceGap>,
    pub changes: Vec<MetricUpdate>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeathOutcome {
    pub sequence: Option<u8>,
    pub gap: Option<SequenceGap>,
    pub stale_metric_count: usize,
    pub bd_seq: Option<u64>,
    pub ignored: bool,
    pub reason: Option<String>,
}

fn edge_node_key(group_id: &str, edge_node_id: &str) -> EdgeNodeKey {
    EdgeNodeKey {
        group_id: group_id.to_string(),
        edge_node_id: edge_node_id.to_string(),
    }
}

fn require_sequence(payload: &DecodedPayload, label: &str) -> Result<u8> {
    let sequence = payload
        .seq
        .with_context(|| format!("{label} missing required sequence number"))?;

    u8::try_from(sequence)
        .with_context(|| format!("{label} sequence {sequence} exceeded Sparkplug's 0-255 range"))
}

fn track_sequence(
    _reorder_timeout: Duration,
    last_sequence: &mut Option<u8>,
    observed: u8,
) -> Option<SequenceGap> {
    let gap = last_sequence.and_then(|last| {
        let expected = next_sequence(last);
        (expected != observed).then_some(SequenceGap { expected, observed })
    });

    *last_sequence = Some(observed);
    gap
}

fn next_sequence(previous: u8) -> u8 {
    previous.wrapping_add(1)
}

fn resolve_metric_name(
    alias_map: &BTreeMap<u64, String>,
    metric: &DecodedMetric,
    label: &str,
) -> Result<String> {
    if let Some(name) = &metric.name {
        return Ok(name.clone());
    }

    let alias = metric
        .alias
        .with_context(|| format!("{label} metric omitted both name and alias"))?;

    alias_map
        .get(&alias)
        .cloned()
        .with_context(|| format!("{label} metric referenced unknown alias {alias}"))
}

fn extract_bd_seq(
    metrics: &[DecodedMetric],
    alias_map: Option<&BTreeMap<u64, String>>,
) -> Result<Option<u64>> {
    for metric in metrics {
        let name = match (&metric.name, metric.alias, alias_map) {
            (Some(name), _, _) => name.clone(),
            (None, Some(alias), Some(alias_map)) => match alias_map.get(&alias) {
                Some(name) => name.clone(),
                None => continue,
            },
            _ => continue,
        };

        if name != "bdSeq" {
            continue;
        }

        return match &metric.value {
            DecodedMetricValue::Signed(value) => {
                if *value < 0 {
                    bail!("bdSeq must not be negative")
                }
                Ok(Some(*value as u64))
            }
            DecodedMetricValue::Unsigned(value) => Ok(Some(*value)),
            _ => bail!("bdSeq metric must decode to an integer"),
        };
    }

    Ok(None)
}

fn resolve_metric_datatype(
    alias_map: &BTreeMap<u64, String>,
    metric_datatypes: &BTreeMap<String, u32>,
    name: Option<&str>,
    alias: Option<u64>,
) -> Option<u32> {
    if let Some(name) = name {
        return metric_datatypes.get(name).copied();
    }

    alias
        .and_then(|alias| alias_map.get(&alias))
        .and_then(|name| metric_datatypes.get(name))
        .copied()
}

fn ensure_declared_metric(
    metric_datatypes: &BTreeMap<String, u32>,
    name: &str,
    label: &str,
) -> Result<()> {
    if metric_datatypes.contains_key(name) {
        return Ok(());
    }

    bail!("{label} metric '{name}' was not declared in the prior birth message")
}

fn snapshot_from_metric(
    metric: &DecodedMetric,
    timestamp_override: Option<u64>,
    stale: bool,
) -> MetricSnapshot {
    MetricSnapshot {
        timestamp: timestamp_override.or(metric.timestamp),
        quality: Some(if stale {
            500
        } else {
            metric.quality.unwrap_or(192)
        }),
        value: Some(metric.value.clone()),
        stale,
    }
}

fn mark_stale(metrics: &mut BTreeMap<String, MetricSnapshot>, timestamp: u64) -> usize {
    for snapshot in metrics.values_mut() {
        snapshot.timestamp = Some(timestamp);
        snapshot.quality = Some(500);
        snapshot.stale = true;
    }

    metrics.len()
}

fn is_effective_change(previous: Option<&MetricSnapshot>, next: &MetricSnapshot) -> bool {
    match previous {
        None => true,
        Some(previous) => {
            previous.quality != next.quality
                || previous.value != next.value
                || previous.stale != next.stale
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::decode::{DecodedMetric, DecodedMetricValue, DecodedPayload};

    use super::HostRuntimeModel;

    #[test]
    fn resolves_aliases_and_only_reports_actual_changes() {
        let mut model = HostRuntimeModel::new(Duration::from_millis(2000));
        model
            .apply_node_birth(
                "G1",
                "E1",
                &DecodedPayload {
                    timestamp: Some(100),
                    seq: Some(0),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("temperature".to_string()),
                        alias: Some(7),
                        timestamp: Some(100),
                        datatype: 9,
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

        let unchanged = model
            .apply_node_data(
                "G1",
                "E1",
                &DecodedPayload {
                    timestamp: Some(101),
                    seq: Some(1),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: None,
                        alias: Some(7),
                        timestamp: Some(101),
                        datatype: 9,
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
        assert!(unchanged.changes.is_empty());

        let changed = model
            .apply_node_data(
                "G1",
                "E1",
                &DecodedPayload {
                    timestamp: Some(102),
                    seq: Some(2),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: None,
                        alias: Some(7),
                        timestamp: Some(102),
                        datatype: 9,
                        datatype_name: "Float".to_string(),
                        quality: None,
                        is_historical: false,
                        is_transient: false,
                        is_null: false,
                        metadata: None,
                        properties: None,
                        value: DecodedMetricValue::Float(26.5),
                    }],
                },
            )
            .unwrap();

        assert_eq!(changed.changes.len(), 1);
        assert_eq!(changed.changes[0].name, "temperature");
        assert_eq!(changed.changes[0].snapshot.quality, Some(192));
        assert_eq!(
            model.node_metric_datatype("G1", "E1", None, Some(7)),
            Some(9)
        );
    }

    #[test]
    fn marks_node_and_device_metrics_stale_on_matching_ndeath() {
        let mut model = HostRuntimeModel::new(Duration::from_millis(2000));
        model
            .apply_node_birth(
                "G1",
                "E1",
                &DecodedPayload {
                    timestamp: Some(100),
                    seq: Some(0),
                    uuid: None,
                    body: None,
                    metrics: vec![
                        DecodedMetric {
                            name: Some("bdSeq".to_string()),
                            alias: None,
                            timestamp: Some(100),
                            datatype: 4,
                            datatype_name: "Int64".to_string(),
                            quality: None,
                            is_historical: false,
                            is_transient: false,
                            is_null: false,
                            metadata: None,
                            properties: None,
                            value: DecodedMetricValue::Signed(4),
                        },
                        DecodedMetric {
                            name: Some("temperature".to_string()),
                            alias: Some(2),
                            timestamp: Some(100),
                            datatype: 9,
                            datatype_name: "Float".to_string(),
                            quality: None,
                            is_historical: false,
                            is_transient: false,
                            is_null: false,
                            metadata: None,
                            properties: None,
                            value: DecodedMetricValue::Float(25.0),
                        },
                    ],
                },
            )
            .unwrap();
        model
            .apply_device_birth(
                "G1",
                "E1",
                "D1",
                &DecodedPayload {
                    timestamp: Some(101),
                    seq: Some(1),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("pressure".to_string()),
                        alias: Some(8),
                        timestamp: Some(101),
                        datatype: 9,
                        datatype_name: "Float".to_string(),
                        quality: None,
                        is_historical: false,
                        is_transient: false,
                        is_null: false,
                        metadata: None,
                        properties: None,
                        value: DecodedMetricValue::Float(4.5),
                    }],
                },
            )
            .unwrap();

        let death = model
            .apply_node_death(
                "G1",
                "E1",
                &DecodedPayload {
                    timestamp: Some(200),
                    seq: None,
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("bdSeq".to_string()),
                        alias: None,
                        timestamp: Some(200),
                        datatype: 4,
                        datatype_name: "Int64".to_string(),
                        quality: None,
                        is_historical: false,
                        is_transient: false,
                        is_null: false,
                        metadata: None,
                        properties: None,
                        value: DecodedMetricValue::Signed(4),
                    }],
                },
                999,
            )
            .unwrap();

        assert!(!death.ignored);
        assert_eq!(death.stale_metric_count, 3);

        let node = model.edge_nodes.values().next().unwrap();
        assert!(!node.online);
        assert_eq!(node.metrics["temperature"].quality, Some(500));
        assert_eq!(node.metrics["temperature"].timestamp, Some(999));
        let device = node.devices.get("D1").unwrap();
        assert!(!device.online);
        assert_eq!(device.metrics["pressure"].quality, Some(500));
        assert_eq!(device.metrics["pressure"].timestamp, Some(999));
    }

    #[test]
    fn marks_tracked_metrics_stale_when_host_loses_broker_connection() {
        let mut model = HostRuntimeModel::new(Duration::from_millis(2000));
        model
            .apply_node_birth(
                "G1",
                "E1",
                &DecodedPayload {
                    timestamp: Some(100),
                    seq: Some(0),
                    uuid: None,
                    body: None,
                    metrics: vec![
                        DecodedMetric {
                            name: Some("bdSeq".to_string()),
                            alias: None,
                            timestamp: Some(100),
                            datatype: 4,
                            datatype_name: "Int64".to_string(),
                            quality: None,
                            is_historical: false,
                            is_transient: false,
                            is_null: false,
                            metadata: None,
                            properties: None,
                            value: DecodedMetricValue::Signed(4),
                        },
                        DecodedMetric {
                            name: Some("temperature".to_string()),
                            alias: Some(2),
                            timestamp: Some(100),
                            datatype: 9,
                            datatype_name: "Float".to_string(),
                            quality: None,
                            is_historical: false,
                            is_transient: false,
                            is_null: false,
                            metadata: None,
                            properties: None,
                            value: DecodedMetricValue::Float(25.0),
                        },
                    ],
                },
            )
            .unwrap();
        model
            .apply_device_birth(
                "G1",
                "E1",
                "D1",
                &DecodedPayload {
                    timestamp: Some(101),
                    seq: Some(1),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("pressure".to_string()),
                        alias: Some(8),
                        timestamp: Some(101),
                        datatype: 9,
                        datatype_name: "Float".to_string(),
                        quality: None,
                        is_historical: false,
                        is_transient: false,
                        is_null: false,
                        metadata: None,
                        properties: None,
                        value: DecodedMetricValue::Float(4.5),
                    }],
                },
            )
            .unwrap();

        let stale_metric_count = model.mark_metrics_stale_for_broker_disconnect(777);
        assert_eq!(stale_metric_count, 3);

        let node = model.edge_nodes.values().next().unwrap();
        assert!(node.online);
        assert_eq!(node.metrics["bdSeq"].quality, Some(500));
        assert_eq!(node.metrics["bdSeq"].timestamp, Some(777));
        assert_eq!(node.metrics["temperature"].quality, Some(500));
        assert_eq!(node.metrics["temperature"].timestamp, Some(777));

        let device = node.devices.get("D1").unwrap();
        assert!(device.online);
        assert_eq!(device.metrics["pressure"].quality, Some(500));
        assert_eq!(device.metrics["pressure"].timestamp, Some(777));
    }

    #[test]
    fn rejects_node_data_before_birth() {
        let mut model = HostRuntimeModel::new(Duration::from_millis(2000));

        let error = model
            .apply_node_data(
                "G1",
                "E1",
                &DecodedPayload {
                    timestamp: Some(101),
                    seq: Some(1),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("temperature".to_string()),
                        alias: None,
                        timestamp: Some(101),
                        datatype: 9,
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
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "received NDATA before NBIRTH for edge node G1/E1"
        );
    }

    #[test]
    fn rejects_device_data_before_device_birth() {
        let mut model = HostRuntimeModel::new(Duration::from_millis(2000));
        model
            .apply_node_birth(
                "G1",
                "E1",
                &DecodedPayload {
                    timestamp: Some(100),
                    seq: Some(0),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("temperature".to_string()),
                        alias: Some(7),
                        timestamp: Some(100),
                        datatype: 9,
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

        let error = model
            .apply_device_data(
                "G1",
                "E1",
                "D1",
                &DecodedPayload {
                    timestamp: Some(101),
                    seq: Some(1),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("pressure".to_string()),
                        alias: None,
                        timestamp: Some(101),
                        datatype: 9,
                        datatype_name: "Float".to_string(),
                        quality: None,
                        is_historical: false,
                        is_transient: false,
                        is_null: false,
                        metadata: None,
                        properties: None,
                        value: DecodedMetricValue::Float(4.5),
                    }],
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "received DDATA before DBIRTH for device G1/E1/D1"
        );
    }

    #[test]
    fn rejects_device_birth_before_node_birth() {
        let mut model = HostRuntimeModel::new(Duration::from_millis(2000));

        let error = model
            .apply_device_birth(
                "G1",
                "E1",
                "D1",
                &DecodedPayload {
                    timestamp: Some(101),
                    seq: Some(1),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("pressure".to_string()),
                        alias: Some(8),
                        timestamp: Some(101),
                        datatype: 9,
                        datatype_name: "Float".to_string(),
                        quality: None,
                        is_historical: false,
                        is_transient: false,
                        is_null: false,
                        metadata: None,
                        properties: None,
                        value: DecodedMetricValue::Float(4.5),
                    }],
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "received DBIRTH before NBIRTH for edge node G1/E1"
        );
    }

    #[test]
    fn rejects_device_death_before_node_birth() {
        let mut model = HostRuntimeModel::new(Duration::from_millis(2000));

        let error = model
            .apply_device_death(
                "G1",
                "E1",
                "D1",
                &DecodedPayload {
                    timestamp: Some(101),
                    seq: Some(1),
                    uuid: None,
                    body: None,
                    metrics: vec![],
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "received DDEATH before NBIRTH for edge node G1/E1"
        );
    }

    #[test]
    fn rejects_device_death_before_device_birth() {
        let mut model = HostRuntimeModel::new(Duration::from_millis(2000));
        model
            .apply_node_birth(
                "G1",
                "E1",
                &DecodedPayload {
                    timestamp: Some(100),
                    seq: Some(0),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("temperature".to_string()),
                        alias: Some(7),
                        timestamp: Some(100),
                        datatype: 9,
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

        let error = model
            .apply_device_death(
                "G1",
                "E1",
                "D1",
                &DecodedPayload {
                    timestamp: Some(101),
                    seq: Some(1),
                    uuid: None,
                    body: None,
                    metrics: vec![],
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "received DDEATH before DBIRTH for device G1/E1/D1"
        );
    }

    #[test]
    fn rejects_node_data_metric_not_declared_in_birth() {
        let mut model = HostRuntimeModel::new(Duration::from_millis(2000));
        model
            .apply_node_birth(
                "G1",
                "E1",
                &DecodedPayload {
                    timestamp: Some(100),
                    seq: Some(0),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("temperature".to_string()),
                        alias: Some(7),
                        timestamp: Some(100),
                        datatype: 9,
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

        let error = model
            .apply_node_data(
                "G1",
                "E1",
                &DecodedPayload {
                    timestamp: Some(101),
                    seq: Some(1),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("humidity".to_string()),
                        alias: None,
                        timestamp: Some(101),
                        datatype: 9,
                        datatype_name: "Float".to_string(),
                        quality: None,
                        is_historical: false,
                        is_transient: false,
                        is_null: false,
                        metadata: None,
                        properties: None,
                        value: DecodedMetricValue::Float(55.0),
                    }],
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "NDATA metric 'humidity' was not declared in the prior birth message"
        );
    }

    #[test]
    fn rejects_device_data_metric_not_declared_in_birth() {
        let mut model = HostRuntimeModel::new(Duration::from_millis(2000));
        model
            .apply_node_birth(
                "G1",
                "E1",
                &DecodedPayload {
                    timestamp: Some(100),
                    seq: Some(0),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("temperature".to_string()),
                        alias: Some(7),
                        timestamp: Some(100),
                        datatype: 9,
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
        model
            .apply_device_birth(
                "G1",
                "E1",
                "D1",
                &DecodedPayload {
                    timestamp: Some(101),
                    seq: Some(1),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("pressure".to_string()),
                        alias: Some(8),
                        timestamp: Some(101),
                        datatype: 9,
                        datatype_name: "Float".to_string(),
                        quality: None,
                        is_historical: false,
                        is_transient: false,
                        is_null: false,
                        metadata: None,
                        properties: None,
                        value: DecodedMetricValue::Float(4.5),
                    }],
                },
            )
            .unwrap();

        let error = model
            .apply_device_data(
                "G1",
                "E1",
                "D1",
                &DecodedPayload {
                    timestamp: Some(102),
                    seq: Some(2),
                    uuid: None,
                    body: None,
                    metrics: vec![DecodedMetric {
                        name: Some("vibration".to_string()),
                        alias: None,
                        timestamp: Some(102),
                        datatype: 9,
                        datatype_name: "Float".to_string(),
                        quality: None,
                        is_historical: false,
                        is_transient: false,
                        is_null: false,
                        metadata: None,
                        properties: None,
                        value: DecodedMetricValue::Float(1.2),
                    }],
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "DDATA metric 'vibration' was not declared in the prior birth message"
        );
    }
}
