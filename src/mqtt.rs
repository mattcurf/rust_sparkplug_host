// SPDX-FileCopyrightText: 2026 Matt Curfman
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use std::fs;
use std::sync::Arc;

use anyhow::{Context, Result};
use rumqttc::{
    LastWill, MqttOptions, QoS, Transport,
    tokio_rustls::rustls::{
        ClientConfig, DigitallySignedStruct, Error as TlsError, RootCertStore, SignatureScheme,
        client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        pki_types::{CertificateDer, ServerName, UnixTime},
    },
};

use crate::config::{AppConfig, BrokerScheme};
use crate::state;

pub struct MqttRuntimePlan {
    pub mqtt_options: MqttOptions,
    pub connect_timestamp: u64,
    pub state_topic: String,
    pub will_payload: Vec<u8>,
    pub birth_payload: Vec<u8>,
    pub subscription_filters: Vec<String>,
}

pub fn build_runtime_plan(config: &AppConfig, connect_timestamp: u64) -> Result<MqttRuntimePlan> {
    let state_topic = state::state_topic(&config.host_id);
    let will_payload = state::death_payload(connect_timestamp)?;
    let birth_payload = state::birth_payload(connect_timestamp)?;

    let mut mqtt_options = MqttOptions::new(
        config.client_id.clone(),
        config.broker.host.clone(),
        config.broker.port,
    );
    mqtt_options.set_clean_session(true);
    mqtt_options.set_keep_alive(config.keep_alive);
    mqtt_options.set_request_channel_capacity(100);

    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        mqtt_options.set_credentials(username.clone(), password.clone());
    }

    match config.broker.scheme {
        BrokerScheme::Mqtt => {
            mqtt_options.set_transport(Transport::tcp());
        }
        BrokerScheme::Mqtts => {
            let transport = if config.insecure_skip_tls_verify {
                Transport::tls_with_config(build_insecure_tls_config().into())
            } else if let Some(ca_file) = &config.ca_file {
                let ca = fs::read(ca_file).with_context(|| {
                    format!("failed to read CA certificate from {}", ca_file.display())
                })?;
                Transport::tls(ca, None, None)
            } else {
                Transport::tls_with_default_config()
            };

            mqtt_options.set_transport(transport);
        }
    }

    mqtt_options.set_last_will(LastWill::new(
        state_topic.clone(),
        will_payload.clone(),
        QoS::AtLeastOnce,
        true,
    ));

    Ok(MqttRuntimePlan {
        mqtt_options,
        connect_timestamp,
        state_topic,
        will_payload,
        birth_payload,
        subscription_filters: subscription_filters(&config.host_id),
    })
}

pub fn subscription_filters(host_id: &str) -> Vec<String> {
    vec![
        "spBv1.0/+/NBIRTH/+".to_string(),
        "spBv1.0/+/NDEATH/+".to_string(),
        "spBv1.0/+/NDATA/+".to_string(),
        "spBv1.0/+/DBIRTH/+/+".to_string(),
        "spBv1.0/+/DDEATH/+/+".to_string(),
        "spBv1.0/+/DDATA/+/+".to_string(),
        state::state_topic(host_id),
    ]
}

fn build_insecure_tls_config() -> ClientConfig {
    let mut tls_config = ClientConfig::builder()
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    let supported_verify_schemes = tls_config
        .crypto_provider()
        .signature_verification_algorithms
        .supported_schemes();

    tls_config
        .dangerous()
        .set_certificate_verifier(Arc::new(InsecureServerCertVerifier {
            supported_verify_schemes,
        }));

    tls_config
}

#[derive(Debug)]
struct InsecureServerCertVerifier {
    supported_verify_schemes: Vec<SignatureScheme>,
}

impl ServerCertVerifier for InsecureServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported_verify_schemes.clone()
    }
}
