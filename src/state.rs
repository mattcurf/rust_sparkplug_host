// SPDX-FileCopyrightText: 2026 Matt Curfman
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use serde::{Deserialize, Serialize};

pub const SPARKPLUG_NAMESPACE: &str = "spBv1.0";
pub const STATE_SEGMENT: &str = "STATE";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatePayload {
    pub online: bool,
    pub timestamp: u64,
}

pub fn state_topic(host_id: &str) -> String {
    format!("{SPARKPLUG_NAMESPACE}/{STATE_SEGMENT}/{host_id}")
}

pub fn birth_payload(timestamp: u64) -> Result<Vec<u8>> {
    encode(StatePayload {
        online: true,
        timestamp,
    })
}

pub fn death_payload(timestamp: u64) -> Result<Vec<u8>> {
    encode(StatePayload {
        online: false,
        timestamp,
    })
}

pub fn decode_payload(bytes: &[u8]) -> Result<StatePayload> {
    Ok(serde_json::from_slice(bytes)?)
}

fn encode(payload: StatePayload) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&payload)?)
}

#[cfg(test)]
mod tests {
    use super::{birth_payload, death_payload, decode_payload, state_topic};

    #[test]
    fn creates_state_topic() {
        assert_eq!(state_topic("primary-host"), "spBv1.0/STATE/primary-host");
    }

    #[test]
    fn creates_birth_and_death_payloads() {
        let birth = String::from_utf8(birth_payload(42).unwrap()).unwrap();
        let death = String::from_utf8(death_payload(42).unwrap()).unwrap();

        assert_eq!(birth, r#"{"online":true,"timestamp":42}"#);
        assert_eq!(death, r#"{"online":false,"timestamp":42}"#);
    }

    #[test]
    fn decodes_state_payload() {
        let payload = decode_payload(br#"{"online":false,"timestamp":73}"#).unwrap();
        assert_eq!(payload.online, false);
        assert_eq!(payload.timestamp, 73);
    }
}
