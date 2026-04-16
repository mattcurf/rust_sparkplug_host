// SPDX-FileCopyrightText: 2026 Matt Curfman
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use std::str::FromStr;

use anyhow::{Result, bail};
use sparkplug_rs::{DeviceMessageType, NodeMessageType, TopicName, TopicNamespace};

use crate::state::{SPARKPLUG_NAMESPACE, STATE_SEGMENT};

#[derive(Debug, Clone, PartialEq)]
pub enum SparkplugTopic {
    State {
        host_id: String,
    },
    Node {
        group_id: String,
        message_type: NodeMessageType,
        edge_node_id: String,
    },
    Device {
        group_id: String,
        message_type: DeviceMessageType,
        edge_node_id: String,
        device_id: String,
    },
}

pub fn parse_topic(topic: &str) -> Result<SparkplugTopic> {
    let state_prefix = format!("{SPARKPLUG_NAMESPACE}/{STATE_SEGMENT}/");
    if let Some(host_id) = topic.strip_prefix(&state_prefix) {
        return Ok(SparkplugTopic::State {
            host_id: host_id.to_string(),
        });
    }

    match TopicName::from_str(topic).map_err(|err| anyhow::anyhow!(err.to_string()))? {
        TopicName::NodeMessage {
            namespace,
            group_id,
            node_message_type,
            edge_node_id,
        } => {
            if namespace != TopicNamespace::SPBV1_0 {
                bail!("unsupported Sparkplug namespace in topic '{topic}'");
            }

            Ok(SparkplugTopic::Node {
                group_id,
                message_type: node_message_type,
                edge_node_id,
            })
        }
        TopicName::DeviceMessage {
            namespace,
            group_id,
            device_message_type,
            edge_node_id,
            device_id,
        } => {
            if namespace != TopicNamespace::SPBV1_0 {
                bail!("unsupported Sparkplug namespace in topic '{topic}'");
            }

            Ok(SparkplugTopic::Device {
                group_id,
                message_type: device_message_type,
                edge_node_id,
                device_id,
            })
        }
        TopicName::StateMessage { .. } => bail!(
            "the sparkplug-rs STATE representation omits the spec namespace; use spBv1.0/STATE/<host_id>"
        ),
    }
}

#[cfg(test)]
mod tests {
    use sparkplug_rs::{DeviceMessageType, NodeMessageType};

    use super::{SparkplugTopic, parse_topic};

    #[test]
    fn parses_state_topic() {
        let topic = parse_topic("spBv1.0/STATE/primary-host").unwrap();
        assert_eq!(
            topic,
            SparkplugTopic::State {
                host_id: "primary-host".to_string(),
            }
        );
    }

    #[test]
    fn parses_node_topic() {
        let topic = parse_topic("spBv1.0/G1/NDATA/E1").unwrap();
        assert_eq!(
            topic,
            SparkplugTopic::Node {
                group_id: "G1".to_string(),
                message_type: NodeMessageType::NDATA,
                edge_node_id: "E1".to_string(),
            }
        );
    }

    #[test]
    fn parses_device_topic() {
        let topic = parse_topic("spBv1.0/G1/DDATA/E1/D1").unwrap();
        assert_eq!(
            topic,
            SparkplugTopic::Device {
                group_id: "G1".to_string(),
                message_type: DeviceMessageType::DDATA,
                edge_node_id: "E1".to_string(),
                device_id: "D1".to_string(),
            }
        );
    }
}
