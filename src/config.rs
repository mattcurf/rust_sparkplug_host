// SPDX-FileCopyrightText: 2026 Matt Curfman
// SPDX-License-Identifier: Apache-2.0

use std::env;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "sparkplug_primary_host",
    about = "Scaffold for a Rust Sparkplug B Primary Host"
)]
pub struct Cli {
    #[arg(
        long,
        help = "MQTT broker URL in the form mqtt://host[:port] or mqtts://host[:port]"
    )]
    pub broker: String,

    #[arg(long, help = "Sparkplug host ID used in spBv1.0/STATE/<host_id>")]
    pub host_id: String,

    #[arg(long, help = "MQTT client ID; defaults to <host_id>-mqtt")]
    pub client_id: Option<String>,

    #[arg(long, help = "Optional MQTT username")]
    pub username: Option<String>,

    #[arg(long, help = "Optional MQTT password")]
    pub password: Option<String>,

    #[arg(long, help = "Environment variable containing the MQTT password")]
    pub password_env: Option<String>,

    #[arg(long, help = "Optional CA certificate for mqtts:// connections")]
    pub ca_file: Option<PathBuf>,

    #[arg(
        long,
        help = "Disable TLS certificate and hostname verification for mqtts:// connections"
    )]
    pub insecure_skip_tls_verify: bool,

    #[arg(long, default_value_t = 30, help = "MQTT keep alive in seconds")]
    pub keep_alive_secs: u64,

    #[arg(
        long,
        default_value_t = 2000,
        help = "Reorder timeout in milliseconds for seq gap detection"
    )]
    pub reorder_timeout_ms: u64,
}

impl Cli {
    pub fn into_config(self) -> Result<AppConfig> {
        validate_topic_token("host_id", &self.host_id)?;

        let broker = BrokerEndpoint::parse(&self.broker)?;
        if self.insecure_skip_tls_verify && !broker.scheme.is_tls() {
            bail!("--insecure-skip-tls-verify requires an mqtts:// broker URL")
        }

        let client_id = self
            .client_id
            .unwrap_or_else(|| format!("{}-mqtt", self.host_id));
        validate_topic_token("client_id", &client_id)?;

        let password = resolve_password(self.password, self.password_env)?;

        if broker.scheme.is_tls() && self.ca_file.is_none() {
            // System root certificates are acceptable; no extra validation required.
        }

        Ok(AppConfig {
            broker,
            host_id: self.host_id,
            client_id,
            username: self.username,
            password,
            ca_file: self.ca_file,
            insecure_skip_tls_verify: self.insecure_skip_tls_verify,
            keep_alive: Duration::from_secs(self.keep_alive_secs),
            reorder_timeout: Duration::from_millis(self.reorder_timeout_ms),
        })
    }
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub broker: BrokerEndpoint,
    pub host_id: String,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub ca_file: Option<PathBuf>,
    pub insecure_skip_tls_verify: bool,
    pub keep_alive: Duration,
    pub reorder_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerScheme {
    Mqtt,
    Mqtts,
}

impl BrokerScheme {
    pub const fn is_tls(self) -> bool {
        matches!(self, Self::Mqtts)
    }

    pub const fn default_port(self) -> u16 {
        match self {
            Self::Mqtt => 1883,
            Self::Mqtts => 8883,
        }
    }
}

impl Display for BrokerScheme {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mqtt => f.write_str("mqtt"),
            Self::Mqtts => f.write_str("mqtts"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BrokerEndpoint {
    pub scheme: BrokerScheme,
    pub host: String,
    pub port: u16,
}

impl BrokerEndpoint {
    pub fn parse(input: &str) -> Result<Self> {
        let (scheme, remainder) = input
            .split_once("://")
            .context("broker must include a scheme, e.g. mqtt://localhost:1883")?;

        let scheme = match scheme {
            "mqtt" => BrokerScheme::Mqtt,
            "mqtts" => BrokerScheme::Mqtts,
            other => bail!("unsupported broker scheme '{other}', expected mqtt or mqtts"),
        };

        if remainder.is_empty() {
            bail!("broker host must not be empty");
        }

        if remainder.contains('/') {
            bail!("broker URL must not contain a path; use mqtt://host[:port]");
        }

        let (host, port) = parse_host_port(remainder, scheme.default_port())?;
        if host.is_empty() {
            bail!("broker host must not be empty");
        }

        Ok(Self { scheme, host, port })
    }
}

impl Display for BrokerEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}:{}", self.scheme, self.host, self.port)
    }
}

fn resolve_password(
    password: Option<String>,
    password_env: Option<String>,
) -> Result<Option<String>> {
    match (password, password_env) {
        (Some(_), Some(_)) => bail!("use either --password or --password-env, not both"),
        (Some(password), None) => Ok(Some(password)),
        (None, Some(name)) => {
            let value = env::var(&name)
                .with_context(|| format!("environment variable {name} was not set"))?;
            Ok(Some(value))
        }
        (None, None) => Ok(None),
    }
}

fn validate_topic_token(label: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{label} must not be empty");
    }

    if value.contains('/') || value.contains('+') || value.contains('#') {
        bail!("{label} must not contain '/', '+', or '#' because it is used in MQTT topics");
    }

    Ok(())
}

fn parse_host_port(input: &str, default_port: u16) -> Result<(String, u16)> {
    if let Some(rest) = input.strip_prefix('[') {
        let bracket_index = rest
            .find(']')
            .context("bracketed IPv6 broker host is missing a closing ']' character")?;
        let host = rest[..bracket_index].to_string();
        let suffix = &rest[bracket_index + 1..];

        if suffix.is_empty() {
            return Ok((host, default_port));
        }

        let port = suffix
            .strip_prefix(':')
            .context("bracketed IPv6 broker host must be followed by :<port>")?;

        return Ok((host, parse_port(port)?));
    }

    if input.matches(':').count() > 1 {
        bail!("IPv6 broker hosts must be wrapped in brackets, e.g. mqtt://[::1]:1883");
    }

    if let Some((host, port)) = input.rsplit_once(':') {
        return Ok((host.to_string(), parse_port(port)?));
    }

    Ok((input.to_string(), default_port))
}

fn parse_port(port: &str) -> Result<u16> {
    port.parse::<u16>()
        .with_context(|| format!("invalid broker port '{port}'"))
}

#[cfg(test)]
mod tests {
    use super::{BrokerEndpoint, BrokerScheme, Cli};

    #[test]
    fn parses_default_plain_port() {
        let broker = BrokerEndpoint::parse("mqtt://localhost").unwrap();
        assert_eq!(broker.scheme, BrokerScheme::Mqtt);
        assert_eq!(broker.host, "localhost");
        assert_eq!(broker.port, 1883);
    }

    #[test]
    fn parses_default_tls_port() {
        let broker = BrokerEndpoint::parse("mqtts://broker.example.com").unwrap();
        assert_eq!(broker.scheme, BrokerScheme::Mqtts);
        assert_eq!(broker.port, 8883);
    }

    #[test]
    fn parses_bracketed_ipv6() {
        let broker = BrokerEndpoint::parse("mqtt://[::1]:1884").unwrap();
        assert_eq!(broker.host, "::1");
        assert_eq!(broker.port, 1884);
    }

    #[test]
    fn rejects_insecure_skip_tls_verify_for_plain_mqtt() {
        let error = Cli {
            broker: "mqtt://localhost".to_string(),
            host_id: "host-1".to_string(),
            client_id: None,
            username: None,
            password: None,
            password_env: None,
            ca_file: None,
            insecure_skip_tls_verify: true,
            keep_alive_secs: 30,
            reorder_timeout_ms: 2000,
        }
        .into_config()
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "--insecure-skip-tls-verify requires an mqtts:// broker URL"
        );
    }
}
