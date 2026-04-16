[![Build](https://github.com/mattcurf/rust_sparkplug_host/actions/workflows/build.yml/badge.svg)](https://github.com/mattcurf/rust_sparkplug_host/actions/workflows/build.yml)

# Rust Sparkplug B Primary Host

A Rust baseline for a Sparkplug B Primary Host Application. The binary opens an MQTT session (plain or TLS), publishes the host `STATE` birth/death per the Sparkplug 3.0 rules, subscribes to the Sparkplug namespace, decodes live node/device traffic, tracks in-memory node/device/session state, publishes automatic `NCMD` rebirth requests on sequence gaps or malformed session ordering, and prints effective metric changes to stdout.

For more information about Eclipse Sparkplug B, see https://github.com/eclipse-sparkplug/sparkplug and https://sparkplug.eclipse.org/. (Sparkplug®, Sparkplug Compatible, and the Sparkplug Logo are trademarks of the Eclipse Foundation.)

## Project Notes
This repo is a quick, fun Sparkplug side project of mine, not a polished production host application!

- expect some shortcuts and simplifications that keep the code easy to read and easy to hack on
- use at your own risk
- do not treat it as a definitive reference for Sparkplug primary host architecture, security hardening, high availability, operational tooling, or long-term maintainability best practices
- if you want to build something production-facing from it, plan to review the configuration, credential handling, dependency licensing, observability, testing, persistence, and operational failure paths carefully

## License Notes
The original project code in this repository is licensed under Apache 2.0. Dependencies fetched via Cargo remain subject to their own license terms.

- the `sparkplug-rs`, `rumqttc`, `tokio`, `clap`, `serde`, `serde_json`, and `anyhow` crates pulled in via `Cargo.toml` each ship with their own license terms — review them before redistributing
- do not assume any crate source pulled by Cargo is covered by this repository's Apache 2.0 license

## Implemented Scope
- `config`: CLI parsing, `mqtt://` / `mqtts://` URL parsing, host ID wildcard validation, optional username / password / `--password-env`, optional CA file, optional insecure-TLS test mode
- `mqtt`: MQTT options, retained QoS 1 Last Will on `spBv1.0/STATE/<host_id>`, Sparkplug namespace subscription plan, clean-session configuration
- `state`: Sparkplug host `STATE` topic plus JSON birth / death payload builders that match the 3.0 `{"online":bool,"timestamp":uint64}` format
- `topic`: Sparkplug topic parsing for `NBIRTH`, `NDEATH`, `NDATA`, `DBIRTH`, `DDEATH`, `DDATA`, and `STATE`
- `decode`: Sparkplug protobuf decoding covering metrics, datasets, templates, property sets, arrays, nulls, and quality
- `model`: in-memory node / device / session model with per-edge-node `bdSeq` tracking, alias resolution, sequence validation, broker-disconnect stale handling, and `NDEATH` / `DDEATH` stale handling (quality 500)
- `reorder`: sequence gap / reorder timeout primitives feeding automatic `NCMD` `Node Control/Rebirth=true` requests
- `app`: subscription-before-birth session lifecycle, retained offline-echo republish, automatic rebirth on reorder timeout and malformed session ordering, and graceful retained `STATE` death on `Ctrl+C`

## Deliberate Out-Of-Scope Items
- No multi-MQTT-server topology support. The Sparkplug spec allows a primary host to coordinate across redundant brokers, but this scaffold connects to a single broker only.
- No operator-driven outbound `NCMD` / `DCMD` tooling. The scaffold only emits the automatic `Node Control/Rebirth` commands it needs for its own session-recovery rules; inbound non-rebirth commands are logged and ignored.
- No MQTT 5 session mode. `rumqttc` is configured for MQTT 3.1.1 with `clean_session = true`.
- No persistent state snapshots across restarts. All node / device / session tracking is in memory and is rebuilt on reconnect.
- No historian storage, database integration, or REST / HTTP surfaces.
- No HA coordination across multiple running primary host instances.

## Fixed Runtime Defaults
- Sparkplug namespace: `spBv1.0`
- Host `STATE` topic format: `spBv1.0/STATE/<host_id>`
- MQTT session mode: clean session = true (MQTT 3.1.1 via `rumqttc`)
- Will / birth payload format: JSON `{"online":bool,"timestamp":<unix_ms>}` with QoS 1 and retain = true
- Subscription plan: `spBv1.0/+/NBIRTH/+`, `spBv1.0/+/NDEATH/+`, `spBv1.0/+/NDATA/+`, `spBv1.0/+/DBIRTH/+/+`, `spBv1.0/+/DDEATH/+/+`, `spBv1.0/+/DDATA/+/+`, plus own `spBv1.0/STATE/<host_id>`
- Default MQTT client ID: `<host_id>-mqtt`

## Local Configuration
The binary is configured from the command line — no compile-time edits required. The required flags are:

- `--broker` — MQTT broker URL, for example `mqtt://broker-host-or-ip:1883` or `mqtts://broker-host-or-ip:8883`
- `--host-id` — Sparkplug host ID used in `spBv1.0/STATE/<host_id>`; must not contain `/`, `+`, or `#`

Optional flags:

- `--client-id` — MQTT client ID; defaults to `<host_id>-mqtt`
- `--username` — MQTT username when the broker requires authentication
- `--password` — MQTT password passed on the command line (discouraged because it can be captured by shell history and process listings)
- `--password-env` — name of an environment variable that holds the MQTT password; preferred over `--password`
- `--ca-file` — path to a PEM CA bundle used to verify the broker certificate for `mqtts://` endpoints
- `--insecure-tls` — disable TLS certificate verification for test brokers with untrusted certificates; do not use against production brokers

## Install The Cargo Toolchain

### macOS and Linux

```bash
curl https://sh.rustup.rs -sSf | sh
source "$HOME/.cargo/env"
rustup default stable
rustup component add rustfmt clippy
```

### Verify The Installation

```bash
rustc --version
cargo --version
```

The crate pins `rust-version = "1.93"` in `Cargo.toml`.

## Build

Debug build:

```bash
cargo build
```

Release build:

```bash
cargo build --release
```

Run the unit tests:

```bash
cargo test
```

## Run

### Plain MQTT

```bash
cargo run -- \
  --broker mqtt://localhost:1883 \
  --host-id primary-host-1
```

### MQTT Over TLS

```bash
cargo run -- \
  --broker mqtts://broker.example.com:8883 \
  --host-id primary-host-1 \
  --ca-file ./certs/ca.pem
```

### MQTT Over TLS With Username/Password

```bash
export MQTT_PASSWORD='secret-password'

cargo run -- \
  --broker mqtts://broker.example.com:8883 \
  --host-id primary-host-1 \
  --username scada \
  --password-env MQTT_PASSWORD \
  --ca-file ./certs/ca.pem
```

Stop the running host with `Ctrl+C`. The scaffold publishes a retained QoS 1 `STATE` death message before disconnecting so downstream edge nodes and tools can react to the host going offline.

## What The Binary Does Per Session
On each connection cycle the binary:

- validates the CLI configuration (broker URL, host ID, TLS inputs)
- builds the host `STATE` birth / death payload pair for that connection using the current UTC millisecond timestamp
- connects with a retained QoS 1 MQTT Will on `spBv1.0/STATE/<host_id>` carrying the offline payload
- subscribes to the Sparkplug namespace plus its own `STATE` topic, then publishes the online `STATE` birth only after the subscription acknowledgements arrive
- republishes its online `STATE` birth if it receives a retained copy of its own offline state after reconnect
- decodes `NBIRTH`, `NDEATH`, `NDATA`, `DBIRTH`, `DDEATH`, `DDATA`, and `STATE` payloads
- maintains per-edge-node `bdSeq` and alias caches, resolves aliases for subsequent data messages, and validates sequence monotonicity with `wrapping_add(1)` so `255 → 0` transitions are accepted
- marks previously tracked node and device metrics stale before reconnect if the host loses its broker session
- marks node and device metrics stale with quality 500 on `NDEATH` and `DDEATH` that match the tracked `bdSeq`
- detects sequence gaps, schedules a reorder timeout, and publishes an `NCMD` `Node Control/Rebirth=true` request if the timeout elapses before the missing sequence arrives
- rejects malformed session ordering such as DATA arriving before the corresponding BIRTH and publishes an `NCMD` rebirth request for the offending edge node
- prints connection activity, birth / death announcements, and fully decoded effective metric changes to standard output
- publishes an intentional retained QoS 1 offline `STATE` before disconnecting on `Ctrl+C`

## Validation Status

Validated locally with:

- `cargo build`
- `cargo test`
- manual connection against a local MQTT broker using the `cargo run --` invocations shown above
