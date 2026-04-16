# Contributing

Thanks for taking a look.

This is a small hobby Rust Sparkplug project, so contributions are welcome, but the goal is to keep things simple and fun rather than turning the repo into a large framework.

## Ground Rules

- Keep changes small and focused.
- Prefer straightforward Rust and `tokio` patterns over heavy abstraction.
- Do not commit real broker URIs, usernames, passwords, client credentials, API keys, certificates, or other secrets.
- Update `README.md` when behavior, setup, CLI flags, or operator-facing commands change.
- If you update dependencies, keep `Cargo.lock` in sync and be mindful that upstream crates keep their own license terms.

## Setup

1. Install the Rust toolchain with `rustup`.
2. Use a toolchain new enough for `rust-version = "1.93"`.
3. If you want the usual local checks, install `rustfmt` and `clippy`.

## Build

Run:

```sh
cargo build --locked
```

For a release build:

```sh
cargo build --release --locked
```

## Test

Run:

```sh
cargo test --locked
```

## Before Opening A PR

Please do the following when possible:

1. Run `cargo build --locked` successfully.
2. Run `cargo test --locked` successfully.
3. Run `cargo fmt --check`.
4. If you changed runtime behavior, describe what you tested against a broker and what remains unverified.
5. Double-check that no private credentials, hostnames, broker details, or machine-specific values were introduced.

## Scope Notes

Useful contributions include:

- bug fixes
- build or tooling improvements
- documentation cleanup
- Sparkplug topic or payload correctness fixes
- session handling and rebirth/reorder fixes
- test coverage improvements

Changes that significantly expand scope may be better discussed first.
