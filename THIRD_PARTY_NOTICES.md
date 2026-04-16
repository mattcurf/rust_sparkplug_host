# Third-Party Notices

This repository includes or depends on third-party software.

## Mixed-License Note

The repository's own original code is licensed under Apache 2.0, but some dependencies are provided under different upstream licenses. These third-party components keep their own licenses; they are not relicensed to Apache 2.0 by inclusion in this repository.

## Vendored Code

At the time of writing, this repository does not vendor third-party source trees into the git checkout.

That means the main third-party review surface here is the Rust dependency set resolved through Cargo rather than copied-in upstream source directories.

## Direct Cargo Dependencies

At the time of writing, the direct dependencies declared in `Cargo.toml` are:

- `anyhow` `1.0.102` — license: `MIT OR Apache-2.0`
- `clap` `4.6.0` — license: `MIT OR Apache-2.0`
- `rumqttc` `0.25.1` — license: `Apache-2.0`
- `serde` `1.0.228` — license: `MIT OR Apache-2.0`
- `serde_json` `1.0.149` — license: `MIT OR Apache-2.0`
- `sparkplug-rs` `0.5.1` — license: `EPL-2.0`
- `tokio` `1.52.0` — license: `MIT`

These license were taken from the crates' `Cargo.toml` metadata in the local cargo registry cache used for development.

## Build-Time Managed Dependencies

This project also pulls third-party crates from crates.io during dependency resolution and build steps. The exact resolved dependency graph is recorded in `Cargo.lock`.

Because these dependencies are downloaded into the local cargo environment instead of being vendored into this repository, their licensing terms should be reviewed from the upstream crate metadata and source as part of release or redistribution review.
