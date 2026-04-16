// SPDX-FileCopyrightText: 2026 Matt Curfman
// SPDX-License-Identifier: Apache-2.0

mod app;
mod config;
mod decode;
mod model;
mod mqtt;
mod reorder;
mod state;
mod topic;

use anyhow::Result;
use clap::Parser;

use crate::app::SparkplugPrimaryHost;
use crate::config::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = cli.into_config()?;

    SparkplugPrimaryHost::new(config).run().await
}
