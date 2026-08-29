use anyhow::Context;
use clap::Parser;

use crate::args::{Args, Subcommand};

mod args;
mod client;
mod proxy;
mod server;
mod utils;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let args = Args::parse();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .unwrap();

    #[cfg(debug_assertions)]
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter("off,tow=trace")
        .init();

    #[cfg(not(debug_assertions))]
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter("off,tow=info")
        .init();

    match args.subcommand {
        Subcommand::Client(args) => client::run(args)
            .await
            .context("Failed to connect to the specified address.")?,
        Subcommand::Server(args) => server::run(args)
            .await
            .context("Failed to serve at the specified address.")?,
    }

    Ok(())
}
