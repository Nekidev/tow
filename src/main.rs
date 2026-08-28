use anyhow::Context;
use clap::Parser;

use crate::args::{Args, Subcommand};

mod args;
mod client;
mod server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    
    let args = Args::parse();

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
