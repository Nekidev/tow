use std::net::SocketAddr;

/// A TCP (and UDP) over WebSockets proxy.
#[derive(clap::Parser)]
pub struct Args {
    #[clap(subcommand)]
    pub subcommand: Subcommand,
}

#[derive(clap::Subcommand)]
pub enum Subcommand {
    /// Listen for incoming websocket connections and forward them to a TCP (and UDP) address.
    Server(ServerArgs),
    /// Connect to a websocket server and proxy incoming TCP/UDP connections through it.
    Client(ClientArgs),
}

#[derive(clap::Parser)]
pub struct ServerArgs {
    /// The address at which to listen for new connections.
    #[arg(env = "TOW_SERVER_FROM")]
    pub from: SocketAddr,
    /// The address to forward the connections to.
    #[arg(env = "TOW_SERVER_TO")]
    pub to: String,
}

#[derive(clap::Parser)]
pub struct ClientArgs {
    /// The address at which to listen for new connections.
    #[arg(env = "TOW_CLIENT_FROM")]
    pub from: SocketAddr,
    /// The address to forward the connections to.
    #[arg(env = "TOW_CLIENT_TO")]
    pub to: String,

    /// When set, UDP tunnels will be closed after inactivity instead of on unbind.
    #[arg(short, long, value_name = "MILLISECONDS", env = "TOW_CLIENT_TIMEOUT")]
    pub timeout: Option<u64>,
}
