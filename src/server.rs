use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};

use anyhow::Context;
use axum::{
    Router,
    extract::{State, WebSocketUpgrade, ws::WebSocket},
    response::Response,
    routing,
};
use futures_util::StreamExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};

use crate::{args::ServerArgs, proxy};

#[derive(Clone)]
struct TowState {
    pub address: SocketAddr,
    pub udp_listener: Arc<UdpSocket>,
}

pub async fn run(args: ServerArgs) -> anyhow::Result<()> {
    let udp_listener = Arc::new(
        UdpSocket::bind(args.from)
            .await
            .context("Could not bind UDP to the specified address (`--from`).")?,
    );
    let tcp_listener = TcpListener::bind(args.from)
        .await
        .context("Could not bind TCP to the specified address (`--from`).")?;

    let router = Router::new()
        .route("/", routing::any(gate))
        .with_state(TowState {
            address: args
                .to
                .to_socket_addrs()
                .context("Could not get socket address from `--to` argument.")?
                .next()
                .context("The provided `--to` argument did not resolve to any socket addresses.")?,
            udp_listener,
        });

    axum::serve(tcp_listener, router.into_make_service())
        .await
        .context("An error occurred while listening for incoming connections.")?;

    Ok(())
}

async fn gate(ws: WebSocketUpgrade, State(state): State<TowState>) -> Response {
    ws.on_upgrade(move |ws| handle(ws, state))
}

async fn handle(ws: WebSocket, state: TowState) {
    let Ok(tcp_connection) = TcpStream::connect(state.address).await else {
        return;
    };

    let (client_sender, client_receiver) = ws.split();
    let (server_receiver, server_sender) = tcp_connection.into_split();

    let outgoing_tcp_task = tokio::spawn(proxy::xcp_to_ws(
        server_receiver,
        state.udp_listener.clone(),
        client_sender,
    ));
    let incoming_task = tokio::spawn(proxy::ws_to_xcp(
        server_sender,
        state.udp_listener.clone(),
        client_receiver,
    ));

    tokio::select! {
        _ = outgoing_tcp_task => {},
        _ = incoming_task => {},
    }
}
