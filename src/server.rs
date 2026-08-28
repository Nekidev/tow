use std::net::{SocketAddr, ToSocketAddrs};

use anyhow::Context;
use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::Response,
    routing,
};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};

use crate::args::ServerArgs;

#[derive(Clone, Copy)]
struct TowState {
    pub address: SocketAddr,
}

pub async fn run(args: ServerArgs) -> anyhow::Result<()> {
    let listener = TcpListener::bind(args.from)
        .await
        .context("Could not bind to the specified address (`--from`).")?;

    let router = Router::new()
        .route("/", routing::any(gate))
        .with_state(TowState {
            address: args
                .to
                .to_socket_addrs()
                .context("Could not get socket address from `--to` argument.")?
                .next()
                .context("The provided `--to` argument did not resolve to any socket addresses.")?,
        });

    axum::serve(listener, router.into_make_service())
        .await
        .context("An error occurred while listening for incoming connections.")?;

    Ok(())
}

async fn gate(ws: WebSocketUpgrade, State(state): State<TowState>) -> Response {
    ws.on_upgrade(move |ws| handle(ws, state))
}

async fn handle(ws: WebSocket, state: TowState) {
    let Ok(connection) = TcpStream::connect(state.address).await else {
        return;
    };

    let (client_sender, client_receiver) = ws.split();
    let (server_receiver, server_sender) = connection.into_split();

    let outgoing_task = tokio::spawn(handle_outgoing(server_receiver, client_sender));
    let incoming_task = tokio::spawn(handle_incoming(server_sender, client_receiver));

    tokio::select! {
        _ = outgoing_task => {},
        _ = incoming_task => {},
    }
}

async fn handle_outgoing(
    mut server_receiver: OwnedReadHalf,
    mut client_sender: SplitSink<WebSocket, Message>,
) -> anyhow::Result<()> {
    loop {
        let mut buffer = Vec::new();
        server_receiver
            .read(&mut buffer)
            .await
            .context("Could not read outgoing data.")?;
        client_sender
            .send(Message::Binary(buffer.into()))
            .await
            .context("Could not send binary data to client.")?;
    }
}

async fn handle_incoming(
    mut server_sender: OwnedWriteHalf,
    mut client_receiver: SplitStream<WebSocket>,
) -> anyhow::Result<()> {
    loop {
        let message = client_receiver
            .next()
            .await
            .context("No more messages from client.")?
            .context("Could not read message from client.")?;

        match message {
            Message::Binary(bytes) => server_sender
                .write_all(&bytes)
                .await
                .context("Could not send client's bytes to server.")?,
            Message::Text(text) => server_sender
                .write_all(text.as_bytes())
                .await
                .context("Could not send client's text to server.")?,
            Message::Close(_) => {
                server_sender.forget();
                break;
            }
            _ => {}
        }
    }

    Ok(())
}
