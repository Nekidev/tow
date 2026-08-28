use anyhow::Context;
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
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest, handshake::client::Request},
};

use crate::args::ClientArgs;

pub async fn run(args: ClientArgs) -> anyhow::Result<()> {
    let request = args.to.into_client_request().context("Could not create a WebSocket connection request from the address passed in `--from`. Is it valid? E.g. `wss://example.com`")?;

    let listener = TcpListener::bind(args.from)
        .await
        .context("Could not bind to the specified address (`--from`)")?;

    loop {
        let (stream, _address) = listener
            .accept()
            .await
            .context("Could not accept incoming connection.")?;

        tokio::spawn(handle_connection(stream, request.clone()));
    }
}

/// Proxies the stream to the WebSocket server using the specified request.
async fn handle_connection(client_stream: TcpStream, request: Request) -> anyhow::Result<()> {
    let (server_stream, _response) = tokio_tungstenite::connect_async(request)
        .await
        .context("Could not connect to the WebSocket server.")?;

    let (server_sender, server_receiver) = server_stream.split();
    let (client_receiver, client_sender) = client_stream.into_split();

    let incoming_task = tokio::spawn(handle_incoming(server_receiver, client_sender));
    let outgoing_task = tokio::spawn(handle_outgoing(client_receiver, server_sender));

    tokio::select! {
        _ = incoming_task => {},
        _ = outgoing_task => {},
    }

    Ok(())
}

async fn handle_incoming(
    mut server_receiver: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    mut client_sender: OwnedWriteHalf,
) -> anyhow::Result<()> {
    loop {
        let message = server_receiver
            .next()
            .await
            .context("No more messages from client.")?
            .context("Could not read message from client.")?;

        match message {
            Message::Binary(bytes) => client_sender
                .write_all(&bytes)
                .await
                .context("Could not send client's bytes to server.")?,
            Message::Text(text) => client_sender
                .write_all(text.as_bytes())
                .await
                .context("Could not send client's text to server.")?,
            Message::Close(_) => {
                client_sender.forget();
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

async fn handle_outgoing(
    mut client_receiver: OwnedReadHalf,
    mut server_sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
) -> anyhow::Result<()> {
    loop {
        let mut buffer = Vec::new();
        client_receiver
            .read(&mut buffer)
            .await
            .context("Could not read outgoing data.")?;
        server_sender
            .send(Message::Binary(buffer.into()))
            .await
            .context("Could not send binary data to server.")?;
    }
}
