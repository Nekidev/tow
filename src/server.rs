use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use anyhow::Context;
use futures_util::StreamExt;
use http::{Request, Response};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio_tungstenite::WebSocketStream;

use crate::args::ServerArgs;
use crate::proxy;
use crate::utils::TraceError;

#[derive(Debug, PartialEq, Eq)]
enum NetworkType {
    Tcp,
    Udp,
    None,
}

pub async fn run(args: ServerArgs) -> anyhow::Result<()> {
    let server_address = args
        .to
        .to_socket_addrs()
        .context("Could not get socket address from server URL (`<TO>`).")?
        .next()
        .context("The server URL (`<TO>`) did not resolve to any socket addresses.")?;

    let listener = TcpListener::bind(args.from)
        .await
        .context("Could not bind TCP to the specified address (`--from`).")?;

    tracing::info!(
        "Listening for incoming websocket connections at {}",
        listener
            .local_addr()
            .context("Could not get local address")?
    );

    loop {
        let (stream, client_address) = listener
            .accept()
            .await
            .context("Could not accept incoming WebSocket connection.")?;

        tracing::debug!(%client_address, "Accepted incoming WebSocket connection");

        tokio::spawn(handle_connection(stream, client_address, server_address));
    }
}

async fn handle_connection(
    stream: TcpStream,
    client_address: SocketAddr,
    server_address: SocketAddr,
) -> anyhow::Result<()> {
    let mut network_type: NetworkType = NetworkType::None;

    #[allow(clippy::result_large_err)]
    let callback = |req: &Request<()>, res: Response<()>| {
        let header = req.headers().get("network-type");

        if let Some(header) = header {
            if let Ok(value) = header.to_str() {
                match value {
                    "tcp" => network_type = NetworkType::Tcp,
                    "udp" => network_type = NetworkType::Udp,
                    _ => {
                        return Err(Response::new(Some(
                            "The network-type header contained an invalid value.".into(),
                        )));
                    }
                }
            } else {
                return Err(Response::new(Some(
                    "The network-type header contained an invalid value.".into(),
                )));
            }
        } else {
            return Err(Response::new(Some(
                "No network-type header was present in the request.".into(),
            )));
        }

        Ok(res)
    };

    let stream = tokio_tungstenite::accept_hdr_async(stream, callback)
        .await
        .context("Could not handshake with the client.")
        .err_warn()?;

    tracing::debug!(%client_address, ?network_type, "Handshaked incoming WebSocket connection");

    match network_type {
        NetworkType::None => anyhow::bail!("The network type was invalid."),
        NetworkType::Tcp => handle_tcp_connection(stream, client_address, server_address).await,
        NetworkType::Udp => handle_udp_connection(stream, client_address, server_address).await,
    }
}

async fn handle_tcp_connection(
    stream: WebSocketStream<TcpStream>,
    client_address: SocketAddr,
    server_address: SocketAddr,
) -> anyhow::Result<()> {
    let connection = TcpStream::connect(server_address)
        .await
        .context("Could not connect to the upstream TCP server.")
        .err_error()?;

    tracing::info!(
        "Client address {} has its TCP messages proxied as {}.",
        client_address,
        connection
            .local_addr()
            .context("Could not get local address for TCP connection.")?
    );

    let (ws_tx, ws_rx) = stream.split();
    let (tcp_rx, tcp_tx) = connection.into_split();

    let outgoing_task = tokio::spawn(proxy::tcp_to_ws(tcp_rx, ws_tx));
    let incoming_task = tokio::spawn(proxy::ws_to_tcp(ws_rx, tcp_tx));

    tokio::select! {
        result = outgoing_task => {
            tracing::debug!("TCP outgoing task ended for {client_address}: {result:?}");
        },
        result = incoming_task => {
            tracing::debug!("TCP incoming task ended for {client_address}: {result:?}");
        },
    }

    tracing::debug!(
        "Client address {} disconnected from its TCP tunnel.",
        client_address
    );

    Ok(())
}

async fn handle_udp_connection(
    stream: WebSocketStream<TcpStream>,
    client_address: SocketAddr,
    server_address: SocketAddr,
) -> anyhow::Result<()> {
    let connection = Arc::new(
        UdpSocket::bind("0.0.0.0:0")
            .await
            .context("Could not bind UDP socket.")
            .err_error()?,
    );

    connection
        .connect(server_address)
        .await
        .context("Could not connect to UDP server address.")
        .err_error()?;

    let peer_address = connection
        .peer_addr()
        .context("Could not get server's peer address.")?;

    tracing::info!(
        "Client address {} has its UDP messages proxied as {}.",
        client_address,
        connection
            .local_addr()
            .context("Could not get local address for UDP connection.")?
    );

    let (mut ws_tx, mut ws_rx) = stream.split();

    let outgoing_task = proxy::udp_to_ws(connection.clone(), &mut ws_tx, || ());
    let incoming_task = proxy::ws_to_udp(&mut ws_rx, connection.clone(), peer_address, || ());

    tokio::select! {
        result = outgoing_task => {
            tracing::debug!("UDP outgoing task ended for {client_address}: {result:?}");
        },
        result = incoming_task => {
            tracing::debug!("UDP incoming task ended for {client_address}: {result:?}");
        },
    }

    let mut ws_stream = ws_tx.reunite(ws_rx).unwrap();
    let _ = ws_stream
        .close(None)
        .await
        .context("Could not send websocket close frame.")
        .err_trace();

    tracing::debug!(
        "Client address {} disconnected from its UDP tunnel.",
        client_address
    );

    Ok(())
}
