use std::sync::Arc;

use anyhow::Context;
use futures_util::StreamExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, handshake::client::Request};

use crate::{args::ClientArgs, proxy};

pub async fn run(args: ClientArgs) -> anyhow::Result<()> {
    let request = args.to.into_client_request().context("Could not create a WebSocket connection request from the address passed in `--from`. Is it valid? E.g. `wss://example.com`")?;

    let tcp_listener = TcpListener::bind(args.from)
        .await
        .context("Could not bind to the specified TCP address (`--from`)")?;
    let udp_listener = Arc::new(
        UdpSocket::bind(args.from)
            .await
            .context("Could not bind to the specified UDP address (`--from`).")?,
    );

    loop {
        let (tcp_stream, _address) = tcp_listener
            .accept()
            .await
            .context("Could not accept incoming connection.")?;

        tokio::spawn(handle_connection(
            tcp_stream,
            udp_listener.clone(),
            request.clone(),
        ));
    }
}

/// Proxies the stream to the WebSocket server using the specified request.
async fn handle_connection(
    client_tcp: TcpStream,
    client_udp: Arc<UdpSocket>,
    request: Request,
) -> anyhow::Result<()> {
    let (server_stream, _response) = tokio_tungstenite::connect_async(request)
        .await
        .context("Could not connect to the WebSocket server.")?;

    let (server_sender, server_receiver) = server_stream.split();
    let (client_receiver, client_sender) = client_tcp.into_split();

    let incoming_task = tokio::spawn(proxy::ws_to_xcp(
        client_sender,
        client_udp.clone(),
        server_receiver,
    ));
    let outgoing_task = tokio::spawn(proxy::xcp_to_ws(
        client_receiver,
        client_udp.clone(),
        server_sender,
    ));

    tokio::select! {
        _ = incoming_task => {},
        _ = outgoing_task => {},
    }

    Ok(())
}
