use std::sync::Arc;

use anyhow::Context;
use futures::channel::mpsc;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

pub type WebSocketTx<T> = SplitSink<WebSocketStream<T>, Message>;
pub type WebSocketRx<T> = SplitStream<WebSocketStream<T>>;

pub async fn tcp_to_ws<T, E>(mut tcp: OwnedReadHalf, mut ws: WebSocketTx<T>) -> anyhow::Result<()>
where
    T: AsyncWrite,
    E: std::error::Error + Send + Sync + 'static,
    WebSocketTx<T>: Sink<Message, Error = E> + SinkExt<Message>,
{
    loop {
        let mut buffer = [0u8; 8192];
        let bytes = tcp
            .read(&mut buffer)
            .await
            .context("Could not read bytes from TCP connection.")?;

        tracing::trace!(
            "TCP to WS: {:?}",
            String::from_utf8_lossy(&buffer[0..bytes])
        );

        ws.send(Message::Binary(buffer[0..bytes].to_vec().into()))
            .await
            .context("Could not send TCP message through websocket stream.")?;
    }
}

pub async fn ws_to_tcp<T, E>(mut ws: WebSocketRx<T>, mut tcp: OwnedWriteHalf) -> anyhow::Result<()>
where
    T: AsyncWrite,
    E: std::error::Error + Send + Sync + 'static,
    WebSocketRx<T>: Stream<Item = Result<Message, E>> + StreamExt,
{
    loop {
        let message = ws
            .next()
            .await
            .context("There were no more messages to read from the WebSocket stream.")?
            .context("Could not read from websocket stream.")?;

        tracing::trace!("WS to TCP: {message:?}");

        match message {
            Message::Binary(bytes) => tcp
                .write_all(&bytes)
                .await
                .context("Could not send TCP bytes to the server.")?,
            Message::Close(_) => {
                tcp.forget();
                break;
            }
            _ => {}
        };
    }

    Ok(())
}

pub trait DatagramReader {
    async fn read(&mut self) -> anyhow::Result<Vec<u8>>;
}

impl DatagramReader for mpsc::UnboundedReceiver<Vec<u8>> {
    async fn read(&mut self) -> anyhow::Result<Vec<u8>> {
        self.recv()
            .await
            .context("Could not receive datagram from channel.")
    }
}

impl DatagramReader for Arc<UdpSocket> {
    async fn read(&mut self) -> anyhow::Result<Vec<u8>> {
        let mut buffer = [0u8; 2048];
        let bytes = self
            .recv(&mut buffer)
            .await
            .context("Could not receive datagram from UDP socket.")?;

        if bytes == 0 {
            Ok(Vec::new())
        } else {
            Ok(buffer[0..bytes].to_vec())
        }
    }
}

pub async fn udp_to_ws<T, E>(
    mut udp: impl DatagramReader,
    ws: &mut WebSocketTx<T>,
    on_message: impl Fn(),
) -> anyhow::Result<()>
where
    T: AsyncWrite,
    E: std::error::Error + Send + Sync + 'static,
    WebSocketTx<T>: Sink<Message, Error = E> + SinkExt<Message>,
{
    loop {
        let bytes = udp
            .read()
            .await
            .context("Could not read bytes from UDP connection.")?;

        tracing::trace!("TCP to WS: {:?}", String::from_utf8_lossy(&bytes));

        if bytes.is_empty() {
            break;
        }

        ws.send(Message::Binary(bytes.into()))
            .await
            .context("Could not send UDP message to server.")?;

        on_message();
    }

    Ok(())
}

pub async fn ws_to_udp<T, E, P>(
    ws: &mut WebSocketRx<T>,
    udp: Arc<UdpSocket>,
    peer_address: P,
    on_message: impl Fn(),
) -> anyhow::Result<()>
where
    T: AsyncWrite,
    P: ToSocketAddrs + Copy,
    E: std::error::Error + Send + Sync + 'static,
    WebSocketRx<T>: Stream<Item = Result<Message, E>> + StreamExt,
{
    loop {
        let message = ws
            .next()
            .await
            .context("There were no more messages to read from the WebSocket stream.")?
            .context("Could not read from websocket stream.")?;

        tracing::trace!("WS to UDP: {message:?}");

        match message {
            Message::Binary(bytes) => {
                let _ = udp
                    .send_to(&bytes, peer_address)
                    .await
                    .context("Could not send TCP bytes to the server.")?;
            }
            Message::Close(_) => {
                break;
            }
            _ => {}
        };

        on_message();
    }

    Ok(())
}
