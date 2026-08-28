use std::sync::Arc;

use anyhow::Context;
use axum::extract::ws::{Message as AxumMessage, WebSocket as AxumWebSocket};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream, UdpSocket,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream as TungsteniteWebSocket,
    tungstenite::Message as TungsteniteMessage,
};

#[repr(u8)]
enum MessageType {
    Tcp = 0,
    Udp = 1,
}

pub trait WebSocketTx {
    async fn send(&mut self, bytes: Vec<u8>) -> anyhow::Result<()>;
}

impl WebSocketTx for SplitSink<AxumWebSocket, AxumMessage> {
    async fn send(&mut self, bytes: Vec<u8>) -> anyhow::Result<()> {
        SinkExt::send(self, AxumMessage::Binary(bytes.into()))
            .await
            .context("Could not send bytes through websocket stream.")
    }
}

impl WebSocketTx
    for SplitSink<TungsteniteWebSocket<MaybeTlsStream<TcpStream>>, TungsteniteMessage>
{
    async fn send(&mut self, bytes: Vec<u8>) -> anyhow::Result<()> {
        SinkExt::send(self, TungsteniteMessage::Binary(bytes.into()))
            .await
            .context("Could not send bytes through websocket stream.")
    }
}

pub trait WebSocketRx {
    async fn recv(&mut self) -> anyhow::Result<Vec<u8>>;
}

impl WebSocketRx for SplitStream<AxumWebSocket> {
    async fn recv(&mut self) -> anyhow::Result<Vec<u8>> {
        loop {
            let message = self
                .next()
                .await
                .context("No more messages from client.")?
                .context("Could not read message from client.")?;

            match message {
                AxumMessage::Binary(bytes) => return Ok(bytes.to_vec()),
                AxumMessage::Text(text) => return Ok(text.as_bytes().to_vec()),
                AxumMessage::Close(_) => return Ok(Vec::new()),
                _ => {
                    continue;
                }
            }
        }
    }
}

impl WebSocketRx for SplitStream<TungsteniteWebSocket<MaybeTlsStream<TcpStream>>> {
    async fn recv(&mut self) -> anyhow::Result<Vec<u8>> {
        loop {
            let message = self
                .next()
                .await
                .context("No more messages from client.")?
                .context("Could not read message from client.")?;

            match message {
                TungsteniteMessage::Binary(bytes) => return Ok(bytes.to_vec()),
                TungsteniteMessage::Text(text) => return Ok(text.as_bytes().to_vec()),
                TungsteniteMessage::Close(_) => return Ok(Vec::new()),
                _ => {
                    continue;
                }
            }
        }
    }
}

pub async fn xcp_to_ws(
    mut tcp: OwnedReadHalf,
    udp: Arc<UdpSocket>,
    mut ws: impl WebSocketTx,
) -> anyhow::Result<()> {
    loop {
        let mut tcp_buffer = [0u8; 8196];
        let mut udp_buffer = [0u8; 8196];

        let mut buffer: Vec<u8>;

        tokio::select! {
            result = tcp.read(&mut tcp_buffer) => {
                if let Ok(result) = result {
                    if result == 0 {
                        return Ok(());
                    }

                    buffer = tcp_buffer[0..result].to_vec();
                    buffer.push(MessageType::Tcp as u8);
                } else {
                    result.context("Could not read TCP data.")?;
                    break;
                }
            },
            result = udp.recv(&mut udp_buffer) => {
                if let Ok(result) = result {
                    if result == 0 {
                        return Ok(());
                    }

                    buffer = udp_buffer[0..result].to_vec();
                    buffer.push(MessageType::Udp as u8);
                } else {
                    result.context("Could not read UDP data.")?;
                    break;
                }
            },
        }

        ws.send(buffer)
            .await
            .context("Could not send binary data to WebSocket stream.")?;
    }

    Ok(())
}

pub async fn ws_to_xcp(
    mut tcp: OwnedWriteHalf,
    udp: Arc<UdpSocket>,
    mut ws: impl WebSocketRx,
) -> anyhow::Result<()> {
    loop {
        let message = ws
            .recv()
            .await
            .context("Could not read message from WebSocket connection.")?;

        if message.is_empty() {
            break;
        }

        if message.ends_with(&[MessageType::Udp as u8]) {
            udp.send(&message[0..message.len() - 2])
                .await
                .context("Could not send client's bytes through UDP.")?;
        } else {
            tcp.write_all(&message[0..message.len() - 2])
                .await
                .context("Could not send client's bytes through TCP.")?;
        }
    }

    Ok(())
}
