use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use futures::SinkExt;
use futures::channel::mpsc;
use futures::lock::Mutex;
use futures_util::StreamExt;
use networkrs::sock_diag;
use networkrs::sockets::Protocol;
use papaya::HashMap;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;

use crate::args::ClientArgs;
use crate::proxy;
use crate::utils::{TraceError, utc_now_ms};

pub async fn run(args: ClientArgs) -> anyhow::Result<()> {
    let request = args.to.into_client_request().context("Could not create a WebSocket connection request from the address passed in `<FROM>`. Is it valid? E.g. `wss://example.com`")?;

    let monitor_mode = if let Some(timeout) = args.timeout {
        MonitorMode::Timeout(timeout)
    } else {
        MonitorMode::SockDiag
    };

    let tcp_task = tokio::spawn(run_tcp(args.from, request.clone()));
    let udp_task = tokio::spawn(run_udp(args.from, request.clone(), monitor_mode));

    tokio::select! {
        result = tcp_task => {
            result.context("An error occurred while listening for incoming TCP connections.")?
        }
        result = udp_task => {
            result.context("An error occurred while listening for incoming UDP connections.")?
        }
    }
}

async fn run_tcp(client_address: SocketAddr, request: Request) -> anyhow::Result<()> {
    let listener = TcpListener::bind(client_address)
        .await
        .context("Could not bind to local TCP address.")?;

    tracing::info!("Listening for incoming TCP connections at {client_address}");

    loop {
        let (stream, address) = listener
            .accept()
            .await
            .context("Could not accept incoming TCP connection.")?;

        tokio::spawn(handle_tcp_connection(stream, address, request.clone()));
    }
}

async fn handle_tcp_connection(
    tcp_stream: TcpStream,
    client_address: SocketAddr,
    mut request: Request,
) -> anyhow::Result<()> {
    tracing::info!("Client address {client_address} connected via TCP.");

    request
        .headers_mut()
        .insert("network-type", "tcp".try_into().unwrap());

    let (ws_stream, _response) = tokio_tungstenite::connect_async(request)
        .await
        .context("Could not connect to upstream server.")
        .err_error()?;

    let (ws_tx, ws_rx) = ws_stream.split();
    let (tcp_rx, tcp_tx) = tcp_stream.into_split();

    let incoming_task = tokio::spawn(proxy::ws_to_tcp(ws_rx, tcp_tx));
    let outgoing_task = tokio::spawn(proxy::tcp_to_ws(tcp_rx, ws_tx));

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

struct UdpManager {
    task: Arc<JoinHandle<anyhow::Result<()>>>,
    inode: Option<u64>,
    datagram_tx: mpsc::UnboundedSender<Vec<u8>>,
    close: Arc<Mutex<mpsc::Sender<()>>>,
    last_seen: Arc<AtomicU64>,
}

type Registry = Arc<HashMap<SocketAddr, UdpManager>>;

enum MonitorMode {
    /// Query linux's diag API.
    SockDiag,

    /// Timeout in milliseconds.
    Timeout(u64),
}

async fn run_udp(
    client_address: SocketAddr,
    request: Request,
    monitor_mode: MonitorMode,
) -> anyhow::Result<()> {
    let listener = Arc::new(
        UdpSocket::bind(client_address)
            .await
            .context("Could not bind to local UDP address.")?,
    );

    tracing::info!("Listening for incoming UDP connections at {client_address}");

    let registry: Registry = Arc::new(HashMap::new());

    match monitor_mode {
        MonitorMode::SockDiag => tokio::spawn(monitor_udp_addresses_sock_diag(registry.clone())),
        MonitorMode::Timeout(timeout) => {
            tokio::spawn(monitor_udp_addresses_timeout(registry.clone(), timeout))
        }
    };

    loop {
        let mut buffer = [0u8; 2048];
        let (bytes, address) = listener
            .recv_from(&mut buffer)
            .await
            .context("Could not read UDP message.")?;

        let datagram = buffer[0..bytes].to_vec();

        let registry = registry.pin_owned();

        if let Some(tx) = registry.get(&address)
            && tx.datagram_tx.clone().send(datagram.clone()).await.is_ok()
        {
            // nice 👍
        } else {
            let (close_tx, close_rx) = mpsc::channel(1);
            let (mut datagram_tx, datagram_rx) = mpsc::unbounded();
            datagram_tx.send(datagram).await.unwrap();

            let last_seen = Arc::new(AtomicU64::new(utc_now_ms()));

            let task = tokio::spawn(handle_udp_connection(
                listener.clone(),
                datagram_rx,
                address,
                last_seen.clone(),
                request.clone(),
                close_rx,
            ));

            let manager = UdpManager {
                task: Arc::new(task),
                datagram_tx,
                inode: None,
                close: Arc::new(Mutex::new(close_tx)),
                last_seen,
            };
            registry.insert(address, manager);
        }
    }
}

async fn monitor_udp_addresses_sock_diag(registry: Registry) -> anyhow::Result<()> {
    loop {
        let table = {
            let rows = tokio::task::spawn_blocking(sock_diag::ip_diagnostics)
                .await
                .context("The IP diagnostics getter function panicked.")?
                .context("Could not get TCP/UDP diagnostics.")?;
            let result = HashMap::new();
            let result_pin = result.pin();

            for row in rows {
                if row.protocol != Protocol::Udp {
                    continue;
                }

                result_pin.insert(row.local, row);
            }

            drop(result_pin);
            result
        };
        let table = table.pin_owned();

        let registry = registry.pin_owned();
        for (address, manager) in registry.iter() {
            if let Some(state) = table.get(address) {
                if let Some(inode) = manager.inode {
                    if inode != state.inode {
                        let manager = registry.remove(address);
                        if let Some(manager) = manager {
                            let _ = manager.close.lock().await.send(()).await;
                        }

                        tracing::debug!(
                            "Inode for local UDP address changed from {} to {} meaning rebind, dropping stream",
                            inode,
                            state.inode
                        )
                    }
                } else {
                    registry.update(*address, |manager| UdpManager {
                        task: manager.task.clone(),
                        inode: Some(state.inode),
                        datagram_tx: manager.datagram_tx.clone(),
                        close: manager.close.clone(),
                        last_seen: manager.last_seen.clone(),
                    });
                    tracing::debug!("Inode for local UDP address {} is {}", address, state.inode);
                }
            } else {
                let manager = registry.remove(address);
                if let Some(manager) = manager {
                    let _ = manager.close.lock().await.send(()).await;
                }

                tracing::debug!("UDP local address {} was unbound, dropping stream", address);
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn monitor_udp_addresses_timeout(registry: Registry, timeout: u64) -> anyhow::Result<()> {
    loop {
        let registry = registry.pin_owned();

        for (address, manager) in registry.iter() {
            let last_seen = manager.last_seen.load(Ordering::Relaxed);

            if last_seen < utc_now_ms() - timeout {
                let manager = registry.remove(address);
                if let Some(manager) = manager {
                    let _ = manager.close.lock().await.send(()).await;
                }

                tracing::debug!(
                    "UDP local address {address} has been inactive for over {timeout}ms, dropping stream"
                );
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn handle_udp_connection(
    udp: Arc<UdpSocket>,
    datagram_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    client_address: SocketAddr,
    last_seen: Arc<AtomicU64>,
    mut request: Request,
    mut close: mpsc::Receiver<()>,
) -> anyhow::Result<()> {
    tracing::info!("Client address {client_address} connected via UDP.");

    request
        .headers_mut()
        .insert("network-type", "udp".try_into().unwrap());

    let (ws_stream, _response) = tokio_tungstenite::connect_async(request)
        .await
        .context("Could not connect to upstream server.")
        .err_error()?;

    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    let on_message = || {
        last_seen.swap(utc_now_ms(), Ordering::Relaxed);
    };

    let incoming_task = proxy::ws_to_udp(&mut ws_rx, udp.clone(), client_address, on_message);
    let outgoing_task = proxy::udp_to_ws(datagram_rx, &mut ws_tx, on_message);

    tokio::select! {
        result = outgoing_task => {
            tracing::debug!("UDP outgoing task ended for {client_address}: {result:?}");
        },
        result = incoming_task => {
            tracing::debug!("UDP incoming task ended for {client_address}: {result:?}");
        },
        result = close.recv() => {
            tracing::debug!("UDP got close notification for {client_address}: {result:?}");
        }
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
