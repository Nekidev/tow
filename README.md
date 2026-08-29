# TOW - TCP (and UDP) over WebSockets

`tow` is a tiny tunnel/proxy that sends TCP and UDP connections over WebSocket to bypass deep
protocol inspection restrictions in networks with allowed HTTPS.

To install `tow`, use

```sh
cargo install tow
```

## Usage

On the server, start a proxy with `tow server`. It takes two arguments:

- `<FROM>` - The address to listen for incoming WebSocket connections at.
- `<TO>` - The address to forward the incoming WebSocket connections as TCP/UDP to.

For example,

```sh
tow server 0.0.0.0:80 127.0.0.1:51820
```

On the client, use `tow client` instead. It takes two arguments:

- `<FROM>` - The address to listen for incoming TCP/UDP connections at.
- `<TO>` - The address or URL to forward the incoming TCP/UDP connections as WebSockets to.

For example,

```sh
tow client 127.0.0.1:6767 wss://tow.example.com/
```

## About Connections

UDP does not have connections like TCP does, yet the websockets used to tunnel the data does. To
emulate connections, `tow` queries the system's `sock_diag` to check when a UDP address stops being
in use or its inode changes. When either of those does, the connection is closed.

Connections get a stable IP:PORT pair to the server, though they're not the original IP due to
tunnelling.

## License

This project is available under the MIT license.
