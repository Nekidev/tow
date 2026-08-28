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

## License

This project is available under the MIT license.
