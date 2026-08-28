FROM rust:alpine

WORKDIR /app
COPY Cargo.toml /app
COPY Cargo.lock /app

RUN mkdir /app/src
RUN echo "fn main() {}" > src/main.rs

RUN cargo build --release

COPY src /app/src
RUN cargo build --release

ENTRYPOINT [ "/bin/sh" ]
CMD [ "server" ]
