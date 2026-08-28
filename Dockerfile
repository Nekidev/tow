FROM rust:alpine

WORKDIR /app
COPY . /app/

RUN cargo build --release

ENTRYPOINT [ "/app/target/release/tow" ]
CMD [ "server" ]
