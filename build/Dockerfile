FROM rust:1.91 as builder

WORKDIR /usr/src/app
COPY /src /usr/src/app/src
COPY /config /usr/src/app/config
COPY /Cargo.toml /usr/src/app/Cargo.toml
COPY /benches /usr/src/app/benches

RUN cargo install --path .

RUN cargo build --release
