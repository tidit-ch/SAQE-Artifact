FROM rust:1.91 as chef

RUN apt-get update && \
    apt-get install -y mold clang && \
    rm -rf /var/lib/apt/lists/*

# cargo-chef: speeds up builds by caching dependencies
# cargo-watch: watches for file changes and rebuilds automatically
RUN cargo install cargo-chef cargo-watch

WORKDIR /usr/src/app

RUN mkdir .cargo
COPY .cargo/config.toml /usr/src/app/.cargo/config.toml

FROM chef as planner
COPY Cargo.toml /usr/src/app/

# Create a dummy src/main.rs to allow cargo-chef to analyze dependencies
# We will mount the actual source code in docker-compose
RUN mkdir src && echo "fn main() {}" > src/main.rs

RUN cargo chef prepare --recipe-path recipe.json

FROM chef as builder
COPY --from=planner /usr/src/app/recipe.json recipe.json

COPY benches /usr/src/app/benches/

RUN cargo chef cook --recipe-path recipe.json

ENV CARGO_INCREMENTAL=1
