FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --bin highlane

FROM debian:trixie-slim AS runtime
WORKDIR /app
COPY tools/fetch_lighter_signer.sh /app/tools/fetch_lighter_signer.sh
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl libssl3 libpq5 && \
    bash /app/tools/fetch_lighter_signer.sh && \
    apt-get purge -y --auto-remove curl && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/highlane /usr/local/bin
ENTRYPOINT ["/usr/local/bin/highlane"]
