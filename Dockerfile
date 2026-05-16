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
# Pkl CLI used by `rpkl` at startup to evaluate config.pkl. Static GraalVM
# native binary; checksums published with each apple/pkl release.
ARG PKL_VERSION=0.31.1
# Checksums for pkl-linux-amd64 / pkl-linux-aarch64 from the apple/pkl
# release notes. Verify and update for any new PKL_VERSION bump.
ARG PKL_SHA256_AMD64=618f13955d755cafbfe8c9cba1d27635848cd49dbc6abffd398d2751db1231bf
ARG PKL_SHA256_ARM64=7ef10e743daa921fb94ae7bdb9ec6986f362bf250c55814b9ea2aeb13f2d083e
COPY tools/fetch_lighter_signer.sh /app/tools/fetch_lighter_signer.sh
COPY config.pkl /app/config.pkl
RUN set -eu && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates curl libssl3 libpq5 libstdc++6 zlib1g && \
    bash /app/tools/fetch_lighter_signer.sh && \
    ARCH=$(dpkg --print-architecture) && \
    case "$ARCH" in \
      amd64) PKL_ARCH=linux-amd64; PKL_SHA=$PKL_SHA256_AMD64 ;; \
      arm64) PKL_ARCH=linux-aarch64; PKL_SHA=$PKL_SHA256_ARM64 ;; \
      *) echo "unsupported arch for pkl CLI: $ARCH" >&2; exit 1 ;; \
    esac && \
    curl -fsSL -o /usr/local/bin/pkl \
      "https://github.com/apple/pkl/releases/download/${PKL_VERSION}/pkl-${PKL_ARCH}" && \
    echo "${PKL_SHA}  /usr/local/bin/pkl" | sha256sum -c - && \
    chmod +x /usr/local/bin/pkl && \
    # Smoke-test pkl can actually load its runtime dependencies (libstdc++6,
    # zlib1g). A missing shared library would otherwise only surface the
    # first time the bot boots and tries to evaluate config.pkl.
    pkl --version && \
    # Evaluate the bundled config.pkl at build time so any schema /
    # validation error in the file (typos, missing fields, etc.) breaks the
    # image build instead of the first container start. Output goes to
    # /dev/null — we only care about the exit status.
    pkl eval /app/config.pkl > /dev/null && \
    apt-get purge -y --auto-remove curl && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/highlane /usr/local/bin
ENTRYPOINT ["/usr/local/bin/highlane"]
