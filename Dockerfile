FROM rust:1.92-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY build.rs ./
COPY proto/ proto/
COPY .cargo/ .cargo/
RUN mkdir -p benches && touch benches/storage_bench.rs
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler lld \
    && rm -rf /var/lib/apt/lists/*

COPY .cargo/ .cargo/
COPY --from=planner /app/recipe.json recipe.json
COPY build.rs ./
COPY proto/ proto/
RUN mkdir -p benches && touch benches/storage_bench.rs
RUN cargo chef cook --release --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto/ proto/
COPY src/ src/
RUN mkdir -p benches && touch benches/storage_bench.rs
RUN cargo build --release --bin simple3

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r simple3 && useradd -r -g simple3 -m simple3

COPY --from=builder /app/target/release/simple3 /usr/local/bin/simple3

RUN mkdir -p /data && chown simple3:simple3 /data
VOLUME /data

USER simple3
WORKDIR /data

EXPOSE 8080 50051

ENTRYPOINT ["simple3"]
CMD ["serve", "--data-dir", "/data", "--host", "0.0.0.0"]
