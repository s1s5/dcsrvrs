# ------------- build ----------------
FROM clux/muslrust:1.84.0-stable-2025-01-16 AS builder

RUN mkdir -p /rust
WORKDIR /rust

# ソースコードのコピー
COPY Cargo.toml Cargo.lock /rust/
COPY entity /rust/entity
COPY migration /rust/migration
COPY bench /rust/bench
COPY src /rust/src

# バイナリ名を変更すること
RUN --mount=type=cache,target=/rust/target \
  --mount=type=cache,target=/root/.cargo/registry \
  --mount=type=cache,target=/root/.cargo/git \
  cargo build --release && \
  cp /rust/target/x86_64-unknown-linux-musl/release/dcsrvrs /app

# ------------- runtime ----------------
FROM scratch

ENV RUST_LOG=sqlx=error,sea_orm_migration=error,info

# バイナリのコピー
COPY --from=builder /app /app
ENTRYPOINT [ "/app" ]

