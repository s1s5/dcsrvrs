# ------------- build ----------------
FROM clux/muslrust:1.67.1 as builder

RUN mkdir -p /rust
WORKDIR /rust

# ソースコードのコピー
COPY Cargo.toml Cargo.lock /rust/
COPY entity /rust/entity
COPY migration /rust/migration
COPY src /rust/src

# バイナリ名を変更すること
RUN --mount=type=cache,target=/rust/target \
  --mount=type=cache,target=/root/.cargo/registry \
  --mount=type=cache,target=/root/.cargo/git \
  cargo build --release && \
  cp /rust/target/x86_64-unknown-linux-musl/release/dcsrvrs /app

# ------------- runtime ----------------
FROM scratch

# バイナリのコピー
COPY --from=builder /app /app
ENTRYPOINT [ "/app" ]

