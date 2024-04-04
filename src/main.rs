use axum::{
    extract,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Extension, Router,
};
use clap::Parser;
use dcsrvrs::dbcache::{run_server, DBCacheClient};
use futures_util::TryStreamExt;
use path_clean::PathClean;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::{error, fmt};
use tokio::signal;
use tracing::{debug, info};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Config {
    #[arg(long, default_value = "/tmp/dcsrvrs-data")]
    cache_dir: String,

    #[arg(long, default_value_t = 1073741824)]
    capacity: u64,

    #[arg(long, default_value_t = 134217728)]
    file_size_limit: u64,

    #[arg(long, default_value_t = 32768)]
    blob_threshold: u64,
}

fn path2key(path: PathBuf) -> PathBuf {
    // TODO: 「../」とかで始まるパスの扱い
    path.clean()
}

async fn get_data(
    extract::Path(path): extract::Path<PathBuf>,
    client: Extension<Arc<DBCacheClient>>,
) -> impl IntoResponse {
    // Result<dyn Stream<Item = Result<bytes::Bytes>>, StatusCode> {
    let key: String = path2key(path).to_str().unwrap().into();
    match client.get(&key).await {
        Ok(f) => match f {
            Some(x) => Ok(x.into_response()),
            None => Err(StatusCode::NOT_FOUND),
        },
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn put_data(
    extract::Path(path): extract::Path<PathBuf>,
    headers: axum::http::HeaderMap,
    client: Extension<Arc<DBCacheClient>>,
    // // config: Extension<Config>,
    request: axum::extract::Request,
) -> StatusCode {
    let key: String = path2key(path).to_str().unwrap().into();
    if key.starts_with("-/") {
        return StatusCode::BAD_REQUEST;
    }

    let data = request
        .into_body()
        .into_data_stream()
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err));
    let mut data = tokio_util::io::StreamReader::new(data);

    match client
        .set(
            &key,
            // Pin::new(&mut data.open(config.file_size_limit.bytes())),
            Pin::new(&mut data),
            None,
            dcsrvrs::headers::Headers::from(headers).0,
        )
        .await
    {
        Ok(_) => StatusCode::OK,
        Err(e) => match e {
            dcsrvrs::dbcache::Error::FileSizeLimitExceeded => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        },
    }
}

async fn delete_data(
    extract::Path(path): extract::Path<PathBuf>,
    client: Extension<Arc<DBCacheClient>>,
) -> StatusCode {
    let key: String = path2key(path).to_str().unwrap().into();
    if key.starts_with("-/") {
        return StatusCode::BAD_REQUEST;
    }

    match client.del(&key).await {
        Ok(r) => {
            if r == 0 {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::OK
            }
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[derive(Serialize)]
struct ServerStatus {
    entries: usize,
    size: usize,
    capacity: usize,
}

async fn healthcheck(
    client: Extension<Arc<DBCacheClient>>,
) -> Result<axum::Json<ServerStatus>, StatusCode> {
    match client.stat().await {
        Ok(s) => Ok(axum::Json(ServerStatus {
            entries: s.entries,
            size: s.size,
            capacity: s.capacity,
        })),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn flushall(client: Extension<Arc<DBCacheClient>>) -> StatusCode {
    match client.flushall().await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[derive(Deserialize)]
struct GetKeyArg {
    max_num: i64,
    key: Option<String>,
    store_time: Option<i64>,
    prefix: Option<String>,
}

#[derive(Serialize)]
struct GetKeyResponse {
    key: String,
    store_time: i64,
}

async fn keys(
    client: Extension<Arc<DBCacheClient>>,
    extract::Json(arg): extract::Json<GetKeyArg>,
) -> Result<axum::Json<Vec<GetKeyResponse>>, StatusCode> {
    match client
        .keys(
            arg.max_num,
            arg.key.clone(),
            arg.store_time,
            arg.prefix.clone(),
        )
        .await
    {
        Ok(d) => Ok(axum::Json(
            d.into_iter()
                .map(|e| GetKeyResponse {
                    key: e.0,
                    store_time: e.1,
                })
                .collect(),
        )),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[derive(Debug, Clone)]
pub struct InitializationFailedError;

impl fmt::Display for InitializationFailedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InitializationFailedError")
    }
}
impl error::Error for InitializationFailedError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::Layer::new()
                    .with_ansi(true)
                    .with_file(true)
                    .with_line_number(true)
                    .with_level(true), //.json(),
            )
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .try_init()?;
    }

    let config = Config::parse();

    debug!("config: {:?}", config);

    let (client, disposer) = run_server(
        &PathBuf::from(&config.cache_dir),
        config.blob_threshold.try_into().unwrap(),
        config.file_size_limit.try_into().unwrap(),
        config.capacity.try_into().unwrap(),
    )
    .await
    .expect("failed to run cache server");

    let cors = tower_http::cors::CorsLayer::new()
        .allow_credentials(false)
        .allow_headers(tower_http::cors::Any)
        .allow_origin(tower_http::cors::AllowOrigin::mirror_request());

    let router = Router::new()
        .route("/*path", get(get_data).put(put_data).delete(delete_data))
        .route("/-/healthcheck/", get(healthcheck))
        .route("/-/flushall/", post(flushall))
        .route("/-/keys/", post(keys))
        .layer(Extension(config))
        .layer(Extension(client))
        .layer(cors);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await?;
    tracing::info!("listening on {}", listener.local_addr()?);
    let r = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await;

    disposer.dispose().await?;
    // panic after dispose
    r?;

    info!("server shutdown");

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
