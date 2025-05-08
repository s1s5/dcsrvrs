use std::collections::HashMap;
use std::fmt::Write;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::{error, fmt};

use axum::{
    extract,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Extension, Router,
};
use clap::Parser;
use dcsrvrs::dbcache::DBCacheServerBuilder;
use dcsrvrs::util::normalize_path;
use futures_util::TryStreamExt;
use log::error;
use path_clean::PathClean;
use serde::{Deserialize, Serialize};
use tokio::signal;
use tracing::{debug, info};

use dcsrvrs::{
    dbcache::{DBCacheClient, KeyTaskResult},
    imcache::InmemoryCache,
};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Config {
    #[arg(long, default_value = "/tmp/dcsrvrs-data")]
    cache_dir: String,

    #[arg(long, default_value_t = 1usize << 30)]
    capacity: usize,

    #[arg(long, default_value_t = 128 << 20 )]
    file_size_limit: usize,

    #[arg(long, default_value_t = 1 << 15)]
    blob_threshold: usize,

    #[arg(long, default_value_t = false)]
    disable_inmemory_cache: bool,

    #[arg(long, default_value_t = 64)]
    inmemory_num_chunks: usize,

    #[arg(long, default_value_t = 1 << 20)]
    inmemory_bytes_per_chunk: usize,

    #[arg(long, default_value_t = 1209600)] // 14days
    max_age: i64,

    #[arg(long, default_value_t = 1 << 15)]
    inmemory_max_num_of_entries: usize,

    #[arg(long, default_value_t = 1000)]
    auto_reconnect_threshold: usize,

    #[arg(long, action)]
    disable_auto_evict: bool,
}

fn path2key(path: PathBuf) -> PathBuf {
    normalize_path(&path).clean()
}

async fn get_data(
    extract::Path(path): extract::Path<PathBuf>,
    client: Extension<Arc<DBCacheClient>>,
) -> impl IntoResponse {
    // Result<dyn Stream<Item = Result<bytes::Bytes>>, StatusCode> {
    let key: String = path2key(path.clone()).to_str().unwrap().into();
    match client.get(&key).await {
        Ok(f) => match f {
            Some(x) => Ok(x.into_response()),
            None => Err(StatusCode::NOT_FOUND),
        },
        Err(err) => {
            error!("get path={path:?}, error={err:?}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn put_data(
    extract::Path(path): extract::Path<PathBuf>,
    headers: axum::http::HeaderMap,
    client: Extension<Arc<DBCacheClient>>,
    // // config: Extension<Config>,
    request: axum::extract::Request,
) -> StatusCode {
    let key: String = path2key(path.clone()).to_str().unwrap().into();
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
        Err(err) => match err {
            dcsrvrs::dbcache::Error::FileSizeLimitExceeded => StatusCode::BAD_REQUEST,
            _ => {
                error!("put path={path:?}, error={err:?}");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        },
    }
}

async fn delete_data(
    extract::Path(path): extract::Path<PathBuf>,
    client: Extension<Arc<DBCacheClient>>,
) -> StatusCode {
    let key: String = path2key(path.clone()).to_str().unwrap().into();
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
        Err(err) => {
            error!("delete path={path:?}, error={err:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
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
        Err(err) => {
            error!("healthcheck error={err:?}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Serialize)]
struct EvictResponse {
    entries: usize,
    num_bytes: usize,
}

#[derive(Serialize)]
struct EvictResultAndServerStatus {
    stat: ServerStatus,
    evict: EvictResponse,
}

async fn evict_and_get_stat_(
    client: &DBCacheClient,
) -> Result<EvictResultAndServerStatus, dcsrvrs::dbcache::errors::Error> {
    let e = client.evict().await?;
    let s = client.stat().await?;
    Ok(EvictResultAndServerStatus {
        stat: ServerStatus {
            entries: s.entries,
            size: s.size,
            capacity: s.capacity,
        },
        evict: EvictResponse {
            entries: e.0,
            num_bytes: e.1,
        },
    })
}

async fn evict_and_get_stat(
    client: Extension<Arc<DBCacheClient>>,
) -> Result<axum::Json<EvictResultAndServerStatus>, StatusCode> {
    match evict_and_get_stat_(&client).await {
        Ok(s) => Ok(axum::Json(s)),
        Err(err) => {
            error!("healthcheck error={err:?}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn flushall(client: Extension<Arc<DBCacheClient>>) -> StatusCode {
    match client.flushall().await {
        Ok(r) => {
            info!("flushall {r:?}");
            StatusCode::OK
        }
        Err(err) => {
            error!("flushall failed. error={err:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn reset_connection(client: Extension<Arc<DBCacheClient>>) -> StatusCode {
    match client.reset_connection().await {
        Ok(_) => StatusCode::OK,
        Err(err) => {
            error!("reset_connection failed. error={err:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn vacuum(client: Extension<Arc<DBCacheClient>>) -> StatusCode {
    match client.vacuum().await {
        Ok(_) => StatusCode::OK,
        Err(err) => {
            error!("vacuum failed. error={err:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

#[derive(Deserialize)]
struct EvictExpiredParam {
    page_size: Option<u64>,
    max_iter: Option<usize>,
}

async fn evict_expired(
    client: Extension<Arc<DBCacheClient>>,
    extract::Json(payload): extract::Json<EvictExpiredParam>,
) -> Result<axum::Json<EvictResponse>, StatusCode> {
    match client
        .evict_expired(
            payload.page_size.unwrap_or(100),
            payload.max_iter.unwrap_or(1),
        )
        .await
    {
        Ok((entries, num_bytes)) => Ok(axum::Json(EvictResponse { entries, num_bytes })),
        Err(err) => {
            error!("evict failed. error={err:?}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Deserialize)]
struct EvictOldParam {
    goal_size: Option<usize>,
    page_size: Option<u64>,
    max_iter: Option<usize>,
}

async fn evict_old(
    client: Extension<Arc<DBCacheClient>>,
    extract::Json(payload): extract::Json<EvictOldParam>,
) -> Result<axum::Json<EvictResponse>, StatusCode> {
    match client
        .evict_old(
            payload.goal_size.unwrap_or(0),
            payload.page_size.unwrap_or(100),
            payload.max_iter.unwrap_or(1),
        )
        .await
    {
        Ok((entries, num_bytes)) => Ok(axum::Json(EvictResponse { entries, num_bytes })),
        Err(err) => {
            error!("evict failed. error={err:?}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Deserialize)]
struct EvictAgedParam {
    store_time_lt: i64,
    page_size: Option<u64>,
    max_iter: Option<usize>,
}

async fn evict_aged(
    client: Extension<Arc<DBCacheClient>>,
    extract::Json(payload): extract::Json<EvictAgedParam>,
) -> Result<axum::Json<EvictResponse>, StatusCode> {
    match client
        .evict_aged(
            payload.store_time_lt,
            payload.page_size.unwrap_or(100),
            payload.max_iter.unwrap_or(1),
        )
        .await
    {
        Ok((entries, num_bytes)) => Ok(axum::Json(EvictResponse { entries, num_bytes })),
        Err(err) => {
            error!("evict failed. error={err:?}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn evict(
    client: Extension<Arc<DBCacheClient>>,
) -> Result<axum::Json<EvictResponse>, StatusCode> {
    match client.evict().await {
        Ok((entries, num_bytes)) => Ok(axum::Json(EvictResponse { entries, num_bytes })),
        Err(err) => {
            error!("evict failed. error={err:?}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn reset_stat(
    client: Extension<Arc<DBCacheClient>>,
) -> Result<axum::Json<ServerStatus>, StatusCode> {
    match client.reset_stat().await {
        Ok(s) => Ok(axum::Json(ServerStatus {
            entries: s.entries,
            size: s.size,
            capacity: s.capacity,
        })),
        Err(err) => {
            error!("evict failed. error={err:?}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Deserialize)]
struct GetKeyArg {
    max_num: i64,
    key: Option<String>,
    store_time: Option<i64>,
    prefix: Option<String>,
    key_contains: Option<String>,
}

#[derive(Serialize)]
struct GetKeyResponse {
    key: String,
    store_time: i64,
    expire_time: Option<i64>,
    access_time: i64,
    size: i64,
    sha256sum: String,
    headers: HashMap<String, String>,
}

impl From<KeyTaskResult> for GetKeyResponse {
    fn from(value: KeyTaskResult) -> Self {
        Self {
            key: value.key,
            store_time: value.store_time,
            expire_time: value.expire_time,
            access_time: value.access_time,
            size: value.size,
            sha256sum: value
                .sha256sum
                .into_iter()
                .fold(String::new(), |mut output, b| {
                    let _ = write!(output, "{b:02x}");
                    output
                }),
            headers: value.headers,
        }
    }
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
            arg.key_contains.clone(),
        )
        .await
    {
        Ok(d) => Ok(axum::Json(d.into_iter().map(|e| e.into()).collect())),
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
    let inmemory_cache = if config.disable_inmemory_cache {
        None
    } else {
        Some(Arc::new(InmemoryCache::new(
            config.inmemory_num_chunks,
            config.inmemory_bytes_per_chunk,
            config.inmemory_max_num_of_entries,
        )))
    };
    let (client, disposer) = DBCacheServerBuilder::new(&PathBuf::from(&config.cache_dir))
        .blob_threshold(config.blob_threshold)
        .size_limit(config.file_size_limit)
        .capacity(config.capacity)
        .inmemory_cache(inmemory_cache)
        .auto_evict(!config.disable_auto_evict)
        .auto_reconnect_threshold(config.auto_reconnect_threshold)
        .max_age(config.max_age)
        .build()
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
        .route("/-/resetconnection/", post(reset_connection))
        .route("/-/vacuum/", post(vacuum))
        .route("/-/healthcheckwithevict/", get(evict_and_get_stat))
        .route("/-/evictexpired/", post(evict_expired))
        .route("/-/evictold/", post(evict_old))
        .route("/-/evictaged/", post(evict_aged))
        .route("/-/evict/", post(evict))
        .route("/-/resetstat/", post(reset_stat))
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
