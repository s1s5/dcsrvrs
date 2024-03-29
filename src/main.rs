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
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::{error, fmt};
use tokio::signal::ctrl_c;
use tokio::signal::unix::{signal, SignalKind};
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
    client: Extension<DBCacheClient>,
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
    client: Extension<DBCacheClient>,
    // // config: Extension<Config>,
    data: extract::BodyStream,
) -> StatusCode {
    let key: String = path2key(path).to_str().unwrap().into();
    let data = data.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err));
    let mut data = tokio_util::io::StreamReader::new(data);

    match client
        .set(
            &key,
            // Pin::new(&mut data.open(config.file_size_limit.bytes())),
            Pin::new(&mut data),
            None,
            dcsrvrs::headers::Headers::from(headers),
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
    client: Extension<DBCacheClient>,
) -> StatusCode {
    let key: String = path2key(path).to_str().unwrap().into();
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
    client: Extension<DBCacheClient>,
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

async fn flushall(client: Extension<DBCacheClient>) -> StatusCode {
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
    client: Extension<DBCacheClient>,
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
        write!(f, "invalid first item to double")
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
    .unwrap();

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

    let addr = SocketAddr::from(([0, 0, 0, 0], 8000));
    let server = axum::Server::bind(&addr).serve(router.into_make_service());

    info!("server listening {:?}", addr);

    server
        .with_graceful_shutdown(async {
            let mut sig_int = signal(SignalKind::interrupt()).unwrap();
            let mut sig_term = signal(SignalKind::terminate()).unwrap();
            tokio::select! {
                _ = sig_int.recv() => debug!("receive SIGINT"),
                _ = sig_term.recv() => debug!("receive SIGTERM"),
                _ = ctrl_c() => debug!("receive Ctrl C"),
            }
            // rx.await.ok();
            debug!("gracefully shutting down");
        })
        .await?;

    disposer.dispose().await.unwrap();
    info!("server shutdown");

    Ok(())
}
