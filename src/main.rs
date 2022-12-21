#[macro_use]
extern crate rocket;
use dcsrvrs::dbcache::{run_server, DBCacheClient};
use envy;
use path_clean::PathClean;
use rocket::response::stream::ReaderStream;
use rocket::serde::{json::Json, Serialize};
use rocket::{data::ToByteUnit, http::Status, Data, State};
use serde::Deserialize;
use std::path::PathBuf;
use std::pin::Pin;
use std::{error, fmt};
use tokio::io::AsyncRead;

#[derive(Deserialize, Debug)]
struct Config {
    #[serde(default = "default_cache_dir")]
    cache_dir: String,

    #[serde(default = "default_capacity")]
    capacity: u64,

    #[serde(default = "default_file_size_limit")]
    file_size_limit: u64,

    #[serde(default = "default_blob_threshold")]
    blob_threshold: u64,
}

fn default_cache_dir() -> String {
    "/tmp/dcsrvrs-data".into()
}

fn default_capacity() -> u64 {
    1073741824
}

fn default_file_size_limit() -> u64 {
    134217728
}

fn default_blob_threshold() -> u64 {
    32768
}

fn path2key(path: PathBuf) -> PathBuf {
    // TODO: 「../」とかで始まるパスの扱い
    path.clean()
}

#[get("/<path..>")]
async fn get_data(
    path: PathBuf,
    client: &State<DBCacheClient>,
) -> Result<ReaderStream![Pin<Box<dyn AsyncRead + Send>>], Status> {
    let key: String = path2key(path).to_str().unwrap().into();
    match client.get(&key).await {
        Ok(f) => match f {
            Some(x) => Ok(ReaderStream::one(x)),
            None => Err(Status::NotFound),
        },
        Err(_) => Err(Status::InternalServerError),
    }
}

#[put("/<path..>", data = "<data>")]
async fn put_data(
    data: Data<'_>,
    path: PathBuf,
    client: &State<DBCacheClient>,
    config: &State<Config>,
) -> Status {
    let key: String = path2key(path).to_str().unwrap().into();

    match client
        .set(
            &key,
            Pin::new(&mut data.open(config.file_size_limit.bytes())),
            None,
        )
        .await
    {
        Ok(_) => Status::Ok,
        Err(e) => match e {
            dcsrvrs::dbcache::Error::FileSizeLimitExceeded => Status::BadRequest,
            _ => Status::InternalServerError,
        },
    }
}

#[delete("/<path..>")]
async fn delete_data(path: PathBuf, client: &State<DBCacheClient>) -> Status {
    let key: String = path2key(path).to_str().unwrap().into();
    match client.del(&key).await {
        Ok(r) => {
            if r == 0 {
                Status::NotFound
            } else {
                Status::Ok
            }
        }
        Err(_) => Status::InternalServerError,
    }
}

#[derive(Serialize)]
#[serde(crate = "rocket::serde")]
struct ServerStatus {
    entries: usize,
    size: usize,
    capacity: usize,
}

#[get("/-/healthcheck")]
async fn healthcheck(client: &State<DBCacheClient>) -> Result<Json<ServerStatus>, Status> {
    match client.stat().await {
        Ok(s) => Ok(Json(ServerStatus {
            entries: s.entries,
            size: s.size,
            capacity: s.capacity,
        })),
        Err(_) => Err(Status::InternalServerError),
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

#[rocket::main]
async fn main() {
    env_logger::init();
    let config = envy::from_env::<Config>().unwrap();
    debug!("config: {:?}", config);

    let (client, disposer) = run_server(
        &PathBuf::from(&config.cache_dir),
        config.blob_threshold.try_into().unwrap(),
        config.file_size_limit.try_into().unwrap(),
        config.capacity.try_into().unwrap(),
    )
    .await
    .unwrap();

    let result = rocket::build()
        .mount("/", routes![get_data, put_data, delete_data, healthcheck])
        .manage(config)
        .manage(client)
        .launch()
        .await;

    disposer.dispose().await.unwrap();

    match result {
        Ok(_) => {}
        Err(e) => error!("server failed unexpectedly: {:?}", e),
    }
}
