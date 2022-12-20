#[macro_use]
extern crate rocket;
use dcsrvrs::dbcache::{run_server, DBCacheClient};
use dcsrvrs::lru_disk_cache::{AddFile, LruDiskCache};
use envy;
use path_clean::PathClean;
use rocket::response::stream::ReaderStream;
use rocket::serde::{json::Json, Serialize};
use rocket::{
    data::ToByteUnit, fs::NamedFile, http::Status, response::status::NotFound, Data, State,
};
use serde::Deserialize;
use std::pin::Pin;
use std::{error, fmt};
use std::{fs, path::PathBuf, sync::Mutex};
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
    "/tmp/dcsrvrs".into()
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
    // let f = {
    //     let mut lru_cache = lru_cache_state.lock().unwrap();
    //     lru_cache.get_filename(key)
    // };
    // match f {
    //     Ok(f) => match NamedFile::open(f).await.ok() {
    //         Some(f) => Ok(f),
    //         None => Err(NotFound(())),
    //     },
    //     Err(_) => Err(NotFound(())),
    // }
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

    // let abs_path = {
    //     let lru_cache = lru_cache_state.lock().unwrap();
    //     let pb = lru_cache.rel_to_abs_path(key);
    //     pb
    // };

    // match fs::create_dir_all(abs_path.parent().expect("Bad path?")) {
    //     Ok(_) => {}
    //     Err(_error) => return Status::InternalServerError,
    // };
    // match data
    //     .open(config.file_size_limit.bytes())
    //     .into_file(&abs_path)
    //     .await
    // {
    //     Ok(_file) => {}
    //     Err(_error) => {
    //         return Status::InternalServerError;
    //     }
    // };
    // {
    //     let mut lru_cache = lru_cache_state.lock().unwrap();
    //     let size = std::fs::metadata(&abs_path).unwrap().len();
    //     match lru_cache.add_file(AddFile::AbsPath(abs_path.into()), size) {
    //         Ok(_) => {}
    //         Err(_error) => return Status::InternalServerError,
    //     }
    // }

    // Status::Ok
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
    // match {
    //     let mut lru_cache = lru_cache_state.lock().unwrap();
    //     lru_cache.remove(key)
    // } {
    //     Ok(r) => match r {
    //         Some(_) => Status::Ok,
    //         None => Status::NotFound,
    //     },
    //     Err(_error) => Status::InternalServerError,
    // }
}

#[derive(Serialize)]
#[serde(crate = "rocket::serde")]
struct ServerStatus {
    size: u64,
    len: usize,
    capacity: u64,
}

#[get("/-/healthcheck")]
fn healthcheck() -> Json<ServerStatus> {
    // let lru_cache = lru_cache_state.lock().unwrap();
    // Json(ServerStatus {
    //     size: lru_cache.size(),
    //     len: lru_cache.len(),
    //     capacity: lru_cache.capacity(),
    // })
    Json(ServerStatus {
        size: 0,
        len: 0,
        capacity: 0,
    })
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

struct A {}

impl Drop for A {
    fn drop(&mut self) {
        println!("drop A");
    }
}
// #[launch]
// async fn rocket() -> _ {}

#[rocket::main]
async fn main() {
    env_logger::init();
    // let connection = sea_orm::Database::connect(&database_url).await?;
    // Migrator::up(&connection, None).await?;
    let config = envy::from_env::<Config>().unwrap();
    debug!("config: {:?}", config);
    // let lru_cache =
    //     Mutex::new(LruDiskCache::new(config.cache_dir.clone(), config.size_limit).unwrap());

    let a = A {};
    let (client, disposer) = run_server(
        &PathBuf::from("/tmp/dcsrvrs-data"),
        config.blob_threshold.try_into().unwrap(),
        config.file_size_limit.try_into().unwrap(),
        config.capacity.try_into().unwrap(),
    )
    .await
    .unwrap();

    let result = rocket::build()
        .mount("/", routes![get_data, put_data, delete_data, healthcheck])
        // .manage(lru_cache)
        .manage(config)
        // .manage(a)
        .manage(client)
        .launch()
        .await;

    disposer.dispose().await.unwrap();

    // If the server shut down (by visiting `/shutdown`), `result` is `Ok`.
    result.expect("server failed unexpectedly");
}
