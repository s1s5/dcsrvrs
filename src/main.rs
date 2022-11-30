#[macro_use]
extern crate rocket;
use dcsrvrs::lru_disk_cache::{lru_cache, AddFile, LruDiskCache};
use envy;
use path_clean::PathClean;
use rocket::serde::{json::Json, Serialize};
use rocket::{
    data::ToByteUnit, fs::NamedFile, http::Status, response::status::NotFound, Data, State,
};
use serde::Deserialize;
use std::{fs, path::PathBuf, sync::Mutex};

#[derive(Deserialize, Debug)]
struct Config {
    #[serde(default = "default_cache_dir")]
    cache_dir: String,

    #[serde(default = "default_size_limit")]
    size_limit: u64,

    #[serde(default = "default_file_size_limit")]
    file_size_limit: u64,
}

fn default_cache_dir() -> String {
    "/tmp/dcsrvrs".into()
}

fn default_size_limit() -> u64 {
    1073741824
}

fn default_file_size_limit() -> u64 {
    134217728
}

fn path2key(path: PathBuf) -> PathBuf {
    // TODO: 「../」とかで始まるパスの扱い
    path.clean()
}

#[get("/<path..>")]
async fn get_data(
    path: PathBuf,
    lru_cache_state: &State<Mutex<LruDiskCache>>,
) -> Result<NamedFile, NotFound<()>> {
    let key = path2key(path);
    let f = {
        let mut lru_cache = lru_cache_state.lock().unwrap();
        lru_cache.get_filename(key)
    };
    match f {
        Ok(f) => match NamedFile::open(f).await.ok() {
            Some(f) => Ok(f),
            None => Err(NotFound(())),
        },
        Err(_) => Err(NotFound(())),
    }
}

#[put("/<path..>", data = "<data>")]
async fn put_data(
    data: Data<'_>,
    path: PathBuf,
    lru_cache_state: &State<Mutex<LruDiskCache>>,
    config: &State<Config>,
) -> Status {
    let key = path2key(path);
    let abs_path = {
        let lru_cache = lru_cache_state.lock().unwrap();
        let pb = lru_cache.rel_to_abs_path(key);
        pb
    };

    match fs::create_dir_all(abs_path.parent().expect("Bad path?")) {
        Ok(_) => {}
        Err(_error) => return Status::InternalServerError,
    };
    match data
        .open(config.file_size_limit.bytes())
        .into_file(&abs_path)
        .await
    {
        Ok(_file) => {}
        Err(_error) => {
            return Status::InternalServerError;
        }
    };
    {
        let mut lru_cache = lru_cache_state.lock().unwrap();
        let size = std::fs::metadata(&abs_path).unwrap().len();
        match lru_cache.add_file(AddFile::AbsPath(abs_path.into()), size) {
            Ok(_) => {}
            Err(_error) => return Status::InternalServerError,
        }
    }

    Status::Ok
}

#[delete("/<path..>")]
async fn delete_data(path: PathBuf, lru_cache_state: &State<Mutex<LruDiskCache>>) -> Status {
    let key = path2key(path);
    match {
        let mut lru_cache = lru_cache_state.lock().unwrap();
        lru_cache.remove(key)
    } {
        Ok(r) => match r {
            Some(_) => Status::Ok,
            None => Status::NotFound,
        },
        Err(_error) => Status::InternalServerError,
    }
}

#[derive(Serialize)]
#[serde(crate = "rocket::serde")]
struct ServerStatus {
    size: u64,
    len: usize,
    capacity: u64,
}

#[get("/-/healthcheck")]
fn healthcheck(lru_cache_state: &State<Mutex<LruDiskCache>>) -> Json<ServerStatus> {
    let lru_cache = lru_cache_state.lock().unwrap();
    Json(ServerStatus {
        size: lru_cache.size(),
        len: lru_cache.len(),
        capacity: lru_cache.capacity(),
    })
}

#[launch]
fn rocket() -> _ {
    let config = envy::from_env::<Config>().unwrap();
    let lru_cache =
        Mutex::new(LruDiskCache::new(config.cache_dir.clone(), config.size_limit).unwrap());
    rocket::build()
        .mount("/", routes![get_data, put_data, delete_data, healthcheck])
        .manage(lru_cache)
        .manage(config)
}
