use dcsrvrs::lru_disk_cache::{lru_cache, AddFile, LruDiskCache, ReadSeek};
use rocket::response::stream::ReaderStream;
use rocket::State;
use rocket::{data::ToByteUnit, fs::NamedFile, Data};
use std::io;

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};
use uuid::Uuid;

#[macro_use]
extern crate rocket;

#[get("/<path..>")]
async fn get_data(
    path: PathBuf,
    lru_cache_state: &State<Mutex<LruDiskCache>>,
) -> io::Result<ReaderStream<ReadSeek>> {
    let lru_cache = lru_cache_state.lock().unwrap();
    match lru_cache.get(path) {
        Ok(f) => Ok(ReaderStream::one(f)),
        Err(error) => Err(io::Error::from("")),
    }
    // NamedFile::open(Path::new("static/").join(path)).await.ok()
}

#[put("/<path..>", data = "<data>")]
async fn put_data(
    data: Data<'_>,
    path: PathBuf,
    lru_cache_state: &State<Mutex<LruDiskCache>>,
) -> &'static str {
    let abs_path = {
        let lru_cache = lru_cache_state.lock().unwrap();
        let pb = lru_cache.rel_to_abs_path(path);
        pb
    };

    data.open(128.kibibytes()).into_file(&abs_path).await;
    {
        let mut lru_cache = lru_cache_state.lock().unwrap();
        let size = std::fs::metadata(&abs_path).unwrap().len();
        lru_cache.add_file(AddFile::AbsPath(abs_path.into()), size);
    }

    "ok"
}

#[launch]
fn rocket() -> _ {
    // build_server()
    let lru_cache = Mutex::new(LruDiskCache::new("/tmp/dcsrvrs", 88).unwrap());
    rocket::build()
        .mount("/", routes![get_data, put_data])
        .manage(lru_cache)
}
