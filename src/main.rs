use dcsrvrs::lru_disk_cache::{lru_cache, AddFile, LruDiskCache, ReadSeek};
use path_clean::{clean, PathClean};
use rocket::response::stream::ReaderStream;
use rocket::State;
use rocket::{data::ToByteUnit, fs::NamedFile, Data};
use std::fs::{self, File};
use std::io;
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};
use uuid::Uuid;
#[macro_use]
extern crate rocket;

fn path2key(path: PathBuf) -> PathBuf {
    // TODO: 「../」とかで始まるパスの扱い
    path.clean()
}

#[get("/<path..>")]
async fn get_data(
    path: PathBuf,
    lru_cache_state: &State<Mutex<LruDiskCache>>,
) -> Option<NamedFile> {
    let key = path2key(path);
    let f = {
        let mut lru_cache = lru_cache_state.lock().unwrap();
        lru_cache.get_filename(key)
    };
    match f {
        Ok(f) => NamedFile::open(f).await.ok(),
        Err(error) => None,
    }
    // NamedFile::open(Path::new("static/").join(path)).await.ok()
}

#[put("/<path..>", data = "<data>")]
async fn put_data(
    data: Data<'_>,
    path: PathBuf,
    lru_cache_state: &State<Mutex<LruDiskCache>>,
) -> &'static str {
    let key = path2key(path);
    let abs_path = {
        let lru_cache = lru_cache_state.lock().unwrap();
        let pb = lru_cache.rel_to_abs_path(key);
        pb
    };

    fs::create_dir_all(abs_path.parent().expect("Bad path?"));
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
    let lru_cache = Mutex::new(LruDiskCache::new("/tmp/dcsrvrs", 1 << 30).unwrap());
    rocket::build()
        .mount("/", routes![get_data, put_data])
        .manage(lru_cache)
}
