use crate::lru_disk_cache::LruDiskCache;
use rocket::{fs::NamedFile, Build, Rocket};
use rocket::{get, put, routes};
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

#[get("/<path..>")]
async fn get_data(path: PathBuf) -> Option<NamedFile> {
    NamedFile::open(Path::new("static/").join(path)).await.ok()
}

#[put("/<path..>")]
fn put_data(path: PathBuf) {}

pub fn build_server() -> Rocket<Build> {
    let lru_cache = Mutex::new(LruDiskCache::new("/", 88));
    rocket::build()
        .mount("/", routes![get_data, put_data])
        .manage(lru_cache)
}
