use rocket::fs::NamedFile;
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use dcsrvrs::server::build_server;
#[macro_use]
extern crate rocket;

// #[get("/<path..>")]
// async fn get_data(path: PathBuf) -> Option<NamedFile> {
//     NamedFile::open(Path::new("static/").join(path)).await.ok()
// }

// #[put("/<path..>")]
// fn put_data(path: PathBuf) {}

#[launch]
fn rocket() -> _ {
    build_server()
    // let lru_cache = Mutex::new(LruDiskCache::new("/", 88));
    // rocket::build()
    //     .mount("/", routes![get_data, put_data])
    //     .manage(lru_cache)
}
