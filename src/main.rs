use dcsrvrs::lru_disk_cache::{AddFile, LruDiskCache};
use path_clean::PathClean;
use rocket::{
    data::ToByteUnit, fs::NamedFile, http::Status, response::status::NotFound, Data, State,
};
use std::{fs, path::PathBuf, sync::Mutex};
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
    match data.open(128.kibibytes()).into_file(&abs_path).await {
        Ok(_file) => {}
        Err(_error) => return Status::InternalServerError,
    };
    {
        let mut lru_cache = lru_cache_state.lock().unwrap();
        let size = std::fs::metadata(&abs_path).unwrap().len();
        match lru_cache.add_file(AddFile::AbsPath(abs_path.into()), size) {
            Ok(_) => {}
            Err(_error) => return Status::InternalServerError,
        }
    }

    Status::Accepted
}

#[launch]
fn rocket() -> _ {
    // build_server()
    let lru_cache = Mutex::new(LruDiskCache::new("/tmp/dcsrvrs", 1 << 30).unwrap());
    rocket::build()
        .mount("/", routes![get_data, put_data])
        .manage(lru_cache)
}
