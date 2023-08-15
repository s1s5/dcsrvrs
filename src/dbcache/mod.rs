mod client;
mod errors;
pub mod ioutil;
mod server;
mod task;

use log::{debug, error};
use std::fs;
use std::path::Path;
use tokio::sync::{mpsc, oneshot};

pub use self::client::DBCacheClient;
pub use self::errors::*;
use self::server::DBCache;
use self::task::*;

pub struct DBCacheDisposer {
    tx: mpsc::Sender<Task>,
}

impl DBCacheDisposer {
    pub async fn dispose(self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::End(EndTask { tx: tx }))
            .await
            .or_else(|_e| Err(Error::SendError))?;
        rx.await.or_else(|e| Err(Error::RecvError(e)))
    }
}

pub async fn run_server(
    cache_dir: &Path,
    blob_threshold: usize,
    size_limit: usize,
    capacity: usize,
) -> Result<(DBCacheClient, DBCacheDisposer), Box<dyn std::error::Error>> {
    let data_root = cache_dir.join("data");
    fs::create_dir_all(&data_root)?;

    let (tx, mut rx) = mpsc::channel(32);
    // let (stx, srx) = oneshot::channel();
    let mut dbcache = DBCache::new(&cache_dir.join("db.sqlite"), &data_root, capacity).await?;

    tokio::spawn(async move {
        // let (tx, mut rx) = mpsc::channel(32);
        // stx.send(tx);
        // dbcache.run(&rx).await;ioutil
        debug!("dbcache server started");
        while let Some(task) = rx.recv().await {
            let res = match task {
                Task::Get(t) => t.tx.send(dbcache.get(&t.key).await).or_else(|_t| Err(())),
                Task::SetBlob(t) => {
                    t.tx.send(dbcache.set_blob(t.key, t.blob, t.expire_time).await)
                        .or_else(|_t| Err(()))
                }
                Task::SetFile(t) => {
                    t.tx.send(
                        dbcache
                            .set_file(t.key, t.size.try_into().unwrap(), t.filename, t.expire_time)
                            .await,
                    )
                    .or_else(|_t| Err(()))
                }
                Task::Del(t) => t.tx.send(dbcache.del(&t.key).await).or_else(|_t| Err(())),
                Task::Stat(t) => {
                    t.tx.send(Ok(Stat {
                        entries: dbcache.entries(),
                        size: dbcache.size(),
                        capacity: dbcache.capacity(),
                    }))
                    .or_else(|_t| Err(()))
                }
                Task::FlushAll(t) => t.tx.send(dbcache.flushall().await).or_else(|_t| Err(())),
                Task::Keys(t) => {
                    t.tx.send(dbcache.keys(t.max_num, t.key, t.store_time, t.prefix).await)
                        .or_else(|_t| Err(()))
                }
                Task::End(t) => {
                    t.tx.send(()).unwrap();
                    break;
                }
            };
            match res {
                Ok(_) => {}
                Err(_) => {
                    error!("some error occurred when send task.");
                    break;
                }
            };
        }
        debug!("dbcache server closed");
    });
    // let tx = srx.await?;

    Ok((
        DBCacheClient::new(&data_root, tx.clone(), blob_threshold, size_limit),
        DBCacheDisposer { tx: tx },
    ))
}

#[cfg(test)]
mod tests {

    use super::ioutil::ByteReader;
    use super::*;
    use std::{path::Path, pin::Pin};
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;

    struct TestFixture {
        /// Temp directory.
        pub tempdir: TempDir,
    }

    impl TestFixture {
        pub fn new() -> TestFixture {
            TestFixture {
                tempdir: tempfile::Builder::new()
                    .prefix("dbcache-test")
                    .tempdir()
                    .unwrap(),
            }
        }
        pub fn get_path(&self) -> &Path {
            self.tempdir.path()
        }
    }

    #[tokio::test]
    async fn test_set_get() {
        let f = TestFixture::new();
        let (dbc, disposer) = run_server(f.get_path(), 32, 128, 128).await.unwrap();

        let key = "some-key";
        let value = vec![0, 1, 2, 3];
        dbc.set(&key, Pin::new(&mut ByteReader::new(value)), None)
            .await
            .unwrap();
        // let r = cache::Entity::find()
        //     .filter(cache::Column::Key.eq(key))
        //     .one(&dbc.conn)
        //     .await
        //     .unwrap()
        //     .unwrap();
        // println!("{:?}", r);
        // assert!(r.size == 4);
        // assert!(r.filename == None);
        // assert!(r.value.unwrap().len() == 4);

        let r = dbc.get(key).await.unwrap().unwrap();
        match r {
            ioutil::Data::Bytes(b) => {
                assert!(b.len() == 4);
                assert!(b[..4] == [0, 1, 2, 3]);
            }
            ioutil::Data::File(_) => {
                assert!(false);
            }
        }

        dbc.del(key).await.unwrap();

        assert!(dbc.get(key).await.unwrap().is_none());
        disposer.dispose().await.unwrap();
    }

    #[tokio::test]
    async fn test_set_get_file() {
        let f = TestFixture::new();
        // let dbc = DBCache::new(&PathBuf::from(f.get_path()), 2).await.unwrap();
        let (dbc, disposer) = run_server(f.get_path(), 2, 128, 128).await.unwrap();
        let key = "some-key";
        let value = vec![0, 1, 2, 3];
        dbc.set(&key, Pin::new(&mut ByteReader::new(value)), None)
            .await
            .unwrap();
        // let r = cache::Entity::find()
        //     .filter(cache::Column::Key.eq(key))
        //     .one(&dbc.conn)
        //     .await
        //     .unwrap()
        //     .unwrap();
        // println!("{:?}", r);
        // assert!(r.size == 4);
        // assert!(r.filename.is_some());
        // assert!(r.value == None);

        let r = dbc.get(key).await.unwrap().unwrap();
        match r {
            ioutil::Data::Bytes(_) => {
                assert!(false);
            }
            ioutil::Data::File(mut f) => {
                let mut buf: Vec<u8> = vec![0; 16];
                let num_read = f.read(&mut buf).await.unwrap();
                assert!(num_read == 4);
                assert!(buf[..4] == [0, 1, 2, 3]);
            }
        }

        dbc.del(key).await.unwrap();

        assert!(dbc.get(key).await.unwrap().is_none());
        disposer.dispose().await.unwrap();
    }
}
