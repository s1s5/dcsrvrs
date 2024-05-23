mod client;
mod errors;
mod server;
mod task;

use log::{debug, error};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

pub use self::client::DBCacheClient;
pub use self::errors::*;
use self::server::DBCache;
use self::task::*;
use crate::imcache::InmemoryCache;
use crate::ioutil;
pub use task::KeyTaskResult;

pub struct DBCacheDisposer {
    tx: mpsc::Sender<Task>,
}

impl DBCacheDisposer {
    pub async fn dispose(self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::End(EndTask { tx }))
            .await
            .map_err(|_e| Error::SendError)?;
        rx.await.map_err(Error::RecvError)
    }
}

pub async fn run_server(
    cache_dir: &Path,
    blob_threshold: usize,
    size_limit: usize,
    capacity: usize,
    inmemory_cache: Option<Arc<InmemoryCache>>,
) -> Result<(Arc<DBCacheClient>, DBCacheDisposer), Box<dyn std::error::Error>> {
    let data_root = cache_dir.join("data");
    fs::create_dir_all(&data_root)?;

    let (tx, mut rx) = mpsc::channel(32);
    // let (stx, srx) = oneshot::channel();
    let mut dbcache = DBCache::new(
        &cache_dir.join("db.sqlite"),
        &data_root,
        capacity,
        inmemory_cache.clone(),
    )
    .await?;

    tokio::spawn(async move {
        // let (tx, mut rx) = mpsc::channel(32);
        // stx.send(tx);
        // dbcache.run(&rx).await;ioutil
        debug!("dbcache server started");
        while let Some(task) = rx.recv().await {
            info!("task: {:?}", task);
            let start = std::time::Instant::now();
            let res = match task {
                Task::Get(t) => {
                    t.tx.send(dbcache.get(&t.key).await)
                        .or_else(|t| t.map(|_| ()))
                }
                Task::Touch(t) => {
                    t.tx.send(dbcache.touch(&t.key).await)
                        .or_else(|t| t.map(|_| ()))
                }
                Task::SetBlob(t) => {
                    t.tx.send(
                        dbcache
                            .set_blob(t.key, t.sha256sum, t.blob, t.expire_time, t.headers)
                            .await,
                    )
                    .or_else(|t| t.map(|_| ()))
                }
                Task::SetFile(t) => {
                    t.tx.send(
                        dbcache
                            .set_file(
                                t.key,
                                t.size.try_into().unwrap(),
                                t.sha256sum,
                                t.filename,
                                t.expire_time,
                                t.headers,
                            )
                            .await,
                    )
                    .or_else(|t| t.map(|_| ()))
                }
                Task::Del(t) => {
                    t.tx.send(dbcache.del(&t.key).await)
                        .or_else(|t| t.map(|_| ()))
                }
                Task::Stat(t) => {
                    t.tx.send(Ok(Stat {
                        entries: dbcache.entries(),
                        size: dbcache.size(),
                        capacity: dbcache.capacity(),
                    }))
                    .or_else(|t| t.map(|_| ()))
                }
                Task::FlushAll(t) => {
                    t.tx.send(dbcache.flushall().await)
                        .or_else(|t| t.map(|_| ()))
                }
                Task::Keys(t) => {
                    t.tx.send(
                        dbcache
                            .keys(t.max_num, t.key, t.store_time, t.prefix, t.key_contains)
                            .await,
                    )
                    .or_else(|t| t.map(|_| ()))
                }
                Task::End(t) => {
                    t.tx.send(()).unwrap();
                    break;
                }
            };
            info!("elapsed_time: {:?}", start.elapsed());
            match res {
                Ok(_) => {}
                Err(e) => {
                    error!("some error occurred when send task. {:?}", e);
                    // break;
                }
            };
        }
        debug!("dbcache server closed");
    });
    // let tx = srx.await?;

    Ok((
        Arc::new(DBCacheClient::new(
            &data_root,
            tx.clone(),
            blob_threshold,
            size_limit,
            inmemory_cache,
        )),
        DBCacheDisposer { tx },
    ))
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::{collections::HashMap, path::Path, pin::Pin};
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, BufReader};

    use crate::ioutil;

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
        let (dbc, disposer) = run_server(f.get_path(), 32, 128, 128, None).await.unwrap();

        let key = "some-key";
        let value = vec![0, 1, 2, 3];
        let headers = HashMap::new();
        dbc.set(
            key,
            Pin::new(&mut BufReader::new(value.as_slice())),
            None,
            headers,
        )
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
        match r.into_inner() {
            ioutil::DataInternal::Bytes(b) => {
                assert!(b.len() == 4);
                assert!(b[..4] == [0, 1, 2, 3]);
            }
            ioutil::DataInternal::File(_) => {
                panic!();
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
        let (dbc, disposer) = run_server(f.get_path(), 2, 128, 128, None).await.unwrap();
        let key = "some-key";
        let value = vec![0, 1, 2, 3];
        let headers = HashMap::new();
        dbc.set(
            key,
            Pin::new(&mut BufReader::new(value.as_slice())),
            None,
            headers,
        )
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
        match r.into_inner() {
            ioutil::DataInternal::Bytes(_) => {
                panic!();
            }
            ioutil::DataInternal::File(mut f) => {
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

    #[tokio::test]
    async fn test_api() -> anyhow::Result<()> {
        let f = TestFixture::new();

        let (dbc, disposer) = run_server(f.get_path(), 4, 32, 128, None).await.unwrap();

        let stat = dbc.stat().await?;
        assert!(
            stat == Stat {
                entries: 0,
                size: 0,
                capacity: 128
            }
        );
        let data = vec![0, 1, 2, 3];
        dbc.set(
            "blobv",
            Pin::new(&mut BufReader::new(data.as_slice())),
            None,
            HashMap::new(),
        )
        .await?;

        let keys = dbc.keys(100, None, None, None, None).await?;

        assert!(keys.len() == 1);
        assert!(keys[0].key == *"blobv");

        let stat = dbc.stat().await?;
        assert!(
            stat == Stat {
                entries: 1,
                size: 4,
                capacity: 128
            }
        );

        dbc.flushall().await?;
        let stat = dbc.stat().await?;
        assert!(
            stat == Stat {
                entries: 0,
                size: 0,
                capacity: 128
            }
        );

        disposer.dispose().await.unwrap();

        Ok(())
    }
}
