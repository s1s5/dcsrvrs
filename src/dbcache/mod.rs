mod client;
mod errors;
mod ioutil;
mod server;
mod task;

pub use self::client::DBCacheClient;
pub use self::errors::*;
use self::server::DBCache;
use self::task::*;

use chrono::{DateTime, Local, Utc};
use entity::cache;
use log::{debug, error, info, log_enabled, Level};
use migration::{Migrator, MigratorTrait};
use sea_orm::entity::ModelTrait;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, EntityTrait, FromQueryResult, PaginatorTrait, QueryFilter,
    QuerySelect, Set, SqlxSqliteConnector,
};
use sea_orm::{DbConn, DbErr};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::error;
use std::fmt;

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::{cmp, fs};
use tokio::fs::{remove_file, File};
use tokio::io::{copy, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt},
    sync::Mutex,
};
use uuid::Uuid;

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
        // dbcache.run(&rx).await;
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
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

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

        let mut r = dbc.get(key).await.unwrap().unwrap();
        let mut buf: Vec<u8> = vec![0; 16];
        let num_read = r.read(&mut buf).await.unwrap();
        assert!(num_read == 4);
        assert!(buf[..4] == [0, 1, 2, 3]);

        dbc.del(key).await.unwrap();

        assert!(dbc.get(key).await.unwrap().is_none());
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

        let mut r = dbc.get(key).await.unwrap().unwrap();
        let mut buf: Vec<u8> = vec![0; 16];
        let num_read = r.read(&mut buf).await.unwrap();
        assert!(num_read == 4);
        assert!(buf[..4] == [0, 1, 2, 3]);

        dbc.del(key).await.unwrap();

        assert!(dbc.get(key).await.unwrap().is_none());
    }
}
