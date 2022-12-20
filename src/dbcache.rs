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

#[derive(FromQueryResult)]
struct LimitedCacheRow {
    key: String,
    size: i64,
    filename: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DbFieldError;

impl fmt::Display for DbFieldError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid first item to double")
    }
}

// This is important for other errors to wrap this one.
impl error::Error for DbFieldError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        // 基本となるエラー、原因は記録されていない。
        None
    }
}

struct ByteReader {
    pos: usize,
    data: Vec<u8>,
}

impl ByteReader {
    fn new(data: Vec<u8>) -> ByteReader {
        ByteReader { pos: 0, data: data }
    }
}

impl AsyncRead for ByteReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let me = self.get_mut();
        loop {
            let n = cmp::min(me.data.len() - me.pos, buf.remaining());
            buf.put_slice(&me.data[me.pos..][..n]);
            me.pos += n;

            // if me.pos == me.data.len() {
            //     me.data.truncate(0);
            //     me.pos = 0;
            // };
            return std::task::Poll::Ready(Ok(()));
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Db(DbErr),
    Io(io::Error),
    Schema(DbFieldError),
    RecvError(oneshot::error::RecvError),
    // SendError(mpsc::error::SendError<Task>),
    SendError, // channel closed
    FileSizeLimitExceeded,
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid first item to double")
    }
}

// This is important for other errors to wrap this one.
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        // 基本となるエラー、原因は記録されていない。
        None
    }
}

pub struct GetTask {
    tx: oneshot::Sender<Result<Option<Pin<Box<dyn AsyncRead + Send>>>, Error>>,
    key: String,
}
pub struct SetBlobTask {
    tx: oneshot::Sender<Result<(), Error>>,
    key: String,
    blob: Vec<u8>,
    expire_time: Option<i64>,
}
pub struct SetFileTask {
    tx: oneshot::Sender<Result<(), Error>>,
    key: String,
    size: usize,
    filename: String,
    expire_time: Option<i64>,
}
pub struct DelTask {
    tx: oneshot::Sender<Result<u64, Error>>,
    key: String,
}
pub struct EndTask {
    tx: oneshot::Sender<()>,
}

pub enum Task {
    Get(GetTask),
    SetBlob(SetBlobTask),
    SetFile(SetFileTask),
    Del(DelTask),
    End(EndTask),
}

pub struct DBCache {
    conn: DbConn,
    data_root: PathBuf,
    entries: usize,
    size: usize,
    capacity: usize,
}

impl DBCache {
    pub async fn new(
        db_path: &Path,
        data_root: &Path,
        capacity: usize,
    ) -> Result<DBCache, Box<dyn std::error::Error>> {
        let connection_options = SqliteConnectOptions::new()
            .filename(db_path.to_str().unwrap())
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal);

        let sqlite_pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect_with(connection_options)
            .await?;

        // let conn = Database::connect(db_url)
        //     .await
        //     .expect("Failed to setup the database");
        let conn = SqlxSqliteConnector::from_sqlx_sqlite_pool(sqlite_pool);
        Migrator::up(&conn, None)
            .await
            .expect("Failed to run migrations for tests");

        let (mut entries, mut size) = (0, 0);
        let mut pages = cache::Entity::find()
            .select_only()
            .column(cache::Column::Key)
            .column(cache::Column::Size)
            .column(cache::Column::Filename)
            .into_model::<LimitedCacheRow>()
            .paginate(&conn, 1000);
        debug!("calculating cache size");
        while let Some(el) = pages.fetch_and_next().await? {
            debug!("loading cache keys");
            for e in el {
                entries += 1;
                size += e.size;
            }
        }

        Ok(DBCache {
            conn: conn,
            data_root: data_root.into(),
            entries: entries,
            size: size as usize,
            capacity: capacity,
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<Pin<Box<dyn AsyncRead + Send>>>, Error> {
        let c: Option<cache::Model> = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(&self.conn)
            .await
            .or_else(|e| Err(Error::Db(e)))?;
        match c {
            None => Ok(None),
            Some(v) => {
                let now = Local::now().timestamp();

                if v.expire_time.filter(|f| f > &now).is_some() {
                    return Ok(None);
                }

                {
                    // アクセス日時の更新
                    let mut av: cache::ActiveModel = v.clone().into();
                    av.access_time = Set(Local::now().timestamp());
                    av.update(&self.conn).await.or_else(|e| Err(Error::Db(e)))?;
                }

                if v.value.is_some() {
                    Ok(Some(Box::pin(ByteReader::new(v.value.unwrap()))
                        as Pin<Box<dyn AsyncRead + Send>>))
                } else if v.filename.is_some() {
                    tokio::fs::File::open(self.data_root.join(v.filename.unwrap()))
                        .await
                        .and_then(|f| Ok(Some(Box::pin(f) as Pin<Box<dyn AsyncRead + Send>>)))
                        .or_else(|e| Err(Error::Io(e)))
                } else {
                    Err(Error::Schema(DbFieldError {}))
                }
            }
        }
    }

    async fn set_internal(
        &mut self,
        key: String,
        size: i64,
        blob: Option<Vec<u8>>,
        filename: Option<String>,
        expire_time: Option<i64>,
    ) -> Result<(), Error> {
        let old_size = match cache::Entity::find()
            .select_only()
            .column(cache::Column::Key)
            .column(cache::Column::Size)
            .column(cache::Column::Filename)
            .filter(cache::Column::Key.eq(key.clone()))
            .into_model::<LimitedCacheRow>()
            .one(&self.conn)
            .await
            .or_else(|e| Err(Error::Db(e)))?
        {
            Some(r) => r.size,
            None => -1,
        };

        let now: DateTime<Utc> = Utc::now();
        let c = cache::ActiveModel {
            key: Set(key),
            store_time: Set(now.timestamp()),
            expire_time: Set(expire_time),
            access_time: Set(now.timestamp()),
            size: Set(size),
            filename: Set(filename),
            value: Set(blob),
        };

        if old_size < 0 {
            self.entries += 1;
            self.size += size as usize;
            c.insert(&self.conn).await.or_else(|e| Err(Error::Db(e)))?;
        } else {
            self.size = ((self.size as i64) + size - old_size) as usize;
            c.update(&self.conn).await.or_else(|e| Err(Error::Db(e)))?;
        }
        Ok(())
    }

    async fn set_blob(
        &mut self,
        key: String,
        blob: Vec<u8>,
        expire_time: Option<i64>,
    ) -> Result<(), Error> {
        self.set_internal(
            key,
            blob.len().try_into().unwrap(),
            Some(blob),
            None,
            expire_time,
        )
        .await?;
        self.truncate().await?;
        Ok(())
    }

    async fn set_file(
        &mut self,
        key: String,
        size: i64,
        filename: String,
        expire_time: Option<i64>,
    ) -> Result<(), Error> {
        self.set_internal(key, size, None, Some(filename), expire_time)
            .await?;
        self.truncate().await?;
        Ok(())
    }

    async fn del(&mut self, key: &str) -> Result<u64, Error> {
        let c = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(&self.conn)
            .await
            .or_else(|e| Err(Error::Db(e)))?;
        Ok(match c {
            Some(e) => {
                println!("{}, {}", self.size, e.size);
                self.entries -= 1;
                self.size -= e.size as usize;

                e.delete(&self.conn)
                    .await
                    .or_else(|e| Err(Error::Db(e)))?
                    .rows_affected
            }
            None => 0,
        })
    }

    async fn truncate(&mut self) -> Result<(usize, usize), Error> {
        if self.size < self.capacity {
            return Ok((0, 0));
        }

        let mut keys: Vec<String> = Vec::new();
        let mut deleted_files: Vec<String> = Vec::new();
        let mut deleted_size: usize = 0;
        let mut pages = cache::Entity::find()
            .select_only()
            .column(cache::Column::Key)
            .column(cache::Column::Size)
            .column(cache::Column::Filename)
            .into_model::<LimitedCacheRow>()
            .paginate(&self.conn, 100);

        'outer: while let Some(el) = pages
            .fetch_and_next()
            .await
            .or_else(|e| Err(Error::Db(e)))?
        {
            for e in el {
                deleted_size += e.size as usize;
                keys.push(e.key);
                if e.filename.is_some() {
                    deleted_files.push(e.filename.unwrap());
                }

                if self.capacity + deleted_size > self.size {
                    break 'outer;
                }
            }
        }
        let res = cache::Entity::delete_many()
            .filter(cache::Column::Key.is_in(keys))
            .exec(&self.conn)
            .await
            .or_else(|e| Err(Error::Db(e)))?;
        Ok((res.rows_affected.try_into().unwrap(), deleted_size))
    }

    // pub async fn run(&mut self, rx: &mut mpsc::Receiver<Task>) {
    //     // while let Some(task) = rx.recv().await {
    //     //     match task {
    //     //         Task::Get(t) => {
    //     //             t.tx.send(self.get(&t.key).await);
    //     //         }
    //     //         Task::SetBlob(t) => {
    //     //             t.tx.send(self.set_blob(t.key, t.blob, t.expire_time).await);
    //     //         }
    //     //         Task::SetFile(t) => {
    //     //             t.tx.send(
    //     //                 self.set_file(t.key, t.size.try_into().unwrap(), t.filename, t.expire_time)
    //     //                     .await,
    //     //             );
    //     //         }
    //     //         Task::Del(t) => {
    //     //             t.tx.send(self.del(&t.key).await);
    //     //         }
    //     //         Task::End => {
    //     //             break;
    //     //         }
    //     //     }
    //     // }
    // }
}

#[derive(Debug)]
pub struct DBCacheClient {
    blob_threshold: usize,
    size_limit: usize,
    data_root: PathBuf,
    tx: mpsc::Sender<Task>,
    buf_list: Mutex<Vec<Vec<u8>>>,
}

impl DBCacheClient {
    fn new(
        data_root: &Path,
        tx: mpsc::Sender<Task>,
        blob_threshold: usize,
        size_limit: usize,
    ) -> DBCacheClient {
        DBCacheClient {
            blob_threshold: blob_threshold,
            size_limit: size_limit,
            data_root: data_root.into(),
            tx: tx,
            buf_list: Mutex::new(Vec::new()),
        }
    }

    async fn get_buf(&self) -> Vec<u8> {
        let mut buf_list = self.buf_list.lock().await;
        match buf_list.pop() {
            Some(x) => x,
            None => {
                vec![0; self.blob_threshold]
            }
        }
    }

    async fn del_buf(&self, buf: Vec<u8>) {
        let mut buf_list = self.buf_list.lock().await;
        if buf_list.len() < 10 {
            buf_list.push(buf)
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Pin<Box<dyn AsyncRead + Send>>>, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::Get(GetTask {
                tx: tx,
                key: key.into(),
            }))
            .await
            .or_else(|_e| Err(Error::SendError))?;
        match rx.await {
            Ok(r) => r,
            Err(e) => Err(Error::RecvError(e)),
        }
    }

    async fn set_as_blob(
        &self,
        key: &str,
        buf: &[u8],
        expire_time: Option<i64>,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::SetBlob(SetBlobTask {
                tx: tx,
                key: key.into(),
                blob: buf.into(),
                expire_time: expire_time,
            }))
            .await
            .or_else(|_e| Err(Error::SendError))?;
        match rx.await {
            Ok(r) => r,
            Err(e) => Err(Error::RecvError(e)),
        }
    }

    async fn set_as_file<T: AsyncRead>(
        &self,
        key: &str,
        buf: &[u8],
        mut readable: Pin<&mut T>,
        expire_time: Option<i64>,
    ) -> Result<(), Error> {
        let id: String = Uuid::new_v4().to_string();
        let prefix = &id[..2];
        let path = self.data_root.join(prefix).join(&id);
        fs::create_dir_all(&path.parent().unwrap()).or_else(|e| Err(Error::Io(e)))?;
        let mut writer = File::create(&path).await.or_else(|e| Err(Error::Io(e)))?;
        writer.write_all(buf).await.or_else(|e| Err(Error::Io(e)))?;

        let num_wrote = copy(&mut readable, &mut writer)
            .await
            .or_else(|e| Err(Error::Io(e)))?;

        let size = (buf.len() as u64 + num_wrote).try_into().unwrap();
        if size >= self.size_limit {
            remove_file(&path).await.or_else(|e| Err(Error::Io(e)))?;
            return Err(Error::FileSizeLimitExceeded);
        }

        match {
            let (tx, rx) = oneshot::channel();
            self.tx
                .send(Task::SetFile(SetFileTask {
                    tx: tx,
                    key: key.into(),
                    size: size,
                    expire_time: expire_time,
                    filename: PathBuf::from(prefix).join(id).to_str().unwrap().into(),
                }))
                .await
                .or_else(|_e| Err(Error::SendError))?;
            match rx.await {
                Ok(r) => r,
                Err(e) => Err(Error::RecvError(e)),
            }
        } {
            Ok(t) => Ok(t),
            Err(e) => {
                remove_file(&path).await.or_else(|e| Err(Error::Io(e)))?;
                Err(e)
            }
        }
    }

    async fn read_eager<T: AsyncRead>(
        &self,
        readable: &mut Pin<&mut T>,
        buf: &mut Vec<u8>,
    ) -> Result<usize, io::Error> {
        let mut read = 0;
        while read < buf.len() {
            let t = readable.read(&mut buf[read..]).await?;
            read += t;
            if t == 0 {
                break;
            }
        }
        Ok(read)
    }

    pub async fn set<T: AsyncRead>(
        &self,
        key: &str,
        mut readable: Pin<&mut T>,
        expire_time: Option<i64>,
    ) -> Result<(), Error> {
        let (buf, buf_size) = {
            let mut buf = self.get_buf().await;
            // let read = readable
            //     .read(&mut buf)
            //     .await
            //     .or_else(|e| Err(Error::Io(e)))?;
            let read = self
                .read_eager(&mut readable, &mut buf)
                .await
                .or_else(|e| Err(Error::Io(e)))?;
            (buf, read)
        };

        let r = if buf_size >= self.blob_threshold {
            self.set_as_file(key, &buf[..buf_size], readable, expire_time)
                .await
        } else {
            self.set_as_blob(key, &buf[..buf_size], expire_time).await
        };
        self.del_buf(buf).await;
        r
    }

    pub async fn del(&self, key: &str) -> Result<u64, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::Del(DelTask {
                tx: tx,
                key: key.into(),
            }))
            .await
            .or_else(|_e| Err(Error::SendError))?;
        match rx.await {
            Ok(r) => r,
            Err(e) => Err(Error::RecvError(e)),
        }
    }
}

// impl std::clone::Clone for DBCacheClient {
//     fn clone(&self) -> Self {
//         DBCacheClient {
//             blob_threshold: self.blob_threshold,
//             data_root: self.data_root.clone(),
//             tx: self.tx.clone(),
//             buf_list: Mutex::new(Vec::new()),
//         }
//     }
//     fn clone_from(&mut self, source: &Self) {
//         self.blob_threshold = source.blob_threshold;
//         self.data_root = source.data_root.clone();
//         self.tx = source.tx.clone();
//     }
// }

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
        let (dbc, disposer) = run_server(f.get_path(), 32, 128).await.unwrap();

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
        let (dbc, disposer) = run_server(f.get_path(), 2, 128).await.unwrap();
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
