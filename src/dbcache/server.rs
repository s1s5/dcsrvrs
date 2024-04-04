use super::errors::{DbFieldError, Error};
use super::ioutil;
use crate::imcache::InmemoryCache;
use chrono::{DateTime, Local, Utc};
use entity::cache;
use log::error;
use migration::{Condition, Migrator, MigratorTrait, Order};
use sea_orm::entity::ModelTrait;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, FromQueryResult, Paginator,
    PaginatorTrait, QueryFilter, QuerySelect, SelectModel, Set, SqlxSqliteConnector,
};
use sea_orm::{DbConn, QueryOrder};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, trace};

#[derive(FromQueryResult)]
struct LimitedCacheRow {
    size: i64,
}

#[derive(FromQueryResult)]
struct LimitedCacheRowWithFilename {
    key: String,
    size: i64,
    filename: Option<String>,
}

#[derive(FromQueryResult)]
struct CacheKeyAndStoreTime {
    key: String,
    store_time: i64,
}

pub struct DBCache {
    conn: DbConn,
    data_root: PathBuf,
    entries: usize,
    size: usize,
    capacity: usize,
    evict_interval: f64,
    evict_threshold: f64,
    evicted_at: std::time::Instant,
    evicted_size: usize,
    inmemory: Option<Arc<InmemoryCache>>,
}

impl DBCache {
    pub async fn new(
        db_path: &Path,
        data_root: &Path,
        capacity: usize,
        inmemory: Option<Arc<InmemoryCache>>,
    ) -> Result<DBCache, Error> {
        let connection_options = SqliteConnectOptions::new()
            .filename(
                db_path
                    .to_str()
                    .ok_or(Error::Other(format!("db_path:'{db_path:?}' ValueError")))?,
            )
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal);

        let sqlite_pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect_with(connection_options)
            .await
            .map_err(|err| Error::Other(format!("connection failed {err:?}")))?;

        let conn = SqlxSqliteConnector::from_sqlx_sqlite_pool(sqlite_pool);
        Migrator::up(&conn, None)
            .await
            .expect("Failed to run migrations");

        let (mut entries, mut size) = (0, 0);
        let mut pages = cache::Entity::find()
            .select_only()
            .column(cache::Column::Size)
            .into_model::<LimitedCacheRow>()
            .paginate(&conn, 1000);
        while let Some(el) = pages.fetch_and_next().await.map_err(Error::Db)? {
            for e in el {
                entries += 1;
                size += e.size;
            }
        }

        Ok(DBCache {
            conn,
            data_root: data_root.into(),
            entries,
            size: size as usize,
            capacity,
            evict_interval: 60.0,
            evict_threshold: 0.8,
            evicted_at: std::time::Instant::now(),
            evicted_size: size as usize,
            inmemory,
        })
    }

    pub fn entries(&self) -> usize {
        self.entries
    }
    pub fn size(&self) -> usize {
        self.size
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub async fn get(&self, key: &str) -> Result<Option<ioutil::Data>, Error> {
        let c: Option<cache::Model> = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(&self.conn)
            .await
            .map_err(Error::Db)?;
        match c {
            None => Ok(None),
            Some(v) => {
                let now = Local::now().timestamp();

                if v.expire_time.filter(|f| f < &now).is_some() {
                    return Ok(None);
                }

                {
                    // アクセス日時の更新
                    let mut av: cache::ActiveModel = v.clone().into();
                    av.access_time = Set(Local::now().timestamp());
                    av.update(&self.conn).await.map_err(Error::Db)?;
                }

                let headers: HashMap<String, String> =
                    bincode::deserialize(&v.attr.unwrap_or(Vec::new()))
                        .map_err(|err| Error::Other(format!("deserialize error {err:?}")))?;
                if let Some(value) = v.value {
                    if let Some(inmemory) = self.inmemory.clone() {
                        let _ = inmemory.set(&key, &value, v.expire_time, headers.clone());
                    }

                    Ok(Some(ioutil::Data::new_from_buf(value, headers)))
                } else if let Some(filename) = v.filename {
                    tokio::fs::File::open(self.data_root.join(filename))
                        .await
                        .map(|f| {
                            Some(ioutil::Data::new_from_file(
                                f,
                                v.size.try_into().unwrap(),
                                headers,
                            ))
                        })
                        .map_err(Error::Io)
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
        headers: HashMap<String, String>,
    ) -> Result<(), Error> {
        let old_size = match cache::Entity::find()
            .select_only()
            .column(cache::Column::Size)
            .filter(cache::Column::Key.eq(key.clone()))
            .into_model::<LimitedCacheRow>()
            .one(&self.conn)
            .await
            .map_err(Error::Db)?
        {
            Some(r) => r.size,
            None => -1,
        };
        debug!(
            "old_size:{} entries={}, bytes={}",
            old_size, self.entries, self.size
        );

        let now: DateTime<Utc> = Utc::now();
        let c = cache::ActiveModel {
            key: Set(key),
            store_time: Set(now.timestamp()),
            expire_time: Set(expire_time),
            access_time: Set(now.timestamp()),
            size: Set(size),
            filename: Set(filename),
            value: Set(blob),
            attr: Set(Some(bincode::serialize(&headers).unwrap())),
        };

        if old_size < 0 {
            self.entries += 1;
            self.size += size as usize;
            trace!("insert: {:?}", c);
            c.insert(&self.conn).await.map_err(Error::Db)?;
        } else {
            self.size = ((self.size as i64) + size - old_size) as usize;
            trace!("update: {:?}", c);
            c.update(&self.conn).await.map_err(Error::Db)?;
        }
        debug!("after insert entries={}, bytes={}", self.entries, self.size);
        Ok(())
    }

    pub async fn set_blob(
        &mut self,
        key: String,
        blob: Vec<u8>,
        expire_time: Option<i64>,
        headers: HashMap<String, String>,
    ) -> Result<(), Error> {
        if let Some(inmemory) = self.inmemory.clone() {
            let _ = inmemory.set(&key, &blob, expire_time, headers.clone());
        }

        self.set_internal(
            key,
            blob.len().try_into().unwrap(),
            Some(blob),
            None,
            expire_time,
            headers,
        )
        .await?;
        self.evict().await?;
        Ok(())
    }

    pub async fn set_file(
        &mut self,
        key: String,
        size: i64,
        filename: String,
        expire_time: Option<i64>,
        headers: HashMap<String, String>,
    ) -> Result<(), Error> {
        self.set_internal(key, size, None, Some(filename), expire_time, headers)
            .await?;
        self.evict().await?;
        Ok(())
    }

    pub async fn del(&mut self, key: &str) -> Result<u64, Error> {
        if let Some(inmemory) = self.inmemory.clone() {
            let _ = inmemory.del(key);
        }

        let c = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(&self.conn)
            .await
            .map_err(Error::Db)?;
        Ok(match c {
            Some(x) => {
                self.entries -= 1;
                self.size -= x.size as usize;

                let filename = x.filename.clone();
                let s = x.delete(&self.conn).await.map_err(Error::Db)?.rows_affected;
                if let Some(filename) = filename {
                    tokio::fs::remove_file(self.data_root.join(filename))
                        .await
                        .map_err(Error::Io)?;
                }
                s
            }
            None => 0,
        })
    }

    async fn evict_rows_extract_keys(
        &self,
        mut pages: Paginator<'_, DatabaseConnection, SelectModel<LimitedCacheRowWithFilename>>,
        delete_all: bool,
        goal_size: usize,
    ) -> Result<(Vec<String>, Vec<String>, usize), Error> {
        let mut keys: Vec<String> = Vec::new();
        let mut deleted_files: Vec<String> = Vec::new();
        let mut deleted_size: usize = 0;

        'outer: while let Some(el) = pages.fetch_and_next().await.map_err(Error::Db)? {
            for e in el {
                deleted_size += e.size as usize;
                keys.push(e.key);
                if e.filename.is_some() {
                    deleted_files.push(e.filename.unwrap());
                }

                if (!delete_all) && (goal_size + deleted_size >= self.size) {
                    break 'outer;
                }
            }
        }
        Ok((keys, deleted_files, deleted_size))
    }

    async fn evict_rows_delete(
        &mut self,
        keys: Vec<String>,
        deleted_files: Vec<String>,
        deleted_size: usize,
    ) -> Result<(usize, usize), Error> {
        if keys.is_empty() {
            return Ok((0, 0));
        }
        let del_entries = keys.len();

        let res = cache::Entity::delete_many()
            .filter(cache::Column::Key.is_in(keys))
            .exec(&self.conn)
            .await
            .map_err(Error::Db)?;

        self.entries -= del_entries;
        self.size -= deleted_size;

        for filename in deleted_files {
            match tokio::fs::remove_file(self.data_root.join(&filename)).await {
                Ok(_) => {}
                // ここでエラーで終了させてしまうと、DBの値が更新がself.sizeに反映できない
                Err(e) => error!("failed to remove file {} with {:?}", filename, e),
            }
        }
        Ok((res.rows_affected.try_into().unwrap(), deleted_size))
    }

    async fn evict_expired(&mut self) -> Result<(usize, usize), Error> {
        let now = Local::now().timestamp();
        let (keys, files, size) = self
            .evict_rows_extract_keys(
                cache::Entity::find()
                    .filter(cache::Column::ExpireTime.is_not_null())
                    .filter(cache::Column::ExpireTime.lt(now))
                    .select_only()
                    .column(cache::Column::Key)
                    .column(cache::Column::Size)
                    .column(cache::Column::Filename)
                    .into_model::<LimitedCacheRowWithFilename>()
                    .paginate(&self.conn, 100),
                true,
                self.capacity,
            )
            .await?;
        self.evict_rows_delete(keys, files, size).await
    }

    async fn evict_old(&mut self, goal_size: usize) -> Result<(usize, usize), Error> {
        let (keys, files, size) = self
            .evict_rows_extract_keys(
                cache::Entity::find()
                    .order_by_asc(cache::Column::AccessTime)
                    .select_only()
                    .column(cache::Column::Key)
                    .column(cache::Column::Size)
                    .column(cache::Column::Filename)
                    .into_model::<LimitedCacheRowWithFilename>()
                    .paginate(&self.conn, 100),
                false,
                goal_size,
            )
            .await?;
        self.evict_rows_delete(keys, files, size).await
    }

    async fn evict(&mut self) -> Result<(usize, usize), Error> {
        trace!("before evict entires={}, bytes={}", self.entries, self.size);
        let (expired_entries, expired_size) = if (self.evicted_at
            + std::time::Duration::from_secs_f64(self.evict_interval))
            < std::time::Instant::now()
        {
            self.evict_expired().await?
        } else {
            (0, 0)
        };
        trace!(
            "after evict entires={}, bytes={}, expired.entries={expired_entries}, expired.bytes={expired_size}",
            self.entries,
            self.size,
        );

        let (old_deleted_entries, old_deleted_size) = if self.size < self.capacity {
            (0, 0)
        } else {
            self.evicted_size = self.size;
            let goal_size = (self.evict_threshold * (self.capacity as f64)) as usize;
            self.evict_old(goal_size).await?
        };
        trace!(
            "after evict_old entires={}, bytes={}, old.entries={old_deleted_entries}, old.bytes={old_deleted_size}",
            self.entries,
            self.size
        );

        Ok((
            expired_entries + old_deleted_entries,
            expired_size + old_deleted_size,
        ))
    }

    pub async fn flushall(&mut self) -> Result<(usize, usize), Error> {
        let mut deleted_entries = 0;
        let mut deleted_size = 0;
        loop {
            if self.size == 0 {
                break;
            }

            let (keys, files, size) = self
                .evict_rows_extract_keys(
                    cache::Entity::find()
                        .order_by(cache::Column::AccessTime, Order::Asc)
                        .limit(10000)
                        .select_only()
                        .column(cache::Column::Key)
                        .column(cache::Column::Size)
                        .column(cache::Column::Filename)
                        .into_model::<LimitedCacheRowWithFilename>()
                        .paginate(&self.conn, 1000),
                    true,
                    0,
                )
                .await?;
            let (e, s) = self.evict_rows_delete(keys, files, size).await?;
            deleted_entries += e;
            deleted_size += s;
        }
        Ok((deleted_entries, deleted_size))
    }

    pub async fn keys(
        &self,
        max_num: i64,
        key: Option<String>,
        store_time: Option<i64>,
        prefix: Option<String>,
    ) -> Result<Vec<(String, i64)>, Error> {
        let qs = cache::Entity::find();

        let qs = if key.is_some() && store_time.is_some() {
            qs.filter(
                Condition::any()
                    .add(cache::Column::StoreTime.gt(store_time))
                    .add(
                        Condition::all()
                            .add(cache::Column::StoreTime.eq(store_time))
                            .add(cache::Column::Key.gt(key)),
                    ),
            )
        } else if store_time.is_some() {
            qs.filter(cache::Column::StoreTime.gt(store_time))
        } else {
            qs
        };

        let qs = if let Some(prefix) = prefix {
            qs.filter(cache::Column::Key.starts_with(&prefix))
        } else {
            qs
        };

        let data = qs
            .order_by_asc(cache::Column::StoreTime)
            .order_by_asc(cache::Column::Key)
            .limit(Some(max_num.try_into().unwrap()))
            .select_only()
            .column(cache::Column::Key)
            .column(cache::Column::StoreTime)
            .into_model::<CacheKeyAndStoreTime>()
            .all(&self.conn)
            .await
            .map_err(Error::Db)?;

        Ok(data.into_iter().map(|e| (e.key, e.store_time)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::{
        fs::{self, File},
        io::{AsyncReadExt, AsyncWriteExt},
    };

    #[allow(dead_code)] // tempdirは使わなけど残す必要がある
    struct TestFixture {
        pub tempdir: TempDir,
        pub cache: DBCache,
    }

    impl TestFixture {
        pub async fn new(capacity: usize) -> TestFixture {
            let tempdir = tempfile::Builder::new()
                .prefix("dbcache-test")
                .tempdir()
                .unwrap();
            TestFixture {
                cache: DBCache::new(
                    &tempdir.path().join("db.sqlite"),
                    &tempdir.path().join("data"),
                    capacity,
                    None,
                )
                .await
                .unwrap(),
                tempdir,
            }
        }
    }

    #[tokio::test]
    async fn test_set_get() {
        let mut f = TestFixture::new(1024).await;

        let key = "some-key";
        let value = vec![0, 1, 2, 3];
        let headers = HashMap::new();
        f.cache
            .set_blob(key.into(), value, None, headers)
            .await
            .unwrap();

        assert!(f.cache.entries == 1);
        assert!(f.cache.size == 4);

        // dbに登録されているか
        let r = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(&f.cache.conn)
            .await
            .unwrap()
            .unwrap();

        assert!(r.size == 4);
        assert!(r.filename.is_none());
        assert!(r.value.unwrap().len() == 4);

        // dbcache経由での値の取得
        let r = f.cache.get(key).await.unwrap().unwrap();
        match r.into_inner() {
            ioutil::DataInternal::Bytes(b) => {
                assert!(b.len() == 4);
                assert!(b[..4] == [0, 1, 2, 3]);
            }
            ioutil::DataInternal::File(_) => {
                panic!();
            }
        }
        // let mut buf: Vec<u8> = vec![0; 16];
        // let num_read = r.read(&mut buf).await.unwrap();
        // assert!(num_read == 4);
        // assert!(buf[..4] == [0, 1, 2, 3]);

        f.cache.del(key).await.unwrap();

        assert!(f.cache.entries == 0);
        assert!(f.cache.size == 0);

        assert!(f.cache.get(key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_set_get_file() {
        let mut f = TestFixture::new(1024).await;

        let key = "some-key";
        let abs_path = f.cache.data_root.join("foo");
        {
            fs::create_dir_all(&abs_path.parent().unwrap())
                .await
                .unwrap();
            let mut writer = File::create(&abs_path).await.unwrap();
            writer.write_all(&[0, 1, 2, 3]).await.unwrap();
        }
        let headers = HashMap::new();
        f.cache
            .set_file(
                key.into(),
                4,
                abs_path
                    .strip_prefix(&f.cache.data_root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .into(),
                None,
                headers,
            )
            .await
            .unwrap();

        assert!(f.cache.entries == 1);
        assert!(f.cache.size == 4);

        // databaseに値が登録されているか
        let r = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(&f.cache.conn)
            .await
            .unwrap()
            .unwrap();

        assert!(r.size == 4);
        assert!(r.filename.is_some() && r.filename == Some(String::from("foo")));
        assert!(r.value.is_none());

        // dbcache経由での値の取得
        let r = f.cache.get(key).await.unwrap().unwrap();
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
        // let mut buf: Vec<u8> = vec![0; 16];
        // let num_read = r.read(&mut buf).await.unwrap();
        // assert!(num_read == 4);
        // assert!(buf[..4] == [0, 1, 2, 3]);

        f.cache.del(key).await.unwrap();
        assert!(!abs_path.exists());

        assert!(f.cache.entries == 0);
        assert!(f.cache.size == 0);

        assert!(f.cache.get(key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_evict_old() -> anyhow::Result<()> {
        let mut f = TestFixture::new((13.0 / 0.8f64).ceil() as usize).await;
        let value = vec![0, 1, 2, 3, 4, 5];
        let headers = HashMap::new();
        f.cache
            .set_blob("A".into(), value.clone(), None, headers.clone())
            .await?;
        f.cache
            .set_blob("B".into(), value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("C".into(), value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("D".into(), value.clone(), None, headers.clone())
            .await?;

        assert!(f.cache.entries == 2);
        assert!(f.cache.size == 12);
        assert!(f.cache.get("A").await.unwrap().is_none());
        assert!(f.cache.get("B").await.unwrap().is_none());
        assert!(f.cache.get("C").await.unwrap().is_some());
        assert!(f.cache.get("D").await.unwrap().is_some());

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let data = f.cache.get("C").await?;
        assert!(data.is_some());

        f.cache
            .set_blob("E".into(), value.clone(), None, headers.clone())
            .await
            .unwrap();
        assert!(f.cache.get("C").await.unwrap().is_some());
        assert!(f.cache.get("D").await.unwrap().is_none());
        assert!(f.cache.get("E").await.unwrap().is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_flushall() -> anyhow::Result<()> {
        let mut f = TestFixture::new((13.0 / 0.8f64).ceil() as usize).await;
        let value = vec![0, 1, 2, 3, 4, 5];
        let headers = HashMap::new();
        f.cache
            .set_blob("A".into(), value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("B".into(), value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("C".into(), value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("D".into(), value.clone(), None, headers.clone())
            .await?;

        assert!(f.cache.entries == 2);
        assert!(f.cache.size == 12);

        f.cache.flushall().await?;

        assert!(f.cache.entries == 0);
        assert!(f.cache.size == 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_keys() -> anyhow::Result<()> {
        let mut f = TestFixture::new(128).await;
        let value = vec![0, 1, 2, 3, 4, 5];
        let headers = HashMap::new();
        f.cache
            .set_blob("A".into(), value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("B".into(), value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("C".into(), value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("D".into(), value.clone(), None, headers.clone())
            .await?;

        let keys = f.cache.keys(100, None, None, None).await?;

        assert!(keys.len() == 4);
        assert!(keys[0].0 == "A");

        Ok(())
    }
}
