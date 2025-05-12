use super::errors::{DbFieldError, Error};
use super::{ioutil, KeyTaskResult};
use crate::imcache::InmemoryCache;
use chrono::{DateTime, Local, Utc};
use entity::cache;
use log::error;
use migration::{Condition, Migrator, MigratorTrait, Order};
use sea_orm::entity::ModelTrait;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DbErr, DeleteResult,
    EntityTrait, FromQueryResult, Paginator, PaginatorTrait, QueryFilter, QuerySelect, SelectModel,
    Set, SqlxSqliteConnector, Statement, TransactionTrait,
};
use sea_orm::{DbConn, QueryOrder};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, trace};

#[derive(Debug, Default)]
pub struct DbStat {
    pub db_size: usize,
    pub db_shm_size: usize,
    pub db_wal_size: usize,
}

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
struct LimitedCacheRowWithExpireTime {
    key: String,
    expire_time: Option<i64>,
}

#[derive(FromQueryResult)]
struct CacheKeyAndStoreTime {
    key: String,
    store_time: i64,
    expire_time: Option<i64>,
    access_time: i64,
    size: i64,
    sha256sum: Vec<u8>,
    attr: Option<Vec<u8>>,
}

impl From<CacheKeyAndStoreTime> for KeyTaskResult {
    fn from(val: CacheKeyAndStoreTime) -> Self {
        KeyTaskResult {
            key: val.key,
            store_time: val.store_time,
            expire_time: val.expire_time,
            access_time: val.access_time,
            size: val.size,
            sha256sum: val.sha256sum,
            headers: bincode::deserialize(&val.attr.unwrap_or_default()).unwrap_or(HashMap::new()),
        }
    }
}

struct SetInternalArg {
    key: String,
    size: i64,
    sha256sum: Vec<u8>,
    blob: Option<Vec<u8>>,
    filename: Option<String>,
    expire_time: Option<i64>,
    headers: HashMap<String, String>,
}

pub struct DBCache {
    db_path: PathBuf,
    _conn: Option<DbConn>,
    data_root: PathBuf,
    stat: entity::meta::Stat,
    capacity: usize,
    evict_interval: std::time::Duration,
    evict_threshold: f64,
    evicted_at: std::time::Instant,
    evicted_size: usize,
    inmemory: Option<Arc<InmemoryCache>>,
    auto_evict: bool,
    max_age: i64,
}

async fn create_connection(db_path: &Path) -> Result<DatabaseConnection, Error> {
    let connection_options = SqliteConnectOptions::new()
        .filename(
            db_path
                .to_str()
                .ok_or(Error::Other(format!("db_path:'{db_path:?}' ValueError")))?,
        )
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal);

    debug!("creating connection pool");
    let sqlite_pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect_with(connection_options)
        .await
        .map_err(|err| Error::Other(format!("connection failed {err:?}")))?;

    let conn = SqlxSqliteConnector::from_sqlx_sqlite_pool(sqlite_pool);

    debug!("applying pending migrations");
    Migrator::up(&conn, None)
        .await
        .expect("Failed to run migrations");
    Ok(conn)
}

impl DBCache {
    pub async fn new(
        db_path: &Path,
        data_root: &Path,
        capacity: usize,
        inmemory: Option<Arc<InmemoryCache>>,
        auto_evict: bool,
        max_age: i64,
    ) -> Result<DBCache, Error> {
        debug!("creating dbcache instance");
        let conn = create_connection(db_path).await?;

        let stat = entity::meta::get_stat(&conn)
            .await
            .map_err(|err| Error::Other(format!("failed to get stat {err:?}")))?;
        debug!(
            "num_entries={}, size={}[bytes]",
            stat.num_of_entries, stat.total_size
        );

        Ok(DBCache {
            db_path: db_path.into(),
            _conn: Some(conn),
            data_root: data_root.into(),
            capacity,
            evict_interval: std::time::Duration::from_secs_f64(300.0),
            evict_threshold: 0.999,
            evicted_at: std::time::Instant::now(),
            evicted_size: stat.total_size as usize,
            inmemory,
            auto_evict,
            max_age,
            stat,
        })
    }

    #[inline(always)]
    pub fn entries(&self) -> usize {
        self.stat.num_of_entries as usize
    }

    #[inline(always)]
    pub fn size(&self) -> usize {
        self.stat.total_size as usize
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub async fn get_db_size(&self) -> Result<DbStat, Error> {
        let db_path = self
            .db_path
            .as_os_str()
            .to_str()
            .ok_or(Error::Other("invalid db_path".to_string()))?
            .to_string();
        Ok(DbStat {
            db_size: tokio::fs::metadata(&db_path)
                .await
                .map(|x| x.size() as usize)
                .ok()
                .unwrap_or(0),
            db_wal_size: tokio::fs::metadata(format!("{db_path}-wal"))
                .await
                .map(|x| x.size() as usize)
                .ok()
                .unwrap_or(0),
            db_shm_size: tokio::fs::metadata(format!("{db_path}-shm"))
                .await
                .map(|x| x.size() as usize)
                .ok()
                .unwrap_or(0),
        })
    }

    #[inline(always)]
    fn get_conn(&self) -> &DbConn {
        self._conn.as_ref().unwrap()
    }

    pub async fn get(&self, key: &str) -> Result<Option<ioutil::Data>, Error> {
        let c: Option<cache::Model> = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(self.get_conn())
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
                    av.update(self.get_conn()).await.map_err(Error::Db)?;
                }

                let headers: HashMap<String, String> =
                    bincode::deserialize(&v.attr.unwrap_or(Vec::new()))
                        .map_err(|err| Error::Other(format!("deserialize error {err:?}")))?;
                if let Some(value) = v.value {
                    if let Some(inmemory) = self.inmemory.clone() {
                        let _ = inmemory.set(key, &value, v.expire_time, headers.clone());
                    }

                    Ok(Some(ioutil::Data::new_from_buf(value, headers)))
                } else if let Some(filename) = v.filename {
                    tokio::fs::File::open(self.data_root.join(filename))
                        .await
                        .map(|f| {
                            Some(ioutil::Data::new_from_file(
                                f,
                                v.size.try_into().unwrap(),
                                v.sha256sum.clone(),
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

    pub async fn touch(&self, key: &str) -> Result<bool, Error> {
        let c: Option<LimitedCacheRowWithExpireTime> = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .select_only()
            .column(cache::Column::Key)
            .column(cache::Column::ExpireTime)
            .into_model::<LimitedCacheRowWithExpireTime>()
            .one(self.get_conn())
            .await
            .map_err(Error::Db)?;
        match c {
            None => Ok(false),
            Some(v) => {
                let now = Local::now().timestamp();

                if v.expire_time.filter(|f| f < &now).is_some() {
                    return Ok(false);
                }

                {
                    // アクセス日時の更新
                    let mut av: cache::ActiveModel = cache::ActiveModel {
                        key: Set(v.key),
                        ..Default::default()
                    };
                    av.access_time = Set(Local::now().timestamp());
                    av.update(self.get_conn()).await.map_err(Error::Db)?;
                }
                Ok(true)
            }
        }
    }

    async fn set_internal(&mut self, arg: SetInternalArg) -> Result<(), Error> {
        let old_size = match cache::Entity::find()
            .select_only()
            .column(cache::Column::Size)
            .filter(cache::Column::Key.eq(arg.key.clone()))
            .into_model::<LimitedCacheRow>()
            .one(self.get_conn())
            .await
            .map_err(Error::Db)?
        {
            Some(r) => r.size,
            None => -1,
        };
        debug!(
            "old_size:{} entries={}, bytes={}",
            old_size,
            self.entries(),
            self.size()
        );

        let now: DateTime<Utc> = Utc::now();
        let c = cache::ActiveModel {
            key: Set(arg.key),
            store_time: Set(now.timestamp()),
            expire_time: Set(arg.expire_time),
            access_time: Set(now.timestamp()),
            size: Set(arg.size),
            sha256sum: Set(arg.sha256sum),
            filename: Set(arg.filename),
            value: Set(arg.blob),
            attr: Set(Some(bincode::serialize(&arg.headers).unwrap())),
        };

        let mut stat = self.stat.clone();
        self.stat = self
            .get_conn()
            .transaction::<_, entity::meta::Stat, DbErr>(move |txn| {
                Box::pin(async move {
                    if old_size < 0 {
                        stat.num_of_entries += 1;
                        stat.total_size += arg.size;
                        trace!("insert: {:?}", c);
                        c.insert(txn).await?;
                    } else {
                        stat.total_size = stat.total_size - old_size + arg.size;
                        trace!("update: {:?}", c);
                        c.update(txn).await?;
                    }
                    entity::meta::insert_or_delete_entries(txn, stat).await
                })
            })
            .await
            .map_err(Into::<Error>::into)?;

        debug!(
            "after insert entries={}, bytes={}",
            self.entries(),
            self.size()
        );

        Ok(())
    }

    pub async fn set_blob(
        &mut self,
        key: String,
        sha256sum: Vec<u8>,
        blob: Vec<u8>,
        expire_time: Option<i64>,
        headers: HashMap<String, String>,
    ) -> Result<(), Error> {
        if let Some(inmemory) = self.inmemory.clone() {
            let _ = inmemory.set(&key, &blob, expire_time, headers.clone());
        }

        self.set_internal(SetInternalArg {
            key,
            size: blob.len().try_into().unwrap(),
            sha256sum,
            blob: Some(blob),
            filename: None,
            expire_time,
            headers,
        })
        .await?;
        if self.auto_evict {
            self.evict().await?;
        }
        Ok(())
    }

    pub async fn set_file(
        &mut self,
        key: String,
        size: i64,
        sha256sum: Vec<u8>,
        filename: String,
        expire_time: Option<i64>,
        headers: HashMap<String, String>,
    ) -> Result<(), Error> {
        self.set_internal(SetInternalArg {
            key,
            size,
            sha256sum,
            blob: None,
            filename: Some(filename),
            expire_time,
            headers,
        })
        .await?;
        if self.auto_evict {
            self.evict().await?;
        }
        Ok(())
    }

    pub async fn del(&mut self, key: &str) -> Result<u64, Error> {
        if let Some(inmemory) = self.inmemory.clone() {
            let _ = inmemory.del(key);
        }

        let c = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(self.get_conn())
            .await
            .map_err(Error::Db)?;
        Ok(match c {
            Some(x) => {
                let data_root = self.data_root.clone();
                let mut stat = self.stat.clone();
                let (s, new_stat) = self
                    .get_conn()
                    .transaction::<_, (u64, entity::meta::Stat), Error>(move |txn| {
                        Box::pin(async move {
                            stat.num_of_entries -= 1;
                            stat.total_size -= x.size;

                            let filename = x.filename.clone();
                            let s = x.delete(txn).await.map_err(Error::Db)?.rows_affected;
                            if let Some(filename) = filename {
                                tokio::fs::remove_file(data_root.join(filename))
                                    .await
                                    .map_err(Error::Io)?;
                            }

                            Ok((
                                s,
                                entity::meta::insert_or_delete_entries(txn, stat)
                                    .await
                                    .map_err(Error::Db)?,
                            ))
                        })
                    })
                    .await
                    .map_err(|err| match err {
                        sea_orm::TransactionError::Connection(err) => Error::Db(err),
                        sea_orm::TransactionError::Transaction(err) => err,
                    })?;

                self.stat = new_stat;
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
        mut max_iter: usize,
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

                if (!delete_all) && (goal_size + deleted_size >= self.size()) {
                    break 'outer;
                }
            }
            match max_iter {
                0 => {
                    // run forever
                }
                1 => {
                    break;
                }
                _ => {
                    max_iter -= 1;
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

        let mut stat = self.stat.clone();

        let (res, new_stat) = self
            .get_conn()
            .transaction::<_, (DeleteResult, entity::meta::Stat), DbErr>(move |txn| {
                Box::pin(async move {
                    stat.num_of_entries -= del_entries as i64;
                    stat.total_size -= deleted_size as i64;

                    let res = cache::Entity::delete_many()
                        .filter(cache::Column::Key.is_in(keys))
                        .exec(txn)
                        .await?;
                    Ok((
                        res,
                        entity::meta::insert_or_delete_entries(txn, stat).await?,
                    ))
                })
            })
            .await
            .map_err(Into::<Error>::into)?;

        self.stat = new_stat;

        debug!(
            "del_entries={del_entries}, rows_effected={}, deleted_size={deleted_size}, num_files={}",
            res.rows_affected,
            deleted_files.len()
        );

        for filename in deleted_files {
            match tokio::fs::remove_file(self.data_root.join(&filename)).await {
                Ok(_) => {
                    trace!("file {filename} removed.");
                }
                // ここでエラーで終了させてしまうと、DBの値が更新がself.sizeに反映できない
                Err(e) => error!("failed to remove file {} with {:?}", filename, e),
            }
        }
        Ok((res.rows_affected.try_into().unwrap(), deleted_size))
    }

    pub async fn evict_expired(
        &mut self,
        page_size: u64,
        max_iter: usize,
    ) -> Result<(usize, usize), Error> {
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
                    .paginate(self.get_conn(), page_size),
                true,
                self.capacity,
                max_iter,
            )
            .await?;
        self.evict_rows_delete(keys, files, size).await
    }

    pub async fn evict_old(
        &mut self,
        goal_size: usize,
        page_size: u64,
        max_iter: usize,
    ) -> Result<(usize, usize), Error> {
        let (keys, files, size) = self
            .evict_rows_extract_keys(
                cache::Entity::find()
                    .order_by_asc(cache::Column::AccessTime)
                    .select_only()
                    .column(cache::Column::Key)
                    .column(cache::Column::Size)
                    .column(cache::Column::Filename)
                    .into_model::<LimitedCacheRowWithFilename>()
                    .paginate(self.get_conn(), page_size),
                false,
                goal_size,
                max_iter,
            )
            .await?;
        self.evict_rows_delete(keys, files, size).await
    }

    pub async fn evict_aged(
        &mut self,
        store_time_lt: i64,
        page_size: u64,
        max_iter: usize,
    ) -> Result<(usize, usize), Error> {
        let (keys, files, size) = self
            .evict_rows_extract_keys(
                cache::Entity::find()
                    .order_by_asc(cache::Column::StoreTime)
                    .filter(cache::Column::StoreTime.lt(store_time_lt))
                    .select_only()
                    .column(cache::Column::Key)
                    .column(cache::Column::Size)
                    .column(cache::Column::Filename)
                    .into_model::<LimitedCacheRowWithFilename>()
                    .paginate(self.get_conn(), page_size),
                false,
                0,
                max_iter,
            )
            .await?;
        self.evict_rows_delete(keys, files, size).await
    }

    pub async fn evict(&mut self) -> Result<(usize, usize), Error> {
        debug!(
            "before evict entires={}, bytes={}",
            self.entries(),
            self.size()
        );
        let ((expired_entries, expired_size), (aged_entries, aged_size)) =
            if (self.evicted_at + self.evict_interval) < std::time::Instant::now() {
                let expired_result = self.evict_expired(100, 1).await?;
                let aged_result = if self.max_age > 0 {
                    self.evict_aged(Local::now().timestamp() - self.max_age, 100, 1)
                        .await?
                } else {
                    (0, 0)
                };
                self.evicted_at = std::time::Instant::now();
                (expired_result, aged_result)
            } else {
                ((0, 0), (0, 0))
            };
        debug!(
            "after evict entires={}, bytes={}, expired.entries={expired_entries}, expired.bytes={expired_size}, aged.entries={aged_entries}, aged.size={aged_size}",
            self.entries(),
            self.size(),
        );

        let (old_deleted_entries, old_deleted_size) = if self.size() < self.capacity {
            (0, 0)
        } else {
            self.evicted_size = self.size();
            let goal_size = (self.evict_threshold * (self.capacity as f64)) as usize;
            self.evict_old(goal_size, 1000, 10).await?
        };
        debug!(
            "after evict_old entires={}, bytes={}, old.entries={old_deleted_entries}, old.bytes={old_deleted_size}",
            self.entries(),
            self.size()
        );

        Ok((
            expired_entries + aged_entries + old_deleted_entries,
            expired_size + aged_size + old_deleted_size,
        ))
    }

    pub async fn reset_stat(&mut self) -> Result<(), Error> {
        self.stat = entity::meta::reset_stat(self.get_conn())
            .await
            .map_err(Error::Db)?;
        Ok(())
    }

    pub async fn flushall(&mut self) -> Result<(usize, usize), Error> {
        let mut deleted_entries = 0;
        let mut deleted_size = 0;
        loop {
            if self.size() == 0 {
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
                        .paginate(self.get_conn(), 1000),
                    true,
                    0,
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
        key_contains: Option<String>,
    ) -> Result<Vec<KeyTaskResult>, Error> {
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

        let qs = if let Some(key_contains) = key_contains {
            qs.filter(cache::Column::Key.contains(&key_contains))
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
            .column(cache::Column::ExpireTime)
            .column(cache::Column::AccessTime)
            .column(cache::Column::Size)
            .column(cache::Column::Sha256sum)
            .column(cache::Column::Attr)
            .into_model::<CacheKeyAndStoreTime>()
            .all(self.get_conn())
            .await
            .map_err(Error::Db)?;

        Ok(data.into_iter().map(|e| e.into()).collect())
    }

    pub async fn remove_orphan(&mut self, prefix: u8, dry_run: bool) -> Result<usize, Error> {
        let prefix = format!("{:02x}", prefix);
        let mut files = cache::Entity::find()
            .filter(cache::Column::Filename.is_not_null())
            .filter(cache::Column::Filename.starts_with(&prefix))
            .select_only()
            .column(cache::Column::Key)
            .column(cache::Column::Size)
            .column(cache::Column::Filename)
            .into_model::<LimitedCacheRowWithFilename>()
            .all(self.get_conn())
            .await
            .map_err(Error::Db)?
            .into_iter()
            .filter_map(|x| x.filename.clone().map(|filename| (filename, x)))
            .filter_map(|(filename, x)| filename.split('/').nth(1).map(|y| (y.to_string(), x)))
            .collect::<HashMap<String, _>>();

        let mut entries = match tokio::fs::read_dir(self.data_root.join(&prefix)).await {
            Ok(entries) => entries,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    return Ok(0);
                } else {
                    return Err(Error::Io(err));
                }
            }
        };

        let mut deleted = 0;
        while let Some(entry) = entries.next_entry().await.map_err(Error::Io)? {
            if let Some(filename) = entry
                .path()
                .file_name()
                .and_then(|x| x.to_str())
                .map(|x| x.to_string())
            {
                if files.remove(&filename).is_some() {
                    log::trace!("found {}", entry.path().display());
                } else {
                    log::debug!("remove {}", entry.path().display());
                    deleted += 1;
                    if !dry_run {
                        tokio::fs::remove_file(entry.path())
                            .await
                            .map_err(Error::Io)?;
                    }
                }
            }
        }

        deleted += files.len();
        for (filename, m) in &files {
            log::warn!("key={}, file={} not found", m.key, filename);
        }
        if !dry_run {
            cache::Entity::delete_many()
                .filter(
                    cache::Column::Key
                        .is_in(files.into_values().map(|x| x.key).collect::<Vec<String>>()),
                )
                .exec(self.get_conn())
                .await
                .map_err(Error::Db)?;
        }

        Ok(deleted)
    }

    pub async fn reset_connection(&mut self) -> Result<(), Error> {
        if let Some(old_conn) = self._conn.take() {
            old_conn
                .execute(Statement::from_string(
                    sea_orm::DatabaseBackend::Sqlite,
                    "PRAGMA wal_checkpoint(FULL);".to_string(),
                ))
                .await
                .map_err(Error::Db)?;
            old_conn.close().await.map_err(Error::Db)?;
        }
        self._conn = Some(create_connection(&self.db_path).await?);
        Ok(())
    }

    pub async fn vacuum(&mut self) -> Result<(), Error> {
        self.get_conn()
            .execute(Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "VACUUM".to_string(),
            ))
            .await
            .map_err(Error::Db)?;
        self.reset_connection().await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
                    true,
                    0,
                )
                .await
                .unwrap(),
                tempdir,
            }
        }
    }

    #[tokio::test]
    async fn test_set_get() -> anyhow::Result<()> {
        let mut f = TestFixture::new(1024).await;

        let key = "some-key";
        let value = vec![0, 1, 2, 3];
        let headers = HashMap::new();
        f.cache
            .set_blob(key.into(), vec![], value, None, headers)
            .await
            .unwrap();

        assert!(f.cache.entries() == 1);
        assert!(f.cache.size() == 4);

        // dbに登録されているか
        let r = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(f.cache.get_conn())
            .await
            .unwrap()
            .unwrap();

        assert!(r.size == 4);
        assert!(r.filename.is_none());
        assert!(r.value.unwrap().len() == 4);

        let _stat = f.cache.get_db_size().await?;
        // println!("stat => {_stat:?}");

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

        assert!(f.cache.entries() == 0);
        assert!(f.cache.size() == 0);

        assert!(f.cache.get(key).await.unwrap().is_none());

        Ok(())
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
                vec![],
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

        assert!(f.cache.entries() == 1);
        assert!(f.cache.size() == 4);

        // databaseに値が登録されているか
        let r = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(f.cache.get_conn())
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

        assert!(f.cache.entries() == 0);
        assert!(f.cache.size() == 0);

        assert!(f.cache.get(key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_evict_old() -> anyhow::Result<()> {
        let mut f = TestFixture::new((13.0 / 0.8f64).ceil() as usize).await;
        let value = vec![0, 1, 2, 3, 4, 5];
        let headers = HashMap::new();
        f.cache
            .set_blob("A".into(), vec![], value.clone(), None, headers.clone())
            .await?;
        f.cache
            .set_blob("B".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("C".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("D".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        assert!(f.cache.entries() == 2);
        assert!(f.cache.size() == 12);
        assert!(f.cache.get("A").await.unwrap().is_none());
        assert!(f.cache.get("B").await.unwrap().is_none());
        assert!(f.cache.get("C").await.unwrap().is_some());
        assert!(f.cache.get("D").await.unwrap().is_some());

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let data = f.cache.get("C").await?;
        assert!(data.is_some());

        f.cache
            .set_blob("E".into(), vec![], value.clone(), None, headers.clone())
            .await
            .unwrap();
        assert!(f.cache.get("C").await.unwrap().is_some());
        assert!(f.cache.get("D").await.unwrap().is_none());
        assert!(f.cache.get("E").await.unwrap().is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_evict_aged() -> anyhow::Result<()> {
        let mut f = TestFixture::new((13.0 / 0.8f64).ceil() as usize).await;
        f.cache.auto_evict = false;
        let value = vec![0, 1, 2, 3, 4, 5];
        let headers = HashMap::new();
        f.cache
            .set_blob("A".into(), vec![], value.clone(), None, headers.clone())
            .await?;
        f.cache
            .set_blob("B".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("C".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("D".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        f.cache
            .evict_aged(Local::now().timestamp() + 1, 2, 1)
            .await?;

        assert!(f.cache.entries() == 2);
        assert!(f.cache.size() == 12);
        assert!(f.cache.get("A").await.unwrap().is_none());
        assert!(f.cache.get("B").await.unwrap().is_none());
        assert!(f.cache.get("C").await.unwrap().is_some());
        assert!(f.cache.get("D").await.unwrap().is_some());

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let data = f.cache.get("C").await?;
        assert!(data.is_some());

        f.cache
            .set_blob("E".into(), vec![], value.clone(), None, headers.clone())
            .await
            .unwrap();
        f.cache
            .evict_aged(Local::now().timestamp() + 1, 1, 1)
            .await?;
        assert!(f.cache.get("C").await.unwrap().is_none());
        assert!(f.cache.get("D").await.unwrap().is_some());
        assert!(f.cache.get("E").await.unwrap().is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_evict_aged_2() -> anyhow::Result<()> {
        let mut f = TestFixture::new((100.0f64).ceil() as usize).await;
        f.cache.auto_evict = false;
        let value = vec![0, 1, 2, 3, 4, 5];
        let headers = HashMap::new();
        f.cache
            .set_blob("A".into(), vec![], value.clone(), None, headers.clone())
            .await?;
        f.cache
            .set_blob("B".into(), vec![], value.clone(), None, headers.clone())
            .await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        f.cache
            .set_blob("C".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("D".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        assert!(f.cache.entries() == 4);
        assert!(f.cache.size() == 24);
        assert!(f.cache.get("A").await.unwrap().is_some());
        assert!(f.cache.get("B").await.unwrap().is_some());
        assert!(f.cache.get("C").await.unwrap().is_some());
        assert!(f.cache.get("D").await.unwrap().is_some());

        tokio::time::sleep(Duration::from_secs(1)).await;
        f.cache.evict_interval = std::time::Duration::default();
        f.cache.max_age = 1;
        f.cache.evict().await?;

        assert!(f.cache.entries() == 2);
        assert!(f.cache.size() == 12);
        assert!(f.cache.get("A").await.unwrap().is_none());
        assert!(f.cache.get("B").await.unwrap().is_none());
        assert!(f.cache.get("C").await.unwrap().is_some());
        assert!(f.cache.get("D").await.unwrap().is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_flushall() -> anyhow::Result<()> {
        let mut f = TestFixture::new((13.0 / 0.8f64).ceil() as usize).await;
        let value = vec![0, 1, 2, 3, 4, 5];
        let headers = HashMap::new();
        f.cache
            .set_blob("A".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("B".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("C".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("D".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        assert!(f.cache.entries() == 2);
        assert!(f.cache.size() == 12);

        f.cache.flushall().await?;

        assert!(f.cache.entries() == 0);
        assert!(f.cache.size() == 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_orphan() -> anyhow::Result<()> {
        let mut f = TestFixture::new((13.0 / 0.8f64).ceil() as usize).await;

        let key = "some-key";
        let abs_path = f.cache.data_root.join("00/some-filename");
        fs::create_dir_all(&abs_path.parent().unwrap()).await?;
        tokio::fs::write(&abs_path, b"hello").await?;

        let headers = HashMap::new();
        f.cache
            .set_file(
                key.into(),
                4,
                vec![],
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

        let key2 = "some-key-2";
        let abs_path2 = f.cache.data_root.join("00/some-filename-2");
        tokio::fs::write(&abs_path2, b"world").await?;

        let headers = HashMap::new();
        f.cache
            .set_file(
                key2.into(),
                4,
                vec![],
                abs_path2
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

        let key3 = "some-key-3";
        let abs_path3 = f.cache.data_root.join("01/some-filename-3");
        fs::create_dir_all(&abs_path3.parent().unwrap()).await?;
        tokio::fs::write(&abs_path3, b"world").await?;

        let headers = HashMap::new();
        f.cache
            .set_file(
                key3.into(),
                4,
                vec![],
                abs_path3
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

        let path =
            PathBuf::from(f.tempdir.path()).join("data/00/00a8eefb-f5e3-4ecb-bdc0-31909b261b5f");
        tokio::fs::write(&path, b"hello").await?;

        assert_eq!(1, f.cache.remove_orphan(0, true).await?);
        assert!(tokio::fs::try_exists(&path).await?);

        assert_eq!(1, f.cache.remove_orphan(0, false).await?);
        assert!(!tokio::fs::try_exists(&path).await?);

        tokio::fs::remove_file(&abs_path).await?;
        assert_eq!(1, f.cache.remove_orphan(0, true).await?);
        assert_eq!(1, f.cache.remove_orphan(0, false).await?);
        assert_eq!(0, f.cache.remove_orphan(0, false).await?);

        assert!(tokio::fs::try_exists(&abs_path2).await?);
        assert!(f.cache.get(key2).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_keys() -> anyhow::Result<()> {
        let mut f = TestFixture::new(128).await;
        let value = vec![0, 1, 2, 3, 4, 5];
        let headers = HashMap::new();
        f.cache
            .set_blob("A".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("B".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("C".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        f.cache
            .set_blob("D".into(), vec![], value.clone(), None, headers.clone())
            .await?;

        let keys = f.cache.keys(100, None, None, None, None).await?;

        assert!(keys.len() == 4);
        assert!(keys[0].key == "A");

        let keys = f
            .cache
            .keys(
                100,
                Some(keys[1].key.clone()),
                Some(keys[1].store_time),
                None,
                None,
            )
            .await?;
        assert!(keys.len() == 2);
        assert!(keys[0].key == "C");

        let keys = f
            .cache
            .keys(100, None, None, None, Some("D".to_string()))
            .await?;
        assert!(keys.len() == 1);
        assert!(keys[0].key == "D");

        Ok(())
    }
}
