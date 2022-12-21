use chrono::{DateTime, Local, Utc};
use entity::cache;
use log::error;
use migration::{Migrator, MigratorTrait, Order};
use sea_orm::entity::ModelTrait;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, FromQueryResult, Paginator,
    PaginatorTrait, QueryFilter, QuerySelect, SelectModel, Set, SqlxSqliteConnector,
};
use sea_orm::{DbConn, QueryOrder};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::io::AsyncRead;

use super::errors::{DbFieldError, Error};
use super::ioutil::ByteReader;

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

        let conn = SqlxSqliteConnector::from_sqlx_sqlite_pool(sqlite_pool);
        Migrator::up(&conn, None)
            .await
            .expect("Failed to run migrations for tests");

        let (mut entries, mut size) = (0, 0);
        let mut pages = cache::Entity::find()
            .select_only()
            .column(cache::Column::Size)
            .into_model::<LimitedCacheRow>()
            .paginate(&conn, 1000);
        while let Some(el) = pages.fetch_and_next().await? {
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
            .column(cache::Column::Size)
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

    pub async fn set_blob(
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

    pub async fn set_file(
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

    pub async fn del(&mut self, key: &str) -> Result<u64, Error> {
        let c = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(&self.conn)
            .await
            .or_else(|e| Err(Error::Db(e)))?;
        Ok(match c {
            Some(x) => {
                self.entries -= 1;
                self.size -= x.size as usize;

                let filename = x.filename.clone();
                let s = x
                    .delete(&self.conn)
                    .await
                    .or_else(|e| Err(Error::Db(e)))?
                    .rows_affected;
                if let Some(filename) = filename {
                    tokio::fs::remove_file(self.data_root.join(filename))
                        .await
                        .or_else(|e| Err(Error::Io(e)))?;
                }
                s
            }
            None => 0,
        })
    }

    async fn truncate_rows(
        &self,
        mut pages: Paginator<'_, DatabaseConnection, SelectModel<LimitedCacheRowWithFilename>>,
        delete_all: bool,
    ) -> Result<(usize, usize), Error> {
        let mut keys: Vec<String> = Vec::new();
        let mut deleted_files: Vec<String> = Vec::new();
        let mut deleted_size: usize = 0;

        if (!delete_all) && (self.capacity >= self.size) {
            return Ok((0, 0));
        }

        'outer: while let Some(el) = pages
            .fetch_and_next()
            .await
            .or_else(|e| Err(Error::Db(e)))?
        {
            for e in el {
                deleted_size += e.size as usize;
                println!(
                    "delete: {}, cap={}, size={}, deleted={}, cur={}",
                    &e.key, self.capacity, self.size, deleted_size, e.size
                );
                keys.push(e.key);
                if e.filename.is_some() {
                    deleted_files.push(e.filename.unwrap());
                }

                if (!delete_all) && (self.capacity + deleted_size >= self.size) {
                    break 'outer;
                }
            }
        }
        let res = cache::Entity::delete_many()
            .filter(cache::Column::Key.is_in(keys))
            .exec(&self.conn)
            .await
            .or_else(|e| Err(Error::Db(e)))?;

        for filename in deleted_files {
            match tokio::fs::remove_file(self.data_root.join(&filename)).await {
                Ok(_) => {}
                // ここでエラーで終了させてしまうと、DBの値が更新がself.sizeに反映できない
                Err(e) => error!("failed to remove file {} with {:?}", filename, e),
            }
        }
        Ok((res.rows_affected.try_into().unwrap(), deleted_size))
    }

    async fn truncate_expired(&mut self) -> Result<(usize, usize), Error> {
        let now = Local::now().timestamp();
        let pages = cache::Entity::find()
            .filter(cache::Column::ExpireTime.is_not_null())
            .filter(cache::Column::ExpireTime.lt(now))
            .select_only()
            .column(cache::Column::Key)
            .column(cache::Column::Size)
            .column(cache::Column::Filename)
            .into_model::<LimitedCacheRowWithFilename>()
            .paginate(&self.conn, 100);

        self.truncate_rows(pages, true).await
    }

    async fn truncate_old(&mut self) -> Result<(usize, usize), Error> {
        let pages = cache::Entity::find()
            .order_by(cache::Column::AccessTime, Order::Asc)
            .select_only()
            .column(cache::Column::Key)
            .column(cache::Column::Size)
            .column(cache::Column::Filename)
            .into_model::<LimitedCacheRowWithFilename>()
            .paginate(&self.conn, 100);

        self.truncate_rows(pages, false).await
    }

    async fn truncate(&mut self) -> Result<(usize, usize), Error> {
        // TODO: 毎回やるのは重いかもしれないので、適当にスキップすべきかもしれない
        let (expired_entries, expired_size) = self.truncate_expired().await?;
        self.size -= expired_size; // truncateのmethod内で修正できない

        let (old_deleted_entries, old_deleted_size) = self.truncate_old().await?;
        self.size -= old_deleted_size;
        Ok((
            expired_entries + old_deleted_entries,
            expired_size + old_deleted_size,
        ))
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
                )
                .await
                .unwrap(),
                tempdir: tempdir,
            }
        }
    }

    #[tokio::test]
    async fn test_set_get() {
        let mut f = TestFixture::new(1024).await;

        let key = "some-key";
        let value = vec![0, 1, 2, 3];
        f.cache.set_blob(key.into(), value, None).await.unwrap();

        // dbに登録されているか
        let r = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(&f.cache.conn)
            .await
            .unwrap()
            .unwrap();

        assert!(r.size == 4);
        assert!(r.filename == None);
        assert!(r.value.unwrap().len() == 4);

        // dbcache経由での値の取得
        let mut r = f.cache.get(key).await.unwrap().unwrap();
        let mut buf: Vec<u8> = vec![0; 16];
        let num_read = r.read(&mut buf).await.unwrap();
        assert!(num_read == 4);
        assert!(buf[..4] == [0, 1, 2, 3]);

        f.cache.del(key).await.unwrap();

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
            writer.write_all(&vec![0, 1, 2, 3]).await.unwrap();
        }
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
            )
            .await
            .unwrap();

        // databaseに値が登録されているか
        let r = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(&f.cache.conn)
            .await
            .unwrap()
            .unwrap();
        println!("{:?}", r);
        assert!(r.size == 4);
        assert!(r.filename.is_some() && r.filename == Some(String::from("foo")));
        assert!(r.value.is_none());

        // dbcache経由での値の取得
        let mut r = f.cache.get(key).await.unwrap().unwrap();
        let mut buf: Vec<u8> = vec![0; 16];
        let num_read = r.read(&mut buf).await.unwrap();
        assert!(num_read == 4);
        assert!(buf[..4] == [0, 1, 2, 3]);

        f.cache.del(key).await.unwrap();
        assert!(!abs_path.exists());

        assert!(f.cache.get(key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_truncate_old() {
        let mut f = TestFixture::new(12).await;
        let value = vec![0, 1, 2, 3, 4, 5];
        f.cache
            .set_blob("A".into(), value.clone(), None)
            .await
            .unwrap();
        f.cache
            .set_blob("B".into(), value.clone(), None)
            .await
            .unwrap();
        f.cache
            .set_blob("C".into(), value.clone(), None)
            .await
            .unwrap();
        f.cache
            .set_blob("D".into(), value.clone(), None)
            .await
            .unwrap();

        assert!(f.cache.get("A".into()).await.unwrap().is_none());
        assert!(f.cache.get("B".into()).await.unwrap().is_none());
        assert!(f.cache.get("C".into()).await.unwrap().is_some());
        assert!(f.cache.get("D".into()).await.unwrap().is_some());
    }
}
