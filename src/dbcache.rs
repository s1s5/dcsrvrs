use chrono::{DateTime, Local};
use entity::cache;
use migration::{DbErr, Migrator, MigratorTrait};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};
use sea_orm::{Database, DbConn};
use std::cmp;
use std::error;
use std::fmt;
use std::pin::Pin;
use tokio::io::{BufReader, Ready};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt},
    sync::Mutex,
};

#[derive(Debug, Clone)]
struct DbFieldError;

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
        cx: &mut std::task::Context<'_>,
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

pub struct DBCache {
    conn: DbConn,
    sqlite_cache_size: usize,
    buf_list: Mutex<Vec<Vec<u8>>>,
}

impl DBCache {
    async fn new() -> Result<DBCache, DbErr> {
        let database_url = std::env::var("DATABASE_URL").unwrap();
        let conn = Database::connect(&database_url)
            .await
            .expect("Failed to setup the database");
        Migrator::up(&conn, None)
            .await
            .expect("Failed to run migrations for tests");

        Ok(DBCache {
            conn: conn,
            sqlite_cache_size: 1 << 15,
            buf_list: Mutex::new(Vec::new()),
        })
    }

    async fn get_buf(&self) -> Vec<u8> {
        let mut buf_list = self.buf_list.lock().await;
        match buf_list.pop() {
            Some(x) => x,
            None => {
                vec![0; self.sqlite_cache_size]
            }
        }
    }

    async fn del_buf(&self, buf: Vec<u8>) {
        let mut buf_list = self.buf_list.lock().await;
        if buf_list.len() < 10 {
            buf_list.push(buf)
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Box<dyn AsyncRead>>, Box<dyn error::Error>> {
        let c: Option<cache::Model> = cache::Entity::find()
            .filter(cache::Column::Key.eq(key))
            .one(&self.conn)
            .await?;
        match c {
            None => Ok(None),
            Some(v) => {
                let now = Local::now().timestamp();

                if v.expire_time.filter(|f| f > &now).is_some() {
                    return Ok(None);
                }

                if v.value.is_some() {
                    Ok(Some(
                        Box::new(ByteReader::new(v.value.unwrap())) as Box<dyn AsyncRead>
                    ))
                } else if v.filename.is_some() {
                    tokio::fs::File::open(v.filename.unwrap())
                        .await
                        .and_then(|f| Ok(Some(Box::new(f) as Box<dyn AsyncRead>)))
                        .or_else(|e| Err(e.into()))
                } else {
                    Err(DbFieldError {}.into())
                }
            }
        }
    }

    async fn set_as_blob(&self, key: &str, buf: &Vec<u8>) {}

    async fn set_as_file<T: AsyncRead>(
        &self,
        key: &str,
        buf: &Option<Vec<u8>>,
        mut readable: Pin<&mut T>,
    ) {
    }

    async fn set<T: AsyncRead>(
        &self,
        key: &str,
        mut readable: Pin<&mut T>,
        size: Option<usize>,
    ) -> io::Result<()> {
        let (opt_data, size_thres) = {
            match size {
                Some(s) => (None, s),
                None => {
                    let mut buf = self.get_buf().await;
                    let read = readable.read(&mut buf).await?;
                    (Some(buf), read)
                }
            }
        };

        if size_thres > self.sqlite_cache_size {
            self.set_as_file(key, &opt_data, readable);
            if opt_data.is_some() {
                // and_thenとかとどっちがいいのか・・
                self.del_buf(opt_data.unwrap());
            }
        } else {
            let buf = match opt_data {
                Some(b) => b,
                None => {
                    let mut buf = self.get_buf().await;
                    readable.read(&mut buf).await?;
                    buf
                }
            };
            self.set_as_blob(key, &buf);
            self.del_buf(buf);
        }

        Ok(())
    }

    async fn del(&self, key: &str) {}
}
