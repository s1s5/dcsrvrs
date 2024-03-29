use std::fs;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::fs::{remove_file, File};
use tokio::io::{self, AsyncRead, AsyncReadExt};
use tokio::io::{copy, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::errors::Error;
use super::{ioutil, task::*};

#[derive(Debug, Clone)]
pub struct DBCacheClient {
    blob_threshold: usize,
    size_limit: usize,
    data_root: PathBuf,
    tx: mpsc::Sender<Task>,
    buf_list: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl DBCacheClient {
    pub fn new(
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
            buf_list: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn get_buf(&self) -> Vec<u8> {
        let mut buf_list = self.buf_list.lock().unwrap();
        match buf_list.pop() {
            Some(x) => x,
            None => {
                vec![0; self.blob_threshold]
            }
        }
    }

    async fn del_buf(&self, buf: Vec<u8>) {
        let mut buf_list = self.buf_list.lock().unwrap();
        if buf_list.len() < 10 {
            buf_list.push(buf)
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<ioutil::Data>, Error> {
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
        headers: crate::headers::Headers,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::SetBlob(SetBlobTask {
                tx,
                key: key.into(),
                blob: buf.into(),
                expire_time,
                headers,
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
        headers: crate::headers::Headers,
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
                    tx,
                    key: key.into(),
                    size: size,
                    expire_time,
                    filename: PathBuf::from(prefix).join(id).to_str().unwrap().into(),
                    headers,
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
        headers: crate::headers::Headers,
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
            self.set_as_file(key, &buf[..buf_size], readable, expire_time, headers)
                .await
        } else {
            self.set_as_blob(key, &buf[..buf_size], expire_time, headers)
                .await
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

    pub async fn stat(&self) -> Result<Stat, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::Stat(StatTask { tx: tx }))
            .await
            .or_else(|_e| Err(Error::SendError))?;
        match rx.await {
            Ok(r) => r,
            Err(e) => Err(Error::RecvError(e)),
        }
    }

    pub async fn flushall(&self) -> Result<(usize, usize), Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::FlushAll(FlushAllTask { tx: tx }))
            .await
            .or_else(|_e| Err(Error::SendError))?;
        match rx.await {
            Ok(r) => r,
            Err(e) => Err(Error::RecvError(e)),
        }
    }

    pub async fn keys(
        &self,
        max_num: i64,
        key: Option<String>,
        store_time: Option<i64>,
        prefix: Option<String>,
    ) -> Result<Vec<(String, i64)>, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::Keys(KeysTask {
                tx: tx,
                max_num: max_num,
                key: key,
                store_time: store_time,
                prefix: prefix,
            }))
            .await
            .or_else(|_e| Err(Error::SendError))?;
        match rx.await {
            Ok(r) => r,
            Err(e) => Err(Error::RecvError(e)),
        }
    }
}
