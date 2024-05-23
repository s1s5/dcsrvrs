use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::io::{self, AsyncRead, AsyncReadExt};
use tokio::sync::{mpsc, oneshot};
use tracing::error;
use uuid::Uuid;

use super::errors::Error;
use super::task::*;
use crate::imcache::InmemoryCache;
use crate::ioutil;

pub struct DBCacheClient {
    blob_threshold: usize,
    size_limit: usize,
    data_root: PathBuf,
    tx: mpsc::Sender<Task>,
    buf_list: Mutex<Vec<Vec<u8>>>,
    inmemory: Option<Arc<InmemoryCache>>,
}

impl DBCacheClient {
    pub fn new(
        data_root: &Path,
        tx: mpsc::Sender<Task>,
        blob_threshold: usize,
        size_limit: usize,
        inmemory: Option<Arc<InmemoryCache>>,
    ) -> DBCacheClient {
        DBCacheClient {
            blob_threshold,
            size_limit,
            data_root: data_root.into(),
            tx,
            buf_list: Mutex::new(Vec::new()),
            inmemory,
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
        if let Some(inmemory) = self.inmemory.clone() {
            if let Some(data) = inmemory.get(key) {
                let (tx, rx) = oneshot::channel();
                self.tx
                    .send(Task::Touch(TouchTask {
                        tx,
                        key: key.into(),
                    }))
                    .await
                    .map_err(|_e| Error::SendError)?;
                let _ = rx.await;
                return Ok(Some(data));
            }
        }

        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::Get(GetTask {
                tx,
                key: key.into(),
            }))
            .await
            .map_err(|_e| Error::SendError)?;
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
        headers: HashMap<String, String>,
    ) -> Result<(), Error> {
        let mut hasher = Sha256::new();
        hasher.update(buf);
        let sha256sum = hasher.finalize().to_vec();

        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::SetBlob(SetBlobTask {
                tx,
                key: key.into(),
                blob: buf.into(),
                expire_time,
                headers,
                sha256sum,
            }))
            .await
            .map_err(|_e| Error::SendError)?;
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
        headers: HashMap<String, String>,
    ) -> Result<(), Error> {
        let (abs_path, rel_path) = {
            let id: String = Uuid::new_v4().to_string();
            let prefix = &id[..2];
            let rel_path = PathBuf::from(prefix).join(id);
            (self.data_root.join(&rel_path), rel_path)
        };

        let (size, delete_file_on_failed, sha256sum) = {
            fs::create_dir_all(abs_path.parent().unwrap()).map_err(Error::Io)?;
            let mut hasher = Sha256::new();
            let mut writer = File::create(&abs_path).await.map_err(Error::Io)?;
            let delete_file_on_failed = DeleteFileOnFailed::new(&abs_path);
            hasher.update(buf);
            writer.write_all(buf).await.map_err(Error::Io)?;
            // let num_wrote = copy(&mut readable, &mut writer).await.map_err(Error::Io)?;
            let mut size = buf.len();
            let mut buf = [0u8; 32768];
            while size < self.size_limit {
                let read = readable.read(&mut buf[..]).await.map_err(Error::Io)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buf[..read]);
                writer.write_all(&buf[..read]).await.map_err(Error::Io)?;
                size += read;
            }
            writer.flush().await.map_err(Error::Io)?;
            drop(writer);

            if size >= self.size_limit {
                return Err(Error::FileSizeLimitExceeded);
            }
            (size, delete_file_on_failed, hasher.finalize().to_vec())
        };

        let res = {
            let (tx, rx) = oneshot::channel();
            self.tx
                .send(Task::SetFile(SetFileTask {
                    tx,
                    key: key.into(),
                    size,
                    expire_time,
                    filename: rel_path.to_str().unwrap().into(),
                    headers,
                    sha256sum,
                }))
                .await
                .map_err(|_e| Error::SendError)?;
            match rx.await {
                Ok(r) => r,
                Err(e) => Err(Error::RecvError(e)),
            }
        };

        match res {
            Ok(t) => {
                delete_file_on_failed.commit();
                Ok(t)
            }
            Err(e) => Err(e),
        }
    }

    async fn read_eager<T: AsyncRead>(
        &self,
        readable: &mut Pin<&mut T>,
        buf: &mut [u8],
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
        headers: HashMap<String, String>,
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
                .map_err(Error::Io)?;
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
                tx,
                key: key.into(),
            }))
            .await
            .map_err(|_e| Error::SendError)?;
        match rx.await {
            Ok(r) => r,
            Err(e) => Err(Error::RecvError(e)),
        }
    }

    pub async fn stat(&self) -> Result<Stat, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::Stat(StatTask { tx }))
            .await
            .map_err(|_e| Error::SendError)?;
        match rx.await {
            Ok(r) => r,
            Err(e) => Err(Error::RecvError(e)),
        }
    }

    pub async fn flushall(&self) -> Result<(usize, usize), Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::FlushAll(FlushAllTask { tx }))
            .await
            .map_err(|_e| Error::SendError)?;
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
        key_contains: Option<String>,
    ) -> Result<Vec<KeyTaskResult>, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Task::Keys(KeysTask {
                tx,
                max_num,
                key,
                store_time,
                prefix,
                key_contains,
            }))
            .await
            .map_err(|_e| Error::SendError)?;
        match rx.await {
            Ok(r) => r,
            Err(e) => Err(Error::RecvError(e)),
        }
    }
}

struct DeleteFileOnFailed {
    filename: PathBuf,
    failed: bool,
}

impl DeleteFileOnFailed {
    fn new(filename: &Path) -> Self {
        Self {
            filename: PathBuf::from(filename),
            failed: true,
        }
    }
    fn commit(mut self) {
        self.failed = false;
    }
}

impl Drop for DeleteFileOnFailed {
    fn drop(&mut self) {
        if self.failed {
            match std::fs::remove_file(&self.filename) {
                Ok(_) => {}
                Err(err) => {
                    error!("failed to delete file {:?}: Error={:?}", self.filename, err);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use anyhow::{bail, Result};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tokio::io::BufReader;

    #[tokio::test]
    async fn test_set_blob() -> Result<()> {
        let tempdir = tempfile::Builder::new()
            .prefix("dbcache-test")
            .tempdir()
            .unwrap();

        let (tx, rx) = mpsc::channel(1);
        let client = DBCacheClient::new(tempdir.path(), tx, 4092, 8192, None);

        let data = rand_vec(10);

        let handle = tokio::spawn(async move {
            let mut rx = rx;
            match rx.recv().await.ok_or(anyhow::anyhow!("receive none"))? {
                Task::SetBlob(blob) => {
                    blob.tx.send(Ok(())).unwrap();
                    Ok(blob.blob)
                }
                _ => bail!("unexpected task"),
            }
        });

        let mut stream = BufReader::new(data.as_slice());
        client
            .set("0", Pin::new(&mut stream), None, HashMap::new())
            .await?;
        let data_received = handle.await??;
        assert!(vec_equal(&data, &data_received));

        Ok(())
    }

    #[tokio::test]
    async fn test_set_file() -> Result<()> {
        let tempdir = tempfile::Builder::new()
            .prefix("dbcache-test")
            .tempdir()
            .unwrap();

        let (tx, rx) = mpsc::channel(1);
        let client = DBCacheClient::new(tempdir.path(), tx, 16, 8192, None);

        let data = rand_vec(8000);

        let data_root = PathBuf::from(tempdir.path());
        let handle = tokio::spawn(async move {
            let mut rx = rx;
            match rx.recv().await.ok_or(anyhow::anyhow!("receive none"))? {
                Task::SetFile(ft) => {
                    let data = std::fs::read(data_root.join(ft.filename))?;
                    ft.tx.send(Ok(())).unwrap();
                    Ok(data)
                }
                _ => bail!("unexpected task"),
            }
        });

        let mut stream = BufReader::new(data.as_slice());
        client
            .set("0", Pin::new(&mut stream), None, HashMap::new())
            .await?;
        let data_received = handle.await??;
        assert!(vec_equal(&data, &data_received));
        Ok(())
    }

    fn rand_vec(bytes: usize) -> Vec<u8> {
        let mut data = vec![0u8; bytes];
        let mut rng = StdRng::from_entropy();
        rng.fill(&mut data[..]);
        data
    }

    fn vec_equal(va: &[u8], vb: &[u8]) -> bool {
        (va.len() == vb.len()) &&  // zip stops at the shortest
     va.iter()
       .zip(vb)
       .all(|(a,b)| *a == *b)
    }
}
