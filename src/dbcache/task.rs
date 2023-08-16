use super::errors::Error;
use super::ioutil;
use std::fmt;
use tokio::sync::oneshot;

pub struct GetTask {
    pub tx: oneshot::Sender<Result<Option<ioutil::Data>, Error>>,
    pub key: String,
}

impl fmt::Debug for GetTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Point").field("key", &self.key).finish()
    }
}

#[derive(Debug)]
pub struct SetBlobTask {
    pub tx: oneshot::Sender<Result<(), Error>>,
    pub key: String,
    pub blob: Vec<u8>,
    pub expire_time: Option<i64>,
    pub headers: crate::headers::Headers,
}

#[derive(Debug)]
pub struct SetFileTask {
    pub tx: oneshot::Sender<Result<(), Error>>,
    pub key: String,
    pub size: usize,
    pub filename: String,
    pub expire_time: Option<i64>,
    pub headers: crate::headers::Headers,
}

#[derive(Debug)]
pub struct DelTask {
    pub tx: oneshot::Sender<Result<u64, Error>>,
    pub key: String,
}

#[derive(PartialEq, Debug)]
pub struct Stat {
    pub entries: usize,
    pub size: usize,
    pub capacity: usize,
}

#[derive(Debug)]
pub struct StatTask {
    pub tx: oneshot::Sender<Result<Stat, Error>>,
}

#[derive(Debug)]
pub struct FlushAllTask {
    pub tx: oneshot::Sender<Result<(usize, usize), Error>>,
}

#[derive(Debug)]
pub struct KeysTask {
    pub tx: oneshot::Sender<Result<Vec<(String, i64)>, Error>>,
    pub max_num: i64,
    pub key: Option<String>,
    pub store_time: Option<i64>,
    pub prefix: Option<String>,
}
#[derive(Debug)]
pub struct EndTask {
    pub tx: oneshot::Sender<()>,
}

#[derive(Debug)]
pub enum Task {
    Get(GetTask),
    SetBlob(SetBlobTask),
    SetFile(SetFileTask),
    Del(DelTask),
    Stat(StatTask),
    FlushAll(FlushAllTask),
    Keys(KeysTask),
    End(EndTask),
}
