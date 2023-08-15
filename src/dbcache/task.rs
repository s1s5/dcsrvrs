use super::errors::Error;
use super::ioutil;
use tokio::sync::oneshot;

pub struct GetTask {
    pub tx: oneshot::Sender<Result<Option<ioutil::Data>, Error>>,
    pub key: String,
}
pub struct SetBlobTask {
    pub tx: oneshot::Sender<Result<(), Error>>,
    pub key: String,
    pub blob: Vec<u8>,
    pub expire_time: Option<i64>,
}
pub struct SetFileTask {
    pub tx: oneshot::Sender<Result<(), Error>>,
    pub key: String,
    pub size: usize,
    pub filename: String,
    pub expire_time: Option<i64>,
}
pub struct DelTask {
    pub tx: oneshot::Sender<Result<u64, Error>>,
    pub key: String,
}

pub struct Stat {
    pub entries: usize,
    pub size: usize,
    pub capacity: usize,
}

pub struct StatTask {
    pub tx: oneshot::Sender<Result<Stat, Error>>,
}

pub struct FlushAllTask {
    pub tx: oneshot::Sender<Result<(usize, usize), Error>>,
}

pub struct KeysTask {
    pub tx: oneshot::Sender<Result<Vec<(String, i64)>, Error>>,
    pub max_num: i64,
    pub key: Option<String>,
    pub store_time: Option<i64>,
    pub prefix: Option<String>,
}
pub struct EndTask {
    pub tx: oneshot::Sender<()>,
}

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
