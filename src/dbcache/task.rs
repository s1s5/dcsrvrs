use std::collections::HashMap;

use super::errors::Error;
use super::ioutil;
use derivative::Derivative;
use tokio::sync::oneshot;

fn show_len(blob: &[u8], fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
    write!(fmt, "{}", blob.len())
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct GetTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<Option<ioutil::Data>, Error>>,
    pub key: String,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct TouchTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<bool, Error>>,
    pub key: String,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SetBlobTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<(), Error>>,
    pub key: String,
    #[derivative(Debug(format_with = "show_len"))]
    pub blob: Vec<u8>,
    pub sha256sum: Vec<u8>,
    pub expire_time: Option<i64>,
    pub headers: HashMap<String, String>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SetFileTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<(), Error>>,
    pub key: String,
    pub size: usize,
    pub filename: String,
    pub sha256sum: Vec<u8>,
    pub expire_time: Option<i64>,
    pub headers: HashMap<String, String>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct DelTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<u64, Error>>,
    pub key: String,
}

#[derive(Derivative, PartialEq)]
#[derivative(Debug)]
pub struct Stat {
    pub entries: usize,
    pub size: usize,
    pub capacity: usize,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct StatTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<Stat, Error>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct FlushAllTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<(usize, usize), Error>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct KeyTaskResult {
    pub key: String,
    pub store_time: i64,
    pub expire_time: Option<i64>,
    pub access_time: i64,
    pub size: i64,
    #[derivative(Debug = "ignore")]
    pub sha256sum: Vec<u8>,
    pub headers: HashMap<String, String>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct KeysTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<Vec<KeyTaskResult>, Error>>,
    pub max_num: i64,
    pub key: Option<String>,
    pub store_time: Option<i64>,
    pub prefix: Option<String>,
    pub key_contains: Option<String>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct EvictExpiredTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<(usize, usize), Error>>,
    pub page_size: u64,
    pub max_iter: usize,
}
#[derive(Derivative)]
#[derivative(Debug)]
pub struct EvictOldTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<(usize, usize), Error>>,
    pub goal_size: usize,
    pub page_size: u64,
    pub max_iter: usize,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct EvictAgedTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<(usize, usize), Error>>,
    pub store_time_lt: i64,
    pub page_size: u64,
    pub max_iter: usize,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct EvictTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<(usize, usize), Error>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ResetConnectionTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<(), Error>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct VacuumTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<(), Error>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct EndTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<()>,
}

#[derive(Debug)]
pub enum Task {
    Get(GetTask),
    Touch(TouchTask),
    SetBlob(SetBlobTask),
    SetFile(SetFileTask),
    Del(DelTask),
    Stat(StatTask),
    FlushAll(FlushAllTask),
    Keys(KeysTask),
    ResetConnection(ResetConnectionTask),
    Vacuum(VacuumTask),
    EvictExpired(EvictExpiredTask),
    EvictOld(EvictOldTask),
    EvictAged(EvictAgedTask),
    Evict(EvictTask),
    End(EndTask),
}
