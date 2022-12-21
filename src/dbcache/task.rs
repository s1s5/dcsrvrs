use super::errors::Error;
use std::pin::Pin;
use tokio::io::AsyncRead;
use tokio::sync::oneshot;

pub struct GetTask {
    pub tx: oneshot::Sender<Result<Option<Pin<Box<dyn AsyncRead + Send>>>, Error>>,
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
pub struct EndTask {
    pub tx: oneshot::Sender<()>,
}

pub enum Task {
    Get(GetTask),
    SetBlob(SetBlobTask),
    SetFile(SetFileTask),
    Del(DelTask),
    End(EndTask),
}