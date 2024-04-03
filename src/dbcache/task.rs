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
pub struct SetBlobTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<(), Error>>,
    pub key: String,
    #[derivative(Debug(format_with = "show_len"))]
    pub blob: Vec<u8>,
    pub expire_time: Option<i64>,
    pub headers: crate::headers::Headers,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SetFileTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<(), Error>>,
    pub key: String,
    pub size: usize,
    pub filename: String,
    pub expire_time: Option<i64>,
    pub headers: crate::headers::Headers,
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
pub struct KeysTask {
    #[derivative(Debug = "ignore")]
    pub tx: oneshot::Sender<Result<Vec<(String, i64)>, Error>>,
    pub max_num: i64,
    pub key: Option<String>,
    pub store_time: Option<i64>,
    pub prefix: Option<String>,
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
    SetBlob(SetBlobTask),
    SetFile(SetFileTask),
    Del(DelTask),
    Stat(StatTask),
    FlushAll(FlushAllTask),
    Keys(KeysTask),
    End(EndTask),
}
