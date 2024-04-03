use sea_orm::DbErr;
use tokio::sync::oneshot;

#[derive(Debug, Clone)]
pub struct DbFieldError;

impl std::fmt::Display for DbFieldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DbFieldError")
    }
}

// This is important for other errors to wrap this one.
impl std::error::Error for DbFieldError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        // 基本となるエラー、原因は記録されていない。
        None
    }
}

#[derive(Debug)]
pub enum Error {
    Db(DbErr),
    Io(std::io::Error),
    Schema(DbFieldError),
    RecvError(oneshot::error::RecvError),
    // SendError(mpsc::error::SendError<Task>),
    SendError, // channel closed
    FileSizeLimitExceeded,
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error {:?}", self)
    }
}

// This is important for other errors to wrap this one.
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        // 基本となるエラー、原因は記録されていない。
        None
    }
}
