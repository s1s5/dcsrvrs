pub struct Data {
    pub headers: crate::headers::Headers,
    pub size: usize,
    pub data: DataInternal,
}
pub enum DataInternal {
    Bytes(Vec<u8>),
    File(tokio::fs::File),
}

impl axum::response::IntoResponse for Data {
    fn into_response(self) -> axum::response::Response {
        let mut builder = axum::response::Response::builder();
        for (key, value) in self.headers.0.iter() {
            builder = builder.header(key, value);
        }
        builder = builder.header("Content-length", self.size);
        match self.data {
            DataInternal::Bytes(b) => builder.body(axum::body::Body::from(b)).unwrap(),
            DataInternal::File(f) => builder
                .body(axum::body::Body::from_stream(
                    tokio_util::io::ReaderStream::new(f),
                ))
                .unwrap(),
        }
    }
}
