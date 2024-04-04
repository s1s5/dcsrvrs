pub struct Data {
    headers: crate::headers::Headers,
    size: usize,
    data: DataInternal,
}

impl Data {
    pub fn new_from_buf(data: Vec<u8>, headers: crate::headers::Headers) -> Self {
        Self {
            headers,
            size: data.len(),
            data: DataInternal::Bytes(data),
        }
    }
    pub fn new_from_file(
        file: tokio::fs::File,
        size: usize,
        headers: crate::headers::Headers,
    ) -> Self {
        Self {
            headers,
            size,
            data: DataInternal::File(file),
        }
    }

    pub fn into_inner(self) -> DataInternal {
        self.data
    }
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
