use std::collections::HashMap;

pub struct Data {
    headers: HashMap<String, String>,
    size: usize,
    data: DataInternal,
}

impl Data {
    pub fn new_from_buf(data: Vec<u8>, headers: HashMap<String, String>) -> Self {
        Self {
            headers,
            size: data.len(),
            data: DataInternal::Bytes(data),
        }
    }

    pub fn new_from_file(
        file: tokio::fs::File,
        size: usize,
        headers: HashMap<String, String>,
    ) -> Self {
        Self {
            headers,
            size,
            data: DataInternal::File(file),
        }
    }

    pub fn unpack(self) -> (HashMap<String, String>, usize, DataInternal) {
        (self.headers, self.size, self.data)
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
        for (key, value) in self.headers.iter() {
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
