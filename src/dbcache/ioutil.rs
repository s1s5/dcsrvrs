use std::pin::Pin;
use tokio::io::{AsyncRead, ReadBuf};

pub struct ByteReader {
    pos: usize,
    data: Vec<u8>,
}

impl ByteReader {
    pub fn new(data: Vec<u8>) -> ByteReader {
        ByteReader { pos: 0, data: data }
    }
}

impl AsyncRead for ByteReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let me = self.get_mut();
        loop {
            let n = std::cmp::min(me.data.len() - me.pos, buf.remaining());
            buf.put_slice(&me.data[me.pos..][..n]);
            me.pos += n;

            // if me.pos == me.data.len() {
            //     me.data.truncate(0);
            //     me.pos = 0;
            // };
            return std::task::Poll::Ready(Ok(()));
        }
    }
}

pub enum Data {
    Bytes(Vec<u8>),
    File(tokio::fs::File),
}

impl axum::response::IntoResponse for Data {
    fn into_response(self) -> axum::response::Response {
        match self {
            Data::Bytes(b) => {
                axum::response::Response::new(axum::body::boxed(axum::body::Full::from(b)))
            }
            Data::File(f) => axum::response::Response::new(axum::body::boxed(
                axum::body::StreamBody::new(tokio_util::io::ReaderStream::new(f)),
            )),
        }
    }
}
