use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Headers(pub HashMap<String, String>);

impl Headers {
    pub fn default() -> Self {
        Headers(HashMap::new())
    }

    pub fn from(headers: axum::http::HeaderMap) -> Self {
        Headers(
            headers
                .iter()
                .filter(|(name, _)| name.as_str().starts_with("x-set-"))
                .filter(|(_, value)| std::str::from_utf8(value.as_bytes()).is_ok())
                .map(|(name, value)| {
                    (
                        name.as_str()[6..].to_string(),
                        std::str::from_utf8(value.as_bytes()).unwrap().to_string(),
                    )
                })
                .collect::<HashMap<_, _>>(),
        )
    }
}

#[axum::async_trait]
impl<S, B> axum::extract::FromRequest<S, B> for Headers
where
    // these bounds are required by `async_trait`
    B: Send + 'static,
    S: Send + Sync,
{
    type Rejection = axum::http::StatusCode;

    async fn from_request(
        req: axum::http::Request<B>,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let v = req
            .headers()
            .iter()
            .filter(|(name, _)| name.as_str().starts_with("x-set-"))
            .filter(|(_, value)| std::str::from_utf8(value.as_bytes()).is_ok())
            .map(|(name, value)| {
                (
                    name.as_str()[6..].to_string(),
                    std::str::from_utf8(value.as_bytes()).unwrap().to_string(),
                )
            })
            .collect::<HashMap<_, _>>();
        Ok(Headers(v))
    }
}
