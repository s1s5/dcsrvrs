use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Headers {
    pub expire_time: Option<i64>,
    pub headers: HashMap<String, String>,
}

impl Headers {
    pub fn from(headers: axum::http::HeaderMap) -> Self {
        Headers {
            expire_time: headers
                .get("x-expires-in")
                .map(|x| x.to_str().ok())
                .unwrap_or_default()
                .map(|x| x.parse::<i64>().ok())
                .unwrap_or_default()
                .map(|x| x + chrono::Local::now().timestamp()),
            headers: headers
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
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderName, HeaderValue};
    use chrono::Local;

    use super::*;

    #[test]
    fn test_from() {
        let st = Local::now().timestamp();
        let input = HeaderMap::from_iter([
            (
                HeaderName::from_static("x-expires-in"),
                HeaderValue::from_static("1000"),
            ),
            (
                HeaderName::from_static("x-set-hello"),
                HeaderValue::from_static("world"),
            ),
            (
                HeaderName::from_static("content-length"),
                HeaderValue::from_static("128"),
            ),
            (
                HeaderName::from_static("other-header"),
                HeaderValue::from_static("some value"),
            ),
        ]);
        let headers = Headers::from(input);

        assert!(headers.expire_time.unwrap() >= st + 1000);
        assert!(headers.expire_time.unwrap() <= Local::now().timestamp() + 1000);

        assert!(headers.headers.keys().len() == 1);
        assert!(
            headers.headers.into_iter().collect::<Vec<_>>()[0]
                == ("hello".to_string(), "world".to_string())
        );
    }
}
