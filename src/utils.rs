use bytes::Bytes;
use futures::future;
use tokio::time::{sleep_until, Instant};

pub fn bulk_string(s: &str) -> Bytes {
    format!("${}\r\n{s}\r\n", s.as_bytes().len()).into()
}

pub async fn sleep_until_if(until: Option<Instant>) {
    if let Some(instant) = until {
        sleep_until(instant).await
    } else {
        future::pending::<()>().await
    }
}
