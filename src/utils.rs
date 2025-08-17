use bytes::Bytes;

pub fn bulk_string(s: &str) -> Bytes {
    format!("${}\r\n{s}\r\n", s.as_bytes().len()).into()
}
