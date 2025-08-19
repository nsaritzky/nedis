use bytes::{Bytes, BytesMut};

use crate::db_value::{DbValue, StreamElement};

pub enum RedisResponse {
    Str(String),
    Int(isize),
    List(Vec<RedisResponse>),
    Nil,
}

impl RedisResponse {
    pub fn to_bytes(&self) -> Bytes {
        match self {
            RedisResponse::Str(s) => format!("${}\r\n{s}\r\n", s.len()).into(),
            RedisResponse::Int(n) => format!(":{n}\r\n").into(),
            RedisResponse::List(array) => {
                let mut buf = BytesMut::new();
                buf.extend_from_slice(format!("*{}\r\n", array.len()).as_bytes());
                for val in array {
                    buf.extend_from_slice(&val.to_bytes());
                }
                buf.freeze()
            }
            RedisResponse::Nil => "$-1\r\n".into(),
        }
    }

    pub fn from_str_vec(v: &Vec<String>) -> Self {
        RedisResponse::List(v.iter().map(|s| RedisResponse::Str(s.clone())).collect())
    }

    pub fn from_stream(key: String, value: Vec<&StreamElement>) -> Self {
        RedisResponse::List(vec![
            key.into(),
            value
                .into_iter()
                .map(|element| {
                    RedisResponse::List(vec![
                        RedisResponse::Str(element.id.clone()),
                        RedisResponse::List(
                            element
                                .value
                                .iter()
                                .flat_map(|(k, v)| [k.clone().into(), v.clone().into()])
                                .collect(),
                        ),
                    ])
                })
                .collect(),
        ])
    }
}

impl FromIterator<RedisResponse> for RedisResponse {
    fn from_iter<T: IntoIterator<Item = RedisResponse>>(iter: T) -> Self {
        RedisResponse::List(iter.into_iter().collect())
    }
}

impl<'a> FromIterator<&'a String> for RedisResponse {
    fn from_iter<T: IntoIterator<Item = &'a String>>(iter: T) -> Self {
        RedisResponse::List(
            iter.into_iter()
                .map(|s| RedisResponse::Str(s.to_string()))
                .collect(),
        )
    }
}

impl FromIterator<String> for RedisResponse {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        RedisResponse::List(iter.into_iter().map(|s| s.into()).collect())
    }
}

impl From<&DbValue> for RedisResponse {
    fn from(value: &DbValue) -> Self {
        match value {
            DbValue::String(s) => RedisResponse::Str(s.clone()),
            DbValue::List(arr) => {
                RedisResponse::from_str_vec(&arr.iter().map(|s| s.clone()).collect())
            }
            DbValue::Stream(arr) => RedisResponse::List(
                arr.iter()
                    .map(|elt| {
                        RedisResponse::List(vec![
                            RedisResponse::Str(elt.id.clone()),
                            RedisResponse::List(
                                elt.value
                                    .iter()
                                    .flat_map(|(k, v)| [k.clone().into(), v.clone().into()])
                                    .collect(),
                            ),
                        ])
                    })
                    .collect(),
            ),
            DbValue::Hash(map) => RedisResponse::List(
                map.iter()
                    .flat_map(|(k, v)| [k.clone().into(), v.clone().into()])
                    .collect(),
            ),
            DbValue::Set(set) => set.iter().collect(),
            DbValue::ZSet(set) => unimplemented!(),
            DbValue::Empty => RedisResponse::Nil,
        }
    }
}

impl From<Vec<&StreamElement>> for RedisResponse {
    fn from(value: Vec<&StreamElement>) -> Self {
        RedisResponse::List(
            value
                .into_iter()
                .map(|element| {
                    RedisResponse::List(vec![
                        RedisResponse::Str(element.id.clone()),
                        RedisResponse::List(
                            element
                                .value
                                .iter()
                                .flat_map(|(k, v)| [k.clone().into(), v.clone().into()])
                                .collect(),
                        ),
                    ])
                })
                .collect(),
        )
    }
}

impl From<String> for RedisResponse {
    fn from(value: String) -> Self {
        RedisResponse::Str(value)
    }
}

impl From<&str> for RedisResponse {
    fn from(value: &str) -> Self {
        RedisResponse::Str(value.to_string())
    }
}

impl From<isize> for RedisResponse {
    fn from(value: isize) -> Self {
        RedisResponse::Int(value)
    }
}
