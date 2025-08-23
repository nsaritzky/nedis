use bytes::{Bytes, BytesMut};

use crate::db_value::{DbValue, StreamElement};

pub enum Response {
    Str(String),
    Int(isize),
    List(Vec<Response>),
    Nil,
    Empty,
    Ok,
}

impl Response {
    pub fn to_bytes(&self) -> Bytes {
        match self {
            Response::Str(s) => format!("${}\r\n{s}\r\n", s.len()).into(),
            Response::Int(n) => format!(":{n}\r\n").into(),
            Response::List(array) => {
                let mut buf = BytesMut::new();
                buf.extend_from_slice(format!("*{}\r\n", array.len()).as_bytes());
                for val in array {
                    buf.extend_from_slice(&val.to_bytes());
                }
                buf.freeze()
            }
            Response::Nil => "$-1\r\n".into(),
            Response::Empty => "*0\r\n".into(),
            Response::Ok => "+OK\r\n".into(),
        }
    }

    pub fn from_str_vec(v: &Vec<String>) -> Self {
        Response::List(v.iter().map(|s| Response::Str(s.clone())).collect())
    }

    pub fn from_stream(key: String, value: Vec<&StreamElement>) -> Self {
        Response::List(vec![
            key.into(),
            value
                .into_iter()
                .map(|element| {
                    Response::List(vec![
                        Response::Str(element.id.clone()),
                        Response::List(
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

impl FromIterator<Response> for Response {
    fn from_iter<T: IntoIterator<Item = Response>>(iter: T) -> Self {
        Response::List(iter.into_iter().collect())
    }
}

impl<'a> FromIterator<&'a String> for Response {
    fn from_iter<T: IntoIterator<Item = &'a String>>(iter: T) -> Self {
        Response::List(
            iter.into_iter()
                .map(|s| Response::Str(s.to_string()))
                .collect(),
        )
    }
}

impl FromIterator<String> for Response {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        Response::List(iter.into_iter().map(|s| s.into()).collect())
    }
}

impl From<&DbValue> for Response {
    fn from(value: &DbValue) -> Self {
        match value {
            DbValue::String(s) => Response::Str(s.clone()),
            DbValue::List(arr) => {
                Response::from_str_vec(&arr.iter().map(|s| s.clone()).collect())
            }
            DbValue::Stream(arr) => Response::List(
                arr.iter()
                    .map(|elt| {
                        Response::List(vec![
                            Response::Str(elt.id.clone()),
                            Response::List(
                                elt.value
                                    .iter()
                                    .flat_map(|(k, v)| [k.clone().into(), v.clone().into()])
                                    .collect(),
                            ),
                        ])
                    })
                    .collect(),
            ),
            DbValue::Hash(map) => Response::List(
                map.iter()
                    .flat_map(|(k, v)| [k.clone().into(), v.clone().into()])
                    .collect(),
            ),
            DbValue::Set(set) => set.iter().collect(),
            DbValue::ZSet(zset) => zset.iter().collect(),
            DbValue::Empty => Response::Nil,
        }
    }
}

impl From<Vec<&StreamElement>> for Response {
    fn from(value: Vec<&StreamElement>) -> Self {
        Response::List(
            value
                .into_iter()
                .map(|element| {
                    Response::List(vec![
                        Response::Str(element.id.clone()),
                        Response::List(
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

impl From<String> for Response {
    fn from(value: String) -> Self {
        Response::Str(value)
    }
}

impl From<&str> for Response {
    fn from(value: &str) -> Self {
        Response::Str(value.to_string())
    }
}

impl From<isize> for Response {
    fn from(value: isize) -> Self {
        Response::Int(value)
    }
}
