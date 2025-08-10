use std::{
    collections::{HashMap, VecDeque},
};

use bytes::{BufMut, Bytes, BytesMut};
use num::BigInt;

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum PrimitiveRedisValue {
    Str(String),
    Error(String),
    Int(isize),
    Null,
    NilString,
    Bool(bool),
    BigInt(BigInt),
    Verbatim(Vec<u8>, Vec<u8>),
}

#[derive(Debug, Clone)]
pub enum RedisValue {
    Primitive(PrimitiveRedisValue),
    Arr(VecDeque<RedisValue>),
    Double(f64),
    Map(HashMap<PrimitiveRedisValue, RedisValue>),
    Attribute(HashMap<PrimitiveRedisValue, RedisValue>),
    Set(Vec<RedisValue>),
    Push(Vec<RedisValue>),
}

impl PrimitiveRedisValue {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        match self {
            PrimitiveRedisValue::Str(s) | PrimitiveRedisValue::Error(s) => {
                buf.put_slice(&bulk_string(s));
            }
            PrimitiveRedisValue::Int(n) => {
                buf.put_slice(format!(":{n}\r\n").as_bytes());
            }
            PrimitiveRedisValue::Null => {
                buf.put_slice(b"_\r\n");
            }
            PrimitiveRedisValue::NilString => {
                buf.put_slice(b"$-1\r\n");
            }
            PrimitiveRedisValue::Bool(val) => {
                if *val {
                    buf.put_slice(b"#t\r\n");
                } else {
                    buf.put_slice(b"#f\r\n");
                }
            }
            PrimitiveRedisValue::BigInt(n) => {
                buf.put_slice(format!("({n}\r\n").as_bytes());
            }
            PrimitiveRedisValue::Verbatim(encoding, data) => {
                buf.put_slice(format!("={}\r\n", encoding.len() + data.len() + 1).as_bytes());
                buf.put_slice(encoding);
                buf.put_slice(b":");
                buf.put_slice(data);
                buf.put_slice(b"\r\n");
            }
        }

        buf.freeze()
    }

    pub fn to_str(&self) -> Option<&str> {
        if let PrimitiveRedisValue::Str(s) = self {
            Some(s)
        } else {
            None
        }
    }
}

impl RedisValue {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        match self {
            RedisValue::Primitive(val) => {
                buf.put_slice(&val.to_bytes());
            }
            RedisValue::Arr(vec) => {
                buf.put_slice(format!("*{}\r\n", vec.len()).as_bytes());
                for val in vec {
                    buf.put_slice(&val.to_bytes());
                }
            }
            RedisValue::Double(x) => {
                buf.put_slice(format!(",{x}\r\n").as_bytes());
            }
            RedisValue::Map(map) => {
                buf.put_slice(format!("%{}\r\n", map.len()).as_bytes());
                for (key, val) in map.iter() {
                    buf.put_slice(&key.to_bytes());
                    buf.put_slice(&val.to_bytes());
                }
            }
            RedisValue::Attribute(map) => {
                buf.put_slice(format!("|{}\r\n", map.len()).as_bytes());
                for (key, val) in map.iter() {
                    buf.put_slice(&key.to_bytes());
                    buf.put_slice(&val.to_bytes());
                }
            }
            RedisValue::Set(vec) => {
                buf.put_slice(format!("~{}\r\n", vec.len()).as_bytes());
                for val in vec {
                    buf.put_slice(&val.to_bytes());
                }
            }
            RedisValue::Push(vec) => {
                buf.put_slice(format!(">{}\r\n", vec.len()).as_bytes());
                for val in vec {
                    buf.put_slice(&val.to_bytes());
                }
            }
        }

        buf.freeze()
    }

    pub fn to_str(&self) -> Option<&str> {
        if let RedisValue::Primitive(PrimitiveRedisValue::Str(s)) = self {
            Some(&s[..])
        } else {
            None
        }
    }

    pub fn to_uppercase_string(&self) -> Option<String> {
        self.to_str().map(|s| s.to_ascii_uppercase())
    }

    pub fn to_primitive(&self) -> Option<&PrimitiveRedisValue> {
        if let RedisValue::Primitive(p) = self {
            Some(p)
        } else {
            None
        }
    }
}

impl From<String> for RedisValue {
    fn from(value: String) -> Self {
        RedisValue::Primitive(PrimitiveRedisValue::Str(value))
    }
}

impl From<PrimitiveRedisValue> for RedisValue {
    fn from(value: PrimitiveRedisValue) -> Self {
        RedisValue::Primitive(value)
    }
}

impl From<isize> for RedisValue {
    fn from(value: isize) -> Self {
        RedisValue::Primitive(PrimitiveRedisValue::Int(value))
    }
}

impl From<VecDeque<RedisValue>> for RedisValue {
    fn from(value: VecDeque<RedisValue>) -> Self {
        RedisValue::Arr(value)
    }
}

#[derive(Debug)]
pub struct StreamElement {
    pub id: String,
    pub value: HashMap<String, String>,
}

impl StreamElement {
    pub fn new(id: String, value: HashMap<String, String>) -> Self {
        StreamElement { id, value }
    }
}

fn bulk_string<'a>(s: &'a str) -> Vec<u8> {
    format!("${}\r\n{s}\r\n", s.len()).as_bytes().to_owned()
}
