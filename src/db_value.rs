use std::collections::VecDeque;

use bytes::Bytes;
use indexmap::IndexMap;

#[derive(Debug)]
pub enum DbValue {
    String(String),
    List(VecDeque<String>),
    Stream(Vec<StreamElement>),
}

impl DbValue {
    pub fn is_string(&self) -> bool {
        if let DbValue::String(_) = self {
            true
        } else {
            false
        }
    }

    pub fn is_list(&self) -> bool {
        if let DbValue::List(_) = self {
            true
        } else {
            false
        }
    }

    pub fn is_stream(&self) -> bool {
        if let DbValue::Stream(_) = self {
            true
        } else {
            false
        }
    }

    pub fn to_stream_vec_mut(&mut self) -> Option<&mut Vec<StreamElement>> {
        if let DbValue::Stream(arr) = self {
            Some(arr)
        } else {
            None
        }
    }

    pub fn to_stream_vec(&self) -> Option<&Vec<StreamElement>> {
        if let DbValue::Stream(arr) = self {
            Some(arr)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamElement {
    pub id: String,
    pub value: IndexMap<String, String>,
}

impl StreamElement {
    pub fn new(id: String, value: IndexMap<String, String>) -> Self {
        StreamElement { id, value }
    }
}

fn bulk_string(s: &str) -> Bytes {
    format!("${}\r\n{s}\r\n", s.len()).into()
}
