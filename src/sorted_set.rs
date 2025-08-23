use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    command_handler::CommandHandler,
    db_item::DbItem,
    db_value::DbValue,
    error::RedisError,
    response::Response,
    shard_map::ShardMapEntry,
    skip_list::SkipList,
    state::{ConnectionState, ServerState},
};

use std::{collections::HashMap, fmt::Debug, hash::Hash};

#[derive(Clone, Debug, Copy, PartialEq, PartialOrd)]
pub struct OrdFloat(f64);

impl OrdFloat {
    pub fn new(value: f64) -> Self {
        if value.is_nan() {
            panic!("Can't make an OrdFloat out of NaN")
        }
        Self(value)
    }

    pub fn to_float(&self) -> f64 {
        self.0
    }
}

impl Eq for OrdFloat {}

impl Hash for OrdFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

impl Ord for OrdFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Clone, Debug)]
pub struct SortedSet<T: Ord + Debug + Eq + Hash> {
    skip_list: SkipList<(OrdFloat, T)>,
    hash_map: HashMap<T, OrdFloat>,
}

pub struct ZsetIter<'a, T: Ord + Debug + Eq + Hash> {
    iter: crate::skip_list::Iter<'a, (OrdFloat, T)>,
}

impl<'a, T: Ord + Debug + Eq + Hash> Iterator for ZsetIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(_, item)| item)
    }
}

impl<'a, T: Ord + Debug + Eq + Hash + Clone + Default> SortedSet<T> {
    pub fn new() -> Self {
        Self {
            skip_list: SkipList::new(),
            hash_map: HashMap::new(),
        }
    }

    pub fn insert_or_update(&mut self, value: T, score: f64) -> bool {
        let ord_float = OrdFloat::new(score);
        if let Some(old_score) = self.hash_map.insert(value.clone(), ord_float) {
            self.skip_list.delete(&(old_score, value.clone()));
            self.skip_list.insert((ord_float, value.clone()));
            true
        } else {
            self.skip_list.insert((ord_float, value));
            false
        }
    }

    pub fn contains(&self, value: &T) -> bool {
        self.hash_map.contains_key(value)
    }

    pub fn get_score(&self, value: &T) -> Option<f64> {
        self.hash_map.get(value).map(|score| score.to_float())
    }

    pub fn get_rank(&self, value: &T) -> Option<usize> {
        self.hash_map
            .get(value)
            .and_then(|score| self.skip_list.search(&(*score, value.clone())))
    }

    pub fn get_range(&self, min: OrdFloat, max: OrdFloat) -> Vec<&T> {
        if min > max {
            return vec![];
        }
        self.skip_list
            .get_starting_at(&(min, T::default()))
            .take_while(|&&(score, _)| score < max)
            .map(|(_, value)| value)
            .collect()
    }

    pub fn get_index_range(&self, a: usize, b: usize) -> Vec<&T> {
        if a > b {
            return vec![];
        }
        self.skip_list
            .from_nth(a)
            .take(b - a + 1)
            .map(|(_, value)| value)
            .collect()
    }

    pub fn remove(&mut self, value: &T) -> Option<(OrdFloat, T)> {
        if let Some(ord_float) = self.hash_map.remove(value) {
            self.skip_list.delete(&(ord_float, value.clone()))
        } else {
            None
        }
    }

    pub fn iter(&self) -> ZsetIter<'_, T> {
        ZsetIter {
            iter: self.skip_list.iter(),
        }
    }

    pub fn len(&self) -> usize {
        self.hash_map.len()
    }
}

pub struct ZADDHandler;
#[async_trait]
impl CommandHandler for ZADDHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 4 {
            return Err(RedisError::WrongArgs("ZADD"));
        }
        let [_command, key, score, value] = args.try_into().unwrap();
        let score: f64 = score.parse().unwrap();

        let count;
        match server_state.db.entry(key).await {
            ShardMapEntry::Occupied(mut occ) => {
                let item = occ.get_mut();
                match item.value_mut() {
                    DbValue::ZSet(zset) => {
                        if zset.insert_or_update(value, score) {
                            count = 0;
                        } else {
                            count = 1;
                        }
                    }
                    _ => return Err(RedisError::WrongType),
                }
                if count == 1 {
                    item.update_timestamp();
                }
            }
            ShardMapEntry::Vacant(vac) => {
                let mut zset = SortedSet::new();
                zset.insert_or_update(value, score);
                vac.insert(DbItem::new(DbValue::ZSet(zset), None));
                count = 1;
            }
        };

        Ok(vec![Response::Int(count as isize).to_bytes()])
    }
}

pub struct ZRankHandler;
#[async_trait]
impl CommandHandler for ZRankHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 3 {
            return Err(RedisError::WrongArgs("ZRANK"));
        }
        let [_command, zset_key, zset_item] = args.try_into().unwrap();

        let value = server_state.db.get(&zset_key).await;

        match value.as_deref() {
            Some(DbValue::ZSet(zset)) => {
                if let Some(rank) = zset.get_rank(&zset_item) {
                    Ok(vec![Response::Int(rank as isize).to_bytes()])
                } else {
                    Ok(vec![Response::Nil.to_bytes()])
                }
            }
            Some(_) => Err(RedisError::WrongType),
            None => Ok(vec![Response::Nil.to_bytes()]),
        }
    }
}

pub struct ZRangeHandler;
#[async_trait]
impl CommandHandler for ZRangeHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 4 {
            return Err(RedisError::WrongArgs("ZRANGE"));
        }
        let [_command, redis_key, min, max] = args.try_into().unwrap();
        let min: isize = min.parse().unwrap();
        let max: isize = max.parse().unwrap();

        let value = server_state.db.get(&redis_key).await;
        let result = match value.as_deref() {
            Some(DbValue::ZSet(zset)) => {
                let min = if min < 0 {
                    0.max(zset.len() as isize + min)
                } else {
                    min
                } as usize;
                let max = if max < 0 {
                    0.max(zset.len() as isize + max)
                } else {
                    max
                } as usize;
                zset.get_index_range(min, max)
            }
            Some(_) => return Err(RedisError::WrongType),
            None => vec![],
        };

        let resp: Response = result.into_iter().collect();
        Ok(vec![resp.to_bytes()])
    }
}

pub struct ZCardHandler;
#[async_trait]
impl CommandHandler for ZCardHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 2 {
            return Err(RedisError::WrongArgs("ZCARD"));
        }
        let [_command, redis_key] = args.try_into().unwrap();

        let value = server_state.db.get(&redis_key).await;
        let result = match value.as_deref() {
            Some(DbValue::ZSet(zset)) => zset.len(),
            Some(_) => return Err(RedisError::WrongType),
            None => 0,
        };

        Ok(vec![Response::Int(result as isize).to_bytes()])
    }
}

pub struct ZScoreHandler;
#[async_trait]
impl CommandHandler for ZScoreHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 3 {
            return Err(RedisError::WrongArgs("ZSCORE"));
        }
        let [_command, zset_key, zset_item] = args.try_into().unwrap();

        let value = server_state.db.get(&zset_key).await;
        let score = match value.as_deref() {
            Some(DbValue::ZSet(zset)) => zset.get_score(&zset_item),
            Some(_) => return Err(RedisError::WrongType),
            None => None,
        };

        if let Some(score) = score {
            Ok(vec![Response::Str(score.to_string()).to_bytes()])
        } else {
            Ok(vec!["$-1\r\n".into()])
        }
    }
}

pub struct ZRemHandler;
#[async_trait]
impl CommandHandler for ZRemHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 3 {
            return Err(RedisError::WrongArgs("ZREM"));
        }
        let [_command, zset_key, zset_item] = args.try_into().unwrap();

        let entry = server_state.db.entry(zset_key).await;
        if let ShardMapEntry::Occupied(mut occ) = entry {
            let value = occ.get_mut().value_mut();
            if let DbValue::ZSet(zset) = value {
                if zset.remove(&zset_item).is_some() {
                    if zset.len() == 0 {
                        occ.remove();
                    }
                    Ok(vec![":1\r\n".into()])
                } else {
                    Ok(vec![":0\r\n".into()])
                }
            } else {
                Err(RedisError::WrongType)
            }
        } else {
            Ok(vec![":0\r\n".into()])
        }
    }
}

#[cfg(test)]
mod test {
    use super::SortedSet;

    #[test]
    fn test_range() {
        let mut zset = SortedSet::new();

        zset.insert_or_update("hello", 2.0);
        zset.insert_or_update("hello again", 1.0);

        zset.remove(&"hello");

        assert_eq!(zset.len(), 1);
        for item in zset.iter() {
            println!("Item: {item:?}");
        }
        assert_eq!(zset.get_index_range(0, 0).len(), 1);
    }
}
