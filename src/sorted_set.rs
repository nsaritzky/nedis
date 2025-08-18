use anyhow::bail;
use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    command_handler::CommandHandler,
    db_value::DbValue,
    response::RedisResponse,
    shard_map::ShardMapEntry,
    skip_list::SkipList,
    state::{ConnectionState, ServerState},
};

use std::{borrow::Borrow, collections::HashMap, fmt::Debug, hash::Hash};

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

#[derive(Debug, Clone)]
struct KeyTuple<K, V>(pub K, pub V);

impl<K: Ord, V> PartialEq for KeyTuple<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<K: Ord, V> Eq for KeyTuple<K, V> {}

impl<K: Ord, V> PartialOrd for KeyTuple<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord, V> Ord for KeyTuple<K, V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

#[derive(Clone, Debug)]
pub struct SortedSet<T: Ord + Debug + Eq + Hash> {
    skip_list: SkipList<(OrdFloat, T)>,
    hash_map: HashMap<T, OrdFloat>,
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
            return vec![]
        }
        self.skip_list
            .from_nth(a)
            .take(b - a + 1)
            .map(|(_, value)| value)
            .collect()
    }

    pub fn remove(&mut self, value: &T) {
        if let Some(ord_float) = self.hash_map.remove(value) {
            self.skip_list.delete(&(ord_float, value.clone()));
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
    ) -> anyhow::Result<Vec<Bytes>> {
        if args.len() != 4 {
            bail!("ZADD: Wrong number of arguments");
        }
        let [_command, key, score, value] = args.try_into().unwrap();
        let score: f64 = score.parse().unwrap();

        let count = match server_state.db.entry(key).await {
            ShardMapEntry::Occupied(mut occ) => match occ.get_mut() {
                (DbValue::ZSet(zset), _) => {
                    if zset.insert_or_update(value, score) {
                        0
                    } else {
                        1
                    }
                }
                _ => bail!("ZADD: Value at key is not a zset"),
            },
            ShardMapEntry::Vacant(vac) => {
                let mut zset = SortedSet::new();
                zset.insert_or_update(value, score);
                vac.insert((DbValue::ZSet(zset), None));
                1
            }
        };

        Ok(vec![RedisResponse::Int(count as isize).to_bytes()])
    }
}

pub struct ZRankHandler;
#[async_trait]
impl CommandHandler for ZRankHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        if args.len() != 3 {
            bail!("ZRANK: Wrong number of args");
        }
        let [_command, zset_key, zset_item] = args.try_into().unwrap();

        match server_state.db.get(&zset_key).await.as_deref() {
            Some((DbValue::ZSet(zset), _)) => {
                if let Some(rank) = zset.get_rank(&zset_item) {
                    Ok(vec![RedisResponse::Int(rank as isize).to_bytes()])
                } else {
                    Ok(vec!["$-1\r\n".into()])
                }
            }
            _ => Ok(vec!["$-1\r\n".into()]),
        }
    }
}

pub struct ZRangeHandler;
#[async_trait]
impl CommandHandler for ZRangeHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        if args.len() != 4 {
            bail!("ZRANGE: Wrong number of args");
        }
        let [_command, redis_key, min, max] = args.try_into().unwrap();
        let min: usize = min.parse().unwrap();
        let max: usize = max.parse().unwrap();

        let value = server_state.db.get(&redis_key).await;
        let result = match value.as_deref() {
            Some((DbValue::ZSet(zset), _)) => zset.get_index_range(min, max),
            Some(_) => bail!("ZRANGE: Value at key is not a zset"),
            None => vec![]
        };

        let resp: RedisResponse = result.into_iter().collect();
        Ok(vec![resp.to_bytes()])
    }
}
