use std::collections::HashMap;

use anyhow::bail;
use bytes::Bytes;
use itertools::Itertools;

use crate::{
    db_item::DbItem, db_value::DbValue, response::RedisResponse, shard_map::ShardMapEntry, state::ServerState
};

pub async fn handle_hget(mut server_state: ServerState, v: &Vec<String>) -> anyhow::Result<Vec<Bytes>> {
    if v.len() < 3 {
        bail!("HGET: Not enough args");
    }
    let redis_key = &v[1];
    let hash_key = &v[2];
    let value = server_state.db.get(redis_key).await;
    let result = match value.as_deref() {
        Some(DbValue::Hash(map)) => map.get(hash_key),
        Some(_) => bail!("HGET: Key does not contain hash"),
        None => bail!("HGET: No value found at key"),
    };
    if let Some(result) = result {
        Ok(vec![result.clone().into()])
    } else {
        Ok(vec!["$-1\r\n".into()])
    }
}

pub async fn handle_hset(
    mut server_state: ServerState,
    mut v: Vec<String>,
) -> anyhow::Result<Vec<Bytes>> {
    if v.len() >= 4 {
        bail!("HSET: Not enough args");
    }
    let mut drain = v.drain(..);
    let redis_key = drain.next().unwrap();
    let keyvals = drain.tuples();

    let entry = server_state.db.entry(redis_key).await;

    let result: anyhow::Result<usize>;
    match entry {
        ShardMapEntry::Occupied(mut occ) => {
            let item = occ.get_mut();
            let value = item.value_mut();
            if let DbValue::Hash(map) = value {
                for (k, v) in keyvals {
                    map.insert(k, v);
                }
                result = Ok(map.len());
            } else {
                bail!("HSET: value is not a hash")
            }
            item.update_timestamp();
        }
        ShardMapEntry::Vacant(vac) => {
            let mut new_hash = HashMap::new();
            for (k, v) in keyvals {
                new_hash.insert(k, v);
            }
            let len = new_hash.len();
            vac.insert(DbItem::new(DbValue::Hash(new_hash), None));
            result = Ok(len)
        }
    };

    result.map(|n| vec![RedisResponse::Int(n as isize).to_bytes()])
}
