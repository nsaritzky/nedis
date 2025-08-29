use crate::command_handler::CommandHandler;
use crate::db_item::DbItem;
use crate::db_value::DbValue;
use crate::error::RedisError;
use crate::response::Response;
use crate::sorted_set::SortedSet;
use crate::state::{ConnectionState, ServerState};
use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

pub struct GeoAddHandler;
#[async_trait]
impl CommandHandler for GeoAddHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 5 {
            return Err(RedisError::WrongArgs("GEOADD"));
        }
        let [_command, key, long, lat, member] = args.try_into().unwrap();
        let (lat, long) = parse_coordinates(&lat, &long)?;

        let mut item = server_state
            .db
            .get_mut_or_insert(key, DbItem::new(DbValue::ZSet(SortedSet::new()), None))
            .await;
        match item.value_mut() {
            DbValue::ZSet(zset) => {
                zset.insert_or_update(member, 0.0);
            }
            _ => return Err(RedisError::WrongType)
        }
        Ok(vec![Response::Int(1).to_bytes()])
    }
}

fn parse_coordinates(lat: &str, long: &str) -> Result<(f64, f64), GeoError> {
    let lat_num = lat.parse::<f64>();
    let long_num = long.parse::<f64>();
    if lat_num.is_err() || long_num.is_err() {
        Err(GeoError::InvalidLatLong(lat.to_string(), long.to_string()))
    } else {
        let lat = lat_num.unwrap();
        let long = long_num.unwrap();
        if lat.abs() > 85.05112878 || long.abs() > 180.0 {
            Err(GeoError::InvalidLatLong(lat.to_string(), long.to_string()))
        } else {
            Ok((lat, long))
        }
    }
}

#[derive(Error, Debug)]
pub enum GeoError {
    #[error("ERR invalid latitude longitude pair {0} {1}")]
    InvalidLatLong(String, String),
}
