use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

use crate::{
    command_handler::CommandHandler,
    db_item::DbItem,
    db_value::DbValue,
    error::RedisError,
    response::Response,
    state::{ConnectionState, ServerState},
    utils::bulk_string,
    GLOBAL_CONFIG,
};

pub struct PingHandler;
#[async_trait]
impl CommandHandler for PingHandler {
    async fn execute(
        &self,
        _args: Vec<String>,
        _server_state: ServerState,
        connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if connection_state.get_subscribe_mode() {
            Ok(vec![
                Response::List(vec!["pong".into(), "".into()]).to_bytes()
            ])
        } else {
            Ok(vec!["+PONG\r\n".into()])
        }
    }
}

pub struct EchoHandler;
#[async_trait]
impl CommandHandler for EchoHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        _server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        Ok(vec![bulk_string(&args[1])])
    }
}

pub struct SetHandler;
#[async_trait]
impl CommandHandler for SetHandler {
    async fn execute(
        &self,
        mut args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() < 3 {
            return Err(RedisError::WrongArgs("SET"));
        }
        let (key, value) = {
            let mut iter = args.drain(1..3);
            (iter.next().unwrap(), iter.next().unwrap())
        };

        match args.get(1) {
            Some(s) if s.to_ascii_uppercase() == "PX" => {
                let expires_in: i64 = args[2].parse().map_err(|_| RedisError::OutOfRange)?;
                if expires_in < 0 {
                    return Err(RedisError::InvalidExpiration("set"));
                }
                let expires_in = expires_in as u64;

                let current_time = SystemTime::now();
                let expires = current_time
                    .checked_add(Duration::from_millis(expires_in))
                    .ok_or(RedisError::OutOfRange)?;

                server_state
                    .db
                    .insert(key, DbItem::new(DbValue::String(value), Some(expires)))
                    .await;
            }
            _ => {
                server_state
                    .db
                    .insert(key, DbItem::new(DbValue::String(value), None))
                    .await;
            }
        }
        Ok(vec!["+OK\r\n".into()])
    }
}

pub struct GetHandler;
#[async_trait]
impl CommandHandler for GetHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        let key = &args[1];

        let value = server_state.db.get(key).await;
        match value.as_deref() {
            Some(DbValue::String(s)) => Ok(vec![Response::Str(s.to_string()).to_bytes()]),
            Some(_) => Err(RedisError::WrongType),
            None => Ok(vec![Response::NilStr.to_bytes()]),
        }
    }
}

pub struct TypeHandler;
#[async_trait]
impl CommandHandler for TypeHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() < 2 {
            return Err(RedisError::WrongArgs("TYPE"));
        }
        let value = server_state.db.get(&args[1]).await;
        match value.as_deref() {
            Some(DbValue::String(_)) | Some(DbValue::List(_)) => Ok(vec!["+string\r\n".into()]),
            Some(DbValue::Stream(_)) => Ok(vec!["+stream\r\n".into()]),
            Some(DbValue::Hash(_)) => Ok(vec!["+hash\r\n".into()]),
            Some(DbValue::Set(_)) => Ok(vec!["+set\r\n".into()]),
            Some(DbValue::ZSet(_)) => Ok(vec!["+zset\r\n".into()]),
            None | Some(DbValue::Empty) => Ok(vec!["+none\r\n".into()]),
        }
    }
}

pub struct InfoHandler;
#[async_trait]
impl CommandHandler for InfoHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        _server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        match args[1].to_ascii_lowercase().as_str() {
            "replication" => {
                let global_config = GLOBAL_CONFIG.get().unwrap();
                let mut lines = vec![format!("role:{}", global_config.role())];
                if let Some(master_config) = global_config.get_master_config() {
                    lines.push(format!("master_replid:{}", master_config.replication_id));
                    lines.push(format!("master_repl_offset:0"));
                }
                Ok(vec![bulk_string(lines.join("\r\n").as_str())])
            }
            _ => Ok(vec![bulk_string("")]),
        }
    }
}

pub struct ConfigHandler;
#[async_trait]
impl CommandHandler for ConfigHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        _server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() < 3 {
            return Err(RedisError::WrongArgs("CONFIG"));
        }
        match args[1].to_ascii_uppercase().as_str() {
            "GET" => {
                let global_config = GLOBAL_CONFIG.get().unwrap();
                let key = args[2].to_ascii_lowercase();
                let value = match key.as_str() {
                    "dir" => global_config.dir.clone(),
                    "dbfilename" => global_config.dbfilename.clone(),
                    _ => return Ok(vec![Response::Empty.to_bytes()]),
                }
                .unwrap_or("".to_string());
                let resp = Response::from_str_vec(&vec![key, value]);
                Ok(vec![resp.to_bytes()])
            }
            cmd => Err(RedisError::InvalidConfigCommand(cmd.to_string())),
        }
    }
}

pub struct KeysHandler;
#[async_trait]
impl CommandHandler for KeysHandler {
    async fn execute(
        &self,
        _args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        let mut buf = BytesMut::new();
        let mut values_buf = BytesMut::new();
        let now = SystemTime::now();
        server_state
            .db
            .retain(|key, item| {
                if item.expires_at().is_none_or(|exp| exp >= now) {
                    values_buf.extend_from_slice(&Response::Str(key.clone()).to_bytes());
                    true
                } else {
                    false
                }
            })
            .await;
        buf.extend_from_slice(format!("*{}\r\n", server_state.db.len()).as_bytes());
        buf.extend_from_slice(&values_buf);
        Ok(vec![buf.freeze()])
    }
}

pub struct ConstantHandler(pub Vec<Bytes>);
#[async_trait]
impl CommandHandler for ConstantHandler {
    async fn execute(
        &self,
        _args: Vec<String>,
        _server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        Ok(self.0.clone())
    }
}

pub struct EmptyHandler;
#[async_trait]
impl CommandHandler for EmptyHandler {
    async fn execute(
        &self,
        _args: Vec<String>,
        _server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        Ok(vec![])
    }
}

pub struct EmptyRDBHandler;
#[async_trait]
impl CommandHandler for EmptyRDBHandler {
    async fn execute(
        &self,
        _args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        server_state.init_offset();
        Ok(vec![])
    }
}
