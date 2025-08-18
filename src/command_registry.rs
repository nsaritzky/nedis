use std::collections::HashMap;

use futures::io::Empty;
use once_cell::sync::Lazy;

use crate::{
    command_handler::CommandHandler, list::{BLPopHandler, LLenHandler, LPopHandler, LPushHandler, LRangeHandler, RPushHandler}, pubsub::{PublishHandler, SubscribeHandler, UnsubscribeHandler}, replication::{PSyncHandler, ReplConfHandler, WaitHandler}, set::{SADDHandler, SCARDHandler, SINTERHandler, SISMEMBERHandler, SREMHandler}, simple_handlers::{
        ConfigHandler, ConstantHandler, EchoHandler, EmptyHandler, EmptyRDBHandler, GetHandler, InfoHandler, KeysHandler, PingHandler, SetHandler, TypeHandler
    }, sorted_set::{ZADDHandler, ZRangeHandler, ZRankHandler}, stream::{XADDHandler, XRangeHandler, XReadHandler}, transactions::{IncrHandler, MultiHandler}
};

pub static REGISTRY: Lazy<HashMap<&'static str, Box<dyn CommandHandler + Send + Sync>>> =
    Lazy::new(|| {
        let mut registry: HashMap<_, Box<dyn CommandHandler + Send + Sync>> = HashMap::new();
        registry.insert("PING", Box::new(PingHandler));
        registry.insert("ECHO", Box::new(EchoHandler));
        registry.insert("SET", Box::new(SetHandler));
        registry.insert("GET", Box::new(GetHandler));
        registry.insert("TYPE", Box::new(TypeHandler));
        registry.insert("RPUSH", Box::new(RPushHandler));
        registry.insert("LRANGE", Box::new(LRangeHandler));
        registry.insert("LPUSH", Box::new(LPushHandler));
        registry.insert("LLEN", Box::new(LLenHandler));
        registry.insert("LPOP", Box::new(LPopHandler));
        registry.insert("BLPOP", Box::new(BLPopHandler));
        registry.insert("XADD", Box::new(XADDHandler));
        registry.insert("XRANGE", Box::new(XRangeHandler));
        registry.insert("XREAD", Box::new(XReadHandler));
        registry.insert("INFO", Box::new(InfoHandler));
        registry.insert("INCR", Box::new(IncrHandler));
        registry.insert("SADD", Box::new(SADDHandler));
        registry.insert("SREM", Box::new(SREMHandler));
        registry.insert("SISMEMBER", Box::new(SISMEMBERHandler));
        registry.insert("SINTER", Box::new(SINTERHandler));
        registry.insert("SCARD", Box::new(SCARDHandler));
        registry.insert("MULTI", Box::new(MultiHandler));
        registry.insert(
            "EXEC",
            Box::new(ConstantHandler(vec!["-ERR EXEC without MULTI\r\n".into()])),
        );
        registry.insert(
            "DISCARD",
            Box::new(ConstantHandler(vec!["-ERR DISCARD without MULTI\r\n".into()])),
        );
        registry.insert("PSYNC", Box::new(PSyncHandler));
        registry.insert("REPLCONF", Box::new(ReplConfHandler));
        registry.insert("WAIT", Box::new(WaitHandler));
        registry.insert("CONFIG", Box::new(ConfigHandler));
        registry.insert("KEYS", Box::new(KeysHandler));
        registry.insert("SUBSCRIBE", Box::new(SubscribeHandler));
        registry.insert("UNSUBSCRIBE", Box::new(UnsubscribeHandler));
        registry.insert("PUBLISH", Box::new(PublishHandler));
        registry.insert("ZADD", Box::new(ZADDHandler));
        registry.insert("ZRANK", Box::new(ZRankHandler));
        registry.insert("EMPTY_RDB_FILE", Box::new(EmptyRDBHandler));
        registry.insert("ZRANGE", Box::new(ZRangeHandler));
        registry
    });
