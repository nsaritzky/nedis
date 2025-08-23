use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    command_handler::CommandHandler,
    error::RedisError,
    response::Response,
    state::{ConnectionState, ServerState, SubscriptionMessage},
};

pub struct SubscribeHandler;
#[async_trait]
impl CommandHandler for SubscribeHandler {
    async fn execute(
        &self,
        mut args: Vec<String>,
        mut server_state: ServerState,
        mut connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 2 {
            return Err(RedisError::WrongArgs("SUBSCRIBE"));
        }
        let key = args.remove(1);
        let (newly_inserted, tx, sub_count) = connection_state.subscribe(key.clone()).await;
        let resp = Response::List(vec![
            "subscribe".into(),
            key.clone().into(),
            sub_count.into(),
        ]);
        if newly_inserted {
            server_state
                .add_subscription(
                    key.clone(),
                    connection_state.get_connection_id(),
                    tx.clone(),
                )
                .await;
            let key_clone = key.clone();
            let mut rx = tx.subscribe();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(SubscriptionMessage::Message(msg)) => {
                            let resp = Response::List(vec![
                                "message".into(),
                                key_clone.clone().into(),
                                msg.into(),
                            ]);
                            if let Err(e) = connection_state.stream_tx.send(resp.to_bytes()).await {
                                println!("Error sending subscription message to stream: {e}");
                                break;
                            }
                        }
                        Ok(SubscriptionMessage::Unsubscribe) => {
                            server_state
                                .remove_subscription(
                                    key_clone,
                                    connection_state.get_connection_id(),
                                )
                                .await;
                            break;
                        }
                        Err(e) => {
                            println!("Error receiving subscripiton message: {e}");
                            break;
                        }
                    }
                }
            });
        }
        Ok(vec![resp.to_bytes()])
    }
}

pub struct UnsubscribeHandler;
#[async_trait]
impl CommandHandler for UnsubscribeHandler {
    async fn execute(
        &self,
        mut args: Vec<String>,
        _server_state: ServerState,
        mut connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 2 {
            return Err(RedisError::WrongArgs("UNSUBSCRIBE"));
        }
        let key = args.remove(1);
        let count = match connection_state.remove_subscription(&key).await {
            (Some(tx), count) => {
                tx.send(SubscriptionMessage::Unsubscribe)?;
                count
            }
            (None, count) => count,
        } as isize;
        let resp = Response::List(vec![
            "unsubscribe".into(),
            key.to_string().into(),
            count.into(),
        ]);
        Ok(vec![resp.to_bytes()])
    }
}

pub struct PublishHandler;
#[async_trait]
impl CommandHandler for PublishHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 3 {
            return Err(RedisError::WrongArgs("PUBLISH"));
        }
        let [_command, key, msg] = args.try_into().unwrap();
        let count: isize = server_state
            .get_subscripiton_count(&key)
            .await
            .try_into()
            .unwrap();
        server_state
            .publish_subscription_message(&key, msg.to_string())
            .await?;
        Ok(vec![Response::from(count).to_bytes()])
    }
}
