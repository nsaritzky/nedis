use anyhow::bail;
use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    command_handler::CommandHandler,
    response::RedisResponse,
    state::{ConnectionState, ServerState, SubscriptionMessage},
};

pub struct SubscribeHandler;
#[async_trait]
impl CommandHandler for SubscribeHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        mut connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        if let Some(key) = args.get(1) {
            let (newly_inserted, tx, sub_count) = connection_state.subscribe(key.to_string()).await;
            let resp = RedisResponse::List(vec![
                "subscribe".into(),
                key.to_string().into(),
                sub_count.into(),
            ]);
            if newly_inserted {
                server_state
                    .add_subscription(
                        key.to_string(),
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
                                let resp = RedisResponse::List(vec![
                                    "message".into(),
                                    key_clone.clone().into(),
                                    msg.into(),
                                ]);
                                if let Err(e) =
                                    connection_state.stream_tx.send(resp.to_bytes()).await
                                {
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
        } else {
            bail!("SUBSCRIBE: No key provided")
        }
    }
}

pub struct UnsubscribeHandler;
#[async_trait]
impl CommandHandler for UnsubscribeHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        _server_state: ServerState,
        mut connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        if let Some(key) = args.get(1) {
            let count = match connection_state.remove_subscription(key).await {
                (Some(tx), count) => {
                    tx.send(SubscriptionMessage::Unsubscribe)?;
                    count
                }
                (None, count) => count,
            } as isize;
            let resp = RedisResponse::List(vec![
                "unsubscribe".into(),
                key.to_string().into(),
                count.into(),
            ]);
            Ok(vec![resp.to_bytes()])
        } else {
            bail!("UNSUBSCRIBE: no key provided")
        }
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
    ) -> anyhow::Result<Vec<Bytes>> {
        if let (Some(key), Some(msg)) = (args.get(1), args.get(2)) {
            let count: isize = server_state
                .get_subscripiton_count(key)
                .await
                .try_into()
                .unwrap();
            server_state
                .publish_subscription_message(key, msg.to_string())
                .await?;
            Ok(vec![RedisResponse::from(count).to_bytes()])
        } else {
            bail!("PUBLISH: key or message not provided");
        }
    }
}
