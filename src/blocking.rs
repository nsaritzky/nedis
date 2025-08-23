use std::collections::{hash_map::Entry, HashMap};

use tokio::{
    sync::{mpsc, oneshot, OwnedRwLockMappedWriteGuard},
    task::JoinHandle,
    time::Instant,
};

use crate::{db_item::DbItem, db_value::DbValue, state::ServerState};

#[derive(Debug)]
pub enum BlockMsg {
    BlPop(String, Option<Instant>, oneshot::Sender<String>),
    //  XRead(String, Option<SystemTime>, oneshot::Sender<String>),
    BlPopUnblock(
        String,
        OwnedRwLockMappedWriteGuard<HashMap<String, DbItem>, DbItem>,
    ),
    //  XReadUnblock(String),
}

pub fn run_block_handler(
    _server_state: ServerState,
    mut receiver: mpsc::Receiver<BlockMsg>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut blpop_blocks = HashMap::new();
        // let mut xread_blocks = HashMap::new();
        loop {
            match receiver.recv().await {
                Some(BlockMsg::BlPop(key, expires, sender)) => match blpop_blocks.entry(key) {
                    Entry::Occupied(mut occ) => {
                        let arr: &mut Vec<_> = occ.get_mut();
                        arr.push((expires, sender));
                    }
                    Entry::Vacant(vac) => {
                        vac.insert(vec![(expires, sender)]);
                    }
                },
                // Some(BlockMsg::XRead(key, expires, sender)) => {
                //     xread_blocks
                //         .entry(key)
                //         .and_modify(|arr: &mut Vec<_>| arr.push((expires, sender)))
                //         .or_insert(vec![(expires, sender)]);
                // }
                Some(BlockMsg::BlPopUnblock(key, mut item)) => {
                    if let Some(blocks) = blpop_blocks.get_mut(&key) {
                        if let DbValue::List(ref mut list) = item.value_mut() {
                            let now = Instant::now();
                            let next_up = blocks.extract_if(.., |(expires, _)| {
                                expires.is_none_or(|t| t >= now)
                            }).take(list.len());
                            let mut updated = false;
                            for (_, sender) in next_up {
                                updated = true;
                                if let Err(_) = sender.send(list.pop_front().unwrap()) {
                                    eprintln!("Sent BLPOP results for {key}, but receiver hang up")
                                }
                            }
                            if updated {
                                item.update_timestamp();
                            }
                        }
                        if blocks.len() == 0 {
                            blpop_blocks.remove(&key);
                        }
                    }
                }
                None => panic!("mpsc channel for blockind handler closed!"),
            }
        }
    })
}
