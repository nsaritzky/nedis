use std::collections::{hash_map::Entry, HashMap};

use tokio::{
    sync::{mpsc, oneshot, OwnedRwLockMappedWriteGuard},
    task::JoinHandle,
    time::Instant,
};

use crate::{
    db_item::DbItem,
    db_value::{DbValue, StreamElement, StreamId},
    state::ServerState,
};

#[derive(Debug)]
pub enum BlockMsg {
    BlPop(String, Option<Instant>, oneshot::Sender<String>),
    XRead {
        key: String,
        expires: Option<Instant>,
        id: StreamId,
        offset: usize,
        returner: oneshot::Sender<Vec<StreamElement>>,
    },
    BlPopUnblock(
        String,
        OwnedRwLockMappedWriteGuard<HashMap<String, DbItem>, DbItem>,
    ),
    XReadUnblock(
        String,
        OwnedRwLockMappedWriteGuard<HashMap<String, DbItem>, DbItem>,
    ),
    Wait(usize, oneshot::Sender<usize>),
    WaitUnblock,
}

pub fn run_block_handler(
    server_state: ServerState,
    mut receiver: mpsc::Receiver<BlockMsg>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut blpop_blocks = HashMap::new();
        let mut xread_blocks = HashMap::new();
        let mut wait_blocks: HashMap<usize, Vec<oneshot::Sender<usize>>> = HashMap::new();
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
                Some(BlockMsg::XRead {
                    key,
                    expires,
                    id,
                    offset,
                    returner,
                }) => match xread_blocks.entry(key) {
                    Entry::Occupied(mut occ) => {
                        let arr: &mut Vec<_> = occ.get_mut();
                        arr.push((expires, id, offset, returner));
                    }
                    Entry::Vacant(vac) => {
                        vac.insert(vec![(expires, id, offset, returner)]);
                    }
                },
                Some(BlockMsg::BlPopUnblock(key, mut item)) => {
                    let _tlock = server_state.transaction_lock.read().await;
                    if let Some(blocks) = blpop_blocks.get_mut(&key) {
                        if let DbValue::List(ref mut list) = item.value_mut() {
                            let now = Instant::now();
                            let (mut processed, mut unexpired) = (0, 0);
                            for (expired, _) in blocks.iter() {
                                if expired.is_none_or(|t| t >= now) {
                                    unexpired += 1;
                                }
                                    processed += 1;
                                if unexpired == list.len() {
                                    break;
                                }
                            }
                            for (_, sender) in blocks
                                .drain(0..processed)
                                .filter(|(expired, _)| expired.is_none_or(|t| t >= now))
                            {
                                if let Err(_) = sender.send(list.pop_front().unwrap()) {
                                    eprintln!("Sent BLPOP results for {key}, but receiver hang up")
                                }
                            }
                            if processed > 0 {
                                item.update_timestamp();
                            }
                        }
                        if blocks.len() == 0 {
                            blpop_blocks.remove(&key);
                        }
                    }
                }
                Some(BlockMsg::XReadUnblock(key, guard)) => {
                    println!("Unblocking xread");
                    let _tlock = server_state.transaction_lock.read().await;
                    if let Some(mut blocks) = xread_blocks.remove(&key) {
                        if let DbValue::Stream(stream_vec) = guard.value() {
                            let last = stream_vec.last().unwrap();
                            let now = Instant::now();
                            let expired_or_returning =
                                blocks.extract_if(.., |(expires, id, offset, _)| {
                                    let result = expires.is_some_and(|t| t < now) || *id <= last.id;
                                    if !result {
                                        *offset = stream_vec.len();
                                    }
                                    result
                                });
                            for (expires, id, offset, returner) in expired_or_returning {
                                if expires.is_none_or(|t| t >= now) {
                                    let result_vec: Vec<_> = stream_vec[offset..]
                                        .iter()
                                        .filter(|&element| element.id >= id)
                                        .cloned()
                                        .collect();
                                    if let Err(_) = returner.send(result_vec) {
                                        eprintln!("Sent XREAD results for key {key}, but receiver hang up")
                                    }
                                }
                            }
                        }
                    }
                }
                Some(BlockMsg::Wait(n, returner)) => {
                    match wait_blocks.entry(n) {
                        Entry::Occupied(mut occ) => {
                            let arr = occ.get_mut();
                            arr.push(returner);
                        }
                        Entry::Vacant(vac) => {vac.insert(vec![returner]);}
                    }

                }
                Some(BlockMsg::WaitUnblock) => {

                }
                None => panic!("mpsc channel for blockind handler closed!"),
            }
        }
    })
}
