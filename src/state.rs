use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::SystemTime,
};

use anyhow::{anyhow, bail};
use atomic_enum::atomic_enum;
use bytes::Bytes;
use either::Either;
use futures::future::join_all;
use tokio::sync::{broadcast, mpsc, Mutex, MutexGuard, RwLock};

use crate::{
    blocking::BlockMsg, db::Db, db_item::DbItem, replica_tracker::ReplicaTracker, shard_map::ShardMap
};

type Blocks = Arc<Mutex<HashMap<String, BTreeSet<(SystemTime, String)>>>>;
type TransactionQueue = Arc<Mutex<Vec<(Vec<String>, usize)>>>;
type Subscriptions =
    Arc<RwLock<HashMap<String, HashMap<usize, broadcast::Sender<SubscriptionMessage>>>>>;

#[derive(Clone)]
pub struct ServerState {
    pub state: Either<MasterState, ReplicaState>,
    pub db: Db,
    pub blocks: Blocks,
    subscriptions: Subscriptions,
    next_connection_id: Arc<AtomicUsize>,
    pub transaction_lock: Arc<RwLock<()>>,
    pub block_tx: mpsc::Sender<BlockMsg>,
}

#[derive(Debug, Clone)]
pub struct MasterState {
    pub replica_tracker: Arc<Mutex<ReplicaTracker>>,
    pub replica_tx: broadcast::Sender<Bytes>,
    offset: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
pub struct ReplicaState {
    offset: Arc<AtomicUsize>,
}

impl ServerState {
    pub fn new(
        is_master: bool,
        replica_tx: Option<broadcast::Sender<Bytes>>,
        db: Option<HashMap<String, DbItem>>,
        block_tx: mpsc::Sender<BlockMsg>,
    ) -> Self {
        ServerState {
            state: if is_master {
                Either::Left(MasterState::new(
                    replica_tx.expect("Master state requires a replica sender"),
                ))
            } else {
                Either::Right(ReplicaState::new())
            },
            db: match db {
                Some(db) => Db::new(ShardMap::from_hash_map(16, db)),
                None => Db::new(ShardMap::new(16)),
            },
            blocks: Arc::new(Mutex::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            next_connection_id: Arc::new(AtomicUsize::new(0)),
            transaction_lock: Arc::new(RwLock::new(())),
            block_tx
        }
    }

    pub fn is_master(&self) -> bool {
        self.state.is_left()
    }

    pub fn master_state(&mut self) -> Option<&mut MasterState> {
        match &mut self.state {
            Either::Left(master) => Some(master),
            Either::Right(_) => None,
        }
    }

    pub fn replica_state(self) -> Option<ReplicaState> {
        self.state.right()
    }

    pub fn add_to_replica_offset(&mut self, message_len: usize) -> anyhow::Result<()> {
        match &self.state {
            Either::Left(_) => bail!("Tried to add to replica offset on the master"),
            Either::Right(replica) => {
                replica.offset.fetch_add(message_len, Ordering::SeqCst);
                Ok(())
            }
        }
    }

    pub fn add_to_master_offset(&mut self, message_len: usize) -> anyhow::Result<()> {
        match &self.state {
            Either::Left(master) => {
                master.offset.fetch_add(message_len, Ordering::SeqCst);
                Ok(())
            }
            Either::Right(_) => bail!("Tried to add to master offset on a replica"),
        }
    }

    pub fn init_offset(&mut self) {
        match &self.state {
            Either::Left(master) => master.offset.store(0, Ordering::SeqCst),
            Either::Right(replica) => replica.offset.store(0, Ordering::SeqCst),
        }
    }

    pub fn load_offset(&self) -> usize {
        match &self.state {
            Either::Left(master) => master.offset.load(Ordering::SeqCst),
            Either::Right(replica) => replica.offset.load(Ordering::SeqCst),
        }
    }

    pub async fn add_subscription(
        &mut self,
        key: String,
        connection_id: usize,
        tx: broadcast::Sender<SubscriptionMessage>,
    ) {
        let mut subs = self.subscriptions.write().await;
        subs.entry(key)
            .and_modify(|senders| {
                senders.insert(connection_id, tx.clone());
            })
            .or_insert(HashMap::from([(connection_id, tx.clone())]));
    }

    pub async fn remove_subscription(&mut self, key: String, connection_id: usize) {
        let mut subs = self.subscriptions.write().await;
        if let Entry::Occupied(mut occ) = subs.entry(key) {
            let senders = occ.get_mut();
            senders.remove(&connection_id);
            if senders.is_empty() {
                occ.remove();
            }
        }
    }

    pub async fn get_subscripiton_count(&self, key: &str) -> usize {
        let subs = self.subscriptions.read().await;
        subs.get(key).map(|senders| senders.len()).unwrap_or(0)
    }

    pub async fn publish_subscription_message(&self, key: &str, msg: String) -> anyhow::Result<()> {
        let subs = self.subscriptions.read().await;
        if let Some(senders) = subs.get(key) {
            let futures = senders.iter().map(|(_, tx)| {
                let msg = msg.clone();
                async move { tx.send(SubscriptionMessage::Message(msg)) }
            });
            let results = join_all(futures).await;
            let failures = results.into_iter().filter(|res| res.is_err()).count();
            if failures > 0 {
                bail!("Failed to send subsrciption message {msg} to {failures} receivers");
            } else {
                Ok(())
            }
        } else {
            bail!("Subscripion key error: {key}");
        }
    }

    pub fn get_connection_id(&mut self) -> usize {
        self.next_connection_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl MasterState {
    pub fn new(replica_tx: broadcast::Sender<Bytes>) -> Self {
        MasterState {
            replica_tracker: Arc::new(Mutex::new(ReplicaTracker::new())),
            replica_tx,
            offset: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn broadcast_to_replicas(&self, msg: Bytes) -> anyhow::Result<()> {
        let _ = self
            .replica_tx
            .send(msg)
            .map_err(|_| anyhow!("Failed to broadcast to replicas"))?;
        Ok(())
    }

    pub async fn create_replica(&mut self) -> usize {
        self.replica_tracker.lock().await.create_replica()
    }

    pub async fn update_offset_tracker(&mut self, replica_id: usize, offset: usize) {
        self.replica_tracker
            .lock()
            .await
            .update_offset(replica_id, offset)
    }

    pub async fn get_replica_tracker(&self) -> MutexGuard<'_, ReplicaTracker> {
        self.replica_tracker.lock().await
    }

    pub fn add_to_offset(&mut self, message_len: usize) {
        self.offset.fetch_add(message_len, Ordering::SeqCst);
    }
}

impl ReplicaState {
    pub fn new() -> Self {
        ReplicaState {
            offset: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[atomic_enum]
#[derive(PartialEq)]
pub enum ConnectionType {
    Client,
    MasterToReplica,
    ReplicaToMaster,
}

const UNSET: usize = usize::MAX;

#[derive(Clone, Debug)]
pub struct ConnectionState {
    is_master: bool,
    id: usize,
    connection_type: Arc<AtomicConnectionType>,
    transaction_active: Arc<AtomicBool>,
    pub transaction_queue: TransactionQueue,
    replica_id: Arc<AtomicUsize>,
    pub stream_tx: mpsc::Sender<Bytes>,
    subscriptions: Arc<RwLock<HashMap<String, broadcast::Sender<SubscriptionMessage>>>>,
    subscribed_mode: Arc<AtomicBool>,
    watched_keys: Arc<RwLock<HashMap<String, SystemTime>>>,
}

impl ConnectionState {
    pub fn new(stream_tx: mpsc::Sender<Bytes>, is_master: bool, id: usize) -> Self {
        ConnectionState {
            is_master,
            id,
            connection_type: Arc::new(AtomicConnectionType::new(ConnectionType::Client)),
            transaction_active: Arc::new(AtomicBool::new(false)),
            transaction_queue: Arc::new(Mutex::new(Vec::new())),
            replica_id: Arc::new(AtomicUsize::new(UNSET)),
            stream_tx,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            subscribed_mode: Arc::new(AtomicBool::new(false)),
            watched_keys: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn set_connection_type(&mut self, connection_type: ConnectionType) {
        self.connection_type
            .store(connection_type, Ordering::Relaxed);
    }

    pub fn get_connection_type(&self) -> ConnectionType {
        self.connection_type.load(Ordering::Relaxed)
    }

    pub fn get_transaction_active(&self) -> bool {
        self.transaction_active.load(Ordering::Relaxed)
    }

    pub fn set_transaction_active(&mut self, active: bool) {
        self.transaction_active.store(active, Ordering::Relaxed)
    }

    pub fn set_replica_id(&mut self, new_id: usize) -> anyhow::Result<()> {
        if self.replica_id.load(Ordering::Relaxed) != UNSET {
            bail!("Tried to set replica id, but it's already set")
        }
        if !self.is_master {
            bail!("Tried to set replica id on the replica itself")
        }
        self.replica_id.store(new_id, Ordering::Relaxed);
        Ok(())
    }

    pub fn get_replica_id(&self) -> Option<usize> {
        match self.replica_id.load(Ordering::Relaxed) {
            UNSET => None,
            id => Some(id),
        }
    }

    pub async fn clear_transactions(&mut self) {
        let mut transaction_queue = self.transaction_queue.lock().await;
        transaction_queue.clear();
    }

    pub async fn queue_command(&mut self, message_len: usize, args: Vec<String>) {
        let mut tq = self.transaction_queue.lock().await;
        tq.push((args, message_len));
    }

    pub async fn subscribe(
        &mut self,
        key: String,
    ) -> (bool, broadcast::Sender<SubscriptionMessage>, isize) {
        let mut subs = self.subscriptions.write().await;
        self.subscribed_mode.store(true, Ordering::Relaxed);
        let count = subs.len();
        match subs.entry(key) {
            Entry::Occupied(e) => (false, e.get().clone(), count as isize),
            Entry::Vacant(vac) => {
                let (tx, _) = broadcast::channel::<SubscriptionMessage>(100);
                vac.insert(tx.clone());
                (true, tx, count as isize + 1)
            }
        }
    }

    pub async fn remove_subscription(
        &mut self,
        key: &str,
    ) -> (Option<broadcast::Sender<SubscriptionMessage>>, usize) {
        let mut subs = self.subscriptions.write().await;
        (subs.remove(key), subs.len())
    }

    pub fn get_subscribe_mode(&self) -> bool {
        self.subscribed_mode.load(Ordering::Relaxed)
    }

    pub fn get_connection_id(&self) -> usize {
        self.id
    }

    pub async fn watch_keys(&mut self, keys: Vec<String>) {
        let mut watched_keys = self.watched_keys.write().await;
        for key in keys {
            watched_keys.insert(key, SystemTime::now());
        }
    }

    pub async fn drain_watched_keys(&mut self) -> HashMap<String, SystemTime> {
        let mut watched_keys = self.watched_keys.write().await;
        std::mem::take(&mut *watched_keys)
    }
}

#[derive(Clone, Debug)]
pub enum SubscriptionMessage {
    Message(String),
    Unsubscribe,
}
