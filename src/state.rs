use std::{
    collections::{BTreeSet, HashMap},
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
use tokio::sync::{broadcast, mpsc, Mutex, MutexGuard, RwLock};

use crate::{db_value::DbValue, replica_tracker::ReplicaTracker};

type Db = Arc<RwLock<HashMap<String, (DbValue, Option<SystemTime>)>>>;
type Blocks = Arc<Mutex<HashMap<String, BTreeSet<(SystemTime, String)>>>>;
type TransactionQueue = Arc<Mutex<Vec<(Vec<String>, usize)>>>;

#[derive(Debug, Clone)]
pub struct ServerState {
    pub state: Either<MasterState, ReplicaState>,
    pub db: Db,
    pub blocks: Blocks,
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
    pub fn new(is_master: bool, replica_tx: Option<broadcast::Sender<Bytes>>, db: Option<HashMap<String, (DbValue, Option<SystemTime>)>>) -> Self {
        ServerState {
            state: if is_master {
                Either::Left(MasterState::new(
                    replica_tx.expect("Master state requires a replica sender"),
                ))
            } else {
                Either::Right(ReplicaState::new())
            },
            db: Arc::new(RwLock::new(db.unwrap_or(HashMap::new()))),
            blocks: Arc::new(Mutex::new(HashMap::new())),
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
    ReplicaToMaster
}

const UNSET: usize = usize::MAX;

#[derive(Clone, Debug)]
pub struct ConnectionState {
    is_master: bool,
    connection_type: Arc<AtomicConnectionType>,
    transaction_active: Arc<AtomicBool>,
    pub transaction_queue: TransactionQueue,
    replica_id: Arc<AtomicUsize>,
    pub stream_tx: mpsc::Sender<Bytes>,
}

impl ConnectionState {
    pub fn new(stream_tx: mpsc::Sender<Bytes>, is_master: bool) -> Self {
        ConnectionState {
            is_master,
            connection_type: Arc::new(AtomicConnectionType::new(ConnectionType::Client)),
            transaction_active: Arc::new(AtomicBool::new(false)),
            transaction_queue: Arc::new(Mutex::new(Vec::new())),
            replica_id: Arc::new(AtomicUsize::new(UNSET)),
            stream_tx
        }
    }

    pub fn set_connection_type(&mut self, connection_type: ConnectionType) {
        self.connection_type.store(connection_type, Ordering::Relaxed);
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
            id => Some(id)
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
}
