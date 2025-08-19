use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
pub struct ReplicaTracker {
    pub offsets: HashMap<usize, usize>,
    next_id: AtomicUsize,
}

impl ReplicaTracker {
    pub fn new() -> Self {
        ReplicaTracker {
            offsets: HashMap::new(),
            next_id: AtomicUsize::new(0),
        }
    }

    pub fn create_replica(&mut self) -> usize {
        let next_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.offsets.insert(next_id, 0);
        println!("Created replica with id {next_id}");
        next_id
    }

    pub fn update_offset(&mut self, replica_id: usize, offset: usize) {
        self.offsets.insert(replica_id, offset);
    }
}
