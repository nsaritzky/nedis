use std::{
    borrow::Borrow,
    collections::{
        hash_map::{self},
        HashMap,
    },
    future::Future,
    hash::{DefaultHasher, Hash, Hasher},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use futures::future::join_all;
use tokio::sync::{RwLock, RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug, Clone)]
pub struct ShardMap<K, V> {
    shard_count: usize,
    size: Arc<AtomicUsize>,
    maps: Box<[Arc<RwLock<HashMap<K, V>>>]>,
}

impl<K, V> ShardMap<K, V> {
    pub fn new(shard_count: usize) -> Self {
        ShardMap {
            shard_count,
            size: Arc::new(AtomicUsize::new(0)),
            maps: (0..shard_count)
                .map(|_| Arc::new(RwLock::new(HashMap::new())))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    async fn get_map_from_index(&self, index: usize) -> RwLockReadGuard<HashMap<K, V>> {
        self.maps[index].read().await
    }
}

impl<K, V> ShardMap<K, V>
where
    K: Eq + Hash,
{
    pub fn from_hash_map(shard_count: usize, map: HashMap<K, V>) -> Self {
        let mut maps = (0..shard_count).map(|_| HashMap::new()).collect::<Vec<_>>();
        let size = map.len();
        for (k, v) in map {
            let bucket_index = Self::get_index_for_key(shard_count as u64, &k);
            maps[bucket_index].insert(k, v);
        }
        Self {
            shard_count,
            size: Arc::new(AtomicUsize::new(size)),
            maps: maps
                .into_iter()
                .map(|m| Arc::new(RwLock::new(m)))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    pub async fn with_value<R, F, Q>(&self, key: &Q, f: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        F: FnOnce(&V) -> R,
    {
        let map = self.get_bucket(key).await;
        map.get(key).map(f)
    }

    pub async fn with_values<R, F, Q>(&self, keys: &[Q], f: F) -> R
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
        F: FnOnce(&[Option<&V>]) -> R,
    {
        let mut key_groups: HashMap<usize, Vec<(usize, &Q)>> = HashMap::new();

        for (original_idx, key) in keys.iter().enumerate() {
            let shard_idx = Self::get_index_for_key(self.shard_count as u64, key);
            key_groups
                .entry(shard_idx)
                .and_modify(|vec| vec.push((original_idx, key)))
                .or_insert(vec![(original_idx, key)]);
        }

        let futures = key_groups
            .keys()
            .map(|shard_idx| async move { (shard_idx, self.maps[*shard_idx].read().await) });

        let guards = join_all(futures).await;

        let guard_map: HashMap<_, _> = guards.iter().map(|(idx, guard)| (*idx, guard)).collect();

        let results: Vec<_> = keys
            .iter()
            .map(|key| {
                let shard_idx = Self::get_index_for_key(self.shard_count as u64, key);
                guard_map.get(&shard_idx).and_then(|guard| guard.get(key))
            })
            .collect();

        f(&results)
    }

    pub async fn with_key_values<R, F, Q>(&self, keys: &[&Q], f: F) -> R
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        F: FnOnce(&[(&Q, Option<&V>)]) -> R,
    {
        let mut key_groups: HashMap<usize, Vec<(usize, &Q)>> = HashMap::new();

        for (original_idx, key) in keys.iter().enumerate() {
            let shard_idx = Self::get_index_for_key(self.shard_count as u64, key);
            key_groups
                .entry(shard_idx)
                .and_modify(|vec| vec.push((original_idx, key)))
                .or_insert(vec![(original_idx, key)]);
        }

        let futures = key_groups
            .keys()
            .map(|shard_idx| async move { (shard_idx, self.maps[*shard_idx].read().await) });

        let guards = join_all(futures).await;

        let guard_map: HashMap<_, _> = guards.iter().map(|(idx, guard)| (*idx, guard)).collect();

        let results: Vec<_> = keys
            .iter()
            .map(|key| {
                let shard_idx = Self::get_index_for_key(self.shard_count as u64, key);
                guard_map.get(&shard_idx).and_then(|guard| guard.get(key))
            })
            .collect();

        let inputs: Vec<_> = keys.iter().map(|&k| k).zip(results).collect();
        f(&inputs[..])
    }

    async fn with_values_mut_raw<R, F, Q>(&self, keys: &[Q], f: F) -> R
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
        F: FnOnce(&mut [Option<*mut V>]) -> R,
    {
        let mut key_groups: HashMap<usize, Vec<(usize, &Q)>> = HashMap::new();

        for (original_idx, key) in keys.iter().enumerate() {
            let shard_idx = Self::get_index_for_key(self.shard_count as u64, key);
            key_groups
                .entry(shard_idx)
                .and_modify(|vec| vec.push((original_idx, key)))
                .or_insert(vec![(original_idx, key)]);
        }

        let futures = key_groups
            .keys()
            .map(|&shard_idx| async move { (shard_idx, self.maps[shard_idx].write().await) });

        let mut guards = join_all(futures).await;

        let mut results = vec![None; keys.len()];

        for (shard_idx, guard) in guards.iter_mut() {
            if let Some(keys_for_shard) = key_groups.get(shard_idx) {
                for &(original_idx, key) in keys_for_shard {
                    if let Some(value) = guard.get_mut(key) {
                        results[original_idx] = Some(value as *mut V);
                    }
                }
            }
        }
        f(&mut results)
    }

    pub async fn with_values_mut<R, F, Q>(&self, keys: &[Q], f: F) -> R
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
        F: FnOnce(&mut [Option<&mut V>]) -> R,
    {
        self.with_values_mut_raw(keys, |ptrs| {
            let mut refs: Vec<Option<&mut V>> = ptrs
                .iter_mut()
                .map(|ptr_opt| ptr_opt.map(|ptr| unsafe { &mut *ptr }))
                .collect();

            f(&mut refs)
        })
        .await
    }

    pub async fn with_values_owned<R, F, Q>(&self, keys: &[Q], f: F) -> R
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
        V: Clone,
        F: FnOnce(&[Option<V>]) -> R,
    {
        let mut key_groups: HashMap<usize, Vec<(usize, &Q)>> = HashMap::new();

        for (original_idx, key) in keys.iter().enumerate() {
            let shard_idx = Self::get_index_for_key(self.shard_count as u64, key);
            key_groups
                .entry(shard_idx)
                .and_modify(|vec| vec.push((original_idx, key)))
                .or_insert(vec![(original_idx, key)]);
        }

        let futures = key_groups
            .keys()
            .map(|&shard_idx| async move { (shard_idx, self.maps[shard_idx].read().await) });

        let guards = join_all(futures).await;

        let mut results = vec![None; keys.len()];

        let guard_map: HashMap<_, _> = guards.iter().map(|(idx, guard)| (*idx, guard)).collect();

        for (original_idx, key) in keys.iter().enumerate() {
            let shard_idx = Self::get_index_for_key(self.shard_count as u64, key);
            if let Some(guard) = guard_map.get(&shard_idx) {
                results[original_idx] = guard.get(key).cloned();
            }
        }

        drop(guards);

        f(&results)
    }

    pub async fn get<Q>(&self, key: &Q) -> Option<RwLockReadGuard<V>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let guard = self.get_bucket(key).await;
        RwLockReadGuard::try_map(guard, |m| m.get(key)).ok()
    }

    pub async fn get_mut<Q>(&mut self, key: &Q) -> Option<RwLockMappedWriteGuard<V>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let guard = self.get_bucket_mut(key).await;
        RwLockWriteGuard::try_map(guard, |m| m.get_mut(key)).ok()
    }

    pub async fn get_or_remove<Q, F>(&self, key: &Q, f: F) -> Option<RwLockReadGuard<V>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        F: FnOnce(&V) -> bool,
    {
        let mut guard = self.get_bucket_mut(key).await;
        if let Some(value) = guard.get(key) {
            if f(value) {
                guard.remove(key);
                None
            } else {
                let read_guard = guard.downgrade();
                Some(RwLockReadGuard::map(read_guard, |g| g.get(key).unwrap()))
            }
        } else {
            None
        }
    }

    pub async fn get_mut_or_remove<Q, F>(&self, key: &Q, f: F) -> Option<RwLockMappedWriteGuard<V>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        F: FnOnce(&V) -> bool,
    {
        let mut guard = self.get_bucket_mut(key).await;
        if let Some(value) = guard.get(key) {
            if f(value) {
                guard.remove(key);
                None
            } else {
                Some(RwLockWriteGuard::map(guard, |g| g.get_mut(key).unwrap()))
            }
        } else {
            None
        }
    }

    pub async fn insert(&mut self, key: K, value: V) -> Option<V> {
        let mut map = self.get_bucket_mut(&key).await;
        let res = map.insert(key, value);
        if res.is_none() {
            self.size.fetch_add(1, Ordering::Relaxed);
        }
        res
    }

    pub async fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let mut map = self.get_bucket_mut(&key).await;
        let res = map.remove(&key);
        if res.is_some() {
            self.size.fetch_sub(1, Ordering::Relaxed);
        }
        res
    }

    pub async fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let map = self.get_bucket(key).await;
        map.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    fn get_shard_count(&self) -> usize {
        self.shard_count
    }

    pub async fn with_entry<F, R>(&mut self, key: K, f: F) -> R
    where
        F: FnOnce(hash_map::Entry<K, V>) -> R,
    {
        let mut guard = self.get_bucket_mut(&key).await;
        let size_before = guard.len();
        let entry = guard.entry(key);
        let res = f(entry);
        let size_after = guard.len();
        let diff = (size_after as isize) - (size_before as isize);
        if diff > 0 {
            self.size.fetch_add(diff as usize, Ordering::Relaxed);
        } else {
            self.size.fetch_sub(diff.abs() as usize, Ordering::Relaxed);
        }
        res
    }

    async fn get_bucket<Q>(&self, k: &Q) -> RwLockReadGuard<HashMap<K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let index = Self::get_index_for_key(self.shard_count as u64, k);
        self.maps[index].read().await
    }

    async fn get_bucket_mut<Q>(&self, k: &Q) -> RwLockWriteGuard<HashMap<K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let index = Self::get_index_for_key(self.shard_count as u64, k);
        self.maps[index].write().await
    }

    fn get_index_for_key<Q>(shard_count: u64, k: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut hasher = DefaultHasher::new();
        k.hash(&mut hasher);
        let hash_value = hasher.finish();
        (hash_value % shard_count) as usize
    }
}

impl<K, V> ShardMap<K, V>
where
    K: Eq + Hash + Clone,
{
    pub async fn entry<'a>(&'a mut self, key: K) -> ShardMapEntry<'a, K, V> {
        let guard = self.get_bucket_mut(&key).await;
        ShardMapEntry::new(guard, self.size.clone(), key)
    }
}

impl<K, V> ShardMap<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub async fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&K, &V),
    {
        for map in self.maps.iter() {
            let guard = map.read().await;
            for (k, v) in guard.iter() {
                f(k, v);
            }
        }
    }

    pub async fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        let mut new_size = 0;
        for map in self.maps.iter() {
            let mut guard = map.write().await;
            guard.retain(&mut f);
            new_size += guard.len();
        }
        self.size.store(new_size, Ordering::Relaxed);
    }

    pub async fn for_each_async<F, Fut, O>(&self, f: F)
    where
        F: FnMut(&K, &V) -> Fut,
        Fut: Future<Output = O>,
    {
    }
}

pub enum ShardMapEntry<'a, K, V> {
    Occupied(OccupiedShardMapEntry<'a, K, V>),
    Vacant(VacantShardMapEntry<'a, K, V>),
}

impl<'a, K, V> ShardMapEntry<'a, K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn new(
        guard: RwLockWriteGuard<'a, HashMap<K, V>>,
        shard_map_size: Arc<AtomicUsize>,
        key: K,
    ) -> Self {
        if guard.contains_key(&key) {
            Self::Occupied(OccupiedShardMapEntry::new(guard, shard_map_size, key))
        } else {
            Self::Vacant(VacantShardMapEntry::new(guard, shard_map_size, key))
        }
    }

    pub fn get(&self) -> Option<&V> {
        match self {
            Self::Occupied(occ) => Some(occ.get()),
            Self::Vacant(_) => None,
        }
    }

    pub fn remove(&mut self) -> Option<V> {
        match self {
            Self::Occupied(occ) => Some(occ.remove()),
            Self::Vacant(_) => None,
        }
    }

    pub fn and_modify<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut V),
    {
        match self {
            Self::Occupied(mut entry) => {
                f(entry.get_mut());
                Self::Occupied(entry)
            }
            Self::Vacant(entry) => Self::Vacant(entry),
        }
    }

    pub fn or_insert<F>(self, default: V) {
        if let Self::Vacant(entry) = self {
            entry.insert(default);
        }
    }
}

pub struct OccupiedShardMapEntry<'a, K, V> {
    guard: RwLockWriteGuard<'a, HashMap<K, V>>,
    shard_map_size: Arc<AtomicUsize>,
    key: K,
}

impl<'a, K, V> OccupiedShardMapEntry<'a, K, V>
where
    K: Eq + Hash + Clone,
{
    fn new(
        guard: RwLockWriteGuard<'a, HashMap<K, V>>,
        shard_map_size: Arc<AtomicUsize>,
        key: K,
    ) -> Self {
        Self {
            guard,
            shard_map_size,
            key,
        }
    }

    pub fn get(&self) -> &V {
        self.guard.get(&self.key).unwrap()
    }

    pub fn get_mut(&mut self) -> &mut V {
        self.guard.get_mut(&self.key).unwrap()
    }

    pub fn insert(&mut self, value: V) -> V {
        if self.guard.contains_key(&self.key) {
            self.shard_map_size.fetch_add(1, Ordering::Relaxed);
        }
        self.guard.insert(self.key.clone(), value).unwrap()
    }

    pub fn remove(&mut self) -> V {
        self.shard_map_size.fetch_sub(1, Ordering::Relaxed);
        self.guard.remove(&self.key).unwrap()
    }

    pub fn with_entry<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(hash_map::Entry<K, V>) -> R,
    {
        let exists_before = self.guard.contains_key(&self.key);
        let entry = self.guard.entry(self.key.clone());
        let res = f(entry);
        let exists_after = self.guard.contains_key(&self.key);
        if exists_before && !exists_after {
            self.shard_map_size.fetch_sub(1, Ordering::Relaxed);
        } else if !exists_before && exists_after {
            self.shard_map_size.fetch_add(1, Ordering::Relaxed);
        }
        res
    }
}

pub struct VacantShardMapEntry<'a, K, V> {
    guard: RwLockWriteGuard<'a, HashMap<K, V>>,
    shard_map_size: Arc<AtomicUsize>,
    key: K,
}

impl<'a, K, V> VacantShardMapEntry<'a, K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn insert(mut self, value: V) {
        self.guard.insert(self.key, value);
    }

    fn new(
        guard: RwLockWriteGuard<'a, HashMap<K, V>>,
        shard_map_size: Arc<AtomicUsize>,
        key: K,
    ) -> Self
    where
        K: Eq + Hash + Clone,
    {
        Self {
            guard,
            shard_map_size,
            key,
        }
    }
}
