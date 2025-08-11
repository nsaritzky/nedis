use std::{
    borrow::Borrow, collections::{hash_map, HashMap}, future::Future, hash::{DefaultHasher, Hash, Hasher}, pin::Pin, sync::{atomic::{AtomicUsize, Ordering}, Arc}, task::{Context, Poll}
};

use tokio::{pin, sync::{RwLock, RwLockReadGuard, RwLockWriteGuard}};
use tokio_stream::Stream;

pub struct ShardMap<K, V> {
    shard_count: usize,
    size: AtomicUsize,
    maps: Box<[Arc<RwLock<HashMap<K, V>>>]>,
}

impl<K, V> ShardMap<K, V> {
    pub fn new(shard_count: usize) -> Self {
        ShardMap {
            shard_count,
            size: AtomicUsize::new(0),
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
    pub async fn with_value<R, F, Q>(&self, key: &Q, f: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        F: FnOnce(&V) -> R,
    {
        let map = self.get_bucket(key).await;
        map.get(key).map(f)
    }

    pub async fn get<Q>(&self, key: &Q) -> Option<RwLockReadGuard<V>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let guard = self.get_bucket(key).await;
        if guard.contains_key(key) {
            Some(RwLockReadGuard::map(guard, |m| &m[key]))
        } else {
            None
        }
    }

    pub async fn insert<Q>(&mut self, key: K, value: V) -> Option<V> {
        let mut map = self.get_bucket_mut(&key).await;
        let res = map.insert(key, value);
        if res.is_none() {
            self.size.fetch_add(1, Ordering::Relaxed);
        }
        res
    }

    pub async fn remove<Q>(&mut self, key: K) -> Option<V> {
        let mut map = self.get_bucket_mut(&key).await;
        let res = map.remove(&key);
        if res.is_some() {
            self.size.fetch_sub(1, Ordering::Relaxed);
        }
        res
    }

    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    fn get_shard_count(&self) -> usize {
        self.shard_count
    }

    async fn get_bucket<Q>(&self, k: &Q) -> RwLockReadGuard<HashMap<K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut hasher = DefaultHasher::new();
        k.hash(&mut hasher);
        let hash_value = hasher.finish();
        let index = (hash_value % self.shard_count as u64) as usize;
        self.maps[index].read().await
    }

    async fn get_bucket_mut<Q>(&self, k: &Q) -> RwLockWriteGuard<HashMap<K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut hasher = DefaultHasher::new();
        k.hash(&mut hasher);
        let hash_value = hasher.finish();
        let index = (hash_value % self.shard_count as u64) as usize;
        self.maps[index].write().await
    }
}

pub struct ShardMapStream<'a, 'b, K, V> {
    shard_map: &'b ShardMap<K, V>,
    current_index: usize,
    current_iter: Option<hash_map::Iter<'a, K, V>>
}

// impl<'a, 'b, K, V> Stream for ShardMapStream<'a, 'b, K, V> {
//     type Item = (&'a K, &'a V);

//     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         let mut map = self.shard_map.get_map_from_index(self.current_index);
//         pin!(map);
//         match map.poll(cx) {
//             Poll::Ready(guard) => {
//                 if let Some(iter) = self.current_iter {
//                     Poll::Ready()
//                 }
//             },
//         }
//     }


// }
