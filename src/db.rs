use std::{
    ops::{Deref, DerefMut}, time::SystemTime
};

use tokio::sync::{RwLockReadGuard, RwLockWriteGuard, RwLockMappedWriteGuard};

use crate::{db_item::DbItem, db_value::DbValue, shard_map::ShardMap};

#[derive(Debug, Clone)]
pub struct Db(ShardMap<String, DbItem>);

impl Deref for Db {
    type Target = ShardMap<String, DbItem>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Db {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Db {
    pub fn new(shard_map: ShardMap<String, DbItem>) -> Self {
        Self(shard_map)
    }

    pub async fn get(&'_ mut self, key: &str) -> Option<RwLockReadGuard<'_, DbValue>> {
        let item = self
            .0
            .get_or_remove(key, |item| {
                item.expires_at().is_some_and(|t| t < SystemTime::now())
            })
            .await;
        item.map(|it| RwLockReadGuard::map(it, |g| g.value()))
    }

    pub async fn get_mut(&'_ mut self, key: &str) -> Option<RwLockMappedWriteGuard<'_, DbItem>> {
        self
            .0
            .get_mut_or_remove(key, |item| {
                item.expires_at().is_some_and(|t| t < SystemTime::now())
            })
            .await
    }

        pub async fn get_item(&'_ mut self, key: &str) -> Option<RwLockReadGuard<'_, DbItem>> {
        self
            .0
            .get_or_remove(key, |item| {
                item.expires_at().is_some_and(|t| t < SystemTime::now())
            })
            .await
        }
}
