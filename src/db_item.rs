use std::time::SystemTime;

use crate::db_value::DbValue;

#[derive(Debug, Clone)]
pub struct DbItem {
    value: DbValue,
    updated_at: SystemTime,
    expires: Option<SystemTime>,
}

impl DbItem {
    pub fn new(value: DbValue, expires: Option<SystemTime>) -> Self {
        Self {
            value,
            updated_at: SystemTime::now(),
            expires,
        }
    }

    pub fn update(&mut self, new_value: DbValue) -> DbValue {
        self.updated_at = SystemTime::now();
        std::mem::replace(&mut self.value, new_value)
    }

    pub fn update_with<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut DbValue) -> R,
    {
        self.updated_at = SystemTime::now();
        f(&mut self.value)
    }

    pub fn expires_at(&self) -> Option<SystemTime> {
        self.expires
    }

    pub fn updated_at(&self) -> SystemTime {
        self.updated_at
    }

    pub fn update_timestamp(&mut self) {
        self.updated_at = SystemTime::now();
    }

    pub fn value(&self) -> &DbValue {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut DbValue {
        &mut self.value
    }
}
