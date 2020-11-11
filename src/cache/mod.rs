// Copyright 2019 Fullstop000 <fullstop1005@gmail.com>.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

pub mod lru;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

/// A `Cache` is an interface that maps keys to values.
/// It has internal synchronization and may be safely accessed concurrently from
/// multiple threads.
/// It may automatically evict entries to make room for new entries.
/// Values have a specified charge against the cache capacity.
/// For example, a cache where the values are variable length strings, may use the
/// length of the string as the charge for the string.
///
/// A builtin cache implementation with a least-recently-used eviction
/// policy is provided.
/// Clients may use their own implementations if
/// they want something more sophisticated (like scan-resistance, a
/// custom eviction policy, variable cache sizing, etc.)
pub trait Cache<K, V>: Sync + Send
where
    K: Sync + Send,
    V: Sync + Send + Clone,
{
    /// Insert a mapping from key->value into the cache and assign it
    /// the specified charge against the total cache capacity.
    fn insert(&self, key: K, value: V, charge: usize) -> Option<V>;

    /// If the cache has no mapping for `key`, returns `None`.
    fn get(&self, key: &K) -> Option<V>;

    /// If the cache contains entry for key, erase it.
    fn erase(&self, key: &K);

    /// Return an estimate of the combined charges of all elements stored in the
    /// cache.
    fn total_charge(&self) -> usize;
}

/// A sharded cache container by key hash
pub struct ShardedCache<K, V, C>
where
    C: Cache<K, V>,
    K: Sync + Send,
    V: Sync + Send + Clone,
{
    shards: Arc<Vec<C>>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V, C> ShardedCache<K, V, C>
where
    C: Cache<K, V>,
    K: Sync + Send + Hash + Eq,
    V: Sync + Send + Clone,
{
    /// Create a new `ShardedCache` with given shards
    pub fn new(shards: Vec<C>) -> Self {
        Self {
            shards: Arc::new(shards),
            _k: PhantomData,
            _v: PhantomData,
        }
    }

    fn find_shard(&self, k: &K) -> usize {
        let mut s = DefaultHasher::new();
        let len = self.shards.len();
        k.hash(&mut s);
        s.finish() as usize % len
    }
}

impl<K, V, C> Cache<K, V> for ShardedCache<K, V, C>
where
    C: Cache<K, V>,
    K: Sync + Send + Hash + Eq,
    V: Sync + Send + Clone,
{
    fn insert(&self, key: K, value: V, charge: usize) -> Option<V> {
        let idx = self.find_shard(&key);
        self.shards[idx].insert(key, value, charge)
    }

    fn get(&self, key: &K) -> Option<V> {
        let idx = self.find_shard(key);
        self.shards[idx].get(key)
    }

    fn erase(&self, key: &K) {
        let idx = self.find_shard(key);
        self.shards[idx].erase(key)
    }

    fn total_charge(&self) -> usize {
        self.shards.iter().fold(0, |acc, s| acc + s.total_charge())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lru::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;

    fn new_test_lru_shards(n: usize) -> Vec<LRUCache<String, String>> {
        (0..n).into_iter().fold(vec![], |mut acc, _| {
            acc.push(LRUCache::new(1 << 20));
            acc
        })
    }

    #[test]
    fn test_concurrent_insert() {
        let cache = Arc::new(ShardedCache::new(new_test_lru_shards(8)));
        let n = 4; // use 4 thread
        let repeated = 10;
        let mut hs = vec![];
        let kv: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(vec![]));
        let total_size = Arc::new(AtomicU64::new(0));
        for i in 0..n {
            let cache = cache.clone();
            let kv = kv.clone();
            let total_size = total_size.clone();
            let h = thread::spawn(move || {
                for x in 1..=repeated {
                    let k = i.to_string().repeat(x);
                    let v = k.clone();
                    {
                        let mut kv = kv.lock().unwrap();
                        (*kv).push((k.clone(), v.clone()));
                    }
                    total_size.fetch_add(x as u64, Ordering::SeqCst);
                    assert_eq!(cache.insert(k, v, x), None);
                }
            });
            hs.push(h);
        }
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(
            total_size.load(Ordering::Relaxed) as usize,
            cache.total_charge()
        );
        for (k, v) in kv.lock().unwrap().clone() {
            assert_eq!(cache.get(&k), Some(v));
        }
    }
}
