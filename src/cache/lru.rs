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

use crate::cache::Cache;
use crate::util::collection::HashMap;

use std::hash::{Hash, Hasher};
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

#[derive(Copy, Clone)]
struct Key<K> {
    k: *const K,
}

impl<K: Hash> Hash for Key<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { (*self.k).hash(state) }
    }
}

impl<K: PartialEq> PartialEq for Key<K> {
    fn eq(&self, other: &Key<K>) -> bool {
        unsafe { (*self.k).eq(&*other.k) }
    }
}

impl<K: Eq> Eq for Key<K> {}

impl<K> Default for Key<K> {
    fn default() -> Self {
        Key { k: ptr::null() }
    }
}

/// Exact node in the `LRUCache`
pub struct LRUHandle<K, V> {
    key: K,
    value: V,
    prev: *mut LRUHandle<K, V>,
    next: *mut LRUHandle<K, V>,
    charge: usize,
}

impl<K, V> LRUHandle<K, V> {
    /// Create new LRUHandle
    pub fn new(key: K, value: V, charge: usize) -> LRUHandle<K, V> {
        LRUHandle {
            key,
            value,
            charge,
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }
}

// TODO: remove mutex
/// LRU cache implementation
pub struct LRUCache<K, V> {
    // The capacity of LRU
    capacity: usize,
    mutex: Mutex<MutexFields<K, V>>,
    // The size of space which have been allocated
    usage: AtomicUsize,
    cache_id: AtomicU64,
    callback: Option<Box<dyn Fn(&K, &V)>>,
}

struct MutexFields<K, V> {
    // Dummy head of LRU list.
    // lru.prev is newest entry, lru.next is oldest entry.
    lru: *mut LRUHandle<K, V>,
    table: HashMap<Key<K>, Box<LRUHandle<K, V>>>,
}

impl<K: Hash + Eq, V> LRUCache<K, V> {
    pub fn new(cap: usize, callback: Option<Box<dyn Fn(&K, &V)>>) -> Self {
        let mutex = unsafe {
            MutexFields {
                lru: Self::create_dummy_node(),
                table: HashMap::default(),
            }
        };
        LRUCache {
            usage: AtomicUsize::new(0),
            capacity: cap,
            mutex: Mutex::new(mutex),
            cache_id: AtomicU64::new(0),
            callback,
        }
    }

    // Unlink the node `n`
    fn lru_remove(n: *mut LRUHandle<K, V>) {
        unsafe {
            (*(*n).next).prev = (*n).prev;
            (*(*n).prev).next = (*n).next;
        }
    }

    // Append the `new_node` to the head of given list `n`
    fn lru_append(n: *mut LRUHandle<K, V>, new_node: *mut LRUHandle<K, V>) {
        unsafe {
            (*new_node).next = n;
            (*new_node).prev = (*n).prev;
            (*(*n).prev).next = new_node;
            (*n).prev = new_node;
        }
    }

    // Create a dummy node whose 'next' and 'prev' are both itself
    unsafe fn create_dummy_node() -> *mut LRUHandle<K, V> {
        let n: *mut LRUHandle<K, V> =
            Box::into_raw(Box::new(mem::MaybeUninit::uninit().assume_init()));
        (*n).next = n;
        (*n).prev = n;
        n
    }
}

impl<K: Hash + Eq, V> Cache<K, V> for LRUCache<K, V> {
    fn insert(&self, key: K, value: V, charge: usize) -> Option<V> {
        let mut mutex_data = self.mutex.lock().unwrap();
        if self.capacity > 0 {
            match mutex_data.table.get_mut(&Key {
                k: &key as *const K,
            }) {
                Some(h) => {
                    // swap the value and move handle to the head
                    let old_v = mem::replace(&mut h.value, value);
                    let p: *mut LRUHandle<K, V> = h.as_mut();
                    Self::lru_remove(p);
                    Self::lru_append(mutex_data.lru, p);
                    if let Some(cb) = &self.callback {
                        cb(&key, &old_v);
                    }
                    Some(old_v)
                }
                None => {
                    let handle = LRUHandle::new(key, value, charge);
                    let mut v = Box::new(handle);
                    let k: *const K = &v.as_ref().key;
                    let value_ptr: *mut LRUHandle<K, V> = v.as_mut();
                    mutex_data.table.insert(Key { k }, v);
                    // Self::lru_remove(value_ptr);
                    Self::lru_append(mutex_data.lru, value_ptr);
                    self.usage.fetch_add(charge, Ordering::Release);
                    // evict last lru entries
                    unsafe {
                        while self.usage.load(Ordering::Acquire) > self.capacity
                            && (*(*mutex_data).lru).next != mutex_data.lru
                        // not the dummy head
                        {
                            let k = &(*(*mutex_data.lru).next).key as *const K;
                            let key = Key { k };
                            if let Some(mut n) = mutex_data.table.remove(&key) {
                                self.usage.fetch_sub(n.charge, Ordering::SeqCst);
                                Self::lru_remove(n.as_mut());
                                if let Some(cb) = &self.callback {
                                    cb(&n.key, &n.value);
                                }
                            }
                        }
                    }
                    None
                }
            }
        } else {
            None
        }
    }

    fn look_up<'a>(&'a self, key: &K) -> Option<&'a V> {
        let k = Key { k: key as *const K };
        let l = self.mutex.lock().unwrap();
        l.table.get(&k).map(|h| {
            let p = h.as_ref() as *const LRUHandle<K, V> as *mut LRUHandle<K, V>;
            Self::lru_remove(p);
            Self::lru_append(l.lru, p);
            unsafe { &(*p).value }
        })
    }

    fn erase(&self, key: &K) {
        let k = Key { k: key as *const K };
        let mut mutex_data = self.mutex.lock().unwrap();
        // remove the key in hashtable
        if let Some(mut n) = mutex_data.table.remove(&k) {
            self.usage.fetch_sub(n.charge, Ordering::SeqCst);
            Self::lru_remove(n.as_mut() as *mut LRUHandle<K, V>);
            if let Some(cb) = &self.callback {
                cb(key, &n.value);
            }
        }
    }

    #[inline]
    fn new_id(&self) -> u64 {
        self.cache_id.fetch_add(1, Ordering::SeqCst)
    }

    fn prune(&self) {
        unimplemented!()
    }

    #[inline]
    fn total_charge(&self) -> usize {
        self.usage.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::coding::{decode_fixed_32, put_fixed_32};
    use std::cell::RefCell;
    use std::rc::Rc;

    const CACHE_SIZE: usize = 100;

    struct CacheTest {
        cache: LRUCache<Vec<u8>, u32>,
        deleted_kv: Rc<RefCell<Vec<(u32, u32)>>>,
    }

    impl CacheTest {
        fn new(cap: usize) -> Self {
            let deleted_kv = Rc::new(RefCell::new(vec![]));
            let cloned = deleted_kv.clone();
            let callback: Box<dyn Fn(&Vec<u8>, &u32)> = Box::new(move |k, v| {
                let key = decode_fixed_32(k);
                cloned.borrow_mut().push((key, *v));
            });
            Self {
                cache: LRUCache::<Vec<u8>, u32>::new(cap, Some(callback)),
                deleted_kv,
            }
        }

        fn look_up(&self, key: u32) -> Option<u32> {
            let mut k = vec![];
            put_fixed_32(&mut k, key);
            self.cache.look_up(&k).and_then(|v| Some(*v))
        }

        fn insert(&self, key: u32, value: u32) {
            self.cache.insert(encoded_u32(key), value, 1);
        }

        fn insert_with_charge(&self, key: u32, value: u32, charge: usize) {
            self.cache.insert(encoded_u32(key), value, charge);
        }

        fn erase(&self, key: u32) {
            let mut k = vec![];
            put_fixed_32(&mut k, key);
            self.cache.erase(&k);
        }

        fn assert_deleted_kv(&self, index: usize, (key, val): (u32, u32)) {
            assert_eq!((key, val), self.deleted_kv.borrow()[index]);
        }

        fn assert_get(&self, key: u32, want: u32) -> &u32 {
            let encoded = encoded_u32(key);
            let h = self.cache.look_up(&encoded).unwrap();
            assert_eq!(want, *h);
            h
        }
    }

    fn encoded_u32(i: u32) -> Vec<u8> {
        let mut v = vec![];
        put_fixed_32(&mut v, i);
        v
    }

    #[test]
    fn test_hit_and_miss() {
        let cache = CacheTest::new(CACHE_SIZE);
        assert_eq!(None, cache.look_up(100));
        cache.insert(100, 101);
        assert_eq!(Some(101), cache.look_up(100));
        assert_eq!(None, cache.look_up(200));
        assert_eq!(None, cache.look_up(300));

        cache.insert(200, 201);
        assert_eq!(Some(101), cache.look_up(100));
        assert_eq!(Some(201), cache.look_up(200));
        assert_eq!(None, cache.look_up(300));

        cache.insert(100, 102);
        assert_eq!(Some(102), cache.look_up(100));
        assert_eq!(Some(201), cache.look_up(200));
        assert_eq!(None, cache.look_up(300));

        assert_eq!(1, cache.deleted_kv.borrow().len());
        cache.assert_deleted_kv(0, (100, 101));
    }

    #[test]
    fn test_erase() {
        let cache = CacheTest::new(CACHE_SIZE);
        cache.erase(200);
        assert_eq!(0, cache.deleted_kv.borrow().len());

        cache.insert(100, 101);
        cache.insert(200, 201);
        cache.erase(100);

        assert_eq!(None, cache.look_up(100));
        assert_eq!(Some(201), cache.look_up(200));
        assert_eq!(1, cache.deleted_kv.borrow().len());
        cache.assert_deleted_kv(0, (100, 101));

        cache.erase(100);
        assert_eq!(None, cache.look_up(100));
        assert_eq!(Some(201), cache.look_up(200));
        assert_eq!(1, cache.deleted_kv.borrow().len());
    }

    #[test]
    fn test_entries_are_pinned() {
        let cache = CacheTest::new(CACHE_SIZE);
        cache.insert(100, 101);
        let v1 = cache.assert_get(100, 101);
        assert_eq!(*v1, 101);
        cache.insert(100, 102);
        let v2 = cache.assert_get(100, 102);
        assert_eq!(1, cache.deleted_kv.borrow().len());
        cache.assert_deleted_kv(0, (100, 101));
        assert_eq!(*v1, 102);
        assert_eq!(*v2, 102);

        cache.erase(100);
        assert_eq!(*v1, 102);
        assert_eq!(*v2, 102);
        assert_eq!(None, cache.look_up(100));
        assert_eq!(2, cache.deleted_kv.borrow().len());
        cache.assert_deleted_kv(1, (100, 102));
    }

    #[test]
    fn test_eviction_policy() {
        let cache = CacheTest::new(CACHE_SIZE);
        cache.insert(100, 101);
        cache.insert(200, 201);
        cache.insert(300, 301);

        // frequently used entry must be kept around
        for i in 0..(CACHE_SIZE + 100) as u32 {
            cache.insert(1000 + i, 2000 + i);
            assert_eq!(Some(2000 + i), cache.look_up(1000 + i));
            assert_eq!(Some(101), cache.look_up(100));
        }
        assert_eq!(cache.cache.mutex.lock().unwrap().table.len(), CACHE_SIZE);
        assert_eq!(Some(101), cache.look_up(100));
        assert_eq!(None, cache.look_up(200));
        assert_eq!(None, cache.look_up(300));
    }

    #[test]
    fn test_use_exceeds_cache_size() {
        let cache = CacheTest::new(CACHE_SIZE);
        let extra = 100;
        let total = CACHE_SIZE + extra;
        // overfill the cache, keeping handles on all inserted entries
        for i in 0..total as u32 {
            cache.insert(1000 + i, 2000 + i)
        }

        // check that all the entries can be found in the cache
        for i in 0..total as u32 {
            if i < extra as u32 {
                assert_eq!(None, cache.look_up(1000 + i))
            } else {
                assert_eq!(Some(2000 + i), cache.look_up(1000 + i))
            }
        }
    }

    #[test]
    fn test_heavy_entries() {
        let cache = CacheTest::new(CACHE_SIZE);
        let light = 1;
        let heavy = 10;
        let mut added = 0;
        let mut index = 0;
        while added < 2 * CACHE_SIZE {
            let weight = if index & 1 == 0 { light } else { heavy };
            cache.insert_with_charge(index, 1000 + index, weight);
            added += weight;
            index += 1;
        }
        let mut cache_weight = 0;
        for i in 0..index {
            let weight = if index & 1 == 0 { light } else { heavy };
            if let Some(val) = cache.look_up(i) {
                cache_weight += weight;
                assert_eq!(1000 + i, val);
            }
        }
        assert!(cache_weight < CACHE_SIZE);
    }

    #[test]
    fn test_zero_size_cache() {
        let cache = CacheTest::new(0);
        cache.insert(100, 101);
        assert_eq!(None, cache.look_up(100));
    }
}
