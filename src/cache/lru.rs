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

use crate::cache::{Cache, Handle as CacheHandle, HandleRef};
use hashbrown::hash_map::HashMap;
use std::cell::RefCell;
use std::fmt::Debug;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::util::hash::hash;

const NUM_SHARD_BITS: usize = 4;
const NUM_SHARD: usize = 1 << NUM_SHARD_BITS;

// TODO: add benchmark for lru

/// A LRUCache that can be accessed safely in multiple threads
pub struct SharedLRUCache<T: 'static + Default + Debug> {
    shards: Vec<LRUCache<T>>,
    last_id: AtomicU64,
}

impl<T: 'static + Default + Debug> SharedLRUCache<T> {
    pub fn new(cap: usize) -> Self {
        let per_shard = (cap + NUM_SHARD - 1) / NUM_SHARD;
        let mut shards = vec![];
        for _ in 0..NUM_SHARD {
            shards.push(LRUCache::new(cap));
        }
        Self {
            shards,
            last_id: AtomicU64::new(0),
        }
    }

    fn shard(&self, key: &[u8]) -> usize {
        (hash(key, 0) >> (32 - NUM_SHARD_BITS)) as usize
    }
}

impl<T: 'static + Default + Debug> Cache<T> for SharedLRUCache<T> {
    fn insert(
        &mut self,
        key: Vec<u8>,
        value: T,
        charge: usize,
        deleter: Option<Box<FnMut(&[u8], &T)>>,
    ) -> HandleRef<T> {
        let s = self.shard(key.as_slice());
        self.shards[s].insert(key, value, charge, deleter)
    }

    fn look_up(&self, key: &[u8]) -> Option<HandleRef<T>> {
        let s = self.shard(key);
        self.shards[s].look_up(key)
    }

    fn release(&mut self, handle: HandleRef<T>) {
        let p = handle.as_ptr() as *mut LRUHandle<T>;
        let hash = unsafe { (*p).hash };
        self.shards[(hash >> (32 - NUM_SHARD_BITS)) as usize].release(handle);
    }

    fn erase(&mut self, key: &[u8]) {
        let s = self.shard(key);
        self.shards[s].erase(key)
    }

    fn new_id(&mut self) -> u64 {
        let i = self.last_id.fetch_add(1, Ordering::SeqCst);
        i + 1
    }

    fn prune(&mut self) {
        for p in self.shards.iter_mut() {
            p.prune();
        }
    }

    fn total_charge(&self) -> usize {
        self.shards
            .iter()
            .fold(0, |sum, lru| sum + lru.mutex.lock().unwrap().usage)
    }
}

/// Exact node in the `LRUCache`
pub struct LRUHandle<T: Default + Debug> {
    value: T,
    deleter: Option<Box<FnMut(&[u8], &T)>>,
    prev: *mut LRUHandle<T>,
    next: *mut LRUHandle<T>,
    hash: u32, // Hash of key; used for fast sharding and comparisons
    charge: usize,
    key: Box<[u8]>,
}

impl<T: Default + Debug> Default for LRUHandle<T> {
    fn default() -> Self {
        LRUHandle {
            value: T::default(),
            deleter: None,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            charge: 0,
            hash: 0,
            key: Vec::new().into_boxed_slice(),
        }
    }
}

impl<T: Default + Debug> Drop for LRUHandle<T> {
    fn drop(&mut self) {
        if let Some(ref mut deleter) = self.deleter {
            (deleter)(&self.key, &self.value);
        }
    }
}

impl<T: Default + Debug> CacheHandle<T> for LRUHandle<T> {
    fn get_value(&self) -> &T {
        &self.value
    }
}
impl<T: Default + Debug> LRUHandle<T> {
    /// Create new LRUHandle
    pub fn new(
        key: Box<[u8]>,
        value: T,
        deleter: Option<Box<FnMut(&[u8], &T)>>,
        charge: usize,
    ) -> LRUHandle<T> {
        let hash = hash(key.as_ref(), 0);
        LRUHandle {
            key,
            value,
            deleter,
            charge,
            hash,
            ..Self::default()
        }
    }
}

// TODO: use a easier way to implement

/// LRU cache implementation
///
/// The cache keeps two linked lists of items in the cache.  All items in the
/// cache are in one list or the other, and never both.  Items still referenced
/// by clients but erased from the cache are in neither list.  The lists are:
///
/// - `in-use`:  contains the items currently referenced by clients, in no
///   particular order.  (This list is used for invariant checking.  If we
///   removed the check, elements that would otherwise be on this list could be
///   left as disconnected singleton lists.)
/// - `lru`:  contains the items not currently referenced by clients, in LRU order
///
/// Elements are moved between these lists by the `Self::inc_ref()` and `Self::dec_ref()` methods,
/// when they detect an element in the cache acquiring or losing its only
/// external reference:
///
/// ```text
///
///                                    used by clients
///                 look_up()       +-------------------+
///         +----------------------->                   |    erase()
///         |          release()    |  in_use, ref=2    +-----------------+
///         |       +---------------+                   |                 |
///         |       |               +-------------------+                 |
///    +----+-------v------+                                   +----------v----------+
///    |                   |                                   |    not in lru,      |
///    |   lru, ref=1      |                                   |    not in table,    |  used by clients
///    |                   |                                   |    ref=1            |
///    +--------+----------+        +-------------------+      +----------+----------+
///             |                   |    not in lru,    |                 |
///             +------------------->    not in table,  <-----------------+
///                  erase()        |    ref=0          |     release()
///                                 +-------------------+
///                                        dropped
///
/// ```
///
pub struct LRUCache<T: Default + Debug> {
    /// The capacity of LRU
    capacity: usize,
    mutex: Mutex<MutexFields<T>>,
}

struct MutexFields<T>
where
    T: Default + Debug,
{
    /// The size of space which have been allocated
    usage: usize,
    /// Dummy head of LRU list.
    /// lru.prev is newest entry, lru.next is oldest entry.
    /// Entries have refs==1 and in_cache==true.
    lru: *mut LRUHandle<T>,

    /// Dummy head of in-use list.
    /// Entries are in use by clients, and have refs >= 2 and in_cache==true.
    in_use: *mut LRUHandle<T>,

    table: HashMap<Vec<u8>, Rc<RefCell<LRUHandle<T>>>>,
}

impl<T: 'static + Default + Debug> LRUCache<T> {
    pub fn new(cap: usize) -> Self {
        let mutex = MutexFields {
            usage: 0,
            lru: Self::create_dummy_node(),
            in_use: Self::create_dummy_node(),
            table: HashMap::new(),
        };
        LRUCache {
            capacity: cap,
            mutex: Mutex::new(mutex),
        }
    }
    pub fn set_capacity(&mut self, cap: usize) {
        self.capacity = cap
    }

    // Unlink the node `n` from the list `n`
    fn lru_remove(n: *mut LRUHandle<T>) {
        unsafe {
            (*(*n).next).prev = (*n).prev;
            (*(*n).prev).next = (*n).next;
        }
    }

    // Append the `new_node` to the head of given list `n`
    fn lru_append(n: *mut LRUHandle<T>, new_node: *mut LRUHandle<T>) {
        unsafe {
            (*new_node).next = n;
            (*new_node).prev = (*n).prev;
            (*(*n).prev).next = new_node;
            (*n).prev = new_node;
        }
    }

    // Increment ref for a LRUHandle
    fn inc_ref(
        in_use: *mut LRUHandle<T>,
        n: &Rc<RefCell<LRUHandle<T>>>,
    ) -> Rc<RefCell<LRUHandle<T>>> {
        if Rc::strong_count(n) == 1 {
            // means the 'n' is only in the 'table' so move to the 'in_use' list
            Self::lru_remove(n.as_ptr());
            Self::lru_append(in_use, n.as_ptr());
        }
        // ref +1 here
        n.clone()
    }

    // Decrement ref for a LRUHandle
    fn dec_ref(lru: *mut LRUHandle<T>, n: HandleRef<T>) {
        let refs = Rc::strong_count(&n);
        // 2 = 1(the given n) + 1(in cache)
        // dec from 2 to 1 because the given n will be dropped
        if refs == 2 {
            let p = n.as_ptr() as *mut LRUHandle<T>;
            // move to 'lru' from 'in_use'
            Self::lru_remove(p);
            Self::lru_append(lru, p);
        }
        // refs is 1 , n is dropped so nothing left
    }

    // unlink the given handle and
    fn finish_erase(data: &mut MutexFields<T>, n: Rc<RefCell<LRUHandle<T>>>) {
        data.usage -= n.borrow().charge;
        Self::lru_remove(n.as_ptr());
        Self::dec_ref(data.lru, n);
    }

    // Create a dummy node whose 'next' and 'prev' are both itself
    fn create_dummy_node() -> *mut LRUHandle<T> {
        let node = Box::into_raw(Box::new(LRUHandle::default()));
        unsafe {
            (*node).next = node;
            (*node).prev = node
        }
        node
    }
}

impl<T: 'static + Default + Debug> Cache<T> for LRUCache<T> {
    fn insert(
        &mut self,
        key: Vec<u8>,
        value: T,
        charge: usize,
        deleter: Option<Box<FnMut(&[u8], &T)>>,
    ) -> HandleRef<T> {
        let mut mutex_data = self.mutex.get_mut().unwrap();
        let handle = LRUHandle::new(key.clone().into_boxed_slice(), value, deleter, charge);
        let r = Rc::new(RefCell::new(handle));
        if self.capacity > 0 {
            Self::lru_append(mutex_data.in_use, r.clone().as_ptr());
            mutex_data.usage += charge;
            if let Some(old) = mutex_data.table.insert(key, r.clone()) {
                Self::finish_erase(mutex_data, old);
            }
            // self and used in hashtable
            assert_eq!(
                Rc::strong_count(&r),
                2,
                "[lru cache] refs is {}, expect 2 when inserted",
                Rc::strong_count(&r)
            );
        }
        // evict unused lru entries
        unsafe {
            while mutex_data.usage > self.capacity && (*mutex_data.lru).next != mutex_data.lru {
                let old = (*mutex_data.lru).next;
                if let Some(n) = mutex_data.table.remove(&(*old).key[..]) {
                    assert_eq!(
                        Rc::strong_count(&n),
                        1,
                        "[lru cache] refs is {}, expect 1 when evicted",
                        Rc::strong_count(&n)
                    );
                    Self::finish_erase(mutex_data, n);
                }
            }
        }
        r.clone()
    }

    fn look_up(&self, key: &[u8]) -> Option<HandleRef<T>> {
        let mutex = self.mutex.lock().unwrap();
        match mutex.table.get(key) {
            Some(handle) => {
                // ref added here
                let h = Self::inc_ref(mutex.in_use, handle);
                Some(h)
            }
            None => None,
        }
    }

    fn release(&mut self, handle: HandleRef<T>) {
        let mutex = self.mutex.get_mut().unwrap();
        Self::dec_ref(mutex.lru, handle);
    }

    fn erase(&mut self, key: &[u8]) {
        let mutex_data = self.mutex.get_mut().unwrap();
        // remove the key in hashtable
        if let Some(n) = mutex_data.table.remove(key) {
            Self::finish_erase(mutex_data, n);
        }
    }

    #[inline]
    fn new_id(&mut self) -> u64 {
        0
    }

    fn prune(&mut self) {
        let data = self.mutex.get_mut().unwrap();
        unsafe {
            while (*data.lru).next != data.lru {
                let h = (*data.lru).next;
                if let Some(v) = data.table.remove((*h).key.as_ref()) {
                    assert_eq!(Rc::strong_count(&v), 1 , "[lru cache] to prune cache, non active entry's ref should be 1, but got {}", Rc::strong_count(&v));
                    Self::finish_erase(data, v);
                }
            }
        }
    }

    #[inline]
    fn total_charge(&self) -> usize {
        self.mutex.lock().unwrap().usage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::coding::{decode_fixed_32, put_fixed_32};

    const CACHE_SIZE: usize = 4;

    struct CacheTest {
        pub cache: Box<dyn Cache<u32>>,
        pub deleted_keys: Rc<RefCell<Vec<u32>>>,
        pub deleted_values: Rc<RefCell<Vec<u32>>>,
    }

    impl CacheTest {
        pub fn new(cap: usize) -> Self {
            Self {
                cache: Box::new(SharedLRUCache::<u32>::new(cap)),
                deleted_keys: Rc::new(RefCell::new(vec![])),
                deleted_values: Rc::new(RefCell::new(vec![])),
            }
        }
        pub fn look_up(&mut self, key: u32) -> Option<u32> {
            let mut k = vec![];
            put_fixed_32(&mut k, key);
            match self.cache.look_up(k.as_slice()) {
                Some(h) => {
                    let v = *h.borrow().get_value();
                    self.cache.release(h);
                    Some(v)
                }
                None => None,
            }
        }

        pub fn insert(&mut self, key: u32, value: u32) {
            let h = self.insert_and_return(key, value);
            self.cache.release(h);
        }

        pub fn insert_with_charge(&mut self, key: u32, value: u32, charge: usize) {
            let h = self.cache.insert(
                encoded_u32(key),
                value,
                charge,
                Some(deleter(
                    self.deleted_keys.clone(),
                    self.deleted_values.clone(),
                )),
            );
            self.cache.release(h)
        }

        pub fn insert_and_return(&mut self, key: u32, value: u32) -> HandleRef<u32> {
            self.cache.insert(
                encoded_u32(key),
                value,
                1,
                Some(deleter(
                    self.deleted_keys.clone(),
                    self.deleted_values.clone(),
                )),
            )
        }
        pub fn erase(&mut self, key: u32) {
            let mut k = vec![];
            put_fixed_32(&mut k, key);
            self.cache.erase(k.as_slice());
        }

        pub fn assert_deleted_keys_and_values(&self, index: usize, entry: (u32, u32)) {
            let (key, val) = entry;
            assert_eq!(key, self.deleted_keys.borrow()[index]);
            assert_eq!(val, self.deleted_values.borrow()[index]);
        }

        pub fn assert_inside_handle(&self, key: u32, want: u32) -> HandleRef<u32> {
            let encoded = encoded_u32(100);
            let h = self.cache.look_up(encoded.as_slice()).unwrap();
            assert_eq!(want, *(h.borrow().get_value()));
            h
        }
    }
    fn deleter(
        deleted_keys: Rc<RefCell<Vec<u32>>>,
        deleted_values: Rc<RefCell<Vec<u32>>>,
    ) -> Box<FnMut(&[u8], &u32)> {
        box move |k, v| {
            let key = decode_fixed_32(k);
            deleted_keys.borrow_mut().push(key);
            deleted_values.borrow_mut().push(*v);
        }
    }

    fn encoded_u32(i: u32) -> Vec<u8> {
        let mut v = vec![];
        put_fixed_32(&mut v, i);
        v
    }

    #[test]
    fn test_hit_and_miss() {
        let mut cache = CacheTest::new(CACHE_SIZE);
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

        assert_eq!(1, cache.deleted_keys.borrow().len());
        cache.assert_deleted_keys_and_values(0, (100, 101));
    }

    #[test]
    fn test_erase() {
        let mut cache = CacheTest::new(CACHE_SIZE);
        cache.erase(200);
        assert_eq!(0, cache.deleted_keys.borrow().len());

        cache.insert(100, 101);
        cache.insert(200, 201);
        cache.erase(100);

        assert_eq!(None, cache.look_up(100));
        assert_eq!(Some(201), cache.look_up(200));
        assert_eq!(1, cache.deleted_keys.borrow().len());
        cache.assert_deleted_keys_and_values(0, (100, 101));

        cache.erase(100);
        assert_eq!(None, cache.look_up(100));
        assert_eq!(Some(201), cache.look_up(200));
        assert_eq!(1, cache.deleted_keys.borrow().len());
    }

    #[test]
    fn test_entries_are_pinned() {
        let mut cache = CacheTest::new(CACHE_SIZE);
        cache.insert(100, 101);
        let h1 = cache.assert_inside_handle(100, 101);

        // (100, 101) is not deleted because h1 holds the ref
        cache.insert(100, 102);
        let h2 = cache.assert_inside_handle(100, 102);
        assert_eq!(0, cache.deleted_keys.borrow().len());
        // (100, 101) is yet deleted yet deleted
        cache.cache.release(h1);
        assert_eq!(1, cache.deleted_keys.borrow().len());
        cache.assert_deleted_keys_and_values(0, (100, 101));

        // still used in h2, so not deleted
        cache.erase(100);
        assert_eq!(None, cache.look_up(100));
        assert_eq!(1, cache.deleted_keys.borrow().len());

        // h2 released, (100, 102) dropped
        cache.cache.release(h2);
        assert_eq!(2, cache.deleted_keys.borrow().len());
        cache.assert_deleted_keys_and_values(1, (100, 102));
    }

    #[test]
    fn test_eviction_policy() {
        let mut cache = CacheTest::new(CACHE_SIZE);
        cache.insert(100, 101);
        cache.insert(200, 201);
        cache.insert(300, 301);

        // the entry in used should never be evicted
        let h = cache.cache.look_up(encoded_u32(300).as_slice()).unwrap();
        // frequently used entry must be kept around as must the things that are still in use
        for i in 0..(CACHE_SIZE + 100) as u32 {
            cache.insert(1000 + i, 2000 + i);
            assert_eq!(Some(2000 + i), cache.look_up(1000 + i));
            assert_eq!(Some(101), cache.look_up(100));
        }
        assert_eq!(Some(101), cache.look_up(100));
        assert_eq!(None, cache.look_up(200));
        assert_eq!(Some(301), cache.look_up(300));
        cache.cache.release(h)
    }

    #[test]
    fn test_prune() {
        let mut cache = CacheTest::new(CACHE_SIZE);
        cache.insert(1, 100);
        cache.insert(2, 200);
        assert_eq!(2, cache.cache.total_charge());

        let h = cache.cache.look_up(encoded_u32(1).as_slice()).expect("");
        cache.cache.prune(); // (2, 200) is deleted here
        cache.cache.release(h);

        assert_eq!(Some(100), cache.look_up(1));
        assert_eq!(None, cache.look_up(2));

        assert_eq!(1, cache.cache.total_charge());

        cache.assert_deleted_keys_and_values(0, (2, 200));
    }

    #[test]
    fn test_use_exceeds_cache_size() {
        let mut cache = CacheTest::new(CACHE_SIZE);
        let mut handles = vec![];
        // overfill the cache, keeping handles on all inserted entries
        for i in 0..(CACHE_SIZE + 100) as u32 {
            handles.push(cache.insert_and_return(1000 + i, 2000 + i))
        }

        // check that all the entries can be found in the cache
        for i in 0..handles.len() as u32 {
            assert_eq!(Some(2000 + i), cache.look_up(1000 + i))
        }

        // release all
        for h in handles.drain(..) {
            cache.cache.release(h)
        }
    }

    #[test]
    fn test_heavy_entries() {
        let mut cache = CacheTest::new(CACHE_SIZE);
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
    fn test_new_id() {
        let mut cache = CacheTest::new(0);
        let a = cache.cache.new_id();
        let b = cache.cache.new_id();
        let c = cache.cache.new_id();
        assert_ne!(a, b);
        assert_ne!(b, c);
    }

    #[test]
    fn test_zero_size_cache() {
        let mut cache = CacheTest::new(0);
        cache.insert(100, 101);
        assert_eq!(None, cache.look_up(100));
    }
}
