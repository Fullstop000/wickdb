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
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::mem;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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

struct LRUEntry<K, V> {
    key: MaybeUninit<K>,
    value: MaybeUninit<V>,
    prev: *mut LRUEntry<K, V>,
    next: *mut LRUEntry<K, V>,
    charge: usize,
}

impl<K, V> LRUEntry<K, V> {
    fn new(key: K, value: V, charge: usize) -> Self {
        LRUEntry {
            key: MaybeUninit::new(key),
            value: MaybeUninit::new(value),
            charge,
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }
    fn new_empty() -> Self {
        LRUEntry {
            key: MaybeUninit::uninit(),
            value: MaybeUninit::uninit(),
            charge: 0,
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }
}

/// LRU cache implementation
pub struct LRUCache<K, V: Clone> {
    // The capacity of LRU
    capacity: usize,
    inner: Arc<Mutex<LRUInner<K, V>>>,
    // The size of space which have been allocated
    usage: Arc<AtomicUsize>,
    // Only for tests
    evict_hook: Option<Box<dyn Fn(&K, &V)>>,
}

struct LRUInner<K, V> {
    table: HashMap<Key<K>, Box<LRUEntry<K, V>>>,
    // head.next is the newest entry
    head: *mut LRUEntry<K, V>,
    tail: *mut LRUEntry<K, V>,
}

impl<K, V> LRUInner<K, V> {
    fn detach(&mut self, n: *mut LRUEntry<K, V>) {
        unsafe {
            (*(*n).next).prev = (*n).prev;
            (*(*n).prev).next = (*n).next;
        }
    }

    fn attach(&mut self, n: *mut LRUEntry<K, V>) {
        unsafe {
            (*n).next = (*self.head).next;
            (*n).prev = self.head;
            (*self.head).next = n;
            (*(*n).next).prev = n;
        }
    }
}

impl<K: Hash + Eq, V: Clone> LRUCache<K, V> {
    pub fn new(cap: usize) -> Self {
        let l = LRUInner {
            table: HashMap::default(),
            head: Box::into_raw(Box::new(LRUEntry::new_empty())),
            tail: Box::into_raw(Box::new(LRUEntry::new_empty())),
        };

        unsafe {
            (*l.head).next = l.tail;
            (*l.tail).prev = l.head;
        }

        LRUCache {
            usage: Arc::new(AtomicUsize::new(0)),
            capacity: cap,
            inner: Arc::new(Mutex::new(l)),
            evict_hook: None,
        }
    }
}

impl<K, V> Cache<K, V> for LRUCache<K, V>
where
    K: Send + Sync + Hash + Eq + Debug,
    V: Send + Sync + Clone,
{
    fn insert(&self, key: K, mut value: V, charge: usize) -> Option<V> {
        let mut l = self.inner.lock().unwrap();
        if self.capacity > 0 {
            match l.table.get_mut(&Key {
                k: &key as *const K,
            }) {
                Some(h) => {
                    let old_p = h as *mut Box<LRUEntry<K, V>>;
                    unsafe { mem::swap(&mut value, &mut (*(*old_p).value.as_mut_ptr())) };
                    let p: *mut LRUEntry<K, V> = h.as_mut();
                    l.detach(p);
                    l.attach(p);
                    if let Some(hk) = &self.evict_hook {
                        hk(&key, &value);
                    }
                    Some(value)
                }
                None => {
                    let mut node = {
                        if self.usage.load(Ordering::Acquire) >= self.capacity {
                            let prev_key = Key {
                                k: unsafe { (*(*l.tail).prev).key.as_ptr() },
                            };
                            let mut n = l.table.remove(&prev_key).unwrap();
                            self.usage.fetch_sub(n.charge, Ordering::Relaxed);
                            if let Some(hk) = &self.evict_hook {
                                unsafe {
                                    hk(&(*n.key.as_ptr()), &(*n.value.as_ptr()));
                                }
                            }
                            unsafe {
                                ptr::drop_in_place(n.key.as_mut_ptr());
                                ptr::drop_in_place(n.value.as_mut_ptr());
                            }
                            n.key = MaybeUninit::new(key);
                            n.value = MaybeUninit::new(value);
                            l.detach(n.as_mut());
                            n
                        } else {
                            Box::new(LRUEntry::new(key, value, charge))
                        }
                    };
                    self.usage.fetch_add(charge, Ordering::Relaxed);
                    l.attach(node.as_mut());
                    l.table.insert(
                        Key {
                            k: node.key.as_ptr(),
                        },
                        node,
                    );
                    None
                }
            }
        } else {
            None
        }
    }

    fn get(&self, key: &K) -> Option<V> {
        let k = Key { k: key as *const K };
        let mut l = self.inner.lock().unwrap();
        if let Some(node) = l.table.get_mut(&k) {
            let p = node.as_mut() as *mut LRUEntry<K, V>;
            l.detach(p);
            l.attach(p);
            Some(unsafe { (*(*p).value.as_ptr()).clone() })
        } else {
            None
        }
    }

    fn erase(&self, key: &K) {
        let k = Key { k: key as *const K };
        let mut l = self.inner.lock().unwrap();
        if let Some(mut n) = l.table.remove(&k) {
            self.usage.fetch_sub(n.charge, Ordering::SeqCst);
            l.detach(n.as_mut() as *mut LRUEntry<K, V>);
            unsafe {
                if let Some(cb) = &self.evict_hook {
                    cb(key, &(*n.value.as_ptr()));
                }
            }
        }
    }

    #[inline]
    fn total_charge(&self) -> usize {
        self.usage.load(Ordering::Acquire)
    }
}

impl<K, V: Clone> Drop for LRUCache<K, V> {
    fn drop(&mut self) {
        let mut l = self.inner.lock().unwrap();
        (*l).table.values_mut().for_each(|e| unsafe {
            ptr::drop_in_place(e.key.as_mut_ptr());
            ptr::drop_in_place(e.value.as_mut_ptr());
        });
        unsafe {
            let _head = *Box::from_raw(l.head);
            let _tail = *Box::from_raw(l.tail);
        }
    }
}

unsafe impl<K: Send, V: Send + Clone> Send for LRUCache<K, V> {}
unsafe impl<K: Sync, V: Sync + Clone> Sync for LRUCache<K, V> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    const CACHE_SIZE: usize = 100;

    struct CacheTest {
        cache: LRUCache<u32, u32>,
        deleted_kv: Rc<RefCell<Vec<(u32, u32)>>>,
    }

    impl CacheTest {
        fn new(cap: usize) -> Self {
            let deleted_kv = Rc::new(RefCell::new(vec![]));
            let cloned = deleted_kv.clone();
            let mut cache = LRUCache::<u32, u32>::new(cap);
            cache.evict_hook = Some(Box::new(move |k, v| {
                cloned.borrow_mut().push((*k, *v));
            }));
            Self { cache, deleted_kv }
        }

        fn get(&self, key: u32) -> Option<u32> {
            self.cache.get(&key)
        }

        fn insert(&self, key: u32, value: u32) {
            self.cache.insert(key, value, 1);
        }

        fn insert_with_charge(&self, key: u32, value: u32, charge: usize) {
            self.cache.insert(key, value, charge);
        }

        fn erase(&self, key: u32) {
            self.cache.erase(&key);
        }

        fn assert_deleted_kv(&self, index: usize, (key, val): (u32, u32)) {
            assert_eq!((key, val), self.deleted_kv.borrow()[index]);
        }

        fn assert_get(&self, key: u32, want: u32) -> u32 {
            let h = self.cache.get(&key).unwrap();
            assert_eq!(want, h);
            h
        }
    }

    #[test]
    fn test_hit_and_miss() {
        let cache = CacheTest::new(CACHE_SIZE);
        assert_eq!(None, cache.get(100));
        cache.insert(100, 101);
        assert_eq!(Some(101), cache.get(100));
        assert_eq!(None, cache.get(200));
        assert_eq!(None, cache.get(300));

        cache.insert(200, 201);
        assert_eq!(Some(101), cache.get(100));
        assert_eq!(Some(201), cache.get(200));
        assert_eq!(None, cache.get(300));

        cache.insert(100, 102);
        assert_eq!(Some(102), cache.get(100));
        assert_eq!(Some(201), cache.get(200));
        assert_eq!(None, cache.get(300));

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

        assert_eq!(None, cache.get(100));
        assert_eq!(Some(201), cache.get(200));
        assert_eq!(1, cache.deleted_kv.borrow().len());
        cache.assert_deleted_kv(0, (100, 101));

        cache.erase(100);
        assert_eq!(None, cache.get(100));
        assert_eq!(Some(201), cache.get(200));
        assert_eq!(1, cache.deleted_kv.borrow().len());
    }

    #[test]
    fn test_entries_are_pinned() {
        let cache = CacheTest::new(CACHE_SIZE);
        cache.insert(100, 101);
        let v1 = cache.assert_get(100, 101);
        assert_eq!(v1, 101);
        cache.insert(100, 102);
        let v2 = cache.assert_get(100, 102);
        assert_eq!(1, cache.deleted_kv.borrow().len());
        cache.assert_deleted_kv(0, (100, 101));
        assert_eq!(v1, 101);
        assert_eq!(v2, 102);

        cache.erase(100);
        assert_eq!(v1, 101);
        assert_eq!(v2, 102);
        assert_eq!(None, cache.get(100));
        assert_eq!(
            vec![(100, 101), (100, 102)],
            cache.deleted_kv.borrow().clone()
        );
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
            assert_eq!(Some(2000 + i), cache.get(1000 + i));
            assert_eq!(Some(101), cache.get(100));
        }
        assert_eq!(cache.cache.inner.lock().unwrap().table.len(), CACHE_SIZE);
        assert_eq!(Some(101), cache.get(100));
        assert_eq!(None, cache.get(200));
        assert_eq!(None, cache.get(300));
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
                assert_eq!(None, cache.get(1000 + i))
            } else {
                assert_eq!(Some(2000 + i), cache.get(1000 + i))
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
            if let Some(val) = cache.get(i) {
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
        assert_eq!(None, cache.get(100));
    }
}
