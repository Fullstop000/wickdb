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

use crate::util::slice::Slice;
use hashbrown::hash_map::HashMap;
use std::sync::{Mutex, Arc};
use std::fmt::Debug;
use std::ptr;
use std::rc::Rc;
use crate::cache::{Handle as CacheHandle, Cache};
use std::cell::RefCell;

const NUM_SHARD_BITS : usize = 4;
const NUM_SHARD : usize = 1 << NUM_SHARD_BITS;

pub struct SharedLRUCache<T: Default + Debug> {
    shards: [LRUCache<T>; NUM_SHARD],
    last_id: u64,
}

pub struct LRUHandle<T: Default + Debug> {
    value: T,
    deleter: Option<Box<FnMut(&Slice, &T)>>,
    prev: *mut LRUHandle<T>,
    next: *mut LRUHandle<T>,
    //  Since we use Hashmap from hashbrown, the fields below are not needed
    //    next_hash: *mut LRUHandle<T>,
    //    hash: u32,
    charge: usize,
    in_cache: bool,
//    key_length: usize,
    refs: u32,
    key: Box<[u8]>,
}

impl<T: Default + Debug> Default for LRUHandle<T> {
    fn default() -> Self {
        LRUHandle{
            value: T::default(),
            deleter: None,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            charge: 0,
            in_cache: false,
            refs: 0,
            key: Vec::new().into_boxed_slice(),
        }
    }
}

impl<T: Default + Debug> Drop for LRUHandle<T> {
    fn drop(&mut self) {
        let key = self.key();
        if let Some(ref mut deleter) = self.deleter {
            (deleter)(&key, &self.value);
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
    pub fn new(key: Box<[u8]>, value: T, deleter: Box<FnMut(&Slice, &T)>, charge: usize) -> LRUHandle<T> {
        LRUHandle {
            key,
            value,
            deleter: Some(deleter),
            charge,
            refs: 1, // for itself
            ..Self::default()
        }
    }

    /// Return a Slice representing the key of LRUHandle
    pub fn key(&self) -> Slice {
        return Slice::from(&self.key[..])
    }
}

// TODO: use a easier way to implement

/// LRU cache implementation
///
/// Cache entries have an "in_cache" boolean indicating whether the cache has a
/// reference on the entry.  The only ways that this can become false without the
/// entry being passed to its "deleter" are via Erase(), via Insert() when
/// an element with a duplicate key is inserted, or on destruction of the cache.
///
/// The cache keeps two linked lists of items in the cache.  All items in the
/// cache are in one list or the other, and never both.  Items still referenced
/// by clients but erased from the cache are in neither list.  The lists are:
/// - in-use:  contains the items currently referenced by clients, in no
///   particular order.  (This list is used for invariant checking.  If we
///   removed the check, elements that would otherwise be on this list could be
///   left as disconnected singleton lists.)
/// - LRU:  contains the items not currently referenced by clients, in LRU order
/// Elements are moved between these lists by the Ref() and Unref() methods,
/// when they detect an element in the cache acquiring or losing its only
/// external reference.
pub struct LRUCache<T: Default + Debug> {
    /// The capacity of LRU
    capacity: usize,
    mutex: Mutex<MutexFields<T>>,
}

struct MutexFields<T: Default + Debug> {

    /// The size of space which have been allocated
    usage: usize,
    /// Dummy head of LRU list.
    /// lru.prev is newest entry, lru.next is oldest entry.
    /// Entries have refs==1 and in_cache==true.
    lru: *mut LRUHandle<T>,

    /// Dummy head of in-use list.
    /// Entries are in use by clients, and have refs >= 2 and in_cache==true.
    in_use: *mut LRUHandle<T>,

    table: HashMap<Slice, *mut LRUHandle<T>>,
}

impl<T: Default + Debug> LRUCache<T> {
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
    unsafe fn inc_ref(&self, n: *mut LRUHandle<T>) {
       // if 'n' on self.lru, append it to in_use list
       if (*n).refs == 1 && (*n).in_cache {
           Self::lru_remove(n);
           Self::lru_append(self.mutex.lock().unwrap().in_use, n);
       }
       (*n).refs += 1;
    }

    // Decrement ref for a LRUHandle
    // If 'refs' is 0, drop the LRUHandle
    // If 'refs' is 1 and is used by lru, move it to `lru` list from `in_use`
    unsafe fn dec_ref(&self, n: *mut LRUHandle<T>) {
        invarint!(
            (*n).refs > 0,
            "[un_ref] the `refs` of LRUHandle should be more than 0, but got {}",
            (*n).refs
        );
        (*n).refs -= 1;
        if (*n).in_cache && (*n).refs == 1 {
            // no longer in use; move it to lru list
            Self::lru_remove(n);
            Self::lru_append(self.mutex.lock().unwrap().lru, n);
        }
    }

    // If e != nullptr, finish removing 'n' from the cache; it has already been
    // removed from the hash table.
    fn finish_erase(&self, n: *mut LRUHandle<T>) {
        unsafe {
            if !n.is_null() && (*n).in_cache {
                Self::lru_remove(n);
                (*n).in_cache == false; // mark it not in cache
                self.dec_ref(n);
            }
        }
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

impl <T: 'static + Default + Debug> Cache<T> for LRUCache<T> {
    fn insert(&mut self, key: Slice, value: T, charge: usize, deleter: Box<FnMut(&Slice, &T)>) -> Rc<RefCell<CacheHandle<T>>> {
        let mut mutex_data = self.mutex.lock().unwrap();
        let handle = LRUHandle::new(
            Vec::from(key.to_slice()).into_boxed_slice(),
            value,
            deleter,
            charge,
        );
        let r = Rc::new(RefCell::new(handle));
        if self.capacity > 0 {
            r.borrow_mut().refs +=1;
            r.borrow_mut().in_cache = true;
            Self::lru_append(mutex_data.in_use, r.as_ptr());
            mutex_data.usage += charge;
            if let Some(old) = mutex_data.table.insert(key, r.as_ptr()) {
                self.finish_erase(old);
            }
        }
        // evict unused lru entries
        unsafe {
            while mutex_data.usage > self.capacity && (*mutex_data.lru).next != mutex_data.lru {
                let old = (*mutex_data.lru).next;
                if (*old).refs == 1 {
                    if let Some(n) = mutex_data.table.remove(&(*old).key()) {
                        self.finish_erase(n);
                    }
                }
            }
        }
        r
    }

    fn look_up(&self, key: &Slice) -> Option<Rc<RefCell<CacheHandle<T>>>> {
        let mut mutex = self.mutex.lock().unwrap();
        match mutex.table.get(key) {
            Some(handle) => {
                unsafe {
                    self.inc_ref(handle);
                };
                Rc::new(RefCell::new(handle))
            }
            None => None
        }
    }

    fn release(&mut self, handle: &CacheHandle<T>) {
        // TODO
    }

    fn value(&mut self, handle: &CacheHandle<T>) -> T {
        // TODO
    }

    fn erase(&mut self, key: Slice) {
        let mut mutex_data = self.mutex.lock().unwrap();
        // remove the key in hashtable
        if let Some(n) = mutex_data.table.remove(&key) {
            self.finish_erase(n);
        }
    }

    fn new_id(&self) -> usize {
        return 0
    }

    fn total_charge(&self) -> usize {
        self.mutex.lock().unwrap().usage
    }
}
