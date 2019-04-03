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
use std::cell::RefCell;
use std::rc::Rc;

mod lru;

/// The CacheHandle is a simple trait for the value in Cache
pub trait Handle<T> {
    fn get_value(&self) -> &T;
}

/// A Cache is an interface that maps keys to values.
/// It has internal synchronization and may be safely accessed concurrently from
/// multiple threads.
/// It may automatically evict entries to make room
/// for new entries.
/// Values have a specified charge against the cache
/// capacity.
/// For example, a cache where the values are variable
/// length strings, may use the length of the string as the charge for
/// the string.
///
/// A builtin cache implementation with a least-recently-used eviction
/// policy is provided.
/// Clients may use their own implementations if
/// they want something more sophisticated (like scan-resistance, a
/// custom eviction policy, variable cache sizing, etc.)
pub trait Cache<T> {
    /// Insert a mapping from key->value into the cache and assign it
    /// the specified charge against the total cache capacity.
    ///
    /// Returns a handle that corresponds to the mapping.  The caller
    /// must call this->Release(handle) when the returned mapping is no
    /// longer needed.
    ///
    /// When the inserted entry is no longer needed, the key and
    /// value will be passed to "deleter".
    fn insert(
        &mut self,
        key: Slice,
        value: T,
        charge: usize,
        deleter: Box<FnMut(&Slice, &T)>,
    ) -> HandleRef<T>;

    /// If the cache has no mapping for "key", returns NULL.
    ///
    /// Else return a handle that corresponds to the mapping.  The caller
    /// must call this->Release(handle) when the returned mapping is no
    /// longer needed.
    fn look_up(&self, key: &Slice) -> Option<HandleRef<T>>;

    /// Release a mapping returned by a previous Lookup().
    /// REQUIRES: handle must not have been released yet.
    /// REQUIRES: handle must have been returned by a method on *this.
    fn release(&mut self, handle: HandleRef<T>);

    /// If the cache contains entry for key, erase it.  Note that the
    /// underlying entry will be kept around until all existing handles
    /// to it have been released.
    fn erase(&mut self, key: Slice);

    /// Return a new numeric id.  May be used by multiple clients who are
    /// sharing the same cache to partition the key space.  Typically the
    /// client will allocate a new id at startup and prepend the id to
    /// its cache keys.
    fn new_id(&self) -> usize;

    /// Return an estimate of the combined charges of all elements stored in the
    /// cache.
    fn total_charge(&self) -> usize;
}

pub type HandleRef<T> = Rc<RefCell<Handle<T>>>;

