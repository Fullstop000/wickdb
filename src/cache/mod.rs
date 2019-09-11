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

use std::rc::Rc;

pub mod lru;

/// The `Handle` is a simple trait for the value in Cache
pub trait Handle<T> {
    /// Returns the value the Handle pointing to
    fn value(&self) -> Option<T>;
}

/// A `Cache` is an interface that maps keys to values.
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
    /// must call `release(handle)` when the returned mapping is no
    /// longer needed.
    ///
    /// When the inserted entry is no longer needed, the key and
    /// value will be passed to "deleter".
    fn insert(
        &self,
        key: Vec<u8>,
        value: T,
        charge: usize,
        deleter: Option<Box<dyn FnMut(&[u8], T)>>,
    ) -> HandleRef<T>;

    /// If the cache has no mapping for `key`, returns `None`.
    ///
    /// Else return a handle that corresponds to the mapping.  The caller
    /// must call `release(handle)` when the returned mapping is no
    /// longer needed.
    fn look_up(&self, key: &[u8]) -> Option<HandleRef<T>>;

    /// Release a mapping returned by a previous `look_up()`.
    /// REQUIRES: handle must not have been released yet.
    /// REQUIRES: handle must have been returned by a method on *this.
    fn release(&self, handle: HandleRef<T>);

    /// If the cache contains entry for key, erase it.  Note that the
    /// underlying entry will be kept around until all existing handles
    /// to it have been released.
    fn erase(&self, key: &[u8]);

    /// Return a new numeric id.  May be used by multiple clients who are
    /// sharing the same cache to partition the key space.  Typically the
    /// client will allocate a new id at startup and prepend the id to
    /// its cache keys.
    fn new_id(&self) -> u64;

    /// Remove all cache entries that are not actively in use. Memory-constrained
    /// applications may wish to call this method to reduce memory usage.
    fn prune(&self);

    /// Return an estimate of the combined charges of all elements stored in the
    /// cache.
    fn total_charge(&self) -> usize;
}

pub type HandleRef<T> = Rc<dyn Handle<T>>;
