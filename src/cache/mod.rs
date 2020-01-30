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
pub trait Cache<K, V> {
    /// Insert a mapping from key->value into the cache and assign it
    /// the specified charge against the total cache capacity.
    fn insert(&self, key: K, value: V, charge: usize) -> Option<V>;

    /// If the cache has no mapping for `key`, returns `None`.
    fn look_up<'a>(&'a self, key: &K) -> Option<&'a V>;

    /// If the cache contains entry for key, erase it.
    fn erase(&self, key: &K);

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
