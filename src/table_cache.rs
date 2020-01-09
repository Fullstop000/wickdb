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
// found in the LICENSE file.

use crate::cache::lru::SharedLRUCache;
use crate::cache::{Cache, HandleRef};
use crate::db::filename::{generate_filename, FileType};
use crate::iterator::{ConcatenateIterator, IterWithCleanup};
use crate::options::{Options, ReadOptions};
use crate::sstable::block::BlockIterator;
use crate::sstable::table::{new_table_iterator, Table, TableIterFactory};
use crate::storage::Storage;
use crate::util::slice::Slice;
use crate::util::status::Result;
use crate::util::varint::VarintU64;
use std::rc::Rc;
use std::sync::Arc;

/// A `TableCache` is the cache for the sst files and the sstable in them
#[derive(Clone)]
pub struct TableCache<S: Storage + Clone> {
    storage: S,
    db_name: &'static str,
    options: Arc<Options>,
    // the key of cache is the file number
    cache: Arc<dyn Cache<Arc<Table>>>,
}

impl<S: Storage + Clone> TableCache<S> {
    pub fn new(db_name: &'static str, options: Arc<Options>, size: usize, storage: S) -> Self {
        let cache = Arc::new(SharedLRUCache::<Arc<Table>>::new(size));
        Self {
            storage,
            db_name,
            options,
            cache,
        }
    }

    // Try to find the sst file from cache. If not found, try to find the file from storage and insert it into the cache
    fn find_table(&self, file_number: u64, file_size: u64) -> Result<HandleRef<Arc<Table>>> {
        let mut key = vec![];
        VarintU64::put_varint(&mut key, file_number);
        match self.cache.look_up(key.as_slice()) {
            Some(handle) => Ok(handle),
            None => {
                let filename = generate_filename(self.db_name, FileType::Table, file_number);
                let table_file = self.storage.open(filename.as_str())?;
                let table = Table::open(table_file, file_size, self.options.clone())?;
                Ok(self.cache.insert(key, Arc::new(table), 1, None))
            }
        }
    }

    /// Evict any entry for the specified file number
    pub fn evict(&self, file_number: u64) {
        let mut key = vec![];
        VarintU64::put_varint(&mut key, file_number);
        self.cache.erase(key.as_slice());
    }

    /// Returns the result of a seek to internal key `key` in specified file
    pub fn get(
        &self,
        options: Rc<ReadOptions>,
        key: &[u8],
        file_number: u64,
        file_size: u64,
    ) -> Result<Option<(Slice, Slice)>> {
        let handle = self.find_table(file_number, file_size)?;
        // every value should be valid so unwrap is safe here
        let res = handle.value().unwrap().internal_get(options, key)?;
        self.cache.release(handle);
        Ok(res)
    }

    /// Create an iterator for the specified `file_number` (the corresponding
    /// file length must be exactly `file_size` bytes).
    /// The table referenced by returning Iterator will be released after the Iterator is dropped.
    ///
    /// Entry format:
    ///     key: internal key
    ///     value: value of user key
    pub fn new_iter(
        &self,
        options: Rc<ReadOptions>,
        file_number: u64,
        file_size: u64,
    ) -> IterWithCleanup<ConcatenateIterator<BlockIterator, TableIterFactory>> {
        match self.find_table(file_number, file_size) {
            Ok(h) => {
                let table = h.value().unwrap();
                let mut iter = IterWithCleanup::new(new_table_iterator(table, options));
                let cache = self.cache.clone();
                iter.register_task(Box::new(move || cache.release(h.clone())));
                iter
            }
            Err(e) => IterWithCleanup::new_with_err(e),
        }
    }
}
