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

use crate::cache::lru::LRUCache;
use crate::cache::Cache;
use crate::db::filename::{generate_filename, FileType};
use crate::options::{Options, ReadOptions};
use crate::sstable::block::BlockIterator;
use crate::sstable::table::{new_table_iterator, Table, TableIterator};
use crate::storage::Storage;
use crate::util::comparator::Comparator;
use crate::util::varint::VarintU64;
use crate::Result;
use std::sync::Arc;

/// A `TableCache` is the cache for the sst files and the sstable in them
pub struct TableCache<S: Storage + Clone> {
    storage: S,
    db_name: &'static str,
    options: Arc<Options>,
    // the key of cache is the file number
    cache: Arc<dyn Cache<Vec<u8>, Arc<Table<S::F>>>>,
}

impl<S: Storage + Clone> TableCache<S> {
    pub fn new(db_name: &'static str, options: Arc<Options>, size: usize, storage: S) -> Self {
        let cache = Arc::new(LRUCache::<Vec<u8>, Arc<Table<S::F>>>::new(size, None));
        Self {
            storage,
            db_name,
            options,
            cache,
        }
    }

    /// Try to find the sst file from cache. If not found, try to find the file from storage and insert it into the cache
    pub fn find_table<C: Comparator + Clone>(
        &self,
        cmp: C,
        file_number: u64,
        file_size: u64,
    ) -> Result<Arc<Table<S::F>>> {
        let mut key = vec![];
        VarintU64::put_varint(&mut key, file_number);
        match self.cache.look_up(&key) {
            Some(v) => Ok(v.clone()),
            None => {
                let filename = generate_filename(self.db_name, FileType::Table, file_number);
                let table_file = self.storage.open(filename.as_str())?;
                let table = Table::open(table_file, file_size, self.options.clone(), cmp)?;
                let value = Arc::new(table);
                let _ = self.cache.insert(key, value.clone(), 1);
                Ok(value)
            }
        }
    }

    /// Evict any entry for the specified file number
    pub fn evict(&self, file_number: u64) {
        let mut key = vec![];
        VarintU64::put_varint(&mut key, file_number);
        self.cache.erase(&key);
    }

    /// Returns the result of a seek to internal key `key` in specified file
    pub fn get<C: Comparator + Clone>(
        &self,
        cmp: C,
        options: ReadOptions,
        key: &[u8],
        file_number: u64,
        file_size: u64,
    ) -> Result<Option<BlockIterator<C>>> {
        let table = self.find_table(cmp.clone(), file_number, file_size)?;
        table.internal_get(options, cmp, key)
    }

    /// Create an iterator for the specified `file_number` (the corresponding
    /// file length must be exactly `file_size` bytes).
    /// The table referenced by returning Iterator will be released after the Iterator is dropped.
    ///
    /// Entry format:
    ///     key: internal key
    ///     value: value of user key
    pub fn new_iter<C: Comparator + Clone>(
        &self,
        cmp: C,
        options: ReadOptions,
        file_number: u64,
        file_size: u64,
    ) -> Result<TableIterator<C, S::F>> {
        let t = self.find_table(cmp.clone(), file_number, file_size)?;
        let iter = new_table_iterator(cmp, t, options);
        Ok(iter)
    }
}

impl<S: Storage + Clone> Clone for TableCache<S> {
    fn clone(&self) -> Self {
        TableCache {
            storage: self.storage.clone(),
            db_name: self.db_name,
            options: self.options.clone(),
            cache: self.cache.clone(),
        }
    }
}
