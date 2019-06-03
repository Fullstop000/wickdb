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

mod arena;
mod skiplist;

use crate::db::format::{InternalKeyComparator, LookupKey, ValueType};
use crate::iterator::Iterator;
use crate::mem::arena::BlockArena;
use crate::mem::skiplist::{Skiplist, SkiplistIterator};
use crate::util::coding::{decode_fixed_64, put_fixed_64};
use crate::util::comparator::Comparator;
use crate::util::slice::Slice;
use crate::util::status::Status;
use crate::util::status::{Result, WickErr};
use crate::util::varint::{VarintU32, MAX_VARINT_LEN_U32};
use std::cmp::Ordering;
use std::slice;
use std::sync::Arc;

pub trait MemoryTable {
    /// Returns an estimate of the number of bytes of data in use by this
    /// data structure. It is safe to call when MemTable is being modified.
    fn approximate_memory_usage(&self) -> usize;

    /// Return an iterator that yields the contents of the memtable.
    ///
    /// The caller must ensure that the underlying MemTable remains live
    /// while the returned iterator is live.
    fn new_iterator<'a>(&'a self) -> Box<dyn Iterator + 'a>;

    /// Add an entry into memtable that maps key to value at the
    /// specified sequence number and with the specified type.
    /// Typically value will be empty if the type is `Deletion`.
    ///
    /// The 'key' and 'value' will be bundled together into an 'entry':
    ///
    /// ```text
    ///   +=================================+
    ///   |       format of the entry       |
    ///   +=================================+
    ///   | varint32 of internal key length |
    ///   +---------------------------------+ ---------------
    ///   | user key bytes                  |
    ///   +---------------------------------+   internal key
    ///   | sequence (7)       |   type (1) |
    ///   +---------------------------------+ ---------------
    ///   | varint32 of value length        |
    ///   +---------------------------------+
    ///   | value bytes                     |
    ///   +---------------------------------+
    /// ```
    ///
    fn add(&self, seq_number: u64, val_type: ValueType, key: &[u8], value: &[u8]);

    /// If memtable contains a value for key, returns it in `Some(Ok())`.
    /// If memtable contains a deletion for key, returns `Some(Err(Status::NotFound))` .
    /// If memtable does not contain the key, return `None`
    fn get(&self, key: &LookupKey) -> Option<Result<Slice>>;
}

// KeyComparator is a wrapper for InternalKeyComparator. It will convert the input mem key
// to the internal key before comparing.
struct KeyComparator {
    cmp: Arc<InternalKeyComparator>,
}

impl Comparator for KeyComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        let ia = extract_varint_encoded_slice(Slice::from(a));
        let ib = extract_varint_encoded_slice(Slice::from(b));
        self.cmp.compare(ia.as_slice(), ib.as_slice())
    }

    fn name(&self) -> &str {
        self.cmp.name()
    }

    fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
        let ia = extract_varint_encoded_slice(Slice::from(a));
        let ib = extract_varint_encoded_slice(Slice::from(b));
        self.cmp.separator(ia.as_slice(), ib.as_slice())
    }

    fn successor(&self, key: &[u8]) -> Vec<u8> {
        let ia = extract_varint_encoded_slice(Slice::from(key));
        self.cmp.successor(ia.as_slice())
    }
}

/// In-memory write buffer
pub struct MemTable {
    cmp: Arc<KeyComparator>,
    table: Skiplist,
}

impl MemTable {
    pub fn new(cmp: Arc<InternalKeyComparator>) -> Self {
        let arena = BlockArena::new();
        let kcmp = Arc::new(KeyComparator { cmp });
        let table = Skiplist::new(kcmp.clone(), Box::new(arena));
        Self { cmp: kcmp, table }
    }
}

unsafe impl Send for MemTable {}
unsafe impl Sync for MemTable {}

impl MemoryTable for MemTable {
    fn approximate_memory_usage(&self) -> usize {
        self.table.arena.memory_used()
    }

    fn new_iterator<'a>(&'a self) -> Box<dyn Iterator + 'a> {
        Box::new(MemTableIterator::new(&self.table))
    }

    fn add(&self, seq_number: u64, val_type: ValueType, key: &[u8], value: &[u8]) {
        let key_size = key.len();
        let internal_key_size = key_size + 8;
        let mut buf = vec![];
        VarintU32::put_varint(&mut buf, internal_key_size as u32);
        buf.extend_from_slice(key);
        put_fixed_64(&mut buf, (seq_number << 8) | val_type as u64);
        VarintU32::put_varint(&mut buf, value.len() as u32);
        buf.extend_from_slice(value);
        // TODO: remove redundant copying
        self.table.insert(Slice::from(buf.as_slice()))
    }

    fn get(&self, key: &LookupKey) -> Option<Result<Slice>> {
        let mk = key.mem_key();
        // internal key
        let mut iter = self.new_iterator();
        iter.seek(&mk);
        if iter.valid() {
            let internal_key = iter.key();
            // only check the user key here
            match self.cmp.cmp.user_comparator.compare(
                Slice::new(internal_key.as_ptr(), internal_key.size() - 8).as_slice(),
                key.user_key().as_slice(),
            ) {
                Ordering::Equal => {
                    let tag = decode_fixed_64(&internal_key.as_slice()[internal_key.size() - 8..]);
                    match ValueType::from(tag & 0xff as u64) {
                        ValueType::Value => return Some(Ok(iter.value())),
                        ValueType::Deletion => {
                            return Some(Err(WickErr::new(Status::NotFound, None)))
                        }
                        ValueType::Unknown => { /* fallback to None*/ }
                    }
                }
                _ => return None,
            }
        }
        None
    }
}

pub struct MemTableIterator<'a> {
    iter: SkiplistIterator<'a>,
}

impl<'a> MemTableIterator<'a> {
    pub fn new(table: &'a Skiplist) -> Self {
        let iter = SkiplistIterator::new(table);
        Self { iter }
    }
}

impl<'a> Iterator for MemTableIterator<'a> {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first()
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last()
    }

    fn seek(&mut self, target: &Slice) {
        self.iter.seek(target)
    }

    fn next(&mut self) {
        self.iter.next()
    }

    fn prev(&mut self) {
        self.iter.prev()
    }

    // returns the internal key
    fn key(&self) -> Slice {
        extract_varint_encoded_slice(self.iter.key())
    }

    // returns the Slice represents the value
    fn value(&self) -> Slice {
        let internal_key = self.key();
        if internal_key.size() != 0 {
            unsafe {
                let val_ptr = internal_key.as_ptr().add(internal_key.size());
                match VarintU32::read(slice::from_raw_parts(val_ptr, MAX_VARINT_LEN_U32)) {
                    Some((len, n)) => {
                        let val_start = val_ptr.add(n);
                        return Slice::new(val_start, len as usize);
                    }
                    None => return Slice::default(),
                }
            }
        }
        Slice::default()
    }

    fn status(&mut self) -> Result<()> {
        Ok(())
    }
}

// Decodes the length (varint u32) from the first of the give slice and
// returns a new slice points to the data according to the extracted length
fn extract_varint_encoded_slice(origin: Slice) -> Slice {
    match VarintU32::read(origin.as_slice()) {
        Some((len, n)) => Slice::from(&origin.as_slice()[n..len as usize + n]),
        None => Slice::default(),
    }
}
