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

pub mod arena;
pub mod skiplist;

use crate::db::format::{InternalKeyComparator, LookupKey, ValueType};
use crate::iterator::Iterator;
use crate::mem::arena::{Arena, BlockArena};
use crate::mem::skiplist::{Skiplist, SkiplistIterator};
use crate::util::coding::{decode_fixed_64, put_fixed_64};
use crate::util::comparator::Comparator;
use crate::util::slice::Slice;
use crate::util::varint::VarintU32;
use crate::{Error, Result};
use std::cmp::Ordering;
use std::rc::Rc;

// KeyComparator is a wrapper for InternalKeyComparator. It will convert the input mem key
// to the internal key before comparing.
#[derive(Clone)]
pub struct KeyComparator {
    icmp: InternalKeyComparator,
}

impl Comparator for KeyComparator {
    fn compare(&self, mut a: &[u8], mut b: &[u8]) -> Ordering {
        let ia = extract_varint32_encoded_slice(&mut a);
        let ib = extract_varint32_encoded_slice(&mut b);
        if ia.is_empty() || ib.is_empty() {
            // Use memcmp directly
            ia.cmp(&ib)
        } else {
            self.icmp.compare(ia, ib)
        }
    }

    fn name(&self) -> &str {
        self.icmp.name()
    }

    fn separator(&self, mut a: &[u8], mut b: &[u8]) -> Vec<u8> {
        let ia = extract_varint32_encoded_slice(&mut a);
        let ib = extract_varint32_encoded_slice(&mut b);
        self.icmp.separator(ia, ib)
    }

    fn successor(&self, mut key: &[u8]) -> Vec<u8> {
        let ia = extract_varint32_encoded_slice(&mut key);
        self.icmp.successor(ia)
    }
}

/// In-memory write buffer
pub struct MemTable {
    cmp: KeyComparator,
    table: Rc<Skiplist<KeyComparator, BlockArena>>,
}

impl MemTable {
    /// Creates a new memory table
    pub fn new(icmp: InternalKeyComparator) -> Self {
        let arena = BlockArena::default();
        let kcmp = KeyComparator { icmp };
        let table = Rc::new(Skiplist::new(kcmp.clone(), arena));
        Self { cmp: kcmp, table }
    }

    /// Returns an estimate of the number of bytes of data in use by this
    /// data structure. It is safe to call when MemTable is being modified.
    pub fn approximate_memory_usage(&self) -> usize {
        self.table.arena.memory_used()
    }

    /// Creates a new `MemTableIterator`
    pub fn iter(&self) -> MemTableIterator {
        MemTableIterator::new(self.table.clone())
    }

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
    pub fn add(&self, seq_number: u64, val_type: ValueType, key: &[u8], value: &[u8]) {
        let key_size = key.len();
        let internal_key_size = key_size + 8;
        let mut buf = vec![];
        VarintU32::put_varint(&mut buf, internal_key_size as u32);
        buf.extend_from_slice(key);
        put_fixed_64(&mut buf, (seq_number << 8) | val_type as u64);
        VarintU32::put_varint_prefixed_slice(&mut buf, value);
        self.table.insert(&buf);
    }

    /// If memtable contains a value for key, returns it in `Some(Ok())`.
    /// If memtable contains a deletion for key, returns `Some(Err(Status::NotFound))` .
    /// If memtable does not contain the key, return `None`
    pub fn get(&self, key: &LookupKey) -> Option<Result<Slice>> {
        let mk = key.mem_key();
        // internal key
        let mut iter = self.iter();
        iter.seek(mk);
        if iter.valid() {
            let internal_key = iter.key();
            // only check the user key here
            match self.cmp.icmp.user_comparator.compare(
                Slice::new(internal_key.as_ptr(), internal_key.size() - 8).as_slice(),
                key.user_key(),
            ) {
                Ordering::Equal => {
                    let tag = decode_fixed_64(&internal_key.as_slice()[internal_key.size() - 8..]);
                    match ValueType::from(tag & 0xff as u64) {
                        ValueType::Value => return Some(Ok(iter.value())),
                        ValueType::Deletion => return Some(Err(Error::NotFound(None))),
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
    iter: SkiplistIterator<'a,KeyComparator, BlockArena>,
}

impl MemTableIterator<'_> {
    pub fn new(table: Rc<Skiplist<KeyComparator, BlockArena>>) -> Self {
        let iter = SkiplistIterator::new(table);
        Self { iter }
    }
}

impl Iterator for MemTableIterator<'_> {
    type Key = Slice;
    type Value = Slice;
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first()
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last()
    }

    fn seek(&mut self, target: &[u8]) {
        self.iter.seek(target)
    }

    fn next(&mut self) {
        self.iter.next()
    }

    fn prev(&mut self) {
        self.iter.prev()
    }

    // Returns the internal key
    fn key(&self) -> Self::Key {
        let key = self.iter.key();
        let mut s = key;
        extract_varint32_encoded_slice(&mut s).into()
    }

    // Returns the Slice represents the value
    fn value(&self) -> Self::Value {
        let key = self.iter.key();
        let mut src = key;
        extract_varint32_encoded_slice(&mut src);
        extract_varint32_encoded_slice(&mut src).into()
    }

    fn status(&mut self) -> Result<()> {
        Ok(())
    }
}

// Decodes the length (varint u32) from `src` and advances it.
fn extract_varint32_encoded_slice<'a>(src: &mut &'a [u8]) -> &'a [u8] {
    if src.is_empty() {
        return src;
    }
    VarintU32::get_varint_prefixed_slice(src).unwrap_or(src)
}

#[cfg(test)]
mod tests {
    use crate::db::format::{InternalKeyComparator, LookupKey, ParsedInternalKey, ValueType};
    use crate::iterator::Iterator;
    use crate::mem::MemTable;
    use crate::util::comparator::BytewiseComparator;
    use std::sync::Arc;

    fn new_mem_table() -> MemTable {
        let icmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));
        MemTable::new(icmp)
    }

    fn add_test_data_set(memtable: &MemTable) -> Vec<(&str, &str)> {
        let tests = vec![
            (2, ValueType::Value, "boo", "boo"),
            (4, ValueType::Value, "foo", "val3"),
            (3, ValueType::Deletion, "foo", ""),
            (2, ValueType::Value, "foo", "val2"),
            (1, ValueType::Value, "foo", "val1"),
        ];
        let mut results = vec![];
        for (seq, t, key, value) in tests.clone().drain(..) {
            memtable.add(seq, t, key.as_bytes(), value.as_bytes());
            results.push((key, value));
        }
        results
    }

    #[test]
    fn test_memtable_add_get() {
        let memtable = new_mem_table();
        memtable.add(1, ValueType::Value, b"foo", b"val1");
        memtable.add(2, ValueType::Value, b"foo", b"val2");
        memtable.add(3, ValueType::Deletion, b"foo", b"");
        memtable.add(4, ValueType::Value, b"foo", b"val3");
        memtable.add(2, ValueType::Value, b"boo", b"boo");

        let v = memtable.get(&LookupKey::new(b"null", 10));
        assert!(v.is_none());
        let v = memtable.get(&LookupKey::new(b"foo", 10));
        assert_eq!(b"val3", v.unwrap().unwrap().as_slice());
        let v = memtable.get(&LookupKey::new(b"foo", 0));
        assert!(v.is_none());
        let v = memtable.get(&LookupKey::new(b"foo", 1));
        assert_eq!(b"val1", v.unwrap().unwrap().as_slice());
        let v = memtable.get(&LookupKey::new(b"foo", 3));
        assert!(v.unwrap().is_err());
        let v = memtable.get(&LookupKey::new(b"boo", 3));
        assert_eq!(b"boo", v.unwrap().unwrap().as_slice());
    }

    #[test]
    fn test_memtable_iter() {
        let memtable = new_mem_table();
        let mut iter = memtable.iter();
        assert!(!iter.valid());
        let entries = add_test_data_set(&memtable);
        // Forward scan
        iter.seek_to_first();
        assert!(iter.valid());
        for (key, value) in entries.iter() {
            let k = iter.key();
            let pkey = ParsedInternalKey::decode_from(k.as_slice()).unwrap();
            assert_eq!(
                pkey.as_str(),
                *key,
                "expected key: {:?}, but got {:?}",
                *key,
                pkey.as_str()
            );
            assert_eq!(
                iter.value().as_str(),
                *value,
                "expected value: {:?}, but got {:?}",
                *value,
                iter.value().as_str()
            );
            iter.next();
        }
        assert!(!iter.valid());

        // Backward scan
        iter.seek_to_last();
        assert!(iter.valid());
        for (key, value) in entries.iter().rev() {
            let k = iter.key();
            let pkey = ParsedInternalKey::decode_from(k.as_slice()).unwrap();
            assert_eq!(
                pkey.as_str(),
                *key,
                "expected key: {:?}, but got {:?}",
                *key,
                pkey.as_str()
            );
            assert_eq!(
                iter.value().as_str(),
                *value,
                "expected value: {:?}, but got {:?}",
                *value,
                iter.value().as_str()
            );
            iter.prev();
        }
        assert!(!iter.valid());
    }
}
