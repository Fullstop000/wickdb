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

use crate::db::format::ValueType;
use crate::mem::MemTable;
use crate::util::coding::{decode_fixed_32, decode_fixed_64, encode_fixed_32, encode_fixed_64};
use crate::util::varint::VarintU32;
use crate::{Comparator, Error, Result};

pub const HEADER_SIZE: usize = 12;

/// `WriteBatch` holds a collection of updates to apply atomically to a DB.
///
///
/// ```text
///
/// The contents structure:
///
///  +---------------------+
///  | sequence number (8) |  the starting seq number
///  +---------------------+
///  | data count (4)      |
///  +---------------------+
///  | data record         |
///  +---------------------+
///
/// The format of data record:
///
///  +----------+--------------+----------+----------------+------------+
///  | key type | key len(var) | key data | value len(var) | value data |
///  +----------+--------------+----------+----------------+------------+
///
/// ```
/// The updates are applied in the order in which they are added
/// to the `WriteBatch`.
///
/// Multiple threads can invoke all methods on a `WriteBatch` without
/// external synchronization, but if any of the threads may call a
/// non-const method, all threads accessing the same WriteBatch must use
/// external synchronization.
///
#[derive(Clone)]
pub struct WriteBatch {
    contents: Vec<u8>,
}

impl Default for WriteBatch {
    fn default() -> Self {
        let contents = vec![0; HEADER_SIZE];
        Self { contents }
    }
}

impl WriteBatch {
    #[inline]
    pub fn data(&self) -> &[u8] {
        self.contents.as_slice()
    }

    /// Stores the mapping "key -> value" in the database
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.set_count(self.get_count() + 1);
        self.contents.push(ValueType::Value as u8);
        VarintU32::put_varint(&mut self.contents, key.len() as u32);
        self.contents.extend_from_slice(key);
        VarintU32::put_varint(&mut self.contents, value.len() as u32);
        self.contents.extend_from_slice(value);
    }

    /// If the database contains a mapping for "key", erase it. Else do nothing
    pub fn delete(&mut self, key: &[u8]) {
        self.set_count(self.get_count() + 1);
        self.contents.push(ValueType::Deletion as u8);
        VarintU32::put_varint(&mut self.contents, key.len() as u32);
        self.contents.extend_from_slice(key);
    }

    /// The size of the database changes caused by this batch.
    #[inline]
    pub fn approximate_size(&self) -> usize {
        self.contents.len()
    }

    /// Copies the operations in "source" to this batch.
    pub fn append(&mut self, mut src: WriteBatch) {
        assert!(
            src.contents.len() >= HEADER_SIZE,
            "[batch] malformed WriteBatch (too small) to append"
        );
        self.set_count(self.get_count() + src.get_count());
        src.contents.drain(0..HEADER_SIZE);
        self.contents.append(&mut src.contents)
    }

    /// Clears all updates buffered in this batch
    #[inline]
    pub fn clear(&mut self) {
        self.contents.clear();
        self.contents.resize(HEADER_SIZE, 0);
        self.set_count(0);
    }

    /// Insert all the records in the batch into the given `MemTable`
    pub fn insert_into<C: Comparator>(&self, mem: &MemTable<C>) -> Result<()> {
        if self.contents.len() < HEADER_SIZE {
            return Err(Error::Corruption(
                "[batch] malformed WriteBatch (too small)".to_owned(),
            ));
        }
        let mut s = &self.contents[HEADER_SIZE..];
        let mut found = 0;
        let mut seq = self.get_sequence();
        while !s.is_empty() {
            found += 1;
            let tag = s[0];
            s = &s[1..];
            match ValueType::from(u64::from(tag)) {
                ValueType::Value => {
                    if let Some(key) = VarintU32::get_varint_prefixed_slice(&mut s) {
                        if let Some(value) = VarintU32::get_varint_prefixed_slice(&mut s) {
                            mem.add(seq, ValueType::Value, key, value);
                            seq += 1;
                            continue;
                        }
                    }
                    return Err(Error::Corruption("[batch] bad WriteBatch put".to_owned()));
                }
                ValueType::Deletion => {
                    if let Some(key) = VarintU32::get_varint_prefixed_slice(&mut s) {
                        mem.add(seq, ValueType::Deletion, key, b"");
                        seq += 1;
                        continue;
                    }
                    return Err(Error::Corruption(
                        "[batch] bad WriteBatch delete".to_owned(),
                    ));
                }
                ValueType::Unknown => {
                    return Err(Error::Corruption(
                        "[batch] unknown WriteBatch value type".to_owned(),
                    ))
                }
            }
        }
        if found != self.get_count() {
            return Err(Error::Corruption(
                "[batch] WriteBatch has wrong count".to_owned(),
            ));
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn set_contents(&mut self, src: &mut Vec<u8>) {
        self.contents.clear();
        self.contents.append(src);
    }

    /// Returns the number of entires included in this entry
    #[inline]
    pub fn get_count(&self) -> u32 {
        decode_fixed_32(&self.contents[8..])
    }

    #[inline]
    pub(crate) fn set_count(&mut self, count: u32) {
        encode_fixed_32(&mut self.contents[8..], count)
    }

    #[inline]
    pub(crate) fn set_sequence(&mut self, seq: u64) {
        encode_fixed_64(&mut self.contents, seq)
    }

    /// Returns the seq number of this batch
    #[inline]
    pub fn get_sequence(&self) -> u64 {
        decode_fixed_64(&self.contents)
    }

    /// Returns false when this batch contains no entries to be written
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.get_count() == 0
    }
}

#[cfg(test)]
mod tests {
    use crate::batch::WriteBatch;
    use crate::db::format::{InternalKeyComparator, ParsedInternalKey, ValueType};
    use crate::iterator::Iterator;
    use crate::mem::MemTable;
    use crate::util::comparator::BytewiseComparator;

    fn print_contents(batch: &WriteBatch) -> String {
        let mem = MemTable::new(
            1 << 32,
            InternalKeyComparator::new(BytewiseComparator::default()),
        );
        let result = batch.insert_into(&mem);
        let mut iter = mem.iter();
        iter.seek_to_first();
        let mut s = String::new();
        let mut count = 0;
        while iter.valid() {
            if let Some(ikey) = ParsedInternalKey::decode_from(iter.key()) {
                match ikey.value_type {
                    ValueType::Value => {
                        let tmp = format!(
                            "Put({}, {})",
                            ikey.as_str(),
                            std::str::from_utf8(iter.value()).unwrap()
                        );
                        s.push_str(tmp.as_str());
                        count += 1
                    }
                    ValueType::Deletion => {
                        let tmp = format!("Delete({})", ikey.as_str());
                        s.push_str(tmp.as_str());
                        count += 1
                    }
                    _ => {}
                }
                s.push('@');
                s.push_str(ikey.seq.to_string().as_str());
                s.push('|');
            }
            iter.next();
        }
        if result.is_err() {
            s.push_str("ParseError()")
        } else if count != batch.get_count() {
            s.push_str("CountMisMatch")
        }
        s
    }

    #[test]
    fn test_empty_batch() {
        let b = WriteBatch::default();
        assert_eq!("", print_contents(&b).as_str());
        assert!(b.is_empty());
    }

    #[test]
    fn test_multiple_records() {
        let mut b = WriteBatch::default();
        b.put("foo".as_bytes(), "bar".as_bytes());
        b.delete("box".as_bytes());
        b.put("baz".as_bytes(), "boo".as_bytes());
        b.set_sequence(100);
        assert_eq!(100, b.get_sequence());
        assert_eq!(3, b.get_count());
        assert_eq!(
            "Put(baz, boo)@102|Delete(box)@101|Put(foo, bar)@100|",
            print_contents(&b).as_str()
        );
    }

    #[test]
    fn test_corrupted_batch() {
        let mut b = WriteBatch::default();
        b.put("foo".as_bytes(), "bar".as_bytes());
        b.delete("box".as_bytes());
        b.set_sequence(200);
        b.contents.truncate(b.contents.len() - 1);
        assert_eq!(
            "Put(foo, bar)@200|ParseError()",
            print_contents(&b).as_str()
        );
    }

    #[test]
    fn test_append_batch() {
        let mut b1 = WriteBatch::default();
        let mut b2 = WriteBatch::default();
        b1.set_sequence(200);
        b2.set_sequence(300);
        b1.append(b2.clone());
        assert_eq!("", print_contents(&b1));
        b2.put("a".as_bytes(), "va".as_bytes());
        b1.append(b2.clone());
        assert_eq!("Put(a, va)@200|", print_contents(&b1));
        b2.clear();
        b2.put("b".as_bytes(), "vb".as_bytes());
        b1.append(b2.clone());
        assert_eq!("Put(a, va)@200|Put(b, vb)@201|", print_contents(&b1));
        b2.delete("foo".as_bytes());
        b1.append(b2.clone());
        assert_eq!(
            "Put(a, va)@200|Put(b, vb)@202|Put(b, vb)@201|Delete(foo)@203|",
            print_contents(&b1)
        );
    }

    #[test]
    fn test_approximate_size() {
        let mut b = WriteBatch::default();
        let empty_size = b.approximate_size();
        b.put("foo".as_bytes(), "bar".as_bytes());
        let one_key_size = b.approximate_size();
        assert!(empty_size < one_key_size);

        b.put("baz".as_bytes(), "boo".as_bytes());
        let two_keys_size = b.approximate_size();
        assert!(one_key_size < two_keys_size);

        b.delete("box".as_bytes());
        let post_delete_size = b.approximate_size();
        assert!(two_keys_size < post_delete_size);
    }
}
