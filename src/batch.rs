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

use crate::mem::{MemTable, MemoryTable};
use std::rc::Rc;
use crate::db::format::ValueType;
use std::cell::RefCell;
use crate::util::slice::Slice;
use crate::util::status::{Result, WickErr, Status};
use crate::util::coding::{encode_fixed_64, decode_fixed_64};
use crate::util::varint::VarintU32;

const HEADER_SIZE: usize = 12;

pub trait BatchHandler {
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn delete(&mut self, key: &[u8]);
}

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
    pub(super) contents: Vec<u8>,
    count: u32,
}

impl WriteBatch {
    pub fn new() -> Self {
        let contents = vec![0;HEADER_SIZE];
        Self {
            contents,
            count: 0,
        }
    }

    /// Stores the mapping "key -> value" in the database
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.count +=1;
        self.contents.push(ValueType::Value as u8);
        VarintU32::put_varint(&mut self.contents, key.len() as u32);
        self.contents.extend_from_slice(key);
        VarintU32::put_varint(&mut self.contents, value.len() as u32);
        self.contents.extend_from_slice(value);
    }

    /// If the database contains a mapping for "key", erase it. Else do nothing
    pub fn delete(&mut self, key: &[u8]) {
        self.count +=1;
        self.contents.push(ValueType::Deletion as u8);
        VarintU32::put_varint(&mut self.contents, key.len() as u32);
        self.contents.extend_from_slice(key);
    }

    /// The size of the database changes caused by this batch.
    pub fn approximate_size(&self) -> usize {
        self.contents.len()
    }

    /// Copies the operations in "source" to this batch.
    pub fn append(&mut self, mut src: WriteBatch) {
        assert!(src.contents.len() >= HEADER_SIZE, "[batch] malformed WriteBatch (too small) to append");
        self.count += src.count;
        src.contents.drain(0..HEADER_SIZE);
        self.contents.append(&mut src.contents)
    }

    /// Clears all updates buffered in this batch
    pub fn clear(&mut self) {
        self.contents.clear();
        self.contents.resize(HEADER_SIZE, 0);
        self.count = 0;
    }

    pub fn insert_into(&self, mem: Rc<RefCell<MemTable>>) -> Result<()> {
        let mut handler = MemTableInserter {
            seq: self.sequence(),
            memtable: mem,
        };
        self.iterate(&mut handler)
    }

    /// Iterates every entry in the batch and calls the given BatchHandler
    pub fn iterate(&self, handler: &mut BatchHandler) ->Result<()> {
        if self.contents.len() < HEADER_SIZE {
            return Err(WickErr::new(Status::Corruption, Some("[batch] malformed WriteBatch (too small)")));
        }
        let mut s = Slice::from(&self.contents.as_slice()[HEADER_SIZE..]);
        let mut found = 0;
        while !s.empty() {
            found += 1;
            let tag = s[0];
            s.remove_prefix(1);
            match ValueType::from(tag as u64) {
                ValueType::Value => {
                    if let Some(key) = VarintU32::get_varint_prefixed_slice(&mut s) {
                        if let Some(value) = VarintU32::get_varint_prefixed_slice(&mut s) {
                            handler.put(key.to_slice(), value.to_slice());
                            continue;
                        }
                    }
                    return Err(WickErr::new(Status::Corruption, Some("[batch] bad WriteBatch put")))
                }
                ValueType::Deletion => {
                    if let Some(key) = VarintU32::get_varint_prefixed_slice(&mut s) {
                        handler.delete(key.to_slice());
                        continue;
                    }
                    return Err(WickErr::new(Status::Corruption, Some("[batch] bad WriteBatch delete")))
                }
                ValueType::Unknown => return Err(WickErr::new(Status::Corruption, Some("[batch] unknown WriteBatch value type")))
            }
        }
        if found != self.count() {
            return Err(WickErr::new(Status::Corruption, Some("[batch] WriteBatch has wrong count")));
        }
        Ok(())
    }

    #[inline]
    pub fn count(&self) -> u32 {
//        decode_fixed_32(&self.contents.as_slice()[8..])
        self.count
    }

    #[inline]
    pub fn set_sequence(&mut self, seq: u64) {
        encode_fixed_64(self.contents.as_mut_slice(), seq)
    }

    #[inline]
    pub fn sequence(&self) -> u64{
        decode_fixed_64(self.contents.as_slice())
    }
}

pub struct MemTableInserter {
    seq: u64,
    memtable: Rc<RefCell<MemTable>>,
}

impl MemTableInserter {
    pub fn new(mem: Rc<RefCell<MemTable>>) -> Self {
        Self {
            seq: 0,
            memtable: mem,
        }
    }
}

impl BatchHandler for MemTableInserter {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.memtable.borrow_mut().add(self.seq, ValueType::Value, key, value);
        self.seq += 1;
    }

    fn delete(&mut self, key: &[u8]) {
        self.memtable.borrow_mut().add(self.seq, ValueType::Deletion, key, "".as_bytes());
        self.seq += 1;
    }
}

#[cfg(test)]
mod tests {
    use crate::batch::WriteBatch;
    use crate::mem::{MemTable, MemoryTable};
    use crate::db::format::{InternalKeyComparator, ParsedInternalKey, ValueType};
    use crate::util::comparator::BytewiseComparator;
    use std::rc::Rc;
    use std::cell::RefCell;

    fn print_contents(batch: &WriteBatch) -> String {
        let mem =Rc::new(RefCell::new(MemTable::new(InternalKeyComparator::new(Box::new(BytewiseComparator::new())))));
        let result = batch.insert_into(mem.clone());
        let borrowed = mem.borrow();
        let mut iter = borrowed.new_iterator();
        iter.as_mut().seek_to_first();
        let mut s = String::new();
        let mut count = 0;
        while iter.valid() {
            if let Some(ikey) = ParsedInternalKey::decode_from(iter.key()) {
                match ikey.value_type {
                    ValueType::Value => {
                        let tmp = format!("Put({}, {})", ikey.user_key.as_str(), iter.value().as_str());
                        s.push_str(tmp.as_str());
                        count+=1
                    }
                    ValueType::Deletion => {
                        let tmp = format!("Delete({})", ikey.user_key.as_str());
                        s.push_str(tmp.as_str());
                        count+=1
                    }
                    _ => {}
                }
                s.push('@');
                s.push_str(ikey.seq.to_string().as_str());
                s.push('|');
            }
            iter.next();
        };
        if result.is_err() {
            s.push_str("ParseError()")
        } else if count != batch.count() {
            s.push_str("CountMisMatch")
        }
        s
    }

    #[test]
    fn test_empty_batch() {
        let b = WriteBatch::new();
        assert_eq!("", print_contents(&b).as_str());
        assert_eq!(0, b.count());
    }

    #[test]
    fn test_multiple_records() {
        let mut b = WriteBatch::new();
        b.put("foo".as_bytes(), "bar".as_bytes());
        b.delete("box".as_bytes());
        b.put("baz".as_bytes(), "boo".as_bytes());
        b.set_sequence(100);
        assert_eq!(100, b.sequence());
        assert_eq!(3, b.count());
        assert_eq!("Put(baz, boo)@102|Delete(box)@101|Put(foo, bar)@100|", print_contents(&b).as_str());
    }

    #[test]
    fn test_corrupted_batch() {
        let mut b = WriteBatch::new();
        b.put("foo".as_bytes(), "bar".as_bytes());
        b.delete("box".as_bytes());
        b.set_sequence(200);
        b.contents.truncate(b.contents.len() - 1);
        assert_eq!("Put(foo, bar)@200|ParseError()", print_contents(&b).as_str());
    }

    #[test]
    fn test_append_batch() {
        let mut b1 = WriteBatch::new();
        let mut b2 = WriteBatch::new();
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
        assert_eq!("Put(a, va)@200|Put(b, vb)@202|Put(b, vb)@201|Delete(foo)@203|", print_contents(&b1));
    }

    #[test]
    fn test_approximate_size() {
        let mut b = WriteBatch::new();
        let empty_size = b.approximate_size();
        b.put("foo".as_bytes(), "bar".as_bytes());
        let one_key_size = b.approximate_size();
        assert!(empty_size < one_key_size);

        b.put("baz".as_bytes(), "boo".as_bytes());
        let two_keys_size = b.approximate_size();
        assert!(one_key_size< two_keys_size);

        b.delete("box".as_bytes());
        let post_delete_size = b.approximate_size();
        assert!(two_keys_size < post_delete_size);
    }
}