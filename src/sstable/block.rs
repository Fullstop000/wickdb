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

use crate::iterator::Iterator;
use crate::util::coding::{decode_fixed_32, put_fixed_32};
use crate::util::comparator::Comparator;
use crate::util::slice::Slice;
use crate::util::status::{Result, Status, WickErr};
use crate::util::varint::VarintU32;
use std::cmp::{min, Ordering};
use std::rc::Rc;
use std::sync::Arc;

/// `Block` is consist of one or more key/value entries and a block trailer.
/// Block entry shares key prefix with its preceding key until a `restart`
/// point reached. A block should contains at least one restart point.
/// First restart point are always zero.
///
/// Block Key/value entry:
///
/// ```text
///
///     +-------+---------+-----------+---------+--------------------+--------------+----------------+
///     | shared (varint) | not shared (varint) | value len (varint) | key (varlen) | value (varlen) |
///     +-----------------+---------------------+--------------------+--------------+----------------+
///
/// ```
///
#[derive(Clone, Debug)]
pub struct Block {
    data: Rc<Vec<u8>>,
    // offset in data of restart array
    restart_offset: u32,
}

impl Block {
    /// Create a `Block` instance.
    ///
    /// # Errors
    ///
    /// If the given `data` is invalid, return an error with `Status::Corruption`
    ///
    pub fn new(data: Vec<u8>) -> Result<Self> {
        let size = data.len();
        if size >= 4 {
            let max_restarts_allowed = (size - 4) / 4;
            let restarts_len = Self::restarts_len(data.as_slice()) as usize;
            // make sure the size is enough for restarts
            if restarts_len <= max_restarts_allowed {
                return Ok(Self {
                    data: Rc::new(data),
                    restart_offset: (size - (1 + restarts_len) * 4) as u32,
                });
            }
        };
        Err(WickErr::new(
            Status::Corruption,
            Some("[block] read invalid block content"),
        ))
    }

    /// Create a BlockIterator for current block.
    pub fn iter(&self, cmp: Arc<dyn Comparator>) -> Box<dyn Iterator> {
        let num_restarts = Self::restarts_len(self.data.as_slice());
        Box::new(BlockIterator::new(
            cmp,
            self.data.clone(),
            self.restart_offset,
            num_restarts,
        ))
    }

    // decoded the restarts length from block data
    #[inline]
    fn restarts_len(data: &[u8]) -> u32 {
        let size = data.len();
        decode_fixed_32(&data[size - 4..])
    }
}

impl Default for Block {
    fn default() -> Self {
        Self {
            data: Rc::new(vec![]),
            restart_offset: 0,
        }
    }
}

/// Iterator for every entry in the block
pub struct BlockIterator {
    cmp: Arc<dyn Comparator>,

    err: Option<WickErr>,
    // underlying block data
    // should never be modified in iterator
    data: Rc<Vec<u8>>,
    /*
      restarts
    */
    restarts: u32,      // restarts array starting offset
    restarts_len: u32,  // length of restarts array
    restart_index: u32, // current restart index

    // current block offset entry
    current: u32,

    /*
     entry
    */
    shared: u32,     // shared length
    not_shared: u32, // not shared length
    value_len: u32,  // value length
    key_offset: u32, // the offset of the key in the block
    key: Vec<u8>,    // buffer for a concated key
}

impl BlockIterator {
    pub fn new(cmp: Arc<Comparator>, data: Rc<Vec<u8>>, restarts: u32, restarts_len: u32) -> Self {
        // should be 0
        Self {
            cmp,
            err: None,
            data,
            restarts,
            restarts_len,
            restart_index: 0,
            current: 0,
            shared: 0,
            not_shared: 0,
            value_len: 0,
            key_offset: 0,
            key: vec![],
        }
    }

    // return the offset in data just past the end of the current entry
    #[inline]
    fn next_entry_offset(&self) -> u32 {
        self.key_offset + self.shared + self.not_shared + self.value_len
    }

    #[inline]
    fn get_restart_point(&self, index: u32) -> u32 {
        decode_fixed_32(&self.data[self.restarts as usize + index as usize * 4..])
    }

    fn seek_to_restart_point(&mut self, index: u32) {
        self.key.clear();
        self.restart_index = index;
        self.current = self.get_restart_point(index);
    }

    // decodes a block entry from `current`
    // mark as corrupted when the current entry tail overflows the starting offset of restarts
    fn parse_block_entry(&mut self) -> bool {
        let offset = self.current;
        let src = &self.data[offset as usize..];
        let (shared, n0) = VarintU32::common_read(src);
        let (not_shared, n1) = VarintU32::common_read(&src[n0 as usize..]);
        let (value_len, n2) = VarintU32::common_read(&src[(n1 + n0) as usize..]);
        let n = (n0 + n1 + n2) as u32;
        //
        if offset + n + shared + not_shared + value_len > self.restarts {
            self.corruption_err();
            return false;
        }
        self.key_offset = self.current + n;
        self.shared = shared;
        self.not_shared = not_shared;
        self.value_len = value_len;
        let key_len = (shared + not_shared) as usize;
        // update current key
        self.key.resize(key_len, 0);
        let delta = &src[self.key_offset as usize..self.key_offset as usize + key_len];
        for i in shared as usize..self.key.len() {
            self.key[i] = delta[i - shared as usize]
        }
        // update restart index
        if shared == 0
            && self.restart_index + 1 < self.restarts_len
            && self.get_restart_point(self.restart_index + 1) == self.current
        {
            self.restart_index += 1
        }
        true
    }

    #[inline]
    fn corruption_err(&mut self) {
        self.err = Some(WickErr::new(Status::Corruption, Some("bad entry in block")));
        self.key.clear();
        self.current = 0;
        self.restart_index = self.restarts_len
    }

    #[inline]
    fn valid_or_panic(&self) -> bool {
        if !self.valid() {
            panic!(
                "[block iterator] invalid the current data offset {}: overflows the restart {}",
                self.current, self.restarts
            )
        }
        true
    }
}

impl Iterator for BlockIterator {
    #[inline]
    fn valid(&self) -> bool {
        self.current < self.restarts
    }

    fn seek_to_first(&mut self) {
        self.seek_to_restart_point(0);
    }

    fn seek_to_last(&mut self) {
        // seek to the last
        self.seek_to_restart_point(self.restarts_len - 1);
        // keep parsing block
        // TODO: the buffered key cost a lot waste here
        while self.parse_block_entry() && self.next_entry_offset() < self.restarts {}
    }

    // find the first entry in block with key>= target
    fn seek(&mut self, target: &Slice) {
        // binary search in restart array to find the last restart point with a key < target
        let mut left = 0;
        let mut right = self.restarts_len - 1;
        while left < right {
            let mid = (left + right + 1) / 2;
            let region_offset = self.get_restart_point(mid);
            let src = &self.data[region_offset as usize..];
            let (shared, n0) = VarintU32::common_read(src);
            let (not_shared, n1) = VarintU32::common_read(&src[n0 as usize..]);
            let (_, n2) = VarintU32::common_read(&src[(n1 + n0) as usize..]);
            if shared != 0 {
                self.corruption_err();
                return;
            }
            let key_offset = region_offset + (n0 + n1 + n2) as u32;
            let key_len = (shared + not_shared) as usize;
            let mid_key = &src[key_offset as usize..key_offset as usize + key_len];
            match self.cmp.compare(&mid_key, target.as_slice()) {
                Ordering::Less => left = mid,
                _ => right = mid - 1,
            }
        }

        // linear search (with restart block) for first key >= target
        // if all the keys > target, we seek to the start
        // if all the keys < target, we seek to the last
        self.seek_to_restart_point(left);
        loop {
            if !self.parse_block_entry() {
                return;
            }
            match self.cmp.compare(self.key.as_slice(), target.as_slice()) {
                Ordering::Less => {}
                _ => return,
            }
            self.current = self.next_entry_offset();
        }
    }

    fn next(&mut self) {
        self.valid_or_panic();
        // should set the next current offset first
        self.current = self.next_entry_offset();
        self.parse_block_entry();
    }

    // seek to prev restart offset and scan backwards to a restart point before current
    fn prev(&mut self) {
        let original = self.current;
        while self.get_restart_point(self.restart_index) >= original {}
    }

    fn key(&self) -> Slice {
        self.valid_or_panic();
        Slice::from(self.key.as_slice())
    }

    fn value(&self) -> Slice {
        self.valid_or_panic();
        let val_offset = self.next_entry_offset() - self.value_len;
        let val = &self.data[val_offset as usize..(val_offset + self.value_len) as usize];
        Slice::from(val)
    }

    fn status(&mut self) -> Result<()> {
        if let Some(_err) = &self.err {
            return Err(self.err.take().unwrap());
        }
        Ok(())
    }
}

/// `BlockBuilder` generates blocks where keys are prefix-compressed:
///
/// When we store a key, we drop the prefix shared with the previous
/// string.  This helps reduce the space requirement significantly.
/// Furthermore, once every K keys, we do not apply the prefix
/// compression and store the entire key.  We call this a "restart
/// point".  The tail end of the block stores the offsets of all of the
/// restart points, and can be used to do a binary search when looking
/// for a particular key.  Values are stored as-is (without compression)
/// immediately following the corresponding key.
pub struct BlockBuilder {
    block_restart_interval: usize,
    cmp: Arc<dyn Comparator>,
    // destination buffer
    buffer: Vec<u8>,
    // restart points
    restarts: Vec<u32>,
    // number of entries emitted since restart
    counter: usize,
    finished: bool,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new(block_restart_interval: usize, cmp: Arc<dyn Comparator>) -> Self {
        assert!(
            block_restart_interval >= 1,
            "[block builder] invalid 'block_restart_interval' {} ",
            block_restart_interval,
        );
        Self {
            block_restart_interval,
            cmp,
            buffer: vec![],
            finished: false,
            counter: 0,
            restarts: vec![0; 1], //first restart point is at offset 0
            last_key: vec![],
        }
    }

    /// Returns the bytes size of `buffer` + bytes size of `restarts` + restart array length
    pub fn current_size_estimate(&self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + 4
    }

    /// Appends the block restarts metadata and returns the block data
    pub fn finish(&mut self) -> &[u8] {
        for restart in self.restarts.iter() {
            put_fixed_32(&mut self.buffer, *restart)
        }
        put_fixed_32(&mut self.buffer, self.restarts.len() as u32);
        self.finished = true;
        self.buffer.as_slice()
    }

    /// Appends key and value to the buffer
    ///
    /// # Panic
    ///
    /// * If this BlockBuilder is finished
    /// * If the `counter` is invalid
    /// * If the given `key` is not consistent with the previous one
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(
            !self.finished,
            "[block builder] add key value to a finished BlockBuilder "
        );
        assert!(
            self.counter <= self.block_restart_interval,
            "[block builder] BlockBuilder reaches the counter limit {} ",
            self.block_restart_interval,
        );
        assert!(
            self.buffer.is_empty()
                || self.cmp.compare(key, self.last_key.as_slice()) == Ordering::Greater,
            "[block builder] in consistent new key"
        );
        let mut shared = 0;
        if self.counter < self.block_restart_interval {
            let min_len = min(self.last_key.len(), key.len());
            // calc the sharing the given key with previous key
            while shared < min_len && self.last_key[shared] == key[shared] {
                shared += 1
            }
        } else {
            // restart compression
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
        }
        let non_shared = key.len() - shared;

        // | --- shared --- | --- non_shared --- | --- value length --- |
        VarintU32::put_varint(&mut self.buffer, shared as u32);
        VarintU32::put_varint(&mut self.buffer, non_shared as u32);
        VarintU32::put_varint(&mut self.buffer, value.len() as u32);
        // append delta key and value
        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(value);
        // update last_key
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        // update counter
        self.counter += 1
    }

    /// Returns true iff no entries have been added since the last `reset()`
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}
