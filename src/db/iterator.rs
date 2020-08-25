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

use crate::db::format::ValueType;
use crate::db::format::{extract_user_key, InternalKey, ParsedInternalKey, VALUE_TYPE_FOR_SEEK};
use crate::db::DBImpl;
use crate::iterator::{Iterator, KMergeCore};
use crate::storage::Storage;
use crate::util::comparator::Comparator;
use crate::{Error, Result};
use rand::Rng;
use std::cmp::Ordering;
use std::sync::Arc;

#[derive(Eq, PartialEq)]
enum Direction {
    // When moving forward, the internal iterator is positioned at
    // the exact entry that yields inner.key(), inner.value()
    Forward,
    // When moving backwards, the internal iterator is positioned
    // just before all entries whose user key == inner.key().
    Reverse,
}

/// Memtables and sstables that make the DB representation contain
/// (userkey,seq,type) => uservalue entries.
/// `DBIterator` combines multiple entries for the same userkey found in the DB
/// representation into a single entry while accounting for sequence
/// numbers, deletion markers, overwrites, etc
pub struct DBIterator<I: Iterator, S: Storage + Clone + 'static, C: Comparator> {
    valid: bool,
    db: Arc<DBImpl<S, C>>,
    ucmp: C,
    // The newest sequence acquired.
    // Any key newer than this will be ignored
    sequence: u64,
    err: Option<Error>,
    inner: I,
    direction: Direction,
    // used for randomly picking a yielded key to record read stats
    bytes_util_read_sampling: u64,

    // Current key when direction is Reverse
    saved_key: Vec<u8>,
    // Current value when direction is Reverse
    saved_value: Vec<u8>,
}

impl<I: Iterator, S: Storage + Clone, C: Comparator + 'static> Iterator for DBIterator<I, S, C> {
    fn valid(&self) -> bool {
        self.valid
    }

    fn seek_to_first(&mut self) {
        self.direction = Direction::Forward;
        self.saved_value.clear();
        self.inner.seek_to_first();
        if self.inner.valid() {
            self.find_next_user_entry(false);
        } else {
            self.valid = false;
        }
    }

    fn seek_to_last(&mut self) {
        self.direction = Direction::Reverse;
        self.saved_value.clear();
        self.inner.seek_to_last();
        self.find_prev_user_key();
    }

    fn seek(&mut self, target: &[u8]) {
        self.direction = Direction::Forward;
        self.saved_value.clear();
        self.saved_key.clear();
        let ikey = ParsedInternalKey::new(target, self.sequence, VALUE_TYPE_FOR_SEEK).encode();
        self.inner.seek(ikey.data());
        if self.inner.valid() {
            self.find_next_user_entry(false)
        } else {
            self.valid = false;
        }
    }

    fn next(&mut self) {
        self.valid_or_panic();
        match self.direction {
            Direction::Forward => {
                self.saved_key = Vec::from(extract_user_key(self.inner.key()));
                self.inner.next();
                if !self.inner.valid() {
                    self.valid = false;
                    self.saved_key.clear();
                    return;
                }
            }
            Direction::Reverse => {
                self.direction = Direction::Forward;
                // Inner iterator is pointing just before the entries for inner.key(),
                // so advance into the range of entries for inner.key() and then
                // use the normal skipping code below
                if !self.inner.valid() {
                    self.inner.seek_to_first();
                } else {
                    self.inner.next()
                }
                if !self.inner.valid() {
                    self.valid = false;
                    self.saved_key.clear();
                }
            }
        }
        self.find_next_user_entry(true);
    }

    fn prev(&mut self) {
        self.valid_or_panic();
        // inner iter is pointing at the current entry.  Scan backwards until
        // the key changes so we can use the normal reverse scanning code.
        if self.direction == Direction::Forward {
            self.saved_key = Vec::from(extract_user_key(self.inner.key()));
            loop {
                self.inner.prev();
                if !self.inner.valid() {
                    self.valid = false;
                    self.saved_key.clear();
                    self.saved_value.clear();
                    return;
                }
                if self.ucmp.compare(
                    extract_user_key(self.inner.key()),
                    self.saved_key.as_slice(),
                ) == Ordering::Less
                {
                    break;
                }
            }
            self.direction = Direction::Reverse;
        }
        self.find_prev_user_key();
    }

    fn key(&self) -> &[u8] {
        self.valid_or_panic();
        match self.direction {
            Direction::Forward => extract_user_key(self.inner.key()),
            Direction::Reverse => &self.saved_key,
        }
    }

    fn value(&self) -> &[u8] {
        self.valid_or_panic();
        match self.direction {
            Direction::Forward => self.inner.value(),
            Direction::Reverse => &self.saved_value,
        }
    }

    fn status(&mut self) -> Result<()> {
        if let Some(e) = self.err.take() {
            Err(e)
        } else {
            self.inner.status()
        }
    }
}

impl<I: Iterator, S: Storage + Clone, C: Comparator + 'static> DBIterator<I, S, C> {
    pub fn new(iter: I, db: Arc<DBImpl<S, C>>, sequence: u64, ucmp: C) -> Self {
        Self {
            valid: false,
            db: db.clone(),
            ucmp,
            sequence,
            err: None,
            inner: iter,
            direction: Direction::Forward,
            bytes_util_read_sampling: random_compaction_period(db.options.read_bytes_period),
            saved_key: Default::default(),
            saved_value: Default::default(),
        }
    }

    #[inline]
    fn valid_or_panic(&self) {
        assert!(self.valid(), "invalid iterator")
    }

    // Parse internal key from inner iterator into a `ParsedInternalKey`
    // otherwise records a corruption error
    fn parse_key(&mut self) -> InternalKey {
        let k = self.inner.key();
        let bytes_read = k.len() + self.inner.value().len();
        while self.bytes_util_read_sampling < bytes_read as u64 {
            self.bytes_util_read_sampling +=
                random_compaction_period(self.db.options.read_bytes_period);
            self.db.record_read_sample(k);
        }
        self.bytes_util_read_sampling -= bytes_read as u64;
        InternalKey::decoded_from(k)
    }

    // Try to point the inner iter to yield a internal key whose user key is greater than previous
    // user key with sequence limitation. We only need to find the first entry that has a different
    // user key.
    fn find_next_user_entry(&mut self, mut skipping: bool) {
        let ucmp = self.ucmp.clone();
        let seq = self.sequence;
        loop {
            let saved_key = self.saved_key.clone();
            if let Some(pkey) = self.parse_key().parsed() {
                if pkey.seq <= seq {
                    match pkey.value_type {
                        ValueType::Value => {
                            if skipping
                                && ucmp.compare(pkey.user_key, saved_key.as_slice())
                                    != Ordering::Greater
                            {
                                // not greater than saved_key, so the key is skipped
                            } else {
                                // Found the next user key
                                self.valid = true;
                                if !self.saved_key.is_empty() {
                                    self.saved_key.clear();
                                }
                                return;
                            }
                        }
                        ValueType::Deletion => {
                            // Arrange to skip all upcoming entries for this key since
                            // they are hidden by this deletion.
                            self.saved_key = Vec::from(pkey.user_key);
                            skipping = true;
                        }
                        _ => { /* ignore the unknown value type */ }
                    }
                }
            }
            self.inner.next();
            if !self.inner.valid() {
                break;
            }
        }

        self.saved_key.clear();
        self.valid = false;
    }

    // Try to point the inner iter to yield a internal key whose user key is less than previous
    // user key with sequence limitation.
    // Different with `find_next_user_key`, we should
    // reach the final internal key of a same user key because a internal key with a larger
    // sequence is more forward. To reach the final internal key in reverse direction, the inner
    // iter has to be pointed to the first entry whose user key is less than the current one.
    fn find_prev_user_key(&mut self) {
        let mut value_type = ValueType::Deletion;
        let ucmp = self.ucmp.clone();
        let seq = self.sequence;
        if self.inner.valid() {
            loop {
                let saved_key = self.saved_key.clone();
                if let Some(pkey) = self.parse_key().parsed() {
                    if pkey.seq <= seq {
                        if value_type == ValueType::Value
                            && ucmp.compare(pkey.user_key, saved_key.as_slice()) == Ordering::Less
                        {
                            // found the key that less than
                            break;
                        }
                        value_type = pkey.value_type;
                        match value_type {
                            ValueType::Deletion => {
                                self.saved_key.clear();
                                self.saved_value.clear();
                            }
                            ValueType::Value => {
                                // record the current key for later comparing
                                self.saved_key = Vec::from(extract_user_key(self.inner.key()));
                                // record the current value for later yielding
                                self.saved_value = self.inner.value().to_vec();
                            }
                            _ => { /* ignore the unknown value type */ }
                        }
                    }
                }
                self.inner.prev();
                if !self.inner.valid() {
                    break;
                }
            }
        }
        if value_type != ValueType::Value {
            // We reach the end of inner iter but didn't find a valid user key
            self.valid = false;
            self.saved_key.clear();
            self.saved_value.clear();
            self.direction = Direction::Forward;
        } else {
            self.valid = true;
        }
    }
}

// Picks the number of bytes that can be read until a compaction is scheduled
fn random_compaction_period(read_bytes_period: u64) -> u64 {
    rand::thread_rng().gen_range(0, 2 * read_bytes_period)
}

// Iterating from memtable iterators to table iterators
pub struct DBIteratorCore<C: Comparator, M: Iterator, T: Iterator> {
    cmp: C,
    mem_iters: Vec<M>,
    table_iters: Vec<T>,
}

impl<C: Comparator, M: Iterator, T: Iterator> DBIteratorCore<C, M, T> {
    pub fn new(cmp: C, mem_iters: Vec<M>, table_iters: Vec<T>) -> Self {
        Self {
            cmp,
            mem_iters,
            table_iters,
        }
    }
}

impl<C: Comparator, M: Iterator, T: Iterator> KMergeCore for DBIteratorCore<C, M, T> {
    type Cmp = C;
    fn cmp(&self) -> &Self::Cmp {
        &self.cmp
    }

    fn iters_len(&self) -> usize {
        self.mem_iters.len() + self.table_iters.len()
    }

    // Find the iterator with the smallest 'key' and set it as current
    fn find_smallest(&mut self) -> usize {
        let mut smallest: Option<&[u8]> = None;
        let mut index = self.iters_len();
        for (i, child) in self.mem_iters.iter().enumerate() {
            if self.smaller(&mut smallest, child) {
                index = i
            }
        }

        for (i, child) in self.table_iters.iter().enumerate() {
            if self.smaller(&mut smallest, child) {
                index = i + self.mem_iters.len()
            }
        }
        index
    }

    // Find the iterator with the largest 'key' and set it as current
    fn find_largest(&mut self) -> usize {
        let mut largest: Option<&[u8]> = None;
        let mut index = self.iters_len();
        for (i, child) in self.mem_iters.iter().enumerate() {
            if self.larger(&mut largest, child) {
                index = i
            }
        }

        for (i, child) in self.table_iters.iter().enumerate() {
            if self.larger(&mut largest, child) {
                index = i + self.mem_iters.len()
            }
        }
        index
    }

    fn get_child(&self, i: usize) -> &dyn Iterator {
        if i < self.mem_iters.len() {
            self.mem_iters.get(i).unwrap() as &dyn Iterator
        } else {
            let current = i - self.mem_iters.len();
            self.table_iters.get(current).unwrap() as &dyn Iterator
        }
    }

    fn get_child_mut(&mut self, i: usize) -> &mut dyn Iterator {
        if i < self.mem_iters.len() {
            self.mem_iters.get_mut(i).unwrap() as &mut dyn Iterator
        } else {
            let current = i - self.mem_iters.len();
            self.table_iters.get_mut(current).unwrap() as &mut dyn Iterator
        }
    }

    fn for_each_child<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut dyn Iterator),
    {
        self.mem_iters
            .iter_mut()
            .for_each(|i| f(i as &mut dyn Iterator));
        self.table_iters
            .iter_mut()
            .for_each(|i| f(i as &mut dyn Iterator));
    }

    fn for_not_ith<F>(&mut self, n: usize, mut f: F)
    where
        F: FnMut(&mut dyn Iterator, &Self::Cmp),
    {
        if n < self.mem_iters.len() {
            for (i, child) in self.mem_iters.iter_mut().enumerate() {
                if i != n {
                    f(child as &mut dyn Iterator, &self.cmp)
                }
            }
        } else {
            let current = n - self.mem_iters.len();
            for (i, child) in self.table_iters.iter_mut().enumerate() {
                if i != current {
                    f(child as &mut dyn Iterator, &self.cmp)
                }
            }
        }
    }

    fn take_err(&mut self) -> Result<()> {
        for child in self.mem_iters.iter_mut() {
            let status = child.status();
            if status.is_err() {
                return status;
            }
        }
        for child in self.table_iters.iter_mut() {
            let status = child.status();
            if status.is_err() {
                return status;
            }
        }
        Ok(())
    }
}
