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

use std::cmp::{min, Ordering};

/// A Comparator object provides a total order across `Slice` that are
/// used as keys in an sstable or a database.  A Comparator implementation
/// must be thread-safe since we may invoke its methods concurrently
/// from multiple threads.
pub trait Comparator: Send + Sync + Clone + Default {
    /// Three-way comparison. Returns value:
    ///   `Ordering::Less`    iff `a` < `b`
    ///   `Ordering::Equal`   iff `a` = `b`
    ///   `Ordering::Greater` iff `a` > `b`
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    /// The name of the comparator.  Used to check for comparator
    /// mismatches (i.e., a DB created with one comparator is
    /// accessed using a different comparator.
    ///
    /// The client of this package should switch to a new name whenever
    /// the comparator implementation changes in a way that will cause
    /// the relative ordering of any two keys to change.
    ///
    /// Names starting with "leveldb." are reserved and should not be used
    /// by any clients of this package.
    fn name(&self) -> &str;

    /// Given feasible keys a, b for which Compare(a, b) < 0, Separator returns a
    /// feasible key k such that:
    ///
    /// 1. Compare(a, k) <= 0, and
    /// 2. Compare(k, b) < 0.
    ///
    /// `separator` is used to construct SSTable index blocks. A trivial implementation
    /// is `return a`, but appending fewer bytes leads to smaller SSTables.
    ///
    /// If one key is the prefix of the other or b is smaller than a, returns a
    fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8>;

    /// Given a feasible key s, Successor returns feasible key k such that Compare(k,
    /// a) >= 0.
    /// If the key is a run of \xff, returns itself
    fn successor(&self, key: &[u8]) -> Vec<u8>;
}

#[derive(Default, Clone, Copy)]
pub struct BytewiseComparator {}

impl Comparator for BytewiseComparator {
    #[inline]
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    #[inline]
    fn name(&self) -> &str {
        "leveldb.BytewiseComparator"
    }

    #[inline]
    fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
        let min_size = min(a.len(), b.len());
        let mut diff_index = 0;
        while diff_index < min_size && a[diff_index] == b[diff_index] {
            diff_index += 1;
        }
        if diff_index >= min_size {
            // one is the prefix of the other
        } else {
            let last = a[diff_index];
            if last != 0xff && last + 1 < b[diff_index] {
                let mut res = vec![0; diff_index + 1];
                res[0..=diff_index].copy_from_slice(&a[0..=diff_index]);
                *(res.last_mut().unwrap()) += 1;
                return res;
            }
        }
        a.to_owned()
    }

    #[inline]
    fn successor(&self, key: &[u8]) -> Vec<u8> {
        // Find first character that can be incremented
        for i in 0..key.len() {
            let byte = key[i];
            if byte != 0xff {
                let mut res: Vec<u8> = vec![0; i + 1];
                res[0..=i].copy_from_slice(&key[0..=i]);
                *(res.last_mut().unwrap()) += 1;
                return res;
            }
        }
        key.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytewise_comparator_separator() {
        let mut tests = vec![
            ("", "1111", ""),
            ("1111", "", "1111"),
            ("1111", "111", "1111"),
            ("123", "1234", "123"),
            ("1234", "1234", "1234"),
            ("1", "2", "1"),
            ("1357", "2", "1357"),
            ("1111", "12345", "1111"),
            ("1111", "13345", "12"),
        ];
        let c = BytewiseComparator::default();
        for (a, b, expect) in tests.drain(..) {
            let res = c.separator(a.as_bytes(), b.as_bytes());
            assert_eq!(std::str::from_utf8(&res).unwrap(), expect);
        }
        // special 0xff case
        let a: Vec<u8> = vec![48, 255];
        let b: Vec<u8> = vec![48, 49, 50, 51];
        let res = c.separator(a.as_slice(), b.as_slice());
        assert_eq!(res, a);
    }

    #[test]
    fn test_bytewise_comparator_successor() {
        let mut tests = vec![("", ""), ("111", "2"), ("222", "3")];
        let c = BytewiseComparator::default();
        for (input, expect) in tests.drain(..) {
            let res = c.successor(input.as_bytes());
            assert_eq!(std::str::from_utf8(&res).unwrap(), expect);
        }
        // special 0xff case
        let mut corner_tests = vec![
            (vec![0xff, 0xff, 1], vec![255u8, 255u8, 2]),
            (vec![0xff, 0xff, 0xff], vec![255u8, 255u8, 255u8]),
        ];
        for (input, expect) in corner_tests.drain(..) {
            let res = c.successor(input.as_slice());
            assert_eq!(res, expect)
        }
    }
}
