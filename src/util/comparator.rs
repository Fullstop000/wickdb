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

use super::slice::Slice;
use std::cmp::{Ordering, min};

/// A Comparator object provides a total order across `Slice` that are
/// used as keys in an sstable or a database.  A Comparator implementation
/// must be thread-safe since we may invoke its methods concurrently
/// from multiple threads.
pub trait Comparator {
    /// Three-way comparison. Returns value:
    ///   `Ordering::Less`    iff `self` < `b`
    ///   `Ordering::Equal`   iff `self` = `b`
    ///   `Ordering::Greater` iff `self` > `b`
    fn compare(&self, a: &Slice, b: &Slice) -> Ordering;

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
    /// Separator is used to construct SSTable index blocks. A trivial implementation
    /// is `return a`, but appending fewer bytes leads to smaller SSTables.
    fn separator(&self, a: &Slice, b: &Slice) -> Option<Vec<u8>>;

    /// Given a feasible key a, Successor returns feasible key k such that Compare(k,
    /// a) >= 0.
    fn successor(&self, s: &Slice) -> Option<Vec<u8>>;
}

pub struct BytewiseComparator {}

impl BytewiseComparator {
    pub fn new() -> BytewiseComparator {
        BytewiseComparator {}
    }
}

impl Comparator for BytewiseComparator {
    #[inline]
    fn compare(&self, a: &Slice, b: &Slice) -> Ordering {
        a.compare(b)
    }

    #[inline]
    fn name(&self) -> &str {
        "leveldb.BytewiseComparator"
    }

    #[inline]
    fn separator(&self, a: &Slice, b: &Slice) -> Option<Vec<u8>> {
        let min_size = min(a.size(), b.size());
        let mut diff_index = 0;
        while diff_index < min_size && a[diff_index] == b[diff_index] {
            diff_index += 1;
        };
        if diff_index >= min_size {
            // one is the prefix of the other return None
            None
        } else {
            let last = a[diff_index];
            if last != 0xff && last + 1 < b[diff_index] {
                let mut res = vec![0;diff_index+1];
                res[0..=diff_index].copy_from_slice(&(a.to_slice())[0..=diff_index]);
                *(res.last_mut().unwrap()) += 1;
                return Some(res);
            }
            None
        }
    }

    #[inline]
    fn successor(&self, s: &Slice) -> Option<Vec<u8>> {
        // Find first character that can be incremented
        for i in 0..s.size() {
            let byte = s[i];
            if byte != 0xff {
                let mut res : Vec<u8>= vec![0;i+1];
                res[0..=i].copy_from_slice(&(s.to_slice())[0..=i]);
                *(res.last_mut().unwrap()) += 1;
                return Some(res);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytewise_comparator_separator() {
        let mut tests = vec![
            ("", "1111", None),
            ("1111", "", None),
            ("1111", "111", None),
            ("123", "1234", None),
            ("1234", "1234", None),
            ("1111", "12345", None),
            ("1111", "13345", Some(vec![49,50])),
        ];
        let c = BytewiseComparator::new();
        for (a, b, expect) in tests.drain(..) {
            let res = c.separator(&Slice::from(a), &Slice::from(b));
            assert_eq!(res, expect);
        }
        // special 0xff case
        let a : Vec<u8> = vec![48, 255];
        let b: Vec<u8> = vec![48, 49, 50, 51];
        let res = c.separator(&Slice::from(&a), &Slice::from(&b));
        assert_eq!(res, None);
    }

    #[test]
    fn test_bytewise_comparator_successor() {
        let mut tests = vec![
            ("", None),
            ("111", Some(vec![50])),
            ("222", Some(vec![51])),
        ];
        let c = BytewiseComparator::new();
        for (input, expect) in tests.drain(..) {
            let res = c.successor(&Slice::from(input));
            assert_eq!(res, expect);
        }
        // special 0xff case
        let s: Vec<u8> = vec![0xff, 0xff, 1];
        let res = c.successor(&Slice::from(&s));
        assert_eq!(res, Some(vec![255u8, 255u8, 2]))
    }
}
