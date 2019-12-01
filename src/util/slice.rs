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

use super::byte::compare;
use crate::util::hash::hash;
use bytes::Bytes;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Index;
use std::ptr;
use std::slice;

/// Slice is a simple structure containing a pointer into some external
/// storage and a size.  The user of a Slice must ensure that the slice
/// is not used after the corresponding external storage has been
/// deallocated.
#[derive(Clone, Eq)]
pub struct Slice {
    data: Bytes,
}

impl Slice {
    pub fn new(data: *const u8, size: usize) -> Self {
        let data = if !data.is_null() {
            let slice = unsafe { slice::from_raw_parts(data, size) };
            Bytes::copy_from_slice(&slice[..])
        } else {
            println!("empty slice");
            Bytes::new()
        };

        Self { data }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        println!("as_slice");
        &self.data[..]
    }

    #[inline]
    pub fn copy(&self) -> Vec<u8> {
        Vec::from(self.as_slice())
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn remove_prefix(&mut self, n: usize) {
        if n >= self.data.len() {
            self.data.clear();
        } else {
            self.data.split_to(n);
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        println!("as_ptr");
        if !self.data.is_empty() {
            self.data[..].as_ptr()
        } else {
            ptr::null()
        }
    }

    #[inline]
    pub fn compare(&self, other: &Slice) -> Ordering {
        compare(self.as_slice(), other.as_slice())
    }

    #[inline]
    pub fn clear(&mut self) {
        println!("clear");
        self.data.clear()
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        println!("as str");
        if self.is_empty() {
            ""
        } else {
            unsafe { ::std::str::from_utf8_unchecked(self.as_slice()) }
        }
    }
}

impl Default for Slice {
    fn default() -> Self {
        Self::new(ptr::null(), 0)
    }
}

impl fmt::Debug for Slice {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl PartialEq for Slice {
    fn eq(&self, other: &Slice) -> bool {
        self.compare(other) == Ordering::Equal
    }
}

impl Index<usize> for Slice {
    type Output = u8;

    /// Return the ith byte in the referenced data
    fn index(&self, index: usize) -> &u8 {
        println!("index");
        assert!(
            index < self.data.len(),
            "[slice] out of range. Slice size is [{}] but try to get [{}]",
            self.data.len(),
            index
        );

        &self.data[index]
    }
}

impl Hash for Slice {
    fn hash<H: Hasher>(&self, state: &mut H) {
        println!("hash");
        let hash = hash(self.as_slice(), 0xbc9f1d34);
        state.write_u32(hash);
        state.finish();
    }
}

impl<'a> From<&'a [u8]> for Slice {
    #[inline]
    fn from(v: &'a [u8]) -> Self {
        Slice::new(v.as_ptr(), v.len())
    }
}

impl<'a> From<&'a Vec<u8>> for Slice {
    #[inline]
    fn from(v: &'a Vec<u8>) -> Self {
        Slice::new(v.as_ptr(), v.len())
    }
}

impl<'a> From<&'a str> for Slice {
    #[inline]
    fn from(s: &'a str) -> Self {
        Slice::new(s.as_ptr(), s.len())
    }
}
