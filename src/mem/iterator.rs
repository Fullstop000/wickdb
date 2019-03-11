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

use super::arena::Arena;
use super::skiplist::{Node, Skiplist};
use crate::util::slice::Slice;
use std::ptr;

/// Iteration over the contents of a skip list
// TODO: maybe use a more common trait 'Iterator'
pub struct SkiplistIterator<'a> {
    skl: &'a Skiplist,
    node: *mut Node,
}

impl<'a> SkiplistIterator<'a> {
    pub fn new(skl: &'a Skiplist, node: *mut Node) -> Self {
        Self { skl, node }
    }

    /// Returns true whether the iterator is positioned at a valid node
    #[inline]
    pub fn valid(&self) -> bool {
        !self.node.is_null()
    }

    /// If the head is nullptr, this method will panic. Otherwise return true.
    #[inline]
    pub fn panic_valid(&self) -> bool {
        invarint!(self.valid(), "[skl] Invalid iterator head",);
        true
    }

    /// Return the key of node in current position
    #[inline]
    pub fn key(&self) -> Slice {
        self.panic_valid();
        unsafe { (*(self.node)).key(&self.skl.arena) }
    }

    /// Return the value of node in current position
    #[inline]
    pub fn value(&self) -> Slice {
        self.panic_valid();
        unsafe { (*(self.node)).value(&self.skl.arena) }
    }

    /// Advance to the next position
    #[inline]
    pub fn next(&mut self) {
        self.panic_valid();
        unsafe {
            self.node = (*(self.node)).get_next(1);
        }
    }

    /// Advance to the previous position
    #[inline]
    pub fn prev(&mut self) {
        let key = self.key();
        self.node = self.skl.find_less_than(&key);
    }

    /// Advance to the first node with a key >= target
    #[inline]
    pub fn seek(&mut self, target_key: &Slice) {
        self.node = self.skl.find_greater_or_equal(target_key, None);
    }

    /// Position at the first node in list
    #[inline]
    pub fn seek_to_first(&mut self) {
        self.node = self.skl.head;
    }

    /// Position at the last node in list
    #[inline]
    pub fn seek_to_last(&mut self) {
        self.node = self.skl.find_last();
    }
}
