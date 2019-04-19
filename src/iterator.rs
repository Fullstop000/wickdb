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

use crate::util::slice::Slice;
use crate::util::status::WickErr;

pub trait Iterator {
    /// An iterator is either positioned at a key/value pair, or
    /// not valid.  This method returns true iff the iterator is valid.
    fn valid(&self) -> bool;

    /// Position at the first key in the source.  The iterator is Valid()
    /// after this call iff the source is not empty.
    fn seek_to_first(&mut self);

    /// Position at the last key in the source.  The iterator is
    /// Valid() after this call iff the source is not empty.
    fn seek_to_last(&mut self);

    /// Position at the first key in the source that is at or past target.
    /// The iterator is valid after this call iff the source contains
    /// an entry that comes at or past target.
    fn seek(&mut self, target: &Slice);

    /// Moves to the next entry in the source.  After this call, the iterator is
    /// valid iff the iterator was not positioned at the last entry in the source.
    /// REQUIRES: `valid()`
    fn next(&mut self);

    /// Moves to the previous entry in the source.  After this call, the iterator
    /// is valid iff the iterator was not positioned at the first entry in source.
    /// REQUIRES: `valid()`
    fn prev(&mut self);

    /// Return the key for the current entry.  The underlying storage for
    /// the returned slice is valid only until the next modification of
    /// the iterator.
    /// REQUIRES: `valid()`
    fn key(&self) -> Slice;

    /// Return the value for the current entry.  The underlying storage for
    /// the returned slice is valid only until the next modification of
    /// the iterator.
    /// REQUIRES: `valid()`
    fn value(&self) -> Slice;

    /// If an error has occurred, return it.  Else return an ok status.
    fn status(&mut self) -> Result<(), WickErr>;
}

/// A plain iterator used as default
///
/// # Notice
///
/// The `valid()` is always `false`
pub struct EmptyIterator {
    err: Option<WickErr>,
}

impl EmptyIterator {
    #[inline]
    pub fn new() -> Box<dyn Iterator> {
        box Self { err: None }
    }

    #[inline]
    pub fn new_with_err(e: WickErr) -> Box<dyn Iterator> {
        box Self { err: Some(e) }
    }
}

impl Iterator for EmptyIterator {
    fn valid(&self) -> bool {
        false
    }

    fn seek_to_first(&mut self) {}

    fn seek_to_last(&mut self) {}

    fn seek(&mut self, target: &Slice) {}

    fn next(&mut self) {}

    fn prev(&mut self) {}

    fn key(&self) -> Slice {
        Slice::new_empty()
    }

    fn value(&self) -> Slice {
        Slice::new_empty()
    }

    fn status(&mut self) -> Result<(), WickErr> {
        match self.err.take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}
