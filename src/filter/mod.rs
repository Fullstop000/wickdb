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

use crate::util::slice::Slice;

mod bloom;

/// `FilterPolicy` is an algorithm for probabilistically encoding a set of keys.
/// The canonical implementation is a Bloom filter.
///
/// Every `FilterPolicy` has a name. This names the algorithm itself, not any one
/// particular instance. Aspects specific to a particular instance, such as the
/// set of keys or any other parameters, will be encoded in the byte filter
/// returned by `new_filter_writer`.
///
/// The name may be written to files on disk, along with the filter data. To use
/// these filters, the `FilterPolicy` name at the time of writing must equal the
/// name at the time of reading. If they do not match, the filters will be
/// ignored, which will not affect correctness but may affect performance.
pub trait FilterPolicy {

    /// Return the name of this policy.  Note that if the filter encoding
    /// changes in an incompatible way, the name returned by this method
    /// must be changed.  Otherwise, old incompatible filters may be
    /// passed to methods of this type.
    fn name(&self) -> &str;

    /// `MayContain` returns whether the encoded filter may contain given key.
    /// False positives are possible, where it returns true for keys not in the
    /// original set.
    fn may_contain(&self, key: &Slice) -> bool;

    /// Creates a new `FilterWriter`
    fn new_filter_writer(&self) -> Box<dyn FilterWriter>;
}

pub trait FilterWriter {

    /// `add_key` adds a key to the current filter block.
    fn add_key(&mut self, key: &Slice);

    /// `finish` appends to dst an encoded filter that holds the current set of
    /// keys. The writer state is reset after the call to `Finish` allowing the
    /// writer to be reused for the creation of additional filters.
    fn finish(&mut self) -> Slice;
}