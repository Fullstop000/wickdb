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

mod version_edit;
mod version_set;

/// `Version` is a collection of file metadata for on-disk tables at various
/// levels. In-memory DBs are written to level-0 tables, and compactions
/// migrate data from level N to level N+1. The tables map internal keys (which
/// are a user key, a delete or set bit, and a sequence number) to user values.
///
/// The tables at level 0 are sorted by increasing fileNum. If two level 0
/// tables have fileNums i and j and i < j, then the sequence numbers of every
/// internal key in table i are all less than those for table j. The range of
/// internal keys [fileMetadata.smallest, fileMetadata.largest] in each level 0
/// table may overlap.
///
/// The tables at any non-0 level are sorted by their internal key range and any
/// two tables at the same non-0 level do not overlap.
///
/// The internal key ranges of two tables at different levels X and Y may
/// overlap, for any X != Y.
///
/// Finally, for every internal key in a table at level X, there is no internal
/// key in a higher level table that has both the same user key and a higher
/// sequence number.
pub struct Version {

}