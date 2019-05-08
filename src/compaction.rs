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

use crate::db::format::{InternalKey, InternalKeyComparator};
use crate::version::version_edit::{VersionEdit, FileMetaData};
use crate::util::collection::NodePtr;
use crate::version::Version;
use crate::version::version_set::VersionSet;
use std::sync::Arc;
use crate::util::comparator::Comparator;
use std::cmp::Ordering;
use std::rc::Rc;

/// Information for a manual compaction
pub struct ManualCompaction {
    level: usize,
    done: bool,
    begin: Option<InternalKey>, // None means beginning of key range
    end: Option<InternalKey>, // None means end of key range
}

/// A Compaction encapsulates information about a compaction
pub struct Compaction {
    pub level: usize,
    max_output_file_size: u64,
    pub input_version: Option<NodePtr<Version>>,
    pub edit: VersionEdit,

    // level n and level n + 1
    pub inputs: [Vec<Arc<FileMetaData>>;2],

    // State used to check for number of overlapping grandparent files
    // (parent == level n + 1, grandparent == level n + 2
    pub grand_parents: Vec<Arc<FileMetaData>>,
    pub grand_parent_index: usize,
    // some output key has been seen
    pub seen_key: bool,
    // bytes of overlap between current output and grandparent files
    pub overlapped_bytes: u64,
    // level_ptrs holds indices into input_version.files: our state
    // is that we are positioned at one of the file ranges for each
    // higher level than the ones involved in this compaction (i.e. for
    // all L >= level n + 2)
    pub level_ptrs: Vec<usize>,
}

impl Compaction {
    pub fn new(max_file_size: u64, max_levels: u8, level: usize) -> Self {
        Self {
            level,
            max_output_file_size: max_file_size,
            input_version: None,
            edit: VersionEdit::new(max_levels),
            inputs: [vec![], vec![]],
            grand_parents: vec![],
            grand_parent_index: 0,
            seen_key: false,
            overlapped_bytes: 0,
            level_ptrs: vec![],
        }
    }

    /// Returns the minimal range that covers all entries in self.inputs[0]
    pub fn base_range(&self, icmp: &InternalKeyComparator) -> (Rc<InternalKey>, Rc<InternalKey>) {
        let files = &self.inputs[0];
        assert!(!files.is_empty(), "[compaction] the input[0] shouldn't be empty when trying to get covered range");
        if self.level == 0 { // level 0 files are possible to overlaps with each other
            let mut smallest = files.first().unwrap().smallest.clone();
            let mut largest = files.last().unwrap().largest.clone();
            for f in files.iter().skip(1) {
                if icmp.compare(f.smallest.data(), smallest.data()) == Ordering::Less {
                    smallest = f.smallest.clone();
                }
                if icmp.compare(f.largest.data(), largest.data()) == Ordering::Greater {
                    largest = f.largest.clone();
                }
            }
            (smallest, largest)
        } else { // no overlapping in level > 0 and file is ordered by smallest key
            (files.first().unwrap().smallest.clone(), files.last().unwrap().largest.clone())
        }
    }

    /// Returns the minimal range that covers all entries in self.inputs
    pub fn total_range(&self, icmp: &InternalKeyComparator) -> (Rc<InternalKey>, Rc<InternalKey>) {
        let (mut smallest, mut largest) = self.base_range(icmp);
        let files = &self.inputs[1];
        if !files.is_empty() {
            let first = files.first().unwrap();
            if icmp.compare(first.smallest.data(), smallest.data()) == Ordering::Less {
                smallest = first.smallest.clone()
            }
            let last = files.last().unwrap();
            if icmp.compare(last.largest.data(), largest.data()) == Ordering::Greater {
                largest = last.largest.clone()
            }
        }
        (smallest, largest)
    }
}
