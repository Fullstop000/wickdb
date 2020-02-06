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
use crate::error::Result;
use crate::iterator::{ConcatenateIterator, KMergeIter};
use crate::options::{Options, ReadOptions};
use crate::sstable::table::TableBuilder;
use crate::storage::{File, Storage};
use crate::table_cache::TableCache;
use crate::util::comparator::Comparator;
use crate::version::version_edit::{FileMetaData, VersionEdit};
use crate::version::version_set::{total_file_size, FileIterFactory, SSTableIters};
use crate::version::{LevelFileNumIterator, Version};
use std::cmp::Ordering as CmpOrdering;
use std::sync::Arc;

/// Information for a manual compaction
pub struct ManualCompaction {
    pub level: usize,
    pub done: bool,
    pub begin: Option<InternalKey>, // None means beginning of key range
    pub end: Option<InternalKey>,   // None means end of key range
}

/// A helper enum describing relations between the indexes of `inputs` in `Compaction`
// TODO: use const instead
pub enum CompactionInputsRelation {
    Source = 0, // level n
    Parent = 1, // level n + 1
}

/// A Compaction encapsulates information about a compaction
pub struct Compaction<F: File> {
    options: Arc<Options>,
    // Target level to be compacted
    pub level: usize,
    pub input_version: Option<Arc<Version>>,
    // Summary of the compaction result
    pub edit: VersionEdit,
    // level n and level n + 1
    // This field should be accessed via CompactionInputRelation
    // and the files of level n and level n + 1 are all sorted
    pub inputs: [Vec<Arc<FileMetaData>>; 2],

    // State used to check for number of overlapping grandparent files
    // (parent == level n + 1, grandparent == level n + 2
    pub grand_parents: Vec<Arc<FileMetaData>>,
    pub grand_parent_index: usize,

    // See the comments in `should_stop_before`
    pub seen_key: bool,
    // Bytes of overlap between current output and grandparent files
    pub overlapped_bytes: u64,

    // `level_ptrs` holds indices into `input_version.files`: our state
    // is that we are positioned at one of the file ranges for each
    // higher level than the ones involved in this compaction (i.e. for
    // all L >= level n + 2)
    pub level_ptrs: Vec<usize>,

    // Sequence numbers less than this are not significant since we
    // will never have to service a snapshot below smallest_snapshot.
    // Therefore if we have seen a sequence number S <= smallest_snapshot,
    // we can drop all entries for the same key with sequence numbers < S
    pub oldest_snapshot_alive: u64,

    // all output files information
    pub outputs: Vec<FileMetaData>,

    // current table builder for output sst file
    // we rotate a new builder when the inputs hit
    // the `should_stop_before`
    pub builder: Option<TableBuilder<InternalKeyComparator, F>>,

    // total bytes has been written
    pub total_bytes: u64,
}

impl<O: File> Compaction<O> {
    pub fn new(options: Arc<Options>, level: usize) -> Self {
        let max_levels = options.max_levels as usize;
        let mut level_ptrs = Vec::with_capacity(max_levels);
        for _ in 0..max_levels {
            level_ptrs.push(0)
        }
        Self {
            options: options.clone(),
            level,
            input_version: None,
            edit: VersionEdit::new(options.max_levels),
            inputs: [vec![], vec![]],
            grand_parents: vec![],
            grand_parent_index: 0,
            seen_key: false,
            overlapped_bytes: 0,
            level_ptrs,
            oldest_snapshot_alive: 0,
            outputs: vec![],
            builder: None,
            total_bytes: 0,
        }
    }

    /// Is this a trivial compaction that can be implemented by just
    /// moving a single input file to the next level (no merging or splitting)
    pub fn is_trivial_move(&self) -> bool {
        self.inputs[CompactionInputsRelation::Source as usize].len() == 1
            && self.inputs[CompactionInputsRelation::Parent as usize].is_empty()
            && total_file_size(self.grand_parents.as_slice())
                <= self.options.max_grandparent_overlap_bytes()
    }

    /// Create an iterator that reads over all the compaction input tables with merged order.
    /// We produce different iter for tables in level0 and level >0 :
    ///     level 0:  Since key ranges might be overlapped with each other, we generate
    ///              a table iterator over every single level 0 sst file
    ///     level > 0: a `ConcatenateIterator` for all the sst file in this level
    ///
    /// Entry format:
    ///     key: internal key
    ///     value: value of user key
    pub fn new_input_iterator<S: Storage + Clone + 'static>(
        &self,
        icmp: InternalKeyComparator,
        table_cache: TableCache<S>,
    ) -> Result<KMergeIter<SSTableIters<S>>> {
        let read_options = ReadOptions {
            verify_checksums: self.options.paranoid_checks,
            fill_cache: false,
            snapshot: None,
        };
        // Level-0 files have to be merged together so we generate a merging iterator includes iterators for each level 0 file.
        // For other levels, we will make a concatenating iterator per level.
        let mut level0 =
            Vec::with_capacity(self.inputs[CompactionInputsRelation::Source as usize].len() + 1);
        let mut leveln = Vec::with_capacity(2);
        for (i, input) in self.inputs.iter().enumerate() {
            if !input.is_empty() {
                if self.level + i == 0 {
                    // level0
                    for file in self.inputs[CompactionInputsRelation::Source as usize].iter() {
                        // all the level0 tables are guaranteed being added into the table_cache via minor compaction
                        level0.push(table_cache.new_iter(
                            icmp.clone(),
                            read_options.clone(),
                            file.number,
                            file.file_size,
                        )?);
                    }
                } else {
                    let origin = LevelFileNumIterator::new(icmp.clone(), self.inputs[i].clone());
                    let factory =
                        FileIterFactory::new(icmp.clone(), read_options, table_cache.clone());
                    leveln.push(ConcatenateIterator::new(origin, factory));
                }
            }
        }
        let iter = KMergeIter::new(SSTableIters::new(icmp, level0, leveln));
        Ok(iter)
    }

    /// Returns true iff we should stop building the current output
    /// before processing `ikey` for too much overlapping with grand parents
    pub fn should_stop_before(&mut self, ikey: &[u8], icmp: InternalKeyComparator) -> bool {
        // `seen_key` guarantees that we should continue checking for next `ikey`
        // no matter whether the first `ikey` overlaps with grand parents
        while self.grand_parent_index < self.grand_parents.len()
            && icmp.compare(
                ikey,
                self.grand_parents[self.grand_parent_index].largest.data(),
            ) == CmpOrdering::Greater
        {
            if self.seen_key {
                self.overlapped_bytes += self.grand_parents[self.grand_parent_index].file_size
            }
            self.grand_parent_index += 1;
        }
        self.seen_key = true;
        if self.overlapped_bytes > self.options.max_grandparent_overlap_bytes() {
            // Too much overlap for current output, start new output
            self.overlapped_bytes = 0;
            return true;
        }
        false
    }

    /// Returns false if the information we have available guarantees that
    /// the compaction is producing data in "level+1" for which no relative key exists
    /// in levels greater than "level+1".
    pub fn key_exist_in_deeper_level(&mut self, ukey: &[u8]) -> bool {
        let v = self.input_version.as_ref().unwrap().clone();
        let icmp = v.comparator();
        let ucmp = icmp.user_comparator.as_ref();
        let max_levels = self.options.max_levels as usize;
        if self.level + 2 < max_levels {
            for level in self.level + 2..max_levels {
                let files = v.get_level_files(level);
                while self.level_ptrs[level] < files.len() {
                    let f = files[self.level_ptrs[level]].clone();
                    if ucmp.compare(ukey, f.largest.user_key()) != CmpOrdering::Greater {
                        if ucmp.compare(ukey, f.smallest.user_key()) != CmpOrdering::Less {
                            return true;
                        }
                        break;
                    }
                    // Update current level ptr for a passed file directly since the input `ukey` should be sorted.
                    self.level_ptrs[level] += 1;
                }
            }
        }
        false
    }

    /// Apply deletion for current inputs and current output files to the edit
    pub fn apply_to_edit(&mut self) {
        for (delta, files) in self.inputs.iter().enumerate() {
            for file in files.iter() {
                self.edit.delete_file(self.level + delta, file.number)
            }
        }
        for output in self.outputs.drain(..) {
            self.edit
                .file_delta
                .new_files
                .push((self.level + 1, output))
        }
    }

    /// Calculate the read bytes
    #[inline]
    pub fn bytes_read(&self) -> u64 {
        self.inputs.iter().fold(0, |accumulate, files| {
            accumulate + files.iter().fold(0, |sum, file| sum + file.number)
        })
    }

    /// Calculate the written bytes
    #[inline]
    pub fn bytes_written(&self) -> u64 {
        self.outputs.iter().fold(0, |sum, file| sum + file.number)
    }
}

/// Returns the minimal range that covers all entries in `files`
pub fn base_range<'a>(
    files: &'a [Arc<FileMetaData>],
    level: usize,
    icmp: &InternalKeyComparator,
) -> (&'a InternalKey, &'a InternalKey) {
    assert!(
        !files.is_empty(),
        "[compaction] the input[0] shouldn't be empty when trying to get covered range"
    );
    if level == 0 {
        // level 0 files are possible to overlaps with each other
        let mut smallest = &files.first().unwrap().smallest;
        let mut largest = &files.last().unwrap().largest;
        for f in files.iter().skip(1) {
            if icmp.compare(f.smallest.data(), smallest.data()) == CmpOrdering::Less {
                smallest = &f.smallest;
            }
            if icmp.compare(f.largest.data(), largest.data()) == CmpOrdering::Greater {
                largest = &f.largest;
            }
        }
        (smallest, largest)
    } else {
        // no overlapping in level > 0 and file is ordered by smallest key
        (
            &files.first().unwrap().smallest,
            &files.last().unwrap().largest,
        )
    }
}

/// Returns the minimal range that covers all key ranges in `current_l_files` and `next_l_files`
/// `current_l_files` means current level files to be compacted
/// `next_l_files` means next level files to be compacted
pub fn total_range<'a>(
    current_l_files: &'a [Arc<FileMetaData>],
    next_l_files: &'a [Arc<FileMetaData>],
    level: usize,
    icmp: &InternalKeyComparator,
) -> (&'a InternalKey, &'a InternalKey) {
    let (mut smallest, mut largest) = base_range(current_l_files, level, icmp);
    if !next_l_files.is_empty() {
        let first = next_l_files.first().unwrap();
        if icmp.compare(first.smallest.data(), smallest.data()) == CmpOrdering::Less {
            smallest = &first.smallest
        }
        let last = next_l_files.last().unwrap();
        if icmp.compare(last.largest.data(), largest.data()) == CmpOrdering::Greater {
            largest = &last.largest
        }
    }
    (smallest, largest)
}
/// A helper struct for recording the statistics in compactions
pub struct CompactionStats {
    micros: u64,
    bytes_read: u64,
    bytes_written: u64,
}

impl CompactionStats {
    pub fn new() -> Self {
        CompactionStats {
            micros: 0,
            bytes_read: 0,
            bytes_written: 0,
        }
    }

    /// Add new stats to self
    #[inline]
    pub fn accumulate(&mut self, micros: u64, bytes_read: u64, bytes_written: u64) {
        self.micros += micros;
        self.bytes_read += bytes_read;
        self.bytes_written += bytes_written;
    }
}
