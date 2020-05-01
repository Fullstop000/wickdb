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

use crate::db::format::{
    InternalKey, InternalKeyComparator, LookupKey, ParsedInternalKey, ValueType, MAX_KEY_SEQUENCE,
    VALUE_TYPE_FOR_SEEK,
};
use crate::iterator::Iterator;
use crate::options::{Options, ReadOptions};
use crate::storage::Storage;
use crate::table_cache::TableCache;
use crate::util::coding::put_fixed_64;
use crate::util::comparator::Comparator;
use crate::util::slice::Slice;
use crate::version::version_edit::FileMetaData;
use crate::version::version_set::total_file_size;
use crate::{Error, Result};
use std::cell::RefCell;
use std::cmp::Ordering as CmpOrdering;
use std::mem;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

pub mod version_edit;
pub mod version_set;

/// `Version` is a collection of file metadata for on-disk tables at various
/// levels. In-memory DBs are written to level-0 tables, and compactions
/// migrate data from level N to level N+1. The tables map internal keys (which
/// are a user key, a delete or set bit, and a sequence number) to user values.
///
/// The tables at level 0 are sorted by increasing fileNum. If two level 0
/// tables have fileNums i and j and i < j, then the sequence numbers of every
/// internal key in table i are all less than those for table j. The range of
/// internal keys [smallest, largest] in each level 0
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
    options: Arc<Options>,
    icmp: InternalKeyComparator,
    // files per level in this version
    // sorted by the smallest key in FileMetaData
    // TODO: is this `Arc` necessary ?
    files: Vec<Vec<Arc<FileMetaData>>>,

    // next file to compact based on seek stats
    // TODO: maybe use ShardLock from crossbeam instead.
    //       See https://docs.rs/crossbeam/0.7.1/crossbeam/sync/struct.ShardedLock.html
    file_to_compact: RwLock<Option<Arc<FileMetaData>>>,
    file_to_compact_level: AtomicUsize,

    // level that should be compacted next and its compaction score
    // score < 1 means compaction is not strictly needed.
    // These fields are initialized by `finalize`
    compaction_score: f32,
    compaction_level: usize,
}

/// A helper for representing the file has been seeked
pub struct SeekStats {
    // the file has been seeked
    pub seek_file: Option<Arc<FileMetaData>>,
    // the level the 'seek_file' is at
    pub seek_file_level: Option<usize>,
}
impl SeekStats {
    #[inline]
    pub fn new() -> Self {
        Self {
            seek_file: None,
            seek_file_level: None,
        }
    }
}

impl Version {
    pub fn new(options: Arc<Options>, icmp: InternalKeyComparator) -> Self {
        let max_levels = options.max_levels as usize;
        let mut files = Vec::with_capacity(max_levels);
        for _ in 0..max_levels {
            files.push(Vec::new());
        }
        Self {
            options,
            icmp,
            files,
            file_to_compact: RwLock::new(None),
            file_to_compact_level: AtomicUsize::new(0),
            compaction_score: 0f32,
            compaction_level: 0,
        }
    }

    /// Search the value by the given key in sstables level by level
    pub fn get<S: Storage + Clone + 'static>(
        &self,
        options: ReadOptions,
        key: LookupKey,
        table_cache: &TableCache<S>,
    ) -> Result<(Option<Slice>, SeekStats)> {
        let ikey = key.internal_key();
        let ukey = key.user_key();
        let ucmp = self.icmp.user_comparator.as_ref();
        let mut seek_stats = SeekStats::new();
        for (level, files) in self.files.iter().enumerate() {
            let mut files_to_seek = vec![];
            if files.is_empty() {
                continue;
            }
            if level == 0 {
                // Level-0 files may overlap each other. Find all files that
                // overlap user_key and process them in order from newest to oldest because
                // the last level-0 file always has the newest entries.
                for f in files.iter().rev() {
                    if ucmp.compare(ukey, f.largest.user_key()) != CmpOrdering::Greater
                        && ucmp.compare(ukey, f.smallest.user_key()) != CmpOrdering::Less
                    {
                        files_to_seek.push(f.clone());
                    }
                }
                files_to_seek.sort_by(|a, b| b.number.cmp(&a.number));
            } else {
                let index = Self::find_file(self.icmp.clone(), &self.files[level], &ikey);
                if index >= files.len() {
                    // we reach the end but not found a file matches
                } else {
                    let target = files[index].clone();
                    // If what we found is the first file, it could still not includes the target
                    // so let's check the smallest ukey. We use `ukey` because of the given `LookupKey`
                    // could has the same `ukey` as the `smallest` but a bigger `seq` number than it, which is smaller
                    // in a comparison by `icmp`.
                    if ucmp.compare(ukey, target.smallest.user_key()) != CmpOrdering::Less
                        && level + 1 < self.options.max_levels as usize
                    {
                        files_to_seek.push(target)
                    }
                }
            }

            for file in files_to_seek {
                warn!("Seek file: {:?}", &file);
                seek_stats.seek_file_level = Some(level);
                seek_stats.seek_file = Some(file.clone());
                match table_cache.get(
                    self.icmp.clone(),
                    options,
                    &ikey,
                    file.number,
                    file.file_size,
                )? {
                    None => continue, // keep searching
                    Some(block_iter) => {
                        let encoded_key = block_iter.key();
                        let value = block_iter.value();
                        match ParsedInternalKey::decode_from(encoded_key.as_slice()) {
                            None => return Err(Error::Corruption("bad internal key".to_owned())),
                            Some(parsed_key) => {
                                if self
                                    .options
                                    .comparator
                                    .compare(&parsed_key.user_key, key.user_key())
                                    == CmpOrdering::Equal
                                {
                                    match parsed_key.value_type {
                                        ValueType::Value => {
                                            if value.size() > 30 {
                                                info!("v1 {:?}", &value.as_slice()[..30]);
                                                info!("{:?}: {}", value.as_ptr(), value.size());
                                            }
                                            return Ok((Some(value), seek_stats))
                                        },
                                        ValueType::Deletion => return Ok((None, seek_stats)),
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok((None, seek_stats))
    }

    /// Update seek stats for a sstable file. If it runs out of `allow_seek`,
    /// mark it as a pending compaction file and returns true.
    pub fn update_stats(&self, stats: SeekStats) -> bool {
        if let Some(f) = stats.seek_file {
            let old = f.allowed_seeks.fetch_sub(1, Ordering::SeqCst);
            let mut file_to_compact = self.file_to_compact.write().unwrap();
            if file_to_compact.is_none() && old == 1 {
                *file_to_compact = Some(f);
                self.file_to_compact_level
                    .store(stats.seek_file_level.unwrap(), Ordering::Release);
                return true;
            }
        }
        false
    }

    /// Whether the version needs to be compacted
    pub fn needs_compaction(&self) -> bool {
        self.compaction_score > 1.0 || self.file_to_compact.read().unwrap().is_some()
    }

    /// Return a String includes number of files in every level
    pub fn level_summary(&self) -> String {
        let mut s = String::from("files[ ");
        let summary = self.files.iter().fold(String::new(), |mut acc, files| {
            acc.push_str(format!("{} ", files.len()).as_str());
            acc
        });
        s.push_str(summary.as_str());
        s.push_str("]");
        s
    }

    /// Binary search given files to find earliest index of index whose largest ikey >= given ikey.
    /// If not found, returns the length of files.
    pub fn find_file(
        icmp: InternalKeyComparator,
        files: &[Arc<FileMetaData>],
        ikey: &[u8],
    ) -> usize {
        let mut left = 0_usize;
        let mut right = files.len();
        while left < right {
            let mid = (left + right) / 2;
            let f = &files[mid];
            if icmp.compare(f.largest.data(), ikey) == CmpOrdering::Less {
                // Key at "mid.largest" is < "target".  Therefore all
                // files at or before "mid" are uninteresting
                left = mid + 1;
            } else {
                // Key at "mid.largest" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }
        right
    }

    /// Return the level at which we should place a new memtable compaction
    /// result that covers the range `[smallest_user_key,largest_user_key]`.
    pub fn pick_level_for_memtable_output(
        &self,
        smallest_ukey: &[u8],
        largest_ukey: &[u8],
    ) -> usize {
        let mut level = 0;
        if !self.overlap_in_level(level, Some(smallest_ukey), Some(largest_ukey)) {
            // No overlapping in level 0
            // we might directly push files to next level if there is no overlap in next level
            let smallest_ikey =
                InternalKey::new(smallest_ukey, MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK);
            let largest_ikey = InternalKey::new(largest_ukey, 0, ValueType::Deletion);
            while level < self.options.max_mem_compact_level {
                // Stops if overlaps at next level
                if self.overlap_in_level(level + 1, Some(smallest_ukey), Some(largest_ukey)) {
                    break;
                }
                if level + 2 < self.options.max_levels as usize {
                    // Check that file does not overlap too many grandparent bytes
                    let overlaps = self.get_overlapping_inputs(
                        level + 2,
                        Some(&smallest_ikey),
                        Some(&largest_ikey),
                    );
                    if total_file_size(&overlaps) > self.options.max_grandparent_overlap_bytes() {
                        break;
                    }
                }
                level += 1;
            }
        }
        level
    }

    // Calculate the compaction score of the version
    // The level with highest score will be marked as compaction needed.
    pub fn finalize(&mut self) {
        // pre-computed best level for next compaction
        let mut best_level = 0;
        let mut best_score = 0.0;
        for level in 0..self.options.max_levels as usize {
            let score = {
                if level == 0 {
                    // We treat level-0 specially by bounding the number of files
                    // instead of number of bytes for two reasons:
                    //
                    // (1) With larger write-buffer sizes, it is nice not to do too
                    // many level-0 compactions.
                    //
                    // (2) The files in level-0 are merged on every read and
                    // therefore we wish to avoid too many files when the individual
                    // file size is small (perhaps because of a small write-buffer
                    // setting, or very high compression ratios, or lots of
                    // overwrites/deletions)
                    self.files[level].len() as f64 / self.options.l0_compaction_threshold as f64
                } else {
                    let level_bytes = total_file_size(self.files[level].as_ref());
                    level_bytes as f64 / self.options.max_bytes_for_level(level) as f64
                }
            };
            if score > best_score {
                best_score = score;
                best_level = level;
            }
        }
        self.compaction_level = best_level;
        self.compaction_score = best_score as f32;
    }

    /// Returns `icmp`
    #[inline]
    pub fn comparator(&self) -> InternalKeyComparator {
        self.icmp.clone()
    }

    /// Returns slice of files in given `level`
    ///
    /// # Panic
    ///
    /// `level` is out bound of `files`
    #[inline]
    pub fn get_level_files(&self, level: usize) -> &[Arc<FileMetaData>] {
        assert!(
            level < self.files.len(),
            "[version] invalid level {}, the max level is {}",
            level,
            self.options.max_levels - 1
        );
        self.files[level].as_slice()
    }

    /// Call `func(level, file)` for every file that overlaps `user_key` in
    /// order from newest to oldest.  If an invocation of func returns
    /// false, makes no more calls.
    pub fn for_each_overlapping(
        &self,
        user_key: &[u8],
        internal_key: &[u8],
        mut func: Box<dyn FnMut(usize, Arc<FileMetaData>) -> bool>,
    ) {
        let ucmp = self.icmp.user_comparator.clone();
        for (level, files) in self.files.iter().enumerate() {
            if level == 0 {
                let mut target_files = vec![];
                // Search level 0 files
                for f in files.iter() {
                    if ucmp.compare(user_key, f.smallest.user_key()) != CmpOrdering::Less
                        && ucmp.compare(user_key, f.largest.user_key()) != CmpOrdering::Greater
                    {
                        target_files.push(f.clone());
                    }
                }
                if !target_files.is_empty() {
                    target_files.sort_by(|a, b| b.number.cmp(&a.number))
                }
                for target_file in target_files.iter() {
                    if !func(0, target_file.clone()) {
                        return;
                    }
                }
            } else {
                if files.is_empty() {
                    continue;
                }
                let index = Self::find_file(
                    self.icmp.clone(),
                    self.files[level].as_slice(),
                    &internal_key,
                );
                if index >= files.len() {
                    // we reach the end but not found a file matches
                } else {
                    let target = files[index].clone();
                    // if what we found is just the first file, it could still not includes the target
                    if ucmp.compare(user_key, target.smallest.data()) != CmpOrdering::Less
                        && !func(level, target)
                    {
                        return;
                    }
                }
            }
        }
    }

    /// Record a sample of bytes read at the specified internal key.
    /// Returns true if a new compaction may need to be triggered
    pub fn record_read_sample(&self, internal_key: &[u8]) -> bool {
        if let Some(pkey) = ParsedInternalKey::decode_from(internal_key) {
            let stats = Rc::new(RefCell::new(SeekStats::new()));
            let matches = Rc::new(RefCell::new(0));
            let stats_clone = stats.clone();
            let matches_clone = matches.clone();
            self.for_each_overlapping(
                pkey.user_key,
                internal_key,
                Box::new(move |level, file| {
                    *matches_clone.borrow_mut() += 1;
                    if *matches_clone.borrow() == 1 {
                        // Remember first match
                        stats_clone.borrow_mut().seek_file = Some(file);
                        stats_clone.borrow_mut().seek_file_level = Some(level);
                    }
                    *matches_clone.borrow() < 2
                }),
            );

            // Must have at least two matches since we want to merge across
            // files. But what if we have a single file that contains many
            // overwrites and deletions?  Should we have another mechanism for
            // finding such files?
            if *matches.borrow() >= 2 {
                if let Ok(s) = Rc::try_unwrap(stats) {
                    return self.update_stats(s.into_inner());
                }
            }
        }
        false
    }

    /// Returns true iff some file in the specified level overlaps
    /// some part of `[smallest_ukey,largest_ukey]`.
    /// `smallest_ukey` is `None` represents a key smaller than all the DB's keys.
    /// `largest_ukey` is `None` represents a key largest than all the DB's keys.
    pub fn overlap_in_level(
        &self,
        level: usize,
        smallest_ukey: Option<&[u8]>,
        largest_ukey: Option<&[u8]>,
    ) -> bool {
        if level == 0 {
            // need to check against all files in level 0
            for file in self.files[0].iter() {
                if self.key_is_after_file(file.clone(), smallest_ukey)
                    || self.key_is_before_file(file.clone(), largest_ukey)
                {
                    // No overlap
                    continue;
                } else {
                    return true;
                }
            }
            return false;
        }
        // binary search in level > 0
        let index = {
            if let Some(s_ukey) = smallest_ukey {
                let smallest_ikey = InternalKey::new(s_ukey, MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK);
                Self::find_file(self.icmp.clone(), &self.files[level], smallest_ikey.data())
            } else {
                0
            }
        };
        if index >= self.files[level].len() {
            // beginning of range is after all files, so no overlap
            return false;
        }
        // check whether the upper bound is overlapping
        !self.key_is_before_file(self.files[level][index].clone(), largest_ukey)
    }

    /// Return the approximate offset in the database of the data for
    /// given `ikey` in this version
    // TODO: remove this later
    #[allow(dead_code)]
    pub fn approximate_offset_of<S: Storage + Clone>(
        &self,
        ikey: &InternalKey,
        table_cache: &TableCache<S>,
    ) -> u64 {
        let mut result = 0;
        for (level, files) in self.files.iter().enumerate() {
            for f in files {
                if self.icmp.compare(f.largest.data(), ikey.data()) != CmpOrdering::Greater {
                    // Entire file is before "ikey", so just add the file size
                    result += f.file_size;
                } else if self.icmp.compare(f.smallest.data(), ikey.data()) == CmpOrdering::Greater
                {
                    // Entire file is after "ikey", so ignore
                    if level > 0 {
                        // Files other than level 0 are sorted by `smallest`, so
                        // no further files in this level will contain data for
                        // "ikey"
                        break;
                    }
                } else {
                    // "ikey" falls in the range for this table.  Add the
                    // approximate offset of "ikey" within the table.
                    if let Ok(table) =
                        table_cache.find_table(self.icmp.clone(), f.number, f.file_size)
                    {
                        result += table.approximate_offset_of(self.icmp.clone(), ikey.data());
                    }
                }
            }
        }
        result
    }
    // used for smallest user key
    fn key_is_after_file(&self, file: Arc<FileMetaData>, ukey: Option<&[u8]>) -> bool {
        ukey.is_some()
            && self
                .icmp
                .user_comparator
                .compare(ukey.unwrap(), file.largest.user_key())
                == CmpOrdering::Greater
    }

    // used for biggest user key
    fn key_is_before_file(&self, file: Arc<FileMetaData>, ukey: Option<&[u8]>) -> bool {
        ukey.is_some()
            && self
                .icmp
                .user_comparator
                .compare(ukey.unwrap(), file.smallest.user_key())
                == CmpOrdering::Less
    }

    // Return all files in `level` that overlap [`begin`, `end`]
    // Notice that both `begin` and `end` is `InternalKey` but we
    // compare the user key directly.
    // Since files in level0 probably overlaps with each other, the final output
    // total range could be larger than [begin, end]
    //
    // A `None` begin is considered as -infinite
    // A `None` end is considered as +infinite
    fn get_overlapping_inputs(
        &self,
        level: usize,
        begin: Option<&InternalKey>,
        end: Option<&InternalKey>,
    ) -> Vec<Arc<FileMetaData>> {
        // TODO: the implementation treating level 0 files is somewhat tricky ( since we use unsafe pointer ).
        //       Consider separate this into two single functions: one for level 0, one for level > 0
        let mut result = vec![];
        let cmp = &self.icmp.user_comparator;
        let mut user_begin = begin.map_or(Slice::default(), |ik| Slice::from(ik.user_key()));
        let mut user_end = end.map_or(Slice::default(), |ik| Slice::from(ik.user_key()));
        'outer: loop {
            for file in self.files[level].iter() {
                let file_begin = file.smallest.user_key();
                let file_end = file.largest.user_key();
                if !user_begin.is_empty()
                    && cmp.compare(file_end, user_begin.as_slice()) == CmpOrdering::Less
                {
                    // 'file' is completely before the specified range; skip it
                    continue;
                }
                if !user_end.is_empty()
                    && cmp.compare(file_begin, user_end.as_slice()) == CmpOrdering::Greater
                {
                    // 'file' is completely after the specified range; skip it
                    continue;
                }
                result.push(file.clone());
                if level == 0 {
                    // Level-0 files may overlap each other.  So check if the newly
                    // added file has expanded the range.  If so, restart search to make sure that
                    // we includes all the overlapping level 0 files
                    if !user_begin.is_empty()
                        && cmp.compare(file_begin, user_begin.as_slice()) == CmpOrdering::Less
                    {
                        user_begin = Slice::from(file_begin);
                        result.clear();
                        continue 'outer;
                    }
                    if !user_end.is_empty()
                        && cmp.compare(file_end, user_end.as_slice()) == CmpOrdering::Greater
                    {
                        user_end = Slice::from(file_end);
                        result.clear();
                        continue 'outer;
                    }
                }
            }
            return result;
        }
    }
}

/// file number and file size are both u64, so 2 * size_of(u64)
pub const FILE_META_LENGTH: usize = 2 * mem::size_of::<u64>();

/// An internal iterator.  For a given version/level pair, yields
/// information about the files in the level.  For a given entry, key()
/// is the largest key that occurs in the file, and value() is an
/// 16-byte value containing the file number and file size, both
/// encoded using `encode_fixed_u64`
pub struct LevelFileNumIterator {
    files: Vec<Arc<FileMetaData>>,
    icmp: InternalKeyComparator,
    index: usize,
    value_buf: Vec<u8>,
}

impl LevelFileNumIterator {
    pub fn new(icmp: InternalKeyComparator, files: Vec<Arc<FileMetaData>>) -> Self {
        let index = files.len();
        Self {
            files,
            icmp,
            index,
            value_buf: Vec::with_capacity(FILE_META_LENGTH),
        }
    }

    #[inline]
    fn fill_value_buf(&mut self) {
        if self.valid() {
            let file = &self.files[self.index];
            put_fixed_64(&mut self.value_buf, file.number);
            put_fixed_64(&mut self.value_buf, file.file_size);
        }
    }

    fn valid_or_panic(&self) {
        assert!(self.valid(), "[level file num iterator] out of bounds")
    }
}

impl Iterator for LevelFileNumIterator {
    type Key = Slice;
    type Value = Slice;
    fn valid(&self) -> bool {
        self.index < self.files.len()
    }

    fn seek_to_first(&mut self) {
        self.index = 0;
        self.fill_value_buf();
    }

    fn seek_to_last(&mut self) {
        if self.files.is_empty() {
            self.index = 0;
        } else {
            self.index = self.files.len() - 1;
        }
        self.fill_value_buf();
    }

    fn seek(&mut self, target: &[u8]) {
        let index = Version::find_file(self.icmp.clone(), self.files.as_slice(), target);
        self.index = index;
        self.fill_value_buf();
    }

    fn next(&mut self) {
        self.valid_or_panic();
        self.index += 1;
        self.fill_value_buf();
    }

    fn prev(&mut self) {
        self.valid_or_panic();
        if self.index == 0 {
            // marks as invalid
            self.index = self.files.len();
        } else {
            self.index -= 1;
            self.fill_value_buf();
        }
    }

    // make sure the underlying data's lifetime is longer than returning Slice
    fn key(&self) -> Self::Key {
        self.valid_or_panic();
        Slice::from(self.files[self.index].largest.data())
    }

    // make sure the iterator's lifetime is longer than returning Slice
    fn value(&self) -> Self::Value {
        self.valid_or_panic();
        Slice::from(&self.value_buf)
    }

    fn status(&mut self) -> Result<()> {
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::format::{InternalKey, InternalKeyComparator, ValueType};
    use crate::util::comparator::BytewiseComparator;
    use crate::util::slice::Slice;

    struct FindFileTests {
        pub files: Vec<Arc<FileMetaData>>,
        cmp: InternalKeyComparator,
    }

    impl FindFileTests {
        fn new() -> Self {
            let files: Vec<Arc<FileMetaData>> = Vec::new();
            let cmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));

            Self { files, cmp }
        }

        fn generate(&mut self, smallest: &Slice, largest: &Slice) {
            let mut file = FileMetaData::default();
            file.number = self.files.len() as u64 + 1;
            file.smallest = InternalKey::new(smallest.as_slice(), 100, ValueType::Value);
            file.largest = InternalKey::new(largest.as_slice(), 100, ValueType::Value);
            self.files.push(Arc::new(file));
        }

        fn find(&self, key: &Slice) -> usize {
            let ikey = InternalKey::new(key.as_slice(), 100, ValueType::Value);
            let target = Slice::from(ikey.data());
            Version::find_file(self.cmp.clone(), &self.files, &target.as_slice())
        }
    }

    #[test]
    fn test_find_file_with_single_file() {
        let mut test_suites = FindFileTests::new();
        assert_eq!(0, test_suites.find(&Slice::from("Foo")));
        test_suites.generate(&Slice::from("p"), &Slice::from("q"));
        let test_cases = vec![(0, "a"), (0, "p"), (0, "q"), (1, "q1"), (1, "z")];
        for (expected, input) in test_cases {
            assert_eq!(
                expected,
                test_suites.find(&Slice::from(input)),
                "input {}",
                input
            );
        }
    }

    #[test]
    fn test_find_files_with_various_files() {
        let mut test_suites = FindFileTests::new();
        let files = vec![
            ("150", "200"),
            ("200", "250"),
            ("300", "350"),
            ("400", "450"),
        ];
        for (start, end) in files {
            test_suites.generate(&Slice::from(start), &Slice::from(end));
        }
        let test_cases = vec![
            (0, "100"),
            (0, "150"),
            (1, "201"),
            (2, "251"),
            (2, "301"),
            (2, "350"),
            (3, "351"),
            (4, "451"),
        ];
        for (expected, input) in test_cases {
            assert_eq!(
                expected,
                test_suites.find(&Slice::from(input)),
                "input {}",
                input
            );
        }
    }
}
