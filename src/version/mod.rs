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
use crate::util::coding::encode_fixed_64;
use crate::util::comparator::Comparator;
use crate::version::version_edit::FileMetaData;
use crate::version::version_set::total_file_size;
use crate::{Error, Result};
use std::cell::{Cell, RefCell};
use std::cmp::Ordering as CmpOrdering;
use std::fmt;
use std::mem;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

pub mod version_edit;
pub mod version_set;

/// A helper for representing the file has been seeked
#[derive(Debug)]
pub struct SeekStats {
    // the file has been seeked
    pub file: Arc<FileMetaData>,
    // the level the 'seek_file' is at
    pub level: usize,
}

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
pub struct Version<C: Comparator> {
    vnum: usize, // For debug
    options: Arc<Options<C>>,
    icmp: InternalKeyComparator<C>,
    // files per level in this version
    // sorted by the smallest key in FileMetaData
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

impl<C: Comparator> fmt::Debug for Version<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "vnum: {} ", &self.vnum)?;
        for (level, files) in self.files.iter().enumerate() {
            write!(f, "level {}: [ ", level)?;
            for file in files {
                write!(
                    f,
                    "File {}({}): [{:?}..{:?}], ",
                    file.number, file.file_size, file.smallest, file.largest
                )?;
            }
            writeln!(f, " ]")?;
        }
        Ok(())
    }
}

impl<C: Comparator + 'static> Version<C> {
    pub fn new(options: Arc<Options<C>>, icmp: InternalKeyComparator<C>) -> Self {
        let max_levels = options.max_levels as usize;
        let mut files = Vec::with_capacity(max_levels);
        for _ in 0..max_levels {
            files.push(Vec::new());
        }
        Self {
            vnum: 0,
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
        table_cache: &TableCache<S, C>,
    ) -> Result<(Option<Vec<u8>>, Option<SeekStats>)> {
        let ikey = key.internal_key();
        let ukey = key.user_key();
        let ucmp = &self.icmp.user_comparator;
        let mut seek_stats = None;
        let mut files_to_seek = vec![];
        for (level, files) in self.files.iter().enumerate() {
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
                        files_to_seek.push((f, 0));
                    }
                }
            } else {
                let index = find_file(&self.icmp, files, ikey);
                if index >= files.len() {
                    // we reach the end but not found a file matches
                } else {
                    let target = &files[index];
                    // If what we found is the first file, it could still not includes the target
                    // so let's check the smallest ukey. We use `ukey` because of the given `LookupKey`
                    // may contains the same `ukey` as the `smallest` but a bigger `seq` number than it, which is smaller
                    // in a comparison by `icmp`.
                    if ucmp.compare(ukey, target.smallest.user_key()) != CmpOrdering::Less
                        && level + 1 < self.options.max_levels as usize
                    {
                        files_to_seek.push((target, level));
                    }
                }
            }
        }
        files_to_seek.sort_by(|(a, _), (b, _)| b.number.cmp(&a.number));
        for (file, level) in files_to_seek {
            if seek_stats.is_none() {
                // TODO(fullstop000): leveldb only charge the first file for seek compaction
                seek_stats = Some(SeekStats {
                    file: file.clone(),
                    level,
                });
            }
            match table_cache.get(
                self.icmp.clone(),
                options,
                ikey,
                file.number,
                file.file_size,
            )? {
                None => continue,
                Some(block_iter) => {
                    let encoded_key = block_iter.key();
                    let value = block_iter.value();
                    match ParsedInternalKey::decode_from(encoded_key) {
                        None => return Err(Error::Corruption("bad internal key".to_owned())),
                        Some(parsed_key) => {
                            if self
                                .options
                                .comparator
                                .compare(parsed_key.user_key, key.user_key())
                                == CmpOrdering::Equal
                            {
                                match parsed_key.value_type {
                                    ValueType::Value => {
                                        return Ok((Some(value.to_vec()), seek_stats))
                                    }
                                    ValueType::Deletion => return Ok((None, seek_stats)),
                                    _ => {}
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
    pub fn update_stats(&self, stats: Option<SeekStats>) -> bool {
        if let Some(ss) = stats {
            let old = ss
                .file
                .allowed_seeks
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                    Some(if v > 0 { v - 1 } else { 0 })
                })
                .unwrap();
            let mut file_to_compact = self.file_to_compact.write().unwrap();
            if file_to_compact.is_none() && old == 1 {
                *file_to_compact = Some(ss.file);
                self.file_to_compact_level
                    .store(ss.level, Ordering::Release);
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
        s.push(']');
        s
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
    pub fn comparator(&self) -> InternalKeyComparator<C> {
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
        let ucmp = &self.icmp.user_comparator;
        for (level, files) in self.files.iter().enumerate() {
            if level == 0 {
                let mut target_files = vec![];
                // Search level 0 files
                for f in files.iter() {
                    if ucmp.compare(user_key, f.smallest.user_key()) != CmpOrdering::Less
                        && ucmp.compare(user_key, f.largest.user_key()) != CmpOrdering::Greater
                    {
                        target_files.push(f);
                    }
                }
                if !target_files.is_empty() {
                    target_files.sort_by(|a, b| b.number.cmp(&a.number))
                }
                for target_file in target_files {
                    if !func(0, target_file.clone()) {
                        return;
                    }
                }
            } else {
                if files.is_empty() {
                    continue;
                }
                let index = find_file(&self.icmp, self.files[level].as_slice(), internal_key);
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
            let stats = Rc::new(Cell::new(None));
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
                        stats_clone.set(Some(SeekStats { file, level }));
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
        some_file_overlap_range(
            &self.icmp,
            level > 0,
            &self.files[level],
            smallest_ukey,
            largest_ukey,
        )
    }

    /// Return the approximate offset in the database of the data for
    /// given `ikey` in this version
    pub fn approximate_offset_of<S: Storage + Clone>(
        &self,
        ikey: &InternalKey,
        table_cache: &TableCache<S, C>,
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

    // Return all files in `level` that overlap [`begin`, `end`]
    // Notice that both `begin` and `end` is `InternalKey` but we just compare the user key directly.
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
        let mut result = vec![];
        let cmp = &self.icmp.user_comparator;
        let mut user_begin = begin.map(|ik| ik.user_key());
        let mut user_end = end.map(|ik| ik.user_key());
        'outer: loop {
            for file in self.files[level].iter() {
                let file_begin = file.smallest.user_key();
                let file_end = file.largest.user_key();
                if user_begin.is_some()
                    && cmp.compare(file_end, user_begin.unwrap()) == CmpOrdering::Less
                {
                    // 'file' is completely before the specified range; skip it
                    continue;
                }
                if user_end.is_some()
                    && cmp.compare(file_begin, user_end.unwrap()) == CmpOrdering::Greater
                {
                    // 'file' is completely after the specified range; skip it
                    continue;
                }
                result.push(file.clone());
                if level == 0 {
                    // Level-0 files may overlap each other.  So check if the newly
                    // added file has expanded the range.  If so, restart search to make sure that
                    // we includes all the overlapping level 0 files
                    if user_begin.is_some()
                        && cmp.compare(file_begin, user_begin.unwrap()) == CmpOrdering::Less
                    {
                        user_begin = Some(file_begin);
                        result.clear();
                        continue 'outer;
                    }
                    if user_end.is_some()
                        && cmp.compare(file_end, user_end.unwrap()) == CmpOrdering::Greater
                    {
                        user_end = Some(file_end);
                        result.clear();
                        continue 'outer;
                    }
                }
            }
            return result;
        }
    }
}

// Binary search given files to find earliest index of index whose largest ikey >= given ikey.
// If not found, returns the length of files.
fn find_file<C: Comparator>(
    icmp: &InternalKeyComparator<C>,
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

fn some_file_overlap_range<C: Comparator>(
    icmp: &InternalKeyComparator<C>,
    disjoint: bool,
    files: &[Arc<FileMetaData>],
    smallest_ukey: Option<&[u8]>,
    largest_ukey: Option<&[u8]>,
) -> bool {
    if !disjoint {
        for file in files {
            if key_is_after_file(icmp, file, smallest_ukey)
                || key_is_before_file(icmp, file, largest_ukey)
            {
                // No overlap
                continue;
            } else {
                return true;
            }
        }
        return false;
    }
    // binary search since file ranges are disjoint
    let index = {
        if let Some(s_ukey) = smallest_ukey {
            let smallest_ikey = InternalKey::new(s_ukey, MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK);
            find_file(icmp, files, smallest_ikey.data())
        } else {
            0
        }
    };
    if index >= files.len() {
        // beginning of range is after all files, so no overlap
        return false;
    }
    // check whether the upper bound is overlapping
    !key_is_before_file(icmp, &files[index], largest_ukey)
}
// used for smallest user key
fn key_is_after_file<C: Comparator>(
    icmp: &InternalKeyComparator<C>,
    file: &Arc<FileMetaData>,
    ukey: Option<&[u8]>,
) -> bool {
    ukey.is_some()
        && icmp
            .user_comparator
            .compare(ukey.unwrap(), file.largest.user_key())
            == CmpOrdering::Greater
}

// used for biggest user key
fn key_is_before_file<C: Comparator>(
    icmp: &InternalKeyComparator<C>,
    file: &Arc<FileMetaData>,
    ukey: Option<&[u8]>,
) -> bool {
    ukey.is_some()
        && icmp
            .user_comparator
            .compare(ukey.unwrap(), file.smallest.user_key())
            == CmpOrdering::Less
}
/// file number and file size are both u64, so 2 * size_of(u64)
pub const FILE_META_LENGTH: usize = 2 * mem::size_of::<u64>();

/// An internal iterator.  For a given version/level pair, yields
/// information about the files in the level.  For a given entry, key()
/// is the largest key that occurs in the file, and value() is an
/// 16-byte value containing the file number and file size, both
/// encoded using `encode_fixed_u64`
pub struct LevelFileNumIterator<C: Comparator> {
    files: Vec<Arc<FileMetaData>>,
    icmp: InternalKeyComparator<C>,
    index: usize,
    value_buf: [u8; 16],
}

impl<C: Comparator + 'static> LevelFileNumIterator<C> {
    pub fn new(icmp: InternalKeyComparator<C>, files: Vec<Arc<FileMetaData>>) -> Self {
        let index = files.len();
        Self {
            files,
            icmp,
            index,
            value_buf: [0; 16],
        }
    }

    #[inline]
    fn fill_value_buf(&mut self) {
        if self.valid() {
            let file = &self.files[self.index];
            encode_fixed_64(&mut self.value_buf, file.number);
            encode_fixed_64(&mut self.value_buf[8..], file.file_size);
        }
    }

    fn valid_or_panic(&self) {
        assert!(self.valid(), "[level file num iterator] out of bounds")
    }
}

impl<C: Comparator + 'static> Iterator for LevelFileNumIterator<C> {
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
        let index = find_file(&self.icmp, self.files.as_slice(), target);
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

    fn key(&self) -> &[u8] {
        self.valid_or_panic();
        self.files[self.index].largest.data()
    }

    fn value(&self) -> &[u8] {
        self.valid_or_panic();
        &self.value_buf
    }

    fn status(&mut self) -> Result<()> {
        Ok(())
    }
}
#[cfg(test)]
mod find_file_tests {
    use super::*;
    use crate::db::format::{InternalKey, InternalKeyComparator, ValueType};
    use crate::util::comparator::BytewiseComparator;

    #[derive(Default)]
    struct FindFileTest {
        // Indicate whether file ranges are already disjoint
        overlapping: bool,
        files: Vec<Arc<FileMetaData>>,
        cmp: InternalKeyComparator<BytewiseComparator>,
    }

    impl FindFileTest {
        fn add(&mut self, smallest: &str, largest: &str) {
            let mut file = FileMetaData::default();
            file.number = self.files.len() as u64 + 1;
            file.smallest = InternalKey::new(smallest.as_bytes(), 100, ValueType::Value);
            file.largest = InternalKey::new(largest.as_bytes(), 100, ValueType::Value);
            self.files.push(Arc::new(file));
        }

        fn add_with_seq(&mut self, smallest: (&str, u64), largest: (&str, u64)) {
            let mut file = FileMetaData::default();
            file.number = self.files.len() as u64 + 1;
            file.smallest = InternalKey::new(smallest.0.as_bytes(), smallest.1, ValueType::Value);
            file.largest = InternalKey::new(largest.0.as_bytes(), largest.1, ValueType::Value);
            self.files.push(Arc::new(file));
        }

        fn find(&self, key: &str) -> usize {
            let ikey = InternalKey::new(key.as_bytes(), 100, ValueType::Value);
            find_file(&self.cmp, &self.files, ikey.data())
        }

        fn overlaps(&self, smallest: Option<&str>, largest: Option<&str>) -> bool {
            some_file_overlap_range(
                &self.cmp,
                !self.overlapping,
                &self.files,
                smallest.map(|s| s.as_bytes()),
                largest.map(|s| s.as_bytes()),
            )
        }
    }

    #[test]
    fn test_empty_file_set() {
        let t = FindFileTest::default();
        assert_eq!(0, t.find("foo"));
        assert!(!t.overlaps(Some("a"), Some("z")));
        assert!(!t.overlaps(None, Some("z")));
        assert!(!t.overlaps(Some("a"), None));
        assert!(!t.overlaps(None, None));
    }

    #[test]
    fn test_find_file_with_single_file() {
        let mut t = FindFileTest::default();
        t.add("p", "q");
        // Find tests
        for (expected, input) in vec![(0, "a"), (0, "p"), (0, "p1"), (0, "q"), (1, "q1"), (1, "z")]
        {
            assert_eq!(expected, t.find(input), "input {}", input);
        }
        // Overlap tests
        for (expected, (lhs, rhs)) in vec![
            (false, (Some("a"), Some("b"))),
            (false, (Some("z1"), Some("z2"))),
            (true, (Some("a"), Some("p"))),
            (true, (Some("a"), Some("q"))),
            (true, (Some("p"), Some("p1"))),
            (true, (Some("p"), Some("q"))),
            (true, (Some("p1"), Some("p2"))),
            (true, (Some("p1"), Some("z"))),
            (true, (Some("q"), Some("q"))),
            (true, (Some("q"), Some("q1"))),
            (false, (None, Some("j"))),
            (false, (Some("r"), None)),
            (true, (None, Some("p"))),
            (true, (None, Some("p1"))),
            (true, (Some("q"), None)),
            (true, (None, None)),
        ] {
            assert_eq!(expected, t.overlaps(lhs, rhs))
        }
    }

    #[test]
    fn test_find_files_with_various_files() {
        let mut t = FindFileTest::default();
        for (start, end) in vec![
            ("150", "200"),
            ("200", "250"),
            ("300", "350"),
            ("400", "450"),
        ] {
            t.add(start, end);
        }
        // Find tests
        for (expected, input) in vec![
            (0, "100"),
            (0, "150"),
            (0, "151"),
            (0, "199"),
            (0, "200"),
            (1, "201"),
            (1, "249"),
            (1, "250"),
            (2, "251"),
            (2, "301"),
            (2, "350"),
            (3, "351"),
            (4, "451"),
        ] {
            assert_eq!(expected, t.find(input), "input {}", input);
        }
        // Overlap tests
        for (expected, (lhs, rhs)) in vec![
            (false, (Some("100"), Some("149"))),
            (false, (Some("251"), Some("299"))),
            (false, (Some("451"), Some("500"))),
            (false, (Some("351"), Some("399"))),
            (true, (Some("100"), Some("150"))),
            (true, (Some("100"), Some("200"))),
            (true, (Some("100"), Some("300"))),
            (true, (Some("100"), Some("400"))),
            (true, (Some("100"), Some("500"))),
            (true, (Some("375"), Some("400"))),
            (true, (Some("450"), Some("450"))),
            (true, (Some("450"), Some("500"))),
        ] {
            assert_eq!(expected, t.overlaps(lhs, rhs))
        }
    }

    #[test]
    fn test_multiple_null_boundaries() {
        let mut t = FindFileTest::default();
        for (start, end) in vec![
            ("150", "200"),
            ("200", "250"),
            ("300", "350"),
            ("400", "450"),
        ] {
            t.add(start, end);
        }
        for (expected, (lhs, rhs)) in vec![
            (false, (None, Some("149"))),
            (false, (Some("451"), None)),
            (true, (None, None)),
            (true, (None, Some("150"))),
            (true, (None, Some("199"))),
            (true, (None, Some("200"))),
            (true, (None, Some("201"))),
            (true, (None, Some("400"))),
            (true, (None, Some("800"))),
            (true, (Some("100"), None)),
            (true, (Some("200"), None)),
            (true, (Some("449"), None)),
            (true, (Some("450"), None)),
        ] {
            assert_eq!(expected, t.overlaps(lhs, rhs))
        }
    }

    #[test]
    fn test_overlap_sequence_check() {
        let mut t = FindFileTest::default();
        t.add_with_seq(("200", 5000), ("200", 300));
        for (expected, (lhs, rhs)) in vec![
            (false, (Some("199"), Some("199"))),
            (false, (Some("201"), Some("300"))),
            (true, (Some("200"), Some("200"))),
            (true, (Some("190"), Some("200"))),
            (true, (Some("200"), Some("210"))),
        ] {
            assert_eq!(expected, t.overlaps(lhs, rhs))
        }
    }

    #[test]
    fn test_overlapping_files() {
        let mut t = FindFileTest::default();
        t.overlapping = true;
        t.add("150", "600");
        t.add("400", "500");
        for (expected, (lhs, rhs)) in vec![
            (false, (Some("100"), Some("149"))),
            (false, (Some("601"), Some("700"))),
            (true, (Some("100"), Some("150"))),
            (true, (Some("100"), Some("200"))),
            (true, (Some("100"), Some("300"))),
            (true, (Some("100"), Some("400"))),
            (true, (Some("100"), Some("500"))),
            (true, (Some("375"), Some("400"))),
            (true, (Some("450"), Some("450"))),
            (true, (Some("450"), Some("500"))),
            (true, (Some("450"), Some("700"))),
            (true, (Some("600"), Some("700"))),
        ] {
            assert_eq!(expected, t.overlaps(lhs, rhs))
        }
    }
}
