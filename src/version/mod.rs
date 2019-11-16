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
    InternalKey, InternalKeyComparator, LookupKey, ParsedInternalKey, ValueType,
    VALUE_TYPE_FOR_SEEK,
};
use crate::iterator::Iterator;
use crate::options::{Options, ReadOptions};
use crate::table_cache::TableCache;
use crate::util::coding::put_fixed_64;
use crate::util::comparator::Comparator;
use crate::util::slice::Slice;
use crate::util::status::{Result, Status, WickErr};
use crate::version::version_edit::FileMetaData;
use crate::version::version_set::VersionSet;
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
    icmp: Arc<InternalKeyComparator>,
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
    pub fn new(options: Arc<Options>, icmp: Arc<InternalKeyComparator>) -> Self {
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
    pub fn get(
        &self,
        options: ReadOptions,
        key: LookupKey,
        table_cache: Arc<TableCache>,
    ) -> Result<(Option<Slice>, SeekStats)> {
        let opt = Rc::new(options);
        let ikey = key.internal_key();
        let ukey = key.user_key();
        let ucmp = self.icmp.user_comparator.as_ref();
        let mut files_to_seek = vec![];
        let mut seek_stats = SeekStats::new();
        for (level, files) in self.files.iter().enumerate() {
            if files.is_empty() {
                continue;
            }
            if level == 0 {
                // Level-0 files may overlap each other. Find all files that
                // overlap user_key and process them in order from newest to oldest because
                // the last level-0 file always has the newest entries.
                for f in files.iter().rev() {
                    if ucmp.compare(ukey.as_slice(), f.largest.data()) != CmpOrdering::Greater
                        && ucmp.compare(ukey.as_slice(), f.smallest.data()) != CmpOrdering::Less
                    {
                        files_to_seek.push(f.clone());
                    }
                }
                files_to_seek.sort_by(|a, b| b.number.cmp(&a.number));
            } else {
                let index = Self::find_file(self.icmp.clone(), self.files[level].as_slice(), &ikey);
                if index >= files.len() {
                    // TODO: maybe '==' is enough ?
                    // we reach the end but not found a file matches
                } else {
                    let target = files[index].clone();
                    // if what we found is just the first file, it could still not includes the target
                    if ucmp.compare(ukey.as_slice(), target.smallest.data()) != CmpOrdering::Less {
                        files_to_seek = vec![target];
                    }
                }
            }

            for file in files_to_seek.iter() {
                seek_stats.seek_file_level = Some(level);
                seek_stats.seek_file = Some(file.clone());
                match table_cache.get(opt.clone(), &ikey, file.number, file.file_size)? {
                    None => continue, // keep searching
                    Some((encoded_key, value)) => {
                        match ParsedInternalKey::decode_from(encoded_key) {
                            None => {
                                return Err(WickErr::new(
                                    Status::Corruption,
                                    Some("bad internal key"),
                                ))
                            }
                            Some(parsed_key) => {
                                if self.options.comparator.compare(
                                    parsed_key.user_key.as_slice(),
                                    key.user_key().as_slice(),
                                ) == CmpOrdering::Equal
                                {
                                    match parsed_key.value_type {
                                        ValueType::Value => return Ok((Some(value), seek_stats)),
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

    /// Binary search given files to find earliest index of index whose largest key >= ikey.
    /// If not found, returns the length of files.
    pub fn find_file(
        icmp: Arc<InternalKeyComparator>,
        files: &[Arc<FileMetaData>],
        ikey: &Slice,
    ) -> usize {
        let mut left = 0_usize;
        let mut right = files.len();
        while left < right {
            let mid = (left + right) / 2;
            let f = &files[mid];
            if icmp.compare(f.largest.data(), ikey.as_slice()) == CmpOrdering::Less {
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
        smallest_ukey: &Slice,
        largest_ukey: &Slice,
    ) -> usize {
        let mut level = 0;
        if !self.overlap_in_level(level, smallest_ukey, largest_ukey) {
            // No overlapping in level 0
            // we might directly push files to next level if there is no overlap in next level
            let smallest_ikey = Rc::new(InternalKey::new(
                smallest_ukey,
                u64::max_value(),
                VALUE_TYPE_FOR_SEEK,
            ));
            let largest_ikey = Rc::new(InternalKey::new(largest_ukey, 0, ValueType::Deletion));
            while level < self.options.max_mem_compact_level {
                if self.overlap_in_level(level + 1, smallest_ukey, largest_ukey) {
                    break;
                }
                if level + 2 < self.options.max_levels as usize {
                    // Check that file does not overlap too many grandparent bytes
                    let overlaps = self.get_overlapping_inputs(
                        level + 2,
                        Some(smallest_ikey.clone()),
                        Some(largest_ikey.clone()),
                    );
                    if VersionSet::total_file_size(&overlaps)
                        > self.options.max_grandparent_overlap_bytes()
                    {
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
                    let level_bytes = VersionSet::total_file_size(self.files[level].as_ref());
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
    pub fn comparator(&self) -> Arc<InternalKeyComparator> {
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
        user_key: Slice,
        internal_key: Slice,
        mut func: Box<dyn FnMut(usize, Arc<FileMetaData>) -> bool>,
    ) {
        let ucmp = self.icmp.user_comparator.clone();
        for (level, files) in self.files.iter().enumerate() {
            if level == 0 {
                let mut target_files = vec![];
                // Search level 0 files
                for f in files.iter() {
                    if ucmp.compare(user_key.as_slice(), f.smallest.user_key()) != CmpOrdering::Less
                        && ucmp.compare(user_key.as_slice(), f.largest.user_key())
                            != CmpOrdering::Greater
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
                    if ucmp.compare(user_key.as_slice(), target.smallest.data())
                        != CmpOrdering::Less
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
    pub fn record_read_sample(&self, internal_key: Slice) -> bool {
        if let Some(pkey) = ParsedInternalKey::decode_from(internal_key.clone()) {
            let stats = Rc::new(RefCell::new(SeekStats::new()));
            let matches = Rc::new(RefCell::new(0));
            let stats_clone = stats.clone();
            let matches_clone = matches.clone();
            self.for_each_overlapping(
                pkey.user_key.clone(),
                internal_key,
                Box::new(move |level, file| {
                    *matches_clone.borrow_mut() += 1;
                    if *matches_clone.borrow() == 1 {
                        // Remember first match
                        stats_clone.borrow_mut().seek_file = Some(file.clone());
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

    // Returns true iff some file in the specified level overlaps
    // some part of `[smallest_ukey,largest_ukey]`.
    // `smallest_ukey` is empty represents a key smaller than all the DB's keys.
    // `largest_ukey` is empty represents a key largest than all the DB's keys.
    fn overlap_in_level(&self, level: usize, smallest_ukey: &Slice, largest_ukey: &Slice) -> bool {
        if level == 0 {
            // need to check against all files in level 0
            for file in self.files[0].iter() {
                if self.key_is_after_file(file.clone(), smallest_ukey)
                    || self.key_is_before_file(file.clone(), largest_ukey)
                {
                    continue;
                } else {
                    return true;
                }
            }
            return false;
        }
        // binary search in level > 0
        let index = {
            if !smallest_ukey.is_empty() {
                let smallest_ikey =
                    InternalKey::new(smallest_ukey, u64::max_value(), VALUE_TYPE_FOR_SEEK);
                Self::find_file(
                    self.icmp.clone(),
                    &self.files[level],
                    &Slice::from(smallest_ikey.data()),
                )
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

    fn key_is_after_file(&self, file: Arc<FileMetaData>, ukey: &Slice) -> bool {
        !ukey.is_empty()
            && self
                .icmp
                .user_comparator
                .compare(ukey.as_slice(), file.largest.user_key())
                == CmpOrdering::Greater
    }

    fn key_is_before_file(&self, file: Arc<FileMetaData>, ukey: &Slice) -> bool {
        !ukey.is_empty()
            && self
                .icmp
                .user_comparator
                .compare(ukey.as_slice(), file.smallest.user_key())
                == CmpOrdering::Less
    }

    // Return all files in `level` that overlap [begin, end]
    // Notice that both `begin` and `end` is InternalKey but we
    // compare the user key directly.
    // Since files in level0 probably overlaps with each other, the final output
    // total range could be larger than [begin, end]
    // A None begin is considered as -infinite
    // A None end is considered as +infinite
    fn get_overlapping_inputs(
        &self,
        level: usize,
        begin: Option<Rc<InternalKey>>,
        end: Option<Rc<InternalKey>>,
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
    icmp: Arc<InternalKeyComparator>,
    index: usize,
    value_buf: Vec<u8>,
}

impl LevelFileNumIterator {
    pub fn new(icmp: Arc<InternalKeyComparator>, files: Vec<Arc<FileMetaData>>) -> Self {
        let index = files.len();
        Self {
            files,
            icmp,
            index,
            value_buf: Vec::with_capacity(FILE_META_LENGTH),
        }
    }

    fn valid_or_panic(&self) {
        assert!(self.valid(), "[level file num iterator] out of bounds")
    }
}

impl Iterator for LevelFileNumIterator {
    fn valid(&self) -> bool {
        self.index < self.files.len()
    }

    fn seek_to_first(&mut self) {
        self.index = 0;
    }

    fn seek_to_last(&mut self) {
        if self.files.is_empty() {
            self.index = 0;
        } else {
            self.index = self.files.len() - 1;
        }
    }

    fn seek(&mut self, target: &Slice) {
        let index = Version::find_file(self.icmp.clone(), self.files.as_slice(), target);
        self.index = index;
        let file = &self.files[index];
        // fill the buf
        put_fixed_64(&mut self.value_buf, file.number);
        put_fixed_64(&mut self.value_buf, file.file_size);
    }

    fn next(&mut self) {
        self.valid_or_panic();
        self.index += 1;
    }

    fn prev(&mut self) {
        self.valid_or_panic();
        if self.index == 0 {
            // marks as invalid
            self.index = self.files.len();
        } else {
            self.index -= 1;
        }
    }

    // make sure the underlying data's lifetime is longer than returning Slice
    fn key(&self) -> Slice {
        self.valid_or_panic();
        Slice::from(self.files[self.index].largest.data())
    }

    // make sure the iterator's lifetime is longer than returning Slice
    fn value(&self) -> Slice {
        self.valid_or_panic();
        Slice::from(&self.value_buf[..])
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

    struct FileMetaDatas {
        pub files: Vec<Arc<FileMetaData>>,
    }

    //find_file需要files，这个files是&[Arc],因此需要的是

    impl FileMetaDatas {
        fn new() -> Self {
            let files: Vec<Arc<FileMetaData>> = Vec::new();
            Self { files }
        }

        fn generate(&mut self, smallest: &Slice, largest: &Slice) {
            let mut file = FileMetaData::default();
            file.number = (self.files.len() + 1) as u64;
            file.smallest = Rc::new(InternalKey::new(smallest, 100, ValueType::Value));
            file.largest = Rc::new(InternalKey::new(largest, 100, ValueType::Value));
            self.files.push(Arc::new(file));
        }

        fn find(&self, key: &Slice) -> usize {
            let target = Slice::from(InternalKey::new(key, 100, ValueType::Value).data());
            let bcmp = Arc::new(InternalKeyComparator::new(Arc::new(
                BytewiseComparator::new(),
            )));

            Version::find_file(bcmp, &self.files, &target)
        }
    }

    #[test]
    fn test_find_file() {
        let mut file_metas = FileMetaDatas::new();
        assert_eq!(0, file_metas.find(&Slice::from("Foo")));

        file_metas.generate(&Slice::from("p"), &Slice::from("q"));
        assert_eq!(0, file_metas.find(&Slice::from("a")));
        assert_eq!(0, file_metas.find(&Slice::from("p")));
        assert_eq!(0, file_metas.find(&Slice::from("q")));
        assert_eq!(1, file_metas.find(&Slice::from("q1")));
        assert_eq!(1, file_metas.find(&Slice::from("z")));
    }

    #[test]
    fn test_find_files2() {
        let mut file_metas = FileMetaDatas::new();
        file_metas.generate(&Slice::from("150"), &Slice::from("200"));
        file_metas.generate(&Slice::from("200"), &Slice::from("250"));
        file_metas.generate(&Slice::from("300"), &Slice::from("350"));
        file_metas.generate(&Slice::from("400"), &Slice::from("450"));
        assert_eq!(0, file_metas.find(&Slice::from("100")));
        assert_eq!(0, file_metas.find(&Slice::from("150")));
        assert_eq!(1, file_metas.find(&Slice::from("201")));
        assert_eq!(2, file_metas.find(&Slice::from("251")));
        assert_eq!(2, file_metas.find(&Slice::from("301")));
        assert_eq!(2, file_metas.find(&Slice::from("350")));
        assert_eq!(3, file_metas.find(&Slice::from("351")));
        assert_eq!(4, file_metas.find(&Slice::from("451")));
    }
}
