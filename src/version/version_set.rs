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

use crate::compaction::{
    base_range, total_range, Compaction, CompactionInputs, CompactionReason, CompactionStats,
};
use crate::db::build_table;
use crate::db::filename::{generate_filename, parse_filename, update_current, FileType};
use crate::db::format::{InternalKey, InternalKeyComparator};
use crate::iterator::Iterator;
use crate::iterator::{ConcatenateIterator, DerivedIterFactory, KMergeCore, KMergeIter};
use crate::options::Options;
use crate::record::reader::Reader;
use crate::record::writer::Writer;
use crate::snapshot::{Snapshot, SnapshotList};
use crate::sstable::table::{TableBuilder, TableIterator};
use crate::storage::{File, Storage};
use crate::table_cache::TableCache;
use crate::util::coding::decode_fixed_64;
use crate::util::collection::HashSet;
use crate::util::comparator::Comparator;
use crate::util::reporter::LogReporter;
use crate::version::version_edit::{FileDelta, FileMetaData, VersionEdit};
use crate::version::{LevelFileNumIterator, Version, FILE_META_LENGTH};
use crate::ReadOptions;
use crate::{Error, Result};
use std::cmp::Ordering as CmpOrdering;
use std::ops::Add;
use std::path::MAIN_SEPARATOR;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;

struct LevelDiff {
    // set of new deleted files
    deleted_files: HashSet<u64>,
    // all new added files
    added_files: Vec<FileMetaData>,
}

/// Summarizes the files added and deleted from a set of version edits.
pub struct VersionBuilder<'a, C: Comparator> {
    // file changes for every level
    levels: Vec<LevelDiff>,
    base: &'a Version<C>,
}

impl<'a, C: Comparator + 'static> VersionBuilder<'a, C> {
    pub fn new(max_levels: usize, base: &'a Version<C>) -> Self {
        // let max_levels = base.options.max_levels as usize;
        let mut levels = Vec::with_capacity(max_levels);
        for _ in 0..max_levels {
            levels.push(LevelDiff {
                deleted_files: HashSet::default(),
                added_files: vec![],
            })
        }
        Self { levels, base }
    }

    /// Add the given `FileDelta` for later applying
    /// 'vset.compaction_pointers' will be updated
    /// same as `apply` in C++ implementation
    pub fn accumulate<S: Storage + Clone>(
        &mut self,
        delta: FileDelta,
        vset: &mut VersionSet<S, C>,
    ) {
        // update compcation pointers
        for (level, key) in delta.compaction_pointers {
            vset.compaction_pointer[level] = key;
        }
        // delete files
        for (level, deleted_file) in delta.deleted_files {
            self.levels[level].deleted_files.insert(deleted_file);
        }
        for (level, new_file) in delta.new_files {
            // We arrange to automatically compact this file after
            // a certain number of seeks.  Let's assume:
            //   (1) One seek costs 10ms
            //   (2) Writing or reading 1MB costs 10ms (100MB/s)
            //   (3) A compaction of 1MB does 25MB of IO:
            //         1MB read from this level
            //         10-12MB read from next level (boundaries may be misaligned)
            //         10-12MB written to next level
            // This implies that 25 seeks cost the same as the compaction
            // of 1MB of data.  I.e., one seek costs approximately the
            // same as the compaction of 40KB of data.  We are a little
            // conservative and allow approximately one seek for every 16KB
            // of data before triggering a compaction.
            new_file.init_allowed_seeks();
            self.levels[level].deleted_files.remove(&new_file.number);
            self.levels[level].added_files.push(new_file);
        }
    }

    // Apply all the changes on the base Version and produce a new Version based on it
    // same as `SaveTo` in C++ implementation
    fn apply_to_new(self, icmp: &InternalKeyComparator<C>) -> Version<C> {
        let mut v = Version::new(self.base.options.clone(), icmp.clone());
        v.vnum = self.base.vnum + 1;
        for (level, (base_files, delta)) in self
            .base
            .files
            .clone()
            .into_iter()
            .zip(self.levels)
            .enumerate()
        {
            for f in base_files {
                if !delta.deleted_files.contains(&f.number) {
                    v.files[level].push(f)
                }
            }
            for f in delta.added_files {
                if !delta.deleted_files.contains(&f.number) {
                    v.files[level].push(Arc::new(f));
                }
            }
            // TODO: base.files[level] is already sorted. Instead of appending
            // added_files[level] to the end and sorting afterwards, it might be more
            // efficient to sort added_files[level] and then merge the two sorted slices.
            if level == 0 {
                // sort by file number
                v.files[level].sort_by(|a, b| {
                    if a.largest != b.largest {
                        return icmp.compare(a.largest.data(), b.largest.data());
                    }
                    if a.smallest != b.smallest {
                        return icmp.compare(a.smallest.data(), b.smallest.data());
                    }
                    a.number.cmp(&b.number)
                });
            } else {
                // sort by smallest key
                v.files[level].sort_by(|a, b| icmp.compare(a.smallest.data(), b.smallest.data()));
                // make sure there is no overlap in levels > 0
                assert!(!Self::has_overlapping(icmp, &v.files[level]));
            }
        }
        v
    }

    // Returns true if the given collection of files has overlapping with each other.
    // Only used for files in level > 0
    fn has_overlapping(icmp: &InternalKeyComparator<C>, files: &[Arc<FileMetaData>]) -> bool {
        for fs in files.windows(2) {
            if icmp.compare(fs[0].largest.data(), fs[1].smallest.data()) != CmpOrdering::Less {
                return true;
            }
        }
        false
    }
}

/// The collection of all the Versions produced
pub struct VersionSet<S: Storage + Clone, C: Comparator> {
    // Snapshots that clients might be acquiring
    pub snapshots: SnapshotList,
    // Set of table files to protect them from deletion because they are part of ongoing compaction
    pub pending_outputs: HashSet<u64>,
    // WAL writer
    pub record_writer: Option<Writer<S::F>>,

    db_path: String,
    storage: S,
    options: Arc<Options<C>>,
    icmp: InternalKeyComparator<C>,

    // the next available file number
    next_file_number: u64,
    last_sequence: u64,
    // file number of .log file
    log_number: u64,
    // set 0 when compact memtable
    prev_log_number: u64,

    // the current manifest file number
    manifest_file_number: u64,
    manifest_writer: Option<Writer<S::F>>,

    versions: Vec<Arc<Version<C>>>,

    // Indicates that every level's compaction progress of last compaction.
    compaction_pointer: Vec<InternalKey>,
}

unsafe impl<S: Storage + Clone, C: Comparator> Send for VersionSet<S, C> {}

impl<S: Storage + Clone + 'static, C: Comparator + 'static> VersionSet<S, C> {
    pub fn new(db_path: String, options: Arc<Options<C>>, storage: S) -> Self {
        let max_level = options.max_levels as usize;
        let mut compaction_pointer = Vec::with_capacity(max_level);
        for _ in 0..max_level {
            compaction_pointer.push(InternalKey::default());
        }
        let icmp = InternalKeyComparator::new(options.comparator.clone());
        // Create an empty version as the first
        let first_v = Arc::new(Version::new(options.clone(), icmp.clone()));
        let versions = vec![first_v];
        Self {
            snapshots: SnapshotList::default(),
            pending_outputs: HashSet::default(),
            db_path,
            storage,
            record_writer: None,
            options,
            icmp,
            next_file_number: 0,
            last_sequence: 0,
            log_number: 0,
            prev_log_number: 0,
            manifest_file_number: 0,
            manifest_writer: None,
            versions,
            compaction_pointer,
        }
    }
    /// Returns the number of files in a certain level using latest version
    #[inline]
    pub fn level_files_count(&self, level: usize) -> usize {
        assert!(level < self.options.max_levels as usize);
        let level_files = &self.versions.last().unwrap().files;
        level_files.get(level).map_or(0, |files| files.len())
    }

    /// Returns `prev_log_number`
    #[inline]
    pub fn prev_log_number(&self) -> u64 {
        self.prev_log_number
    }

    /// Returns current file number of .log file
    #[inline]
    pub fn log_number(&self) -> u64 {
        self.log_number
    }

    /// Set current file number of .log file
    #[inline]
    pub fn set_log_number(&mut self, log_num: u64) {
        self.log_number = log_num;
    }

    /// Returns the next file number
    #[inline]
    pub fn get_next_file_number(&self) -> u64 {
        self.next_file_number
    }

    /// Mutate `next_file_number` by the input `new`
    #[inline]
    pub fn set_next_file_number(&mut self, new: u64) {
        self.next_file_number = new;
    }

    /// Increase current next file number by 1 and return previous number
    #[inline]
    pub fn inc_next_file_number(&mut self) -> u64 {
        let n = self.next_file_number;
        self.next_file_number += 1;
        n
    }

    /// Returns the current manifest number
    #[inline]
    pub fn manifest_number(&self) -> u64 {
        self.manifest_file_number
    }

    /// Returns the last sequence of the version set
    #[inline]
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    /// Mutate `last_sequence` by given input `new`
    #[inline]
    pub fn set_last_sequence(&mut self, new: u64) {
        self.last_sequence = new
    }

    /// Get the current newest version.
    #[inline]
    pub fn current(&self) -> Arc<Version<C>> {
        self.versions.last().unwrap().clone()
    }

    /// Create new snapshot with `last_sequence`
    #[inline]
    pub fn new_snapshot(&mut self) -> Arc<Snapshot> {
        self.snapshots.acquire(self.last_sequence)
    }

    /// Returns the collection of all the file iterators in current version
    pub fn current_sst_iter(
        &self,
        read_opt: ReadOptions,
        table_cache: TableCache<S, C>,
    ) -> Result<KMergeIter<SSTableIters<S, C>>> {
        let version = self.current();
        let mut level0 = vec![];
        // Merge all level zero files together since they may overlap
        for file in version.files[0].iter() {
            level0.push(table_cache.new_iter(
                self.icmp.clone(),
                read_opt,
                file.number,
                file.file_size,
            )?);
        }

        let mut leveln = vec![];
        // For levels > 0, we can use a concatenating iterator that sequentially
        // walks through the non-overlapping files in the level, opening them
        // lazily
        for files in version.files.iter().skip(1) {
            if !files.is_empty() {
                let level_file_iter = LevelFileNumIterator::new(self.icmp.clone(), files.clone());
                let factory =
                    FileIterFactory::new(self.icmp.clone(), read_opt, table_cache.clone());
                leveln.push(ConcatenateIterator::new(level_file_iter, factory));
            }
        }
        let iter = KMergeIter::new(SSTableIters {
            cmp: self.icmp.clone(),
            level0,
            leveln,
        });
        Ok(iter)
    }

    /// Apply `edit` to the current version to form a new descriptor that
    /// is both saved to persistent state and installed as the new
    /// current version.
    ///
    /// Only called in situations below:
    ///     * After minor compaction
    ///     * After trivial compaction (only file move)
    ///     * After major compaction
    pub fn log_and_apply(&mut self, mut edit: VersionEdit) -> Result<()> {
        let (v, encoded_edit) = {
            let level_summary_before = self.current().level_summary();
            if let Some(target_log) = edit.log_number {
                assert!(target_log >= self.log_number && target_log < self.next_file_number,
                    "[version set] applying VersionEdit use a invalid log number {}, expect to be at [{}, {})", target_log, self.log_number, self.next_file_number);
            } else {
                edit.set_log_number(self.log_number);
            }

            if edit.prev_log_number.is_none() {
                edit.set_prev_log_number(self.prev_log_number);
            }

            edit.set_next_file(self.next_file_number);
            edit.set_last_sequence(self.last_sequence);

            let mut record = vec![];
            edit.encode_to(&mut record);

            let this = self.current();
            let mut builder = VersionBuilder::new(self.options.max_levels as usize, &this);
            builder.accumulate(edit.file_delta, self);
            let mut v = builder.apply_to_new(&self.icmp);
            v.finalize();
            let summary = v.level_summary();
            info!(
                "level changing result summary : \n\t before {} \n\t now {}",
                level_summary_before, summary
            );
            (v, record)
        };

        // Initialize new manifest file if necessary by creating a temporary file that contains a snapshot of the current version.
        let mut new_manifest_file = String::new();
        if self.manifest_writer.is_none() {
            new_manifest_file =
                generate_filename(&self.db_path, FileType::Manifest, self.manifest_file_number);
            let f = self.storage.create(&new_manifest_file)?;
            debug!("Create new manifest file #{}", self.manifest_file_number);
            let mut writer = Writer::new(f);
            match self.write_snapshot(&mut writer) {
                Ok(()) => self.manifest_writer = Some(writer),
                Err(_) => {
                    return self.storage.remove(&new_manifest_file);
                }
            }
        }

        // Write to current MANIFEST
        // In origin C++ implementation, the relative part unlocks the global mutex. But we dont need
        // to do this in wickdb since we split the mutex into several ones for more subtle controlling.
        if let Some(writer) = self.manifest_writer.as_mut() {
            match writer.add_record(&encoded_edit) {
                Ok(()) => {
                    match writer.sync() {
                        Ok(()) => {
                            // If we just created a MANIFEST file, install it by writing a
                            // new CURRENT file that points to it.
                            if !new_manifest_file.is_empty()
                                && update_current(
                                    &self.storage,
                                    &self.db_path,
                                    self.manifest_file_number,
                                )
                                .is_err()
                            {
                                self.manifest_writer = None;
                                return self.storage.remove(new_manifest_file.as_str());
                            }
                            // install new version
                            self.log_number = edit.log_number.unwrap();
                            self.prev_log_number = edit.prev_log_number.unwrap();
                            self.append_new_version(v);
                        }
                        // omit the sync error
                        Err(e) => {
                            warn!("MANIFEST persistent error: {:?}", e);
                            self.manifest_writer = None;
                            return self.storage.remove(new_manifest_file.as_str());
                        }
                    }
                }
                Err(_) => {
                    self.manifest_writer = None;
                    return self.storage.remove(new_manifest_file.as_str());
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn append_new_version(&mut self, v: Version<C>) {
        self.versions.push(Arc::new(v));
        self.gc();
    }

    /// Return a `Compaction` for compacting the range `[begin,end]` in
    /// the specified level.  Returns `None` if there is nothing in that
    /// level that overlaps the specified range
    pub fn compact_range(
        &mut self,
        level: usize,
        begin: Option<&InternalKey>,
        end: Option<&InternalKey>,
    ) -> Option<Compaction<S::F, C>> {
        let version = self.current();
        let mut overlapping_inputs = version.get_overlapping_inputs(level, begin, end);
        if overlapping_inputs.is_empty() {
            return None;
        }
        // Avoid compacting too much in one shot in case the range is large.
        // But we cannot do this for level-0 since level-0 files can overlap
        // and we must not pick one file and drop another older file if the
        // two files overlap.
        // TODO: The Level 0 files to be compacted could really large. This might hurt the performance.
        if level > 0 {
            let mut total = 0;
            for (i, file) in overlapping_inputs.iter().enumerate() {
                total += file.file_size;
                if total >= version.options.max_file_size {
                    overlapping_inputs.truncate(i + 1);
                    break;
                }
            }
        }
        let mut c = Compaction::new(self.options.clone(), level, CompactionReason::Manual);
        c.input_version = Some(version);
        c.inputs.base = overlapping_inputs;
        Some(self.setup_other_inputs(c))
    }

    /// Pick level and inputs for a new compaction.
    /// Returns `None` if no compaction needs to be done.
    /// Otherwise returns a `Compaction` that
    /// describes the compaction.
    pub fn pick_compaction(&mut self) -> Option<Compaction<S::F, C>> {
        let current = self.current();
        let size_compaction = current.compaction_score >= 1.0;
        let mut file_to_compact = Arc::new(FileMetaData::default());
        let mut seek_compaction = false;
        {
            let guard = current.file_to_compact.read().unwrap();
            if let Some(f) = &(*guard) {
                file_to_compact = f.clone();
                seek_compaction = true;
            }
        }
        // We prefer compactions triggered by too much data in a level over
        // the compactions triggered by seeks
        let mut compaction = {
            if size_compaction {
                let level = current.compaction_level;
                assert!(
                    level + 1 < self.options.max_levels as usize,
                    "[compaction] target compaction level {} should be less Lmax {} - 1",
                    level,
                    self.options.max_levels as usize
                );
                let mut compaction =
                    Compaction::new(self.options.clone(), level, CompactionReason::MaxSize);
                // Pick the first file that comes after compact_pointer[level]
                for file in current.files[level].iter() {
                    if self.compaction_pointer[level].is_empty()
                        || self
                            .icmp
                            .compare(file.largest.data(), self.compaction_pointer[level].data())
                            == CmpOrdering::Greater
                    {
                        compaction.inputs.add_base(file.clone());
                        break;
                    }
                }
                if compaction.inputs.base.is_empty() {
                    if let Some(file) = current.files[0].first() {
                        // Wrap-around to the beginning of the key space
                        compaction.inputs.add_base(file.clone())
                    }
                }
                compaction
            } else if seek_compaction {
                let level = current.file_to_compact_level.load(Ordering::Acquire);
                if level < self.options.max_levels as usize - 1 {
                    let mut compaction =
                        Compaction::new(self.options.clone(), level, CompactionReason::SeekLimit);
                    compaction.inputs.add_base(file_to_compact);
                    compaction
                } else {
                    // We've run out of the levels
                    return None;
                }
            } else {
                return None;
            }
        };
        compaction.input_version = Some(current.clone());
        // Files in level 0 may overlap each other, so pick up all overlapping ones
        if compaction.level == 0 {
            let (smallest, largest) =
                base_range(&compaction.inputs.base, compaction.level, &self.icmp);
            // Note that the next call will discard the file we placed in
            // inputs[0] earlier and replace it with an overlapping set
            // which will include the picked file.
            compaction.inputs.base =
                current.get_overlapping_inputs(compaction.level, Some(smallest), Some(largest));
            assert!(!compaction.inputs.base.is_empty());
        }

        compaction = self.setup_other_inputs(compaction);
        // Avoid recursively trivial sst moving when target level is empty
        if compaction.level > 1
            && seek_compaction
            && compaction.is_trivial_move()
            && current.files[compaction.level + 1].is_empty()
        {
            for f in compaction.inputs.base {
                f.init_allowed_seeks()
            }
            return None;
        }
        Some(compaction)
    }

    /// Persistent given memtable into a single sst file to level0.
    /// If `into_base` is true, the file could be pushed into level1 or level2 if there's no too much overlapping.
    pub fn write_level0_files(
        &mut self,
        db_path: &str,
        table_cache: &TableCache<S, C>,
        mem_iter: &mut dyn Iterator,
        edit: &mut VersionEdit,
        into_base: bool,
    ) -> Result<()> {
        let now = SystemTime::now();
        let mut meta = FileMetaData {
            number: self.inc_next_file_number(),
            ..Default::default()
        };
        info!("Level-0 table #{} : start building", meta.number);
        let build_result = build_table(
            self.options.clone(),
            &self.storage,
            db_path,
            table_cache,
            mem_iter,
            &mut meta,
        );
        let mut level = 0;

        // If `file_size` is zero, the file has been deleted and
        // should not be added to the manifest
        if build_result.is_ok() && meta.file_size > 0 {
            info!(
                "Level-0 table #{} : add {} bytes [{:?}] [key range {:?} ... {:?}]",
                meta.number, meta.file_size, &build_result, &meta.smallest, &meta.largest,
            );
            let smallest_ukey = meta.smallest.user_key();
            let largest_ukey = meta.largest.user_key();
            if into_base {
                let base = self.current();
                level = base.pick_level_for_memtable_output(smallest_ukey, largest_ukey);
                debug!(
                    "Pick up new level for table: level {}, table #{}",
                    level, meta.number
                );
            }
            edit.add_file(
                level,
                meta.number,
                meta.file_size,
                meta.smallest.clone(),
                meta.largest.clone(),
            );
        }
        info!(
            "Compactions stats for Level{}: {:?}",
            level,
            CompactionStats {
                micros: now.elapsed().unwrap().as_micros() as u64,
                bytes_read: 0,
                bytes_written: meta.file_size,
            }
        );
        build_result
    }

    /// Add all living files in all versions into the `pending_outputs` to
    /// prevent them to be deleted
    #[inline]
    pub fn lock_live_files(&mut self) {
        for version in self.versions.iter() {
            for files in version.files.iter() {
                for f in files.iter() {
                    self.pending_outputs.insert(f.number);
                }
            }
        }
    }

    /// Returns the collection of current live files from version metadata
    #[inline]
    pub(crate) fn live_files(&self) -> HashSet<u64> {
        let mut set = HashSet::default();
        for version in self.versions.iter() {
            for files in version.files.iter() {
                for f in files.iter() {
                    set.insert(f.number);
                }
            }
        }
        set
    }

    /// Create new table builder and physical file for current output in Compaction
    pub(crate) fn create_compaction_output_file(
        &mut self,
        c: &mut Compaction<S::F, C>,
    ) -> Result<()> {
        assert!(c.builder.is_none());
        let file_number = self.inc_next_file_number();
        self.pending_outputs.insert(file_number);
        let output = FileMetaData {
            number: file_number,
            ..Default::default()
        };
        let file_name = generate_filename(&self.db_path, FileType::Table, file_number);
        let file = self.storage.create(file_name.as_str())?;
        c.builder = Some(TableBuilder::new(file, self.icmp.clone(), &self.options));
        c.outputs.push(output);
        Ok(())
    }

    /// Recover the last saved Version from MANIFEST file.
    /// Returns whether we need a new MANIFEST file for later usage.
    pub fn recover(&mut self) -> Result<bool> {
        let env = self.storage.clone();
        // Read "CURRENT" file, which contains a pointer to the current manifest file
        let mut current = env.open(&generate_filename(&self.db_path, FileType::Current, 0))?;
        let mut buf = vec![];
        current.read_all(&mut buf)?;
        let (current_manifest, file_name) = match String::from_utf8(buf) {
            Ok(s) => {
                if s.is_empty() {
                    return Err(Error::Corruption("CURRENT file is empty".to_owned()));
                }
                let mut file_name = self.db_path.to_owned();
                file_name.push(MAIN_SEPARATOR);
                let file_name = file_name.add(&s);
                (env.open(&file_name)?, file_name)
            }
            Err(e) => {
                return Err(Error::Corruption(format!(
                    "Invalid CURRENT file content: {}",
                    e
                )));
            }
        };
        let file_length = current_manifest.len()?;
        let base = Version::new(self.options.clone(), self.icmp.clone());
        let mut builder = VersionBuilder::new(self.options.max_levels as usize, &base);
        let reporter = LogReporter::new();
        let mut reader = Reader::new(current_manifest, Some(Box::new(reporter.clone())), true, 0);
        let mut buf = vec![];

        let mut next_file_number = 0;
        let mut has_next_file_number = false;
        let mut log_number = 0;
        let mut has_log_number = false;
        let mut prev_log_number = 0;
        let mut has_prev_log_number = false;
        let mut last_sequence = 0;
        let mut has_last_sequence = false;
        while reader.read_record(&mut buf) {
            if let Err(e) = reporter.result() {
                return Err(e);
            }
            let mut edit = VersionEdit::new(self.options.max_levels);
            edit.decoded_from(&buf)?;
            debug!("Decoded manifest record: {:?}", &edit);
            if let Some(ref cmp_name) = edit.comparator_name {
                if cmp_name.as_str() != self.icmp.user_comparator.name() {
                    return Err(Error::InvalidArgument(
                        cmp_name.clone() + " does not match existing compactor",
                    ));
                }
            }
            builder.accumulate(edit.file_delta, self);
            if let Some(n) = edit.next_file_number {
                next_file_number = n;
                has_next_file_number = true;
            };
            if let Some(n) = edit.log_number {
                log_number = n;
                has_log_number = true;
            };
            if let Some(n) = edit.prev_log_number {
                prev_log_number = n;
                has_prev_log_number = true;
            };
            if let Some(n) = edit.last_sequence {
                last_sequence = n;
                has_last_sequence = true;
            }
        }

        if let Err(e) = reporter.result() {
            return Err(e);
        }

        if !has_next_file_number {
            return Err(Error::Corruption(
                "no meta-nextfile entry in manifest".to_owned(),
            ));
        }
        if !has_log_number {
            return Err(Error::Corruption(
                "no meta-lognumber entry in manifest".to_owned(),
            ));
        }
        if !has_last_sequence {
            return Err(Error::Corruption(
                "no last-sequence-number entry in manifest".to_owned(),
            ));
        }

        if !has_prev_log_number {
            prev_log_number = 0;
        }

        self.mark_file_number_used(prev_log_number);
        self.mark_file_number_used(log_number);

        let mut new_v = builder.apply_to_new(&self.icmp);
        new_v.finalize();
        self.versions.push(Arc::new(new_v));
        self.manifest_file_number = next_file_number;
        self.next_file_number = next_file_number + 1;
        self.last_sequence = last_sequence;
        self.log_number = log_number;
        self.prev_log_number = prev_log_number;
        Ok(!self.should_reuse_manifest(&file_name, file_length))
    }

    /// Forward to `num + 1` as the next file number
    pub fn mark_file_number_used(&mut self, num: u64) {
        if self.next_file_number <= num {
            self.next_file_number = num + 1
        }
    }

    /// Return the maximum overlapping data (in bytes) at next level for any
    /// file at a level >= 1.
    #[allow(dead_code)]
    pub(crate) fn max_next_level_overlapping_bytes(&self) -> u64 {
        let mut res = 0;
        let current = self.current();
        for level in 1..self.options.max_levels - 1 {
            for f in &current.files[level] {
                let overlaps =
                    current.get_overlapping_inputs(level + 1, Some(&f.smallest), Some(&f.largest));
                let sum = total_file_size(&overlaps);
                if sum > res {
                    res = sum
                }
            }
        }
        res
    }

    // Remove all the old versions
    // NOTE: This func always keeps the last element in `versions`
    fn gc(&mut self) {
        let mut i = 0;
        let last = self.versions.len() - 1;
        self.versions.retain(|v| {
            let keep = i == last || Arc::strong_count(v) > 1;
            i += 1;
            keep
        })
    }

    // Create snapshot of current version and persistent to manifest file.
    fn write_snapshot(&self, writer: &mut Writer<S::F>) -> Result<()> {
        let mut edit = VersionEdit::new(self.options.max_levels);
        // Save metadata
        edit.set_comparator_name(String::from(self.icmp.user_comparator.name()));
        // Save compaction pointers
        for level in 0..self.options.max_levels as usize {
            if !self.compaction_pointer[level].is_empty() {
                edit.file_delta
                    .compaction_pointers
                    .push((level, self.compaction_pointer[level].clone()));
            }
        }

        // Save files
        for level in 0..self.options.max_levels as usize {
            for file in self.current().files[level].iter() {
                edit.add_file(
                    level,
                    file.number,
                    file.file_size,
                    file.smallest.clone(),
                    file.largest.clone(),
                );
            }
        }

        let mut record = vec![];
        edit.encode_to(&mut record);
        writer.add_record(&record)?;
        Ok(())
    }

    // Pick up files to compact in `c.level+1` based on given compaction
    // The input files in `c.level` might expand because of getting a large key range from newly picked files
    // in `c.level + 1`. And the final key range in `c.level + 1` should be a subset of `c.level`
    fn setup_other_inputs(&mut self, c: Compaction<S::F, C>) -> Compaction<S::F, C> {
        let mut c = self.add_boundary_inputs(c);
        let current = &self.current();
        let inputs = std::mem::take(&mut c.inputs);
        let not_expand = inputs.base;
        // Calculate the key range in current level after `add_boundary_inputs`
        let (smallest, largest) = base_range(&not_expand, c.level, &self.icmp);
        // figure out the overlapping files in next level
        let overlapping_next_level =
            current.get_overlapping_inputs(c.level + 1, Some(smallest), Some(largest));
        // Re-calculate total key range of inputting files for compaction
        let (all_smallest, all_largest) =
            total_range(&not_expand, &overlapping_next_level, c.level, &self.icmp);

        // See whether we can grow the number of inputs in "level" without
        // changing the number of "level+1" files we pick up.
        let (current_files, next_files) = if !overlapping_next_level.is_empty() {
            // Re-group the current selected files.
            // We fill the compaction 'holes' left by `add_boundary_inputs` here
            let mut expanded0 =
                current.get_overlapping_inputs(c.level, Some(all_smallest), Some(all_largest));
            // Add boundary for expanded L(n) inputs
            // The `expanded0` could have a larger key range than the origin `inputs[0]` in given `c`
            add_boundary_inputs_for_compact_files(
                &self.icmp,
                &current.files[c.level],
                &mut expanded0,
            );
            let expanded0_size = total_file_size(&expanded0);
            let not_expanded_size = total_file_size(&not_expand);
            let next_size = total_file_size(&overlapping_next_level);
            // We do expand the current(`c.level`) inputs and not reach the compaction size limit
            if expanded0.len() > not_expand.len()
                && next_size + expanded0_size <= self.options.expanded_compaction_byte_size_limit()
            {
                let (new_smallest, new_largest) = base_range(&expanded0, c.level, &self.icmp);
                // TODO: use a more sufficient way to checking expanding in L(n+1) ?
                let expanded_next = current.get_overlapping_inputs(
                    c.level + 1,
                    Some(new_smallest),
                    Some(new_largest),
                );
                // the L(n+1) compacting files shouldn't be expanded
                if expanded_next.len() == overlapping_next_level.len() {
                    let expanded_next_size = total_file_size(&expanded_next);
                    info!(
                        "Expanding@{} {}+{} ({}+{} bytes) to {}+{} ({}+{} bytes)",
                        c.level,
                        not_expand.len(),
                        overlapping_next_level.len(),
                        not_expanded_size,
                        next_size,
                        expanded0.len(),
                        expanded_next.len(),
                        expanded0_size,
                        expanded_next_size,
                    );
                    (expanded0, expanded_next)
                } else {
                    // The next level files have been expanded again.
                    // Use previous un-expanded next level files.
                    (expanded0, overlapping_next_level)
                }
            } else {
                (expanded0, overlapping_next_level)
            }
        } else {
            // 'overlapping_next_level' is empty
            (not_expand, overlapping_next_level)
        };

        let (final_smallest, final_largest) =
            total_range(&current_files, &next_files, c.level, &self.icmp);
        // Compute the set of grandparent files that overlap this compaction
        // (parent == level+1; grandparent == level+2)
        if c.level + 2 < self.options.max_levels as usize {
            c.grand_parents = current.get_overlapping_inputs(
                c.level + 2,
                Some(final_smallest),
                Some(final_largest),
            );
        }
        // Update the place where we will do the next compaction for this level.
        // We update this immediately instead of waiting for the VersionEdit
        // to be applied so that if the compaction fails, we will try a different
        // key range next time
        c.edit
            .file_delta
            .compaction_pointers
            .push((c.level, final_largest.clone()));
        self.compaction_pointer[c.level] = final_largest.clone();
        let final_inputs = CompactionInputs {
            base: current_files,
            parent: next_files,
        };
        c.inputs = final_inputs;
        c
    }

    // A helper of 'add_boundary_input_for_compact_files' for files in `c.level`
    fn add_boundary_inputs(&self, mut c: Compaction<S::F, C>) -> Compaction<S::F, C> {
        let level_files = &self.current().files[c.level];
        add_boundary_inputs_for_compact_files(&self.icmp, level_files, &mut c.inputs.base);
        c
    }
    // See if we can reuse the existing MANIFEST file
    fn should_reuse_manifest(&mut self, manifest_file: &str, file_size: u64) -> bool {
        if !self.options.reuse_logs {
            return false;
        }
        if let Some((file_type, file_number)) = parse_filename(manifest_file) {
            if file_type != FileType::Manifest || file_size > self.options.max_file_size {
                // Make new compacted MANIFEST if old one is too big
                return false;
            };
            match self.storage.open(manifest_file) {
                Ok(f) => {
                    info!("Reusing MANIFEST {}", manifest_file);
                    let writer = Writer::new(f);
                    self.manifest_writer = Some(writer);
                    self.manifest_file_number = file_number;
                    true
                }
                Err(e) => {
                    error!("Reuse MANIFEST {:?}", e);
                    false
                }
            }
        } else {
            false
        }
    }
}

// Add SST files which should have been included in `level` compaction but excluded by some reasons (e.g output size limit truncating).
// This guarantees that all the `InternalKey`s with a same user key in level `level` should be compacted. Otherwise, we might encounter a
// snapshot reading issue because the older key remains in a lower level when the newest key is at higher level after compaction.
// `files_to_compact` could be expand after this methods
fn add_boundary_inputs_for_compact_files<C: Comparator>(
    icmp: &InternalKeyComparator<C>,
    level_files: &[Arc<FileMetaData>],
    files_to_compact: &mut Vec<Arc<FileMetaData>>,
) {
    if !files_to_compact.is_empty() {
        // find the largest key in files to compact by internal comparator
        // TODO: could pass an `Option<&InternalKey>` as the largest to avoid searching here
        let mut tmp = &files_to_compact[0];
        for f in files_to_compact.iter().skip(1) {
            if icmp.compare(f.largest.data(), tmp.largest.data()) == CmpOrdering::Greater {
                tmp = f;
            }
        }
        let mut largest_key = &tmp.largest;
        let mut smallest_boundary_file =
            find_smallest_boundary_file(icmp, level_files, largest_key);
        while let Some(file) = &smallest_boundary_file {
            // If a boundary file was found, advance the `largest_key`. Otherwise we're done.
            // This might leave 'holes' in files to be compacted because we only append the last boundary file.
            // The 'holes' will be filled later (by calling `get_overlapping_inputs`).
            files_to_compact.push(file.clone());
            largest_key = &file.largest;
            smallest_boundary_file = find_smallest_boundary_file(icmp, level_files, largest_key);
        }
    }
}

// Iterate all the files in level until find the file whose smallest key has same user key
// and greater sequence number by `InternalComparator` ( actually smaller in digits )
fn find_smallest_boundary_file<C: Comparator>(
    icmp: &InternalKeyComparator<C>,
    level_files: &[Arc<FileMetaData>],
    largest_key: &InternalKey,
) -> Option<Arc<FileMetaData>> {
    let ucmp = &icmp.user_comparator;
    let mut smallest_boundary_file: Option<&Arc<FileMetaData>> = None;
    for f in level_files {
        // f.smallest.ikey > largest.ikey && f.smallest.ukey == largest.ukey
        if icmp.compare(f.smallest.data(), largest_key.data()) == CmpOrdering::Greater
            && ucmp.compare(f.smallest.user_key(), largest_key.user_key()) == CmpOrdering::Equal
        {
            match &smallest_boundary_file {
                None => smallest_boundary_file = Some(f),
                Some(current) => {
                    if icmp.compare(f.smallest.data(), current.smallest.data()) == CmpOrdering::Less
                    {
                        smallest_boundary_file = Some(f);
                    }
                }
            }
        }
    }
    smallest_boundary_file.cloned()
}

pub struct FileIterFactory<S: Storage + Clone, C: Comparator> {
    options: ReadOptions,
    table_cache: TableCache<S, C>,
    icmp: InternalKeyComparator<C>,
}

impl<S: Storage + Clone, C: Comparator> FileIterFactory<S, C> {
    pub fn new(
        icmp: InternalKeyComparator<C>,
        options: ReadOptions,
        table_cache: TableCache<S, C>,
    ) -> Self {
        Self {
            options,
            table_cache,
            icmp,
        }
    }
}

impl<S: Storage + Clone, C: Comparator + 'static> DerivedIterFactory for FileIterFactory<S, C> {
    type Iter = TableIterator<InternalKeyComparator<C>, S::F>;

    // The value is a bytes with fixed encoded file number and fixed encoded file size
    fn derive(&self, value: &[u8]) -> Result<Self::Iter> {
        if value.len() != FILE_META_LENGTH {
            Err(Error::Corruption(
                "file reader invoked with unexpected value".to_owned(),
            ))
        } else {
            let file_number = decode_fixed_64(value);
            let file_size = decode_fixed_64(&value[std::mem::size_of::<u64>()..]);
            self.table_cache
                .new_iter(self.icmp.clone(), self.options, file_number, file_size)
        }
    }
}

/// Calculate the total size of given files
#[inline]
pub fn total_file_size(files: &[Arc<FileMetaData>]) -> u64 {
    files.iter().fold(0, |accum, file| accum + file.file_size)
}

/// An iterator that yields all the entries stored in SST files.
/// The inner implementation is mostly like a merging iterator.
pub struct SSTableIters<S: Storage + Clone, C: Comparator + 'static> {
    cmp: InternalKeyComparator<C>,
    // Level0 table iterators. One iterator for one sst file
    level0: Vec<TableIterator<InternalKeyComparator<C>, S::F>>,
    // ConcatenateIterators for opening SST in level n>1 lazily. One iterator for one level
    leveln: Vec<ConcatenateIterator<LevelFileNumIterator<C>, FileIterFactory<S, C>>>,
}

impl<S: Storage + Clone, C: Comparator> SSTableIters<S, C> {
    pub fn new(
        cmp: InternalKeyComparator<C>,
        level0: Vec<TableIterator<InternalKeyComparator<C>, S::F>>,
        leveln: Vec<ConcatenateIterator<LevelFileNumIterator<C>, FileIterFactory<S, C>>>,
    ) -> Self {
        Self {
            cmp,
            level0,
            leveln,
        }
    }
}

impl<S: Storage + Clone, C: Comparator> KMergeCore for SSTableIters<S, C> {
    type Cmp = InternalKeyComparator<C>;
    fn cmp(&self) -> &Self::Cmp {
        &self.cmp
    }

    fn iters_len(&self) -> usize {
        self.level0.len() + self.leveln.len()
    }

    // Find the iterator with the smallest 'key' and set it as current
    fn find_smallest(&mut self) -> usize {
        let mut smallest: Option<&[u8]> = None;
        let mut index = self.iters_len();
        for (i, child) in self.level0.iter().enumerate() {
            if self.smaller(&mut smallest, child) {
                index = i
            }
        }

        for (i, child) in self.leveln.iter().enumerate() {
            if self.smaller(&mut smallest, child) {
                index = i + self.level0.len()
            }
        }
        index
    }

    // Find the iterator with the largest 'key' and set it as current
    fn find_largest(&mut self) -> usize {
        let mut largest: Option<&[u8]> = None;
        let mut index = self.iters_len();
        for (i, child) in self.level0.iter().enumerate() {
            if self.larger(&mut largest, child) {
                index = i
            }
        }

        for (i, child) in self.leveln.iter().enumerate() {
            if self.larger(&mut largest, child) {
                index = i + self.level0.len()
            }
        }
        index
    }

    fn get_child(&self, i: usize) -> &dyn Iterator {
        if i < self.level0.len() {
            self.level0.get(i).unwrap() as &dyn Iterator
        } else {
            let current = i - self.level0.len();
            self.leveln.get(current).unwrap() as &dyn Iterator
        }
    }

    fn get_child_mut(&mut self, i: usize) -> &mut dyn Iterator {
        if i < self.level0.len() {
            self.level0.get_mut(i).unwrap() as &mut dyn Iterator
        } else {
            let current = i - self.level0.len();
            self.leveln.get_mut(current).unwrap() as &mut dyn Iterator
        }
    }

    fn for_each_child<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut dyn Iterator),
    {
        self.level0
            .iter_mut()
            .for_each(|i| f(i as &mut dyn Iterator));
        self.leveln
            .iter_mut()
            .for_each(|i| f(i as &mut dyn Iterator));
    }

    fn for_not_ith<F>(&mut self, n: usize, mut f: F)
    where
        F: FnMut(&mut dyn Iterator, &Self::Cmp),
    {
        if n < self.level0.len() {
            for (i, child) in self.level0.iter_mut().enumerate() {
                if i != n {
                    f(child as &mut dyn Iterator, &self.cmp)
                }
            }
        } else {
            let current = n - self.level0.len();
            for (i, child) in self.leveln.iter_mut().enumerate() {
                if i != current {
                    f(child as &mut dyn Iterator, &self.cmp)
                }
            }
        }
    }

    fn take_err(&mut self) -> Result<()> {
        for child in self.level0.iter_mut() {
            let status = child.status();
            if status.is_err() {
                return status;
            }
        }
        for child in self.leveln.iter_mut() {
            let status = child.status();
            if status.is_err() {
                return status;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod add_boundary_tests {
    use super::*;
    use crate::db::format::{InternalKey, InternalKeyComparator, ValueType};
    use crate::storage::mem::MemStorage;
    use crate::util::comparator::BytewiseComparator;

    #[derive(Default)]
    struct AddBoundaryInputTests {
        icmp: InternalKeyComparator<BytewiseComparator>,
        level_files: Vec<Arc<FileMetaData>>,
        all: Vec<Arc<FileMetaData>>,
    }

    impl AddBoundaryInputTests {
        fn new_file(
            &mut self,
            number: u64,
            smallest: InternalKey,
            largest: InternalKey,
        ) -> Arc<FileMetaData> {
            let mut f = FileMetaData::default();
            f.number = number;
            f.smallest = smallest;
            f.largest = largest;
            let f = Arc::new(f);
            self.all.push(f.clone());
            f
        }
    }

    #[test]
    fn test_empty_file_sets() {
        let t = AddBoundaryInputTests::default();
        let mut files_to_compact = vec![];
        add_boundary_inputs_for_compact_files(&t.icmp, &t.level_files, &mut files_to_compact);
        assert!(t.level_files.is_empty());
        assert!(files_to_compact.is_empty());
    }

    #[test]
    fn test_empty_level_files() {
        let mut t = AddBoundaryInputTests::default();
        let f = t.new_file(
            1,
            InternalKey::new(b"100", 2, ValueType::Value),
            InternalKey::new(b"100", 1, ValueType::Value),
        );
        let mut files_to_compact = vec![f.clone()];
        add_boundary_inputs_for_compact_files(&t.icmp, &t.level_files, &mut files_to_compact);
        assert_eq!(1, files_to_compact.len());
        assert_eq!(f, files_to_compact[0]);
    }

    #[test]
    fn test_empty_compaction_files() {
        let mut t = AddBoundaryInputTests::default();
        let f = t.new_file(
            1,
            InternalKey::new(b"100", 2, ValueType::Value),
            InternalKey::new(b"100", 1, ValueType::Value),
        );
        t.level_files.push(f);
        let mut files_to_compact = vec![];
        add_boundary_inputs_for_compact_files(&t.icmp, &t.level_files, &mut files_to_compact);
        assert!(files_to_compact.is_empty());
    }

    // ensure the `files_to_compaction` will not be expanded if all the files in the key range are included
    #[test]
    fn test_no_boundary_files() {
        let mut t = AddBoundaryInputTests::default();
        let f1 = t.new_file(
            1,
            InternalKey::new(b"100", 2, ValueType::Value),
            InternalKey::new(b"100", 1, ValueType::Value),
        );
        let f2 = t.new_file(
            1,
            InternalKey::new(b"200", 2, ValueType::Value),
            InternalKey::new(b"200", 1, ValueType::Value),
        );
        let f3 = t.new_file(
            1,
            InternalKey::new(b"300", 2, ValueType::Value),
            InternalKey::new(b"300", 1, ValueType::Value),
        );
        t.level_files.push(f1.clone());
        t.level_files.push(f2.clone());
        t.level_files.push(f3.clone());
        let mut files_to_compact = vec![f2.clone(), f3.clone()];
        add_boundary_inputs_for_compact_files(&t.icmp, &t.level_files, &mut files_to_compact);
        assert_eq!(files_to_compact, vec![f2, f3]);
    }

    #[test]
    fn test_one_boundary_file() {
        let mut t = AddBoundaryInputTests::default();
        let f1 = t.new_file(
            1,
            InternalKey::new(b"100", 3, ValueType::Value),
            InternalKey::new(b"100", 2, ValueType::Value),
        );
        let f2 = t.new_file(
            1,
            InternalKey::new(b"100", 1, ValueType::Value),
            InternalKey::new(b"200", 3, ValueType::Value),
        );
        let f3 = t.new_file(
            1,
            InternalKey::new(b"300", 2, ValueType::Value),
            InternalKey::new(b"300", 1, ValueType::Value),
        );
        t.level_files.push(f3.clone());
        t.level_files.push(f2.clone());
        t.level_files.push(f1.clone());
        let mut files_to_compact = vec![f1.clone()];
        add_boundary_inputs_for_compact_files(&t.icmp, &t.level_files, &mut files_to_compact);
        assert_eq!(files_to_compact, vec![f1, f2]);
    }

    #[test]
    fn test_two_boundary_files() {
        let mut t = AddBoundaryInputTests::default();
        let f1 = t.new_file(
            1,
            InternalKey::new(b"100", 6, ValueType::Value),
            InternalKey::new(b"100", 5, ValueType::Value),
        );
        let f2 = t.new_file(
            1,
            InternalKey::new(b"100", 2, ValueType::Value),
            InternalKey::new(b"100", 1, ValueType::Value),
        );
        let f3 = t.new_file(
            1,
            InternalKey::new(b"100", 4, ValueType::Value),
            InternalKey::new(b"100", 3, ValueType::Value),
        );
        t.level_files.push(f2.clone());
        t.level_files.push(f3.clone());
        t.level_files.push(f1.clone());
        let mut files_to_compact = vec![f1.clone()];
        add_boundary_inputs_for_compact_files(&t.icmp, &t.level_files, &mut files_to_compact);
        assert_eq!(files_to_compact, vec![f1, f3, f2]);
    }

    #[test]
    fn test_disjoint_files() {
        let mut t = AddBoundaryInputTests::default();
        let f1 = t.new_file(
            1,
            InternalKey::new(b"100", 6, ValueType::Value),
            InternalKey::new(b"100", 5, ValueType::Value),
        );
        let f2 = t.new_file(
            1,
            InternalKey::new(b"100", 6, ValueType::Value),
            InternalKey::new(b"100", 5, ValueType::Value),
        );
        let f3 = t.new_file(
            1,
            InternalKey::new(b"100", 2, ValueType::Value),
            InternalKey::new(b"300", 1, ValueType::Value),
        );
        let f4 = t.new_file(
            1,
            InternalKey::new(b"100", 4, ValueType::Value),
            InternalKey::new(b"100", 3, ValueType::Value),
        );
        t.level_files.push(f2.clone());
        t.level_files.push(f3.clone());
        t.level_files.push(f4.clone());

        let mut files_to_compact = vec![f1.clone()];
        add_boundary_inputs_for_compact_files(&t.icmp, &t.level_files, &mut files_to_compact);
        assert_eq!(files_to_compact, vec![f1, f4, f3]);
    }

    fn new_test_file_meta_data(number: u64) -> FileMetaData {
        FileMetaData {
            allowed_seeks: std::sync::atomic::AtomicUsize::new(0),
            file_size: 0,
            number,
            smallest: InternalKey::new(number.to_string().as_bytes(), 1, ValueType::Value),
            largest: InternalKey::new(number.to_string().as_bytes(), 2, ValueType::Value),
        }
    }

    fn new_test_version(files: Vec<Vec<u64>>) -> Version<BytewiseComparator> {
        let file_metadata = files
            .into_iter()
            .map(|f| {
                f.into_iter()
                    .map(|n| Arc::new(new_test_file_meta_data(n)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let opts = Arc::new(Options::<BytewiseComparator>::default());
        let icmp = InternalKeyComparator::new(BytewiseComparator::default());
        let mut v = Version::new(opts.clone(), icmp);
        v.files = file_metadata;
        v
    }

    fn new_test_file_diff(delete: Vec<Vec<u64>>, add: Vec<Vec<u64>>) -> FileDelta {
        let mut deleted_files = crate::util::collection::HashSet::default();
        for (level, files) in delete.into_iter().enumerate() {
            for f in files {
                deleted_files.insert((level, f));
            }
        }
        let mut added_files = vec![];
        for (level, files) in add.into_iter().enumerate() {
            for f in files {
                added_files.push((level, new_test_file_meta_data(f)));
            }
        }
        FileDelta {
            compaction_pointers: vec![],
            deleted_files,
            new_files: added_files,
        }
    }

    impl<C: Comparator> Version<C> {
        fn assert_files(&self, mut expect: Vec<Vec<u64>>) {
            for files in expect.iter_mut() {
                files.sort();
            }
            assert_eq!(self.all_files_num(), expect)
        }

        fn all_files_num(&self) -> Vec<Vec<u64>> {
            let mut files = self
                .files
                .iter()
                .map(|files| files.into_iter().map(|f| f.number).collect::<Vec<_>>())
                .collect::<Vec<_>>();
            for f in files.iter_mut() {
                f.sort();
            }
            files
        }
    }

    #[test]
    fn test_version_builder_accumulate_and_apply() {
        let opts = Arc::new(Options::<BytewiseComparator>::default());
        let mut mock_vset = VersionSet::new("test".to_owned(), opts.clone(), MemStorage::default());
        for (base, diffs, expect) in vec![
            (
                vec![],
                vec![(vec![], vec![])],
                vec![vec![], vec![], vec![], vec![], vec![], vec![], vec![]],
            ),
            (
                vec![vec![1]],
                vec![(vec![vec![1]], vec![vec![2]]), (vec![], vec![vec![3, 4]])],
                vec![
                    vec![2, 3, 4],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                ],
            ),
            (
                vec![vec![], vec![3], vec![], vec![]],
                vec![
                    (
                        vec![vec![1], vec![5], vec![], vec![]],
                        // add 2@0 4,5@1 6,7,8@3
                        vec![vec![2], vec![4, 5], vec![], vec![6, 7, 8]],
                    ),
                    (
                        // delete 5@1
                        vec![vec![], vec![5]],
                        vec![],
                    ),
                ],
                vec![
                    vec![2],
                    vec![3, 4],
                    vec![],
                    vec![6, 7, 8],
                    vec![],
                    vec![],
                    vec![],
                ],
            ),
        ] {
            let v = new_test_version(base);
            let mut vb = VersionBuilder::new(opts.max_levels, &v);
            for (delete, add) in diffs {
                let d = new_test_file_diff(delete, add);
                vb.accumulate(d, &mut mock_vset);
            }
            let new_v = vb.apply_to_new(&v.icmp);
            new_v.assert_files(expect);
        }
    }
}
