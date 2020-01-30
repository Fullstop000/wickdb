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

use crate::compaction::{base_range, total_range, Compaction, CompactionStats, ManualCompaction};
use crate::db::build_table;
use crate::db::filename::{generate_filename, parse_filename, update_current, FileType};
use crate::db::format::{InternalKey, InternalKeyComparator};
use crate::iterator::Iterator;
use crate::iterator::{ConcatenateIterator, DerivedIterFactory};
use crate::options::Options;
use crate::record::reader::Reader;
use crate::record::writer::Writer;
use crate::snapshot::{Snapshot, SnapshotList};
use crate::sstable::table::{TableBuilder, TableIterator};
use crate::storage::{File, Storage};
use crate::table_cache::TableCache;
use crate::util::coding::decode_fixed_64;
use crate::util::collection::HashSet;
use crate::util::comparator::{BytewiseComparator, Comparator};
use crate::util::reporter::LogReporter;
use crate::version::version_edit::{FileDelta, FileMetaData, VersionEdit};
use crate::version::{LevelFileNumIterator, Version, FILE_META_LENGTH};
use crate::ReadOptions;
use crate::{Error, Result};
use std::cmp::Ordering as CmpOrdering;
use std::collections::vec_deque::VecDeque;
use std::ops::Add;
use std::path::MAIN_SEPARATOR;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;

struct LevelState {
    // set of new deleted files
    deleted_files: HashSet<u64>,
    // all new added files
    added_files: Vec<FileMetaData>,
}

/// Summarizes the files added and deleted from a set of version edits.
pub struct VersionBuilder {
    // file changes for every level
    levels: Vec<LevelState>,
    base: Version,
}

impl VersionBuilder {
    pub fn new(base: Version) -> Self {
        let max_levels = base.options.max_levels as usize;
        let mut levels = Vec::with_capacity(max_levels);
        for _ in 0..max_levels {
            levels.push(LevelState {
                deleted_files: HashSet::default(),
                added_files: vec![],
            })
        }
        Self { levels, base }
    }

    /// Add the given VersionEdit for later applying
    /// 'vset.compaction_pointers' will be updated
    /// same as `apply` in C++ implementation
    pub fn accumulate<S: Storage + Clone>(&mut self, delta: FileDelta, vset: &mut VersionSet<S>) {
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
            // TODO: config 16 * 1024 as an option
            let mut allowed_seeks = new_file.file_size as usize / (16 * 1024);
            if allowed_seeks < 100 {
                allowed_seeks = 100 // the min seeks allowed
            }
            new_file
                .allowed_seeks
                .store(allowed_seeks, Ordering::Release);
            self.levels[level].deleted_files.remove(&new_file.number);
            self.levels[level].added_files.push(new_file);
        }
    }

    /// Apply all the changes on the base Version and produce a new Version based on it
    /// same as `save_to` in C++ implementation
    pub fn apply_to_new(&mut self) -> Version {
        // TODO: config this to the option
        let icmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));
        let mut v = Version::new(self.base.options.clone(), icmp.clone());
        for (level, (mut base_files, delta)) in self
            .base
            .files
            .drain(..)
            .zip(self.levels.drain(..))
            .enumerate()
        {
            for file in base_files.drain(..) {
                // filter the deleted files
                if !delta.deleted_files.contains(&file.number) {
                    v.files[level].push(file)
                }
            }
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
                })
            } else {
                // sort by smallest key
                v.files[level].sort_by(|a, b| icmp.compare(a.smallest.data(), b.smallest.data()))
            }
        }
        v
    }
}

/// The collection of all the Versions produced
pub struct VersionSet<S: Storage + Clone> {
    // Snapshots that clients might be acquiring
    pub snapshots: SnapshotList,
    // The compaction stats for every level
    pub compaction_stats: Vec<CompactionStats>,
    // Set of table files to protect from deletion because they are part of ongoing compaction
    pub pending_outputs: HashSet<u64>,
    // Represent a manual compaction, temporarily just for test
    pub manual_compaction: Option<ManualCompaction>,
    // WAL writer
    pub record_writer: Option<Writer<S::F>>,

    // db path
    db_name: &'static str,
    storage: S,
    options: Arc<Options>,
    icmp: InternalKeyComparator,

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

    versions: VecDeque<Arc<Version>>,

    // Indicates that every level's compaction progress of last compaction.
    compaction_pointer: Vec<InternalKey>,
}

unsafe impl<S: Storage + Clone> Send for VersionSet<S> {}

impl<S: Storage + Clone + 'static> VersionSet<S> {
    pub fn new(db_name: &'static str, options: Arc<Options>, storage: S) -> Self {
        let mut compaction_stats = vec![];
        for _ in 0..options.max_levels {
            compaction_stats.push(CompactionStats::new());
        }
        Self {
            snapshots: SnapshotList::new(),
            compaction_stats,
            pending_outputs: HashSet::default(),
            manual_compaction: None,
            db_name,
            storage,
            record_writer: None,
            options: options.clone(),
            icmp: InternalKeyComparator::new(options.comparator.clone()),
            next_file_number: 0,
            last_sequence: 0,
            log_number: 0,
            prev_log_number: 0,
            manifest_file_number: 0,
            manifest_writer: None,
            versions: VecDeque::new(),
            compaction_pointer: vec![],
        }
    }
    /// Returns the number of files in a certain level
    #[inline]
    pub fn level_files_count(&self, level: usize) -> usize {
        assert!(level < self.options.max_levels as usize);
        self.versions.front().unwrap().files[level].len()
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

    /// Whether the current version needs to be compacted
    #[inline]
    pub fn needs_compaction(&self) -> bool {
        if self.manual_compaction.is_some() {
            true
        } else {
            let current = self.current();
            current.compaction_score > 1.0 || current.file_to_compact.read().unwrap().is_some()
        }
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

    /// Get the current newest version
    #[inline]
    pub fn current(&self) -> Arc<Version> {
        self.versions.front().unwrap().clone()
    }

    /// Create new snapshot with `last_sequence`
    #[inline]
    pub fn new_snapshot(&mut self) -> Arc<Snapshot> {
        self.snapshots.snapshot(self.last_sequence)
    }

    /// Returns the collection of all the file iterators in current version
    pub fn current_iters(
        &self,
        read_opt: ReadOptions,
        table_cache: TableCache<S>,
    ) -> Result<Vec<Box<dyn Iterator>>> {
        let version = self.current();
        let mut res: Vec<Box<dyn Iterator>> = vec![];
        // Merge all level zero files together since they may overlap
        for file in version.files[0].iter() {
            res.push(Box::new(table_cache.new_iter(
                self.icmp.clone(),
                read_opt,
                file.number,
                file.file_size,
            )?));
        }

        // For levels > 0, we can use a concatenating iterator that sequentially
        // walks through the non-overlapping files in the level, opening them
        // lazily
        for files in version.files.iter().skip(1) {
            if !files.is_empty() {
                let level_file_iter = LevelFileNumIterator::new(
                    InternalKeyComparator::new(self.options.comparator.clone()),
                    files.clone(),
                );
                let factory =
                    FileIterFactory::new(self.icmp.clone(), read_opt, table_cache.clone());
                let iter = ConcatenateIterator::new(level_file_iter, factory);
                res.push(Box::new(iter));
            }
        }
        Ok(res)
    }

    /// Apply `edit` to the current version to form a new descriptor that
    /// is both saved to persistent state and installed as the new
    /// current version.
    ///
    /// Only called in situations below:
    ///     * After minor compaction
    ///     * After trivial compaction (only file move)
    ///     * After major compaction
    pub fn log_and_apply(&mut self, edit: &mut VersionEdit) -> Result<()> {
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

        let mut v = Version::new(self.options.clone(), self.icmp.clone());
        let mut builder = VersionBuilder::new(v);
        let file_delta = edit.take_file_delta();
        builder.accumulate(file_delta, self);
        v = builder.apply_to_new();
        v.finalize();

        // cleanup all the old versions
        self.gc();

        // Initialize new manifest file if necessary by creating a temporary file that contains a snapshot of the current version.
        let mut new_manifest_file = String::new();
        if self.manifest_writer.is_none() {
            new_manifest_file =
                generate_filename(self.db_name, FileType::Manifest, self.manifest_file_number);
            //            edit.set_next_file(self.next_file_number);
            let f = self.storage.create(new_manifest_file.as_str())?;
            let mut writer = Writer::new(f);
            match self.write_snapshot(&mut writer) {
                Ok(()) => self.manifest_writer = Some(writer),
                Err(_) => {
                    return self.storage.remove(new_manifest_file.as_str());
                }
            }
        }

        // Write to current MANIFEST
        // In origin C++ implementation, the relative part unlocks the global mutex. But we dont need
        // to do this in wickdb since we split the mutex into several ones for more subtle controlling.
        if let Some(writer) = self.manifest_writer.as_mut() {
            match writer.add_record(&record) {
                Ok(()) => {
                    match writer.sync() {
                        Ok(()) => {
                            // If we just created a MANIFEST file, install it by writing a
                            // new CURRENT file that points to it.
                            if !new_manifest_file.is_empty() {
                                match update_current(
                                    &self.storage,
                                    self.db_name,
                                    self.manifest_file_number,
                                ) {
                                    Ok(()) => {}
                                    Err(_) => {
                                        self.manifest_writer = None;
                                        return self.storage.remove(new_manifest_file.as_str());
                                    }
                                }
                            }
                            // install new version
                            self.versions.push_front(Arc::new(v));
                            self.log_number = edit.log_number.unwrap();
                            self.prev_log_number = edit.prev_log_number.unwrap();
                        }
                        // omit the sync error
                        Err(e) => {
                            info!("MANIFEST write: {:?}", e);
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

    /// Return a compaction object for compacting the range `[begin,end]` in
    /// the specified level.  Returns `None` if there is nothing in that
    /// level that overlaps the specified range
    pub fn compact_range(
        &mut self,
        level: usize,
        begin: Option<&InternalKey>,
        end: Option<&InternalKey>,
    ) -> Option<Compaction<S::F>> {
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
        let mut c = Compaction::new(self.options.clone(), level);
        c.input_version = Some(version);
        c.inputs[0] = overlapping_inputs;
        Some(self.setup_other_inputs(c))
    }

    /// Pick level and inputs for a new compaction.
    /// Returns `None` if there is no compaction to be done.
    /// Otherwise returns compaction object that
    /// describes the compaction.
    pub fn pick_compaction(&mut self) -> Option<Compaction<S::F>> {
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
                let mut compaction = Compaction::new(self.options.clone(), level);
                // Pick the first file that comes after compact_pointer[level]
                for file in current.files[level].iter() {
                    if self.compaction_pointer[level].is_empty()
                        || self
                            .icmp
                            .compare(file.largest.data(), self.compaction_pointer[level].data())
                            == CmpOrdering::Greater
                    {
                        compaction.inputs[0].push(file.clone());
                        break;
                    }
                }
                if compaction.inputs[0].is_empty() {
                    if let Some(file) = current.files[0].first() {
                        // Wrap-around to the beginning of the key space
                        compaction.inputs[0].push(file.clone())
                    }
                }
                compaction
            } else if seek_compaction {
                let level = current.file_to_compact_level.load(Ordering::Acquire);
                let mut compaction = Compaction::new(self.options.clone(), level);
                compaction.inputs[0].push(file_to_compact);
                compaction
            } else {
                return None;
            }
        };
        compaction.input_version = Some(current.clone());
        // Files in level 0 may overlap each other, so pick up all overlapping ones
        if compaction.level == 0 {
            let (smallest, largest) =
                base_range(&compaction.inputs[0], compaction.level, &self.icmp);
            // Note that the next call will discard the file we placed in
            // inputs[0] earlier and replace it with an overlapping set
            // which will include the picked file.
            compaction.inputs[0] =
                current.get_overlapping_inputs(compaction.level, Some(smallest), Some(largest));
            assert!(!compaction.inputs[0].is_empty());
        }

        Some(self.setup_other_inputs(compaction))
    }

    /// Persistent given memtable into a single level0 file.
    pub fn write_level0_files(
        &mut self,
        db_name: &str,
        table_cache: TableCache<S>,
        mem_iter: &mut dyn Iterator,
        edit: &mut VersionEdit,
    ) -> Result<()> {
        let base = self.current();
        let now = SystemTime::now();
        let mut meta = FileMetaData::default();
        meta.number = self.inc_next_file_number();
        info!("Level-0 table #{} : started", meta.number);
        let build_result = build_table(
            self.options.clone(),
            &self.storage,
            db_name,
            &table_cache,
            mem_iter,
            &mut meta,
        );
        info!(
            "Level-0 table #{} : {} bytes [{:?}]",
            meta.number, meta.file_size, &build_result
        );
        let mut level = 0;

        // If `file_size` is zero, the file has been deleted and
        // should not be added to the manifest
        if build_result.is_ok() && meta.file_size > 0 {
            let smallest_ukey = meta.smallest.user_key();
            let largest_ukey = meta.largest.user_key();
            level = base.pick_level_for_memtable_output(smallest_ukey, largest_ukey);
            edit.add_file(
                level,
                meta.number,
                meta.file_size,
                meta.smallest.clone(),
                meta.largest.clone(),
            );
        }
        self.compaction_stats[level].accumulate(
            now.elapsed().unwrap().as_micros() as u64,
            0,
            meta.file_size,
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

    /// Create new table builder and physical file for current output in Compaction
    pub fn open_compaction_output_file(&mut self, compact: &mut Compaction<S::F>) -> Result<()> {
        assert!(compact.builder.is_none());
        let file_number = self.inc_next_file_number();
        self.pending_outputs.insert(file_number);
        let mut output = FileMetaData::default();
        output.number = file_number;
        let file_name = generate_filename(self.db_name, FileType::Table, file_number);
        let file = self.storage.create(file_name.as_str())?;
        compact.builder = Some(TableBuilder::new(
            file,
            self.icmp.clone(),
            self.options.clone(),
        ));
        Ok(())
    }

    /// Recover the last saved Version from MANIFEST file.
    /// Returns whether we need a new MANIFEST file for later usage.
    pub fn recover(&mut self) -> Result<bool> {
        let env = self.storage.clone();
        // Read "CURRENT" file, which contains a pointer to the current manifest file
        let mut current = env.open(&generate_filename(self.db_name, FileType::Current, 0))?;
        let mut buf = vec![];
        current.read_all(&mut buf)?;
        let (current_manifest, file_name) = match String::from_utf8(buf) {
            Ok(s) => {
                if s.is_empty() {
                    return Err(Error::Corruption("CURRENT file is empty".to_owned()));
                }
                let mut file_name = self.db_name.to_owned();
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
        let file_length = current_manifest.len();
        let mut builder =
            VersionBuilder::new(Version::new(self.options.clone(), self.icmp.clone()));
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
            if let Some(ref cmp_name) = edit.comparator_name {
                if cmp_name.as_str() != self.icmp.user_comparator.name() {
                    return Err(Error::InvalidArgument(
                        cmp_name.clone() + " does not match existing compactor",
                    ));
                }
            }
            let file_delta = edit.take_file_delta();
            builder.accumulate(file_delta, self);
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

        let mut new_v = builder.apply_to_new();
        new_v.finalize();
        self.versions.push_front(Arc::new(new_v));
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

    // Remove all the old versions
    fn gc(&mut self) {
        self.versions.retain(|v| Arc::strong_count(v) > 1)
    }

    // Create snapshot of current version and persistent to manifest file.
    // Only be called when initializing a new db
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
    fn setup_other_inputs(&mut self, c: Compaction<S::F>) -> Compaction<S::F> {
        let mut c = self.add_boundary_inputs(c);
        let current = &self.current();
        // TODO: remove this clone
        let not_expand = std::mem::replace(&mut c.inputs, [vec![], vec![]])[0].clone();
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
            self.add_boundary_inputs_for_compact_files(c.level, &mut expanded0);
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
        let final_inputs = [current_files, next_files];
        c.inputs = final_inputs;
        c
    }

    // A helper of 'add_boundary_input_for_compact_files' for files in `c.level`
    fn add_boundary_inputs(&self, mut c: Compaction<S::F>) -> Compaction<S::F> {
        self.add_boundary_inputs_for_compact_files(c.level, &mut c.inputs[0]);
        c
    }

    // Add SST files which should have been included in `level` compaction but excluded by some reasons (e.g output size limit truncating).
    // This guarantees that all the `InternalKey`s with a same user key in level `level` should be compacted. Otherwise, we might encounter a
    // snapshot reading issue because the older key remains in a lower level when the newest key is at higher level
    // after compaction.
    fn add_boundary_inputs_for_compact_files(
        &self,
        level: usize,
        files_to_compact: &mut Vec<Arc<FileMetaData>>,
    ) {
        if !files_to_compact.is_empty() {
            // find the largest key in files to compact by internal comparator
            // TODO: could pass an `Option<&InternalKey>` as the largest to avoid searching here
            let mut tmp = files_to_compact[0].clone();
            for f in files_to_compact.iter().skip(1) {
                if self.icmp.compare(f.largest.data(), tmp.largest.data()) == CmpOrdering::Greater {
                    tmp = f.clone();
                }
            }
            let mut largest_key = &tmp.largest;
            let mut smallest_boundary_file = self.find_smallest_boundary_file(level, &largest_key);
            while let Some(file) = &smallest_boundary_file {
                // If a boundary file was found, advance the `largest_key`. Otherwise we're done.
                // This might leave 'holes' in files to be compacted because we only append the last boundary file.
                // The 'holes' will be filled later (by calling `get_overlapping_inputs`).
                files_to_compact.push(file.clone());
                largest_key = &file.largest;
                smallest_boundary_file = self.find_smallest_boundary_file(level, &largest_key);
            }
        }
    }

    // Iterate all the files in level until find the file whose smallest key has same user key
    // and greater sequence number by `InternalComparator` ( actually smaller in digits )
    fn find_smallest_boundary_file(
        &self,
        level: usize,
        largest_key: &InternalKey,
    ) -> Option<Arc<FileMetaData>> {
        let ucmp = &self.icmp.user_comparator;
        let current = self.current();
        let level_files = &current.files[level];
        let mut smallest_boundary_file: Option<Arc<FileMetaData>> = None;
        for f in level_files.iter() {
            if self.icmp.compare(f.smallest.data(), largest_key.data()) == CmpOrdering::Greater
                && ucmp.compare(f.smallest.user_key(), largest_key.user_key()) == CmpOrdering::Equal
            {
                match &smallest_boundary_file {
                    None => smallest_boundary_file = Some(f.clone()),
                    Some(f) => {
                        if self.icmp.compare(f.smallest.data(), f.smallest.data())
                            == CmpOrdering::Less
                        {
                            smallest_boundary_file = Some(f.clone());
                        }
                    }
                }
            }
        }
        smallest_boundary_file
    }

    // See if we can reuse the existing MANIFEST file
    fn should_reuse_manifest(&mut self, manifest_file: &str, file_size: Result<u64>) -> bool {
        if !self.options.reuse_logs {
            return false;
        }
        if let Some((file_type, file_number)) = parse_filename(manifest_file) {
            if file_type != FileType::Manifest {
                return false;
            };
            match file_size {
                Ok(len) => {
                    // Make new compacted MANIFEST if old one is too big
                    if len > self.options.max_file_size {
                        return false;
                    }
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
                }
                Err(_) => false,
            }
        } else {
            false
        }
    }
}

pub struct FileIterFactory<S: Storage + Clone> {
    options: ReadOptions,
    table_cache: TableCache<S>,
    icmp: InternalKeyComparator,
}

impl<S: Storage + Clone> FileIterFactory<S> {
    pub fn new(
        icmp: InternalKeyComparator,
        options: ReadOptions,
        table_cache: TableCache<S>,
    ) -> Self {
        Self {
            options,
            table_cache,
            icmp,
        }
    }
}

impl<S: Storage + Clone> DerivedIterFactory for FileIterFactory<S> {
    type Iter = TableIterator<InternalKeyComparator, S::F>;

    fn derive(&self, value: &[u8]) -> Result<Self::Iter> {
        if value.len() != 2 * FILE_META_LENGTH {
            Err(Error::Corruption(
                "file reader invoked with unexpected value".to_owned(),
            ))
        } else {
            let file_number = decode_fixed_64(value);
            let file_size = decode_fixed_64(&value[8..]);
            self.table_cache.new_iter(
                self.icmp.clone(),
                self.options.clone(),
                file_number,
                file_size,
            )
        }
    }
}

/// Calculate the total size of given files
#[inline]
pub fn total_file_size(files: &[Arc<FileMetaData>]) -> u64 {
    files.iter().fold(0, |accum, file| accum + file.file_size)
}
