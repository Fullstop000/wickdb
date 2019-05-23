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

pub mod filename;
pub mod format;

use crate::batch::WriteBatch;
use crate::options::{ReadOptions, WriteOptions, Options};
use crate::util::slice::Slice;
use crate::util::status::{Result, WickErr, Status};
use crate::snapshot::{Snapshot, SnapshotList};
use std::cell::RefCell;
use std::path::MAIN_SEPARATOR;
use crate::storage::Storage;
use std::rc::Rc;
use crate::db::format::{InternalKeyComparator, InternalFilterPolicy, LookupKey, InternalKey, ParsedInternalKey, ValueType};
use crate::table_cache::TableCache;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use crate::mem::{MemTable, MemoryTable};
use crate::version::version_set::VersionSet;
use hashbrown::HashSet;
use crate::record::writer::Writer;
use std::thread;
use std::time::{Duration, SystemTime};
use std::mem;
use std::cmp::Ordering as CmpOrdering;
use crossbeam_channel::{Receiver, Sender};
use crate::db::filename::{generate_filename, FileType, parse_filename};
use std::collections::vec_deque::VecDeque;
use std::sync::atomic::AtomicUsize;
use crate::compaction::{ManualCompaction, CompactionInputsRelation, CompactionStats, Compaction};
use crate::iterator::Iterator;
use crate::version::version_edit::{FileMetaData, VersionEdit};
use crate::sstable::table::TableBuilder;
use crate::util::collection::NodePtr;
use crate::version::Version;

/// A `DB` is a persistent ordered map from keys to values.
/// A `DB` is safe for concurrent access from multiple threads without
/// any external synchronization.
pub trait DB {
    /// `put` sets the value for the given key. It overwrites any previous value
    /// for that key; a DB is not a multi-map.
    fn put(
        &mut self,
        write_opt: Option<WriteOptions>,
        key: Slice,
        value: Slice,
    ) -> Result<()>;

    /// `get` gets the value for the given key. It returns `Status::NotFound` if the DB
    /// does not contain the key.
    fn get(&self, read_opt: Option<ReadOptions>, key: Slice) -> Result<Slice>;

    /// `delete` deletes the value for the given key. It returns `Status::NotFound` if
    /// the DB does not contain the key.
    fn delete(&mut self, write_opt: Option<WriteOptions>, key: Slice) -> Result<()>;

    /// `apply` applies the operations contained in the `WriteBatch` to the DB atomically.
    fn apply(&mut self, write_opt: Option<WriteOptions>, batch: WriteBatch) -> Result<()>;

    /// `close` closes the DB.
    fn close(&mut self) -> Result<()>;

    fn get_snapshot(&mut self) -> Snapshot;

}

// TODO: make this thread safe
pub struct DBImpl {
    env: Arc<dyn Storage>,
    internal_comparator: Arc<InternalKeyComparator>,
//    internal_filter_policy: InternalFilterPolicy,
    options: Arc<Options>,
    // actually is the dirname of db
    db_name: String,

    record_writer: Writer,

    batch_scheduler: BatchScheduler,
    // the table cache
    table_cache: Rc<RefCell<TableCache>>,

    versions: VersionSet,

    // signal of compaction finished
    background_work_finished_signal: Receiver<()>,
    // whether we have a compaction running
    background_compaction_scheduled: AtomicBool,
    // iff should schedule a manual compaction, temporarily just for test
    manual_compaction: Option<ManualCompaction>,
    // a controlled mutex
    mutex: Mutex<()>,
    // file number of current .log file
    log_file_num: u64,
    mem: MemTable,
    im_mem: Option<MemTable>,// iff the memtable is compacted
    snapshots: SnapshotList,
    // Set of table files to protect from deletion because they are part of ongoing compaction
    pending_outputs: HashSet<u64>,
    // Have we encountered a background error in paranoid mode
    bg_error: RwLock<Option<WickErr>>,
    // Whether the db is closing
    is_shutting_down: AtomicBool,
    // The compaction stats for every level
    compaction_stats: Vec<CompactionStats>,

}

struct BatchScheduler {
    batch_queue: VecDeque<BatchTask>,
    size: AtomicUsize,
}

impl BatchScheduler {
    fn schedule_and_wait(&mut self, options: WriteOptions, batch: WriteBatch) -> Result<()> {
        let (send, recv) = crossbeam_channel::bounded(0);
        let task = BatchTask::new(batch, send, options);
        self.batch_queue.push_back(task);
        match recv.recv() {
            Ok(m) => m,
            Err(e) => Err(WickErr::new_from_raw(Status::Unexpected, None, Box::new(e))),
        }
    }
}

struct BatchTask {
    batch: WriteBatch,
    signal: Sender<Result<()>>,
    options: WriteOptions,
}

impl BatchTask {
    fn new(batch: WriteBatch, signal: Sender<Result<()>>, options: WriteOptions) -> Self {
        Self {
            batch,
            signal,
            options,
        }
    }
}

impl DBImpl {
    pub fn get(&self, options: ReadOptions, key: Slice) -> Result<Option<Slice>> {
        let snapshot = match &options.snapshot {
            Some(snapshot) => snapshot.sequence(),
            None => self.versions.get_last_sequence(),
        };
        let lookup_key = LookupKey::new(key.as_slice(), snapshot);
        // search the memtable
        if let Some(result) = self.mem.get(&lookup_key) {
            match result {
                Ok(value) => return Ok(Some(value)),
                // mem.get only returns Err() when get an Deletion of the key
                Err(_) => return Ok(None),
            }
        }
        // search the immutable memtable
        if let Some(im_mem) = &self.im_mem {
            if let Some(result) = im_mem.get(&lookup_key) {
                match result {
                    Ok(value) => return Ok(Some(value)),
                    Err(_) => return Ok(None),
                }
            }
        }
        let current = self.versions.current();
        let (value, seek_stats) = current.borrow().data.get(options, lookup_key, self.table_cache.clone())?;
        if current.borrow_mut().data.update_stats(seek_stats) {
            self.maybe_schedule_compaction()
        }
        Ok(value)
    }

    pub fn write(&mut self, options: WriteOptions,  batch: WriteBatch) -> Result<()> {
        self.make_room_for_write(batch.is_empty())?;
        self.batch_scheduler.schedule_and_wait(options, batch)
    }

    // Delete any unneeded files and stale in-memory entries.
    #[allow(unused_must_use)]
    fn delete_obsolete_files(&mut self) {
        if self.bg_error.read().is_err() {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect
            return
        }
        VersionSet::add_live_files(self.versions.current(), &mut self.pending_outputs);
        // ignore IO error on purpose
        if let Ok(files) = self.env.list(self.db_name.as_str()) {
            for file in files.iter() {
                if let Some((file_type, number)) = parse_filename(file) {
                    let mut keep = true;
                    match file_type {
                        FileType::Log => keep = number >= self.versions.get_log_number() || number == self.versions.get_prev_log_number(),
                        FileType::Manifest => keep = number >= self.versions.get_manifest_number(),
                        FileType::Table => keep = self.pending_outputs.contains(&number),
                        // Any temp files that are currently being written to must
                        // be recorded in pending_outputs
                        FileType::Temp => keep = self.pending_outputs.contains(&number),
                        _ => {},
                    }
                    if !keep {
                        if file_type == FileType::Table {
                            self.table_cache.borrow_mut().evict(number)
                        }
                        info!("Delete type={:?} #{}", file_type, number);
                        self.env.remove(format!("{}{}{:?}", self.db_name.as_str(), MAIN_SEPARATOR, file).as_str());
                    }
                }
            }
        }
    }

    fn make_room_for_write(&mut self, mut force: bool) -> Result<()> {
        let mutex = self.mutex.lock().unwrap();
        let mut allow_delay = !force;
        loop {
            if let Some(e) = {
                self.bg_error.get_mut().unwrap().take()
            } {
                return Err(e)
            } else if allow_delay && self.versions.level_files_count(0) >= self.options.l0_slowdown_writes_threshold {
                // We are getting close to hitting a hard limit on the number of
                // L0 files.  Rather than delaying a single write by several
                // seconds when we hit the hard limit, start delaying each
                // individual write by 1ms to reduce latency variance.  Also,
                // this delay hands over some CPU to the compaction thread in
                // case it is sharing the same core as the writer.
                thread::sleep(Duration::from_micros(1000));
                allow_delay = false; // do not delay a single write more than once
            } else if !force && self.mem.approximate_memory_usage() <= self.options.write_buffer_size {
                // There is room in current memtable
                break;
            } else if self.im_mem.is_some() {
                info!("Current memtable full; waiting...");
                let _ = self.background_work_finished_signal.recv();
            } else if self.versions.level_files_count(0) >= self.options.l0_stop_writes_threshold {
                info!("Too many L0 files; waiting...");
                let _ = self.background_work_finished_signal.recv();
            } else {
                // there must be no prev log
                let new_log_num = self.versions.get_next_file_number();
                let log_file = {
                    self.env.create(generate_filename(self.db_name.as_str(),FileType::Log, new_log_num).as_str())? };
                self.versions.set_next_file_number(new_log_num + 1);
                self.record_writer = Writer::new(log_file);
                // rotate the mem to immutable mem
                let memtable = mem::replace(&mut self.mem, MemTable::new(self.internal_comparator.clone()));
                self.im_mem = Some(memtable);
                force = false; // do not force another compaction if have room
                self.maybe_schedule_compaction();
            }
        }
        Ok(())
    }

    // Compact immutable memory table to level0 files
    fn compact_mem_table(&mut self) {
        assert!(self.im_mem.is_some(), "[compaction] Unable to compact empty immutable table");
        let base = self.versions.current();
        let mut edit = VersionEdit::new(self.options.max_levels);
        let im_mem = self.im_mem.take().unwrap();
        match self.write_level0_table(&im_mem, &mut edit, Some(base)) {
            Ok(()) => {
                if self.is_shutting_down.load(Ordering::Acquire) {
                    self.record_bg_error(WickErr::new(Status::IOError, Some("Deleting DB during memtable compaction")))
                } else {
                    edit.prev_log_number = Some(0);
                    edit.log_number = Some(self.log_file_num);
                    match self.versions.log_and_apply(&mut edit) {
                        Ok(()) => self.delete_obsolete_files(),
                        Err(e) => {
                            self.record_bg_error(e);
                            self.im_mem = Some(im_mem);
                        }
                    }
                }
            },
            Err(e) => {
                self.record_bg_error(e);
                self.im_mem = Some(im_mem);
            }
        }
    }

    // Persistent given memtable into a single level file.
    // If `base` is not `None`, we might choose a proper level for the generated
    // file other we add it to the level0
    fn write_level0_table(&mut self, mem: &MemTable, edit: &mut VersionEdit, base: Option<NodePtr<Version>>) -> Result<()> {
        let now = SystemTime::now();
        let mut meta = FileMetaData::default();
        meta.number = self.versions.get_next_file_number();
        self.versions.set_next_file_number(meta.number + 1);
        self.pending_outputs.insert(meta.number);
        let mem_iter = mem.new_iterator();
        info!(
            "Level-0 table #{} : started",
            meta.number
        );
        let build_result = self.build_table(mem_iter, &mut meta);
        info!(
            "Level-0 table #{} : {} bytes [{:?}]",
            meta.number, meta.file_size, &build_result
        );
        self.pending_outputs.remove(&meta.number);
        let mut level = 0;

        // Note that if file_size is zero, the file has been deleted and
        // should not be added to the manifest
        if build_result.is_ok() && meta.file_size > 0 {
            let smallest_ukey = Slice::from(meta.smallest.user_key());
            let largest_ukey = Slice::from(meta.largest.user_key());
            if let Some(v) = base {
                level = v.borrow().data.pick_level_for_memtable_output(&smallest_ukey, &largest_ukey);
            }
            edit.add_file(level, meta.number, meta.file_size, meta.smallest.clone(), meta.largest.clone());
        }
        self.compaction_stats[level].accumulate(
            now.elapsed().unwrap().as_micros() as u64,
            0,
            meta.file_size,
        );
        build_result
    }

    // The complete compaction process
    fn background_compaction(&mut self) {
        if self.im_mem.is_some() {
            // minor compaction
            self.compact_mem_table();
        } else {
            let mut is_manual = false;
            match {
                match self.manual_compaction.as_mut() {
                    // manul compaction
                    Some(manual) => {
                        // TODO: refactor this match to FP style?
                        let compaction = self.versions.compact_range(manual.level, manual.begin.clone(), manual.end.clone());
                        manual.done = compaction.is_none();
                        let begin = if let Some(begin) = &manual.begin {
                            format!("{:?}", begin)
                        } else {
                            "(begin)".to_owned()
                        };
                        let end = if let Some(end) = &manual.end {
                            format!("{:?}", end)
                        } else {
                            "(end)".to_owned()
                        };
                        let stop = if let Some(c) = &compaction  {
                            format!("{:?}", c.inputs[CompactionInputsRelation::Source as usize].last().unwrap().largest.clone())
                        } else {
                            "(end)".to_owned()
                        };
                        info!(
                            "Manual compaction at level-{} from {} .. {}; will stop at {}",
                            manual.level, begin, end, stop
                        );
                        is_manual = true;
                        compaction
                    },
                    None => self.versions.pick_compaction()
                }
            } {
                Some(mut compaction) => {
                    if is_manual && compaction.is_trivial_move() {
                        // just move file to next level
                        let f = compaction.inputs[CompactionInputsRelation::Source as usize].first().unwrap();
                        compaction.edit.delete_file(compaction.level, f.number);
                        compaction.edit.add_file(compaction.level + 1, f.number, f.file_size, f.smallest.clone(), f.largest.clone());
                        if let Err(e) = self.versions.log_and_apply(&mut compaction.edit) {
                            debug!("Error in compaction: {:?}", &e);
                            self.record_bg_error(e);
                        }
                        let current_summary = self.versions.current().borrow().data.level_summary();
                        info!(
                            "Moved #{} to level-{} {} bytes, current level summary: {}",
                            f.number, compaction.level + 1, f.file_size, current_summary
                        )
                    } else {
                        self.do_compaction(&mut compaction);
                        self.cleanup_compaction(&mut compaction);
                        self.delete_obsolete_files();
                    }
                    if !self.is_shutting_down.load(Ordering::Acquire) {
                        if let Some(e) = self.bg_error.read().unwrap().as_ref(){
                            info!("Compaction error: {:?}", e)
                        }
                    }
                },
                None => {}
            }
        }
    }

    // Merging files in level n into file in level n + 1 and
    // keep the still-in-use files
    fn do_compaction(&mut self, c: &mut Compaction) {
        let now = SystemTime::now();
        let level = c.level;
        info!(
            "Compacting {}@{} + {}@{} files",
            c.inputs[CompactionInputsRelation::Source as usize].len(), level,
            c.inputs[CompactionInputsRelation::Parent as usize].len(), level + 1
        );
        if self.snapshots.is_empty() {
            c.oldest_snapshot_alive = self.versions.get_last_sequence();
        } else {
            c.oldest_snapshot_alive = self.snapshots.oldest().sequence();
        }

        // TODO: reactor as scheduling an compaction work
        let mut input_iter = c.new_input_iterator(self.internal_comparator.clone(), self.table_cache.clone());
        let mut mem_compaction_duration = 0;
        input_iter.seek_to_first();

        // the current user key to be compacted

        let mut current_ukey = Slice::default();
        let mut has_current_ukey = false;
        let mut last_sequence_for_key = u64::max_value();

        let icmp = self.internal_comparator.clone();
        let ucmp = icmp.user_comparator.as_ref();
        let mut status = Ok(());
        // Iterate every key
        while input_iter.valid() && !self.is_shutting_down.load(Ordering::Acquire) {
            // Prioritize immutable compaction work
            if self.im_mem.is_some() {
                let imm_start = SystemTime::now();
                // TODO: need mutex
                self.compact_mem_table();
                mem_compaction_duration = imm_start.elapsed().unwrap().as_micros() as u64;
            }
            let ikey = input_iter.key();
            // Checkout whether we need rotate a new output file
            if c.should_stop_before(&ikey, icmp.clone()) && c.builder.is_some() {
                status = self.finish_output_file( c, input_iter.valid());
                if status.is_err() {
                    break;
                }
            }
            let mut drop = false;
            match ParsedInternalKey::decode_from(ikey.clone()) {
                Some(key) => {
                    if !has_current_ukey || ucmp.compare(key.user_key.as_slice(), current_ukey.as_slice()) != CmpOrdering::Equal {
                        // First occurrence of this user key
                        current_ukey = key.user_key.clone();
                        has_current_ukey = true;
                        last_sequence_for_key = u64::max_value();
                    }
                    // Keep the still-in-use old key or not
                    if last_sequence_for_key <= c.oldest_snapshot_alive {
                        drop = true
                    } else if key.value_type == ValueType::Deletion && key.seq <= c.oldest_snapshot_alive &&
                        !c.key_exist_in_deeper_level(&key.user_key) {
                        // For this user key:
                        // (1) there is no data in higher levels
                        // (2) data in lower levels will have larger sequence numbers
                        // (3) data in layers that are being compacted here and have
                        //     smaller sequence numbers will be dropped in the next
                        //     few iterations of this loop
                        //     (by last_sequence_for_key <= c.smallest_snapshot above).
                        // Therefore this deletion marker is obsolete and can be dropped.
                        drop = true
                    }
                    last_sequence_for_key = key.seq;
                    if !drop {
                        // Open output file if necessary
                        if c.builder.is_none() {
                            status = self.open_compaction_output_file(c);
                            if status.is_err() {
                                break;
                            }
                        }
                        let last = c.outputs.len() - 1;
                        // TODO: InternalKey::decoded_from adds extra cost of copying
                        if c.builder.as_ref().unwrap().num_entries() == 0 {
                            // We have a brand new builder so use current key as smallest
                            c.outputs[last].smallest = Rc::new(InternalKey::decoded_from(ikey.as_slice()));
                        }
                        // Keep updating the largest
                        c.outputs[last].largest = Rc::new(InternalKey::decoded_from(ikey.as_slice()));
                        c.builder.as_mut().unwrap().add(ikey.as_slice(), input_iter.value().as_slice()).is_ok();
                        let builder = c.builder.as_ref().unwrap();
                        // Rotate a new output file if the current one is big enough
                        if builder.file_size() >= self.options.max_file_size {
                            status = self.finish_output_file(c, input_iter.valid());
                            if status.is_err() {
                                break;
                            }
                        }
                    }
                }
                None => {
                    current_ukey = Slice::default();
                    has_current_ukey = false;
                    last_sequence_for_key = u64::max_value();
                }
            }
            input_iter.next();
        }
        // TODO: simplify the implementation
        if status.is_ok() && self.is_shutting_down.load(Ordering::Acquire) {
            status = Err(WickErr::new(Status::IOError, Some("Deleting DB during compaction")))
        }
        if status.is_ok() && c.builder.is_some() {
            status = self.finish_output_file(c, input_iter.valid())
        }

        if status.is_ok() {
            status = input_iter.status()
        }
        // Calculate the stats of this compaction
        self.compaction_stats[c.level + 1].accumulate(
            now.elapsed().unwrap().as_micros() as u64 - mem_compaction_duration,
            c.bytes_read(),
            c.bytes_written(),
        );
        if status.is_ok() {
            status = self.install_compaction_results(c)
        }
        if let Err(e) = status {
            self.record_bg_error(e)
        }

        let summary = self.versions.current().borrow().data.level_summary();
        info!(
            "compacted to : {}", summary
        )
    }

    // Close unclosed table builder and remove files in `pending_outputs`
    fn cleanup_compaction(&mut self, c: &mut Compaction) {
        if let Some(builder) = c.builder.as_mut() {
            builder.close()
        }
        for output in c.outputs.iter() {
            // TODO: lock required
            self.pending_outputs.remove(&output.number);
        }
    }

    // Replace the `bg_error` with new WickErr if it's None
    fn record_bg_error(&mut self, e: WickErr) {
        let old = self.bg_error.read().unwrap();
        if old.is_none() {
            mem::drop(old);
            let mut x = self.bg_error.write().unwrap();
            *x = Some(e);
        }
    }

    // Check whether db needs to run a compaction. DB will run a compaction when:
    // 1. no background compaction is running
    // 2. DB is not shutting down
    // 3. no error has been encountered
    // 4. there is an immutable table or a manual compaction request or current version needs to be compacted
    fn maybe_schedule_compaction(&self) {
        if self.background_compaction_scheduled.load(Ordering::Acquire) {
            // Already scheduled
        } else if self.is_shutting_down.load(Ordering::Acquire) {
            // DB is being shutting down
        } else if self.bg_error.read().unwrap().is_some() {
            // Got err
        } else if self.im_mem.is_none() && self.manual_compaction.is_none() && !self.versions.needs_compaction() {
            // No work needs to be done
        } else {
            // TODO: schedule a compaction
        }
    }

    // Build a Table file from the contents of `iter`.  The generated file
    // will be named according to `meta.number`.  On success, the rest of
    // meta will be filled with metadata about the generated table.
    // If no data is present in iter, `meta.file_size` will be set to
    // zero, and no Table file will be produced.
    fn build_table<'a>(&self, mut iter: Box<dyn Iterator + 'a>, meta: &mut FileMetaData) -> Result<()> {
        meta.file_size = 0;
        iter.seek_to_first();
        let file_name = generate_filename(self.db_name.as_str(), FileType::Table, meta.number);
        let mut status = Ok(());
        if iter.valid() {
            let file = self.env.create(file_name.as_str())?;
            let mut builder = TableBuilder::new(file, self.options.clone());
            let mut prev_key = Slice::default();
            let smallest_key = iter.key();
            while iter.valid() {
                let key = iter.key();
                let value = iter.value();
                let s = builder.add(key.as_slice(), value.as_slice());
                if s.is_err() {
                    status = s;
                    break
                }
                prev_key = key;
            }
            if status.is_ok() {
                meta.smallest = Rc::new(InternalKey::decoded_from(smallest_key.as_slice()));
                meta.largest = Rc::new(InternalKey::decoded_from(prev_key.as_slice()));
                status = builder.finish(true).and_then(|_| {
                    meta.file_size = builder.file_size();
                    // make sure that the new file is in the cache
                    let mut it = self.table_cache.borrow().new_iter(Rc::new(ReadOptions::default()), meta.number, meta.file_size);
                    it.status()
                })
            }
        }

        let iter_status = iter.status();
        if !iter_status.is_ok() {
            status = iter_status;
        };
        if status.is_err() || meta.file_size == 0 {
            self.env.remove(file_name.as_str())?;
            status
        } else {
            Ok(())
        }
    }

    // Create new table builder and physical file for current output in Compaction
    fn open_compaction_output_file(&mut self, compact: &mut Compaction) -> Result<()> {
        assert!(compact.builder.is_none());
        let file_number = self.versions.get_next_file_number();
        self.versions.set_next_file_number(file_number + 1);
        // TODO: need lock the pending_outputs first
        self.pending_outputs.insert(file_number);
        let mut output = FileMetaData::default();
        output.number = file_number;
        let file_name = generate_filename(self.db_name.as_str(), FileType::Table, file_number);
        let file = self.env.create(file_name.as_str())?;
        compact.builder = Some(TableBuilder::new(file, self.options.clone()));
        Ok(())
    }

    // Finish the current output file by calling `buidler.finish` and insert it into the table cache
    fn finish_output_file(&self, compact: &mut Compaction, input_iter_valid: bool) -> Result<()> {
        assert!(compact.outputs.len() > 0);
        assert!(compact.builder.is_some());
        let current_entries = compact.builder.as_ref().unwrap().num_entries();
        let mut status = Ok(());
        if input_iter_valid {
            status = compact.builder.as_mut().unwrap().finish(true);
        } else {
            compact.builder.as_mut().unwrap().close();
        }
        let current_bytes = compact.builder.as_ref().unwrap().file_size();
        // update current output
        let length = compact.outputs.len();
        compact.outputs[length - 1].file_size = current_bytes;
        compact.total_bytes += current_bytes;
        compact.builder = None;
        if status.is_ok() && current_entries > 0{
            let output_number = compact.outputs[length - 1].number;
            // make sure that the new file is in the cache
            let mut it = self.table_cache.borrow().new_iter(Rc::new(ReadOptions::default()), output_number, current_bytes);
            it.status()?;
            info!(
                "Generated table #{}@{}: {} keys, {} bytes",
                output_number, compact.level, current_entries, current_bytes
            );
        }
        status
    }

    fn install_compaction_results(&mut self, c: &mut Compaction) -> Result<()> {
        info!(
            "Compacted {}@{} + {}@{} files => {} bytes",
            c.inputs[CompactionInputsRelation::Source as usize].len(), c.level,
            c.inputs[CompactionInputsRelation::Parent as usize].len(), c.level + 1,
            c.total_bytes,
        );
        c.apply_to_edit();
        self.versions.log_and_apply(&mut c.edit)
    }
}

