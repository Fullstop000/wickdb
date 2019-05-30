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
use crate::compaction::{Compaction, CompactionInputsRelation};
use crate::db::filename::{generate_filename, parse_filename, FileType};
use crate::db::format::{
    InternalKey, InternalKeyComparator, LookupKey, ParsedInternalKey, ValueType,
};
use crate::iterator::Iterator;
use crate::mem::{MemTable, MemoryTable};
use crate::options::{Options, ReadOptions, WriteOptions};
use crate::record::writer::Writer;
use crate::snapshot::Snapshot;
use crate::sstable::table::TableBuilder;
use crate::storage::{File, Storage};
use crate::table_cache::TableCache;
use crate::util::slice::Slice;
use crate::util::status::{Result, Status, WickErr};
use crate::version::version_edit::{FileMetaData, VersionEdit};
use crate::version::version_set::VersionSet;
use crossbeam_channel::{Receiver, Sender};
use crossbeam_utils::sync::ShardedLock;
use std::cmp::Ordering as CmpOrdering;
use std::collections::vec_deque::VecDeque;
use std::mem;
use std::path::MAIN_SEPARATOR;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::thread;
use std::time::{Duration, SystemTime};

/// A `DB` is a persistent ordered map from keys to values.
/// A `DB` is safe for concurrent access from multiple threads without
/// any external synchronization.
pub trait DB {
    /// `put` sets the value for the given key. It overwrites any previous value
    /// for that key; a DB is not a multi-map.
    fn put(&self, write_opt: WriteOptions, key: Slice, value: Slice) -> Result<()>;

    /// `get` gets the value for the given key. It returns `Status::NotFound` if the DB
    /// does not contain the key.
    fn get(&self, read_opt: ReadOptions, key: Slice) -> Result<Slice>;

    /// `delete` deletes the value for the given key. It returns `Status::NotFound` if
    /// the DB does not contain the key.
    fn delete(&self, write_opt: WriteOptions, key: Slice) -> Result<()>;

    /// `apply` applies the operations contained in the `WriteBatch` to the DB atomically.
    fn apply(&self, write_opt: WriteOptions, batch: WriteBatch) -> Result<()>;

    /// `close` closes the DB.
    fn close(&self) -> Result<()>;

    fn get_snapshot(&self) -> Arc<Snapshot>;
}

/// The wrapper of `DBImpl` for concurrency control.
/// `WickDB` is thread safe and is able to be shared by `clone()` in different threads.
pub struct WickDB {
    inner: Arc<DBImpl>,
}

impl WickDB {
    pub fn get_snapshot(&self) -> Arc<Snapshot> {
        self.inner.get_snapshot()
    }

    pub fn get(&self, options: ReadOptions, key: Slice) -> Result<Option<Slice>> {
        self.inner.get(options, key)
    }

    pub fn put(&self, options: WriteOptions, key: Slice, value: Slice) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key.as_slice(), value.as_slice());
        self.write(options, batch)
    }

    pub fn write(&self, options: WriteOptions, batch: WriteBatch) -> Result<()> {
        self.inner.schedule_batch_and_wait(options, batch)
    }

    pub fn delete(&self, options: WriteOptions, key: Slice) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key.as_slice());
        self.write(options, batch)
    }

    // The thread take batches from the queue and apples them into memtable and WAL.
    //
    // Steps:
    // 1. Grouping the batches in the queue into a big enough batch
    // 2. Make sure there is enough space in the memtable. This might trigger a minor compaction
    //    or even several major compaction.
    // 3. Write into WAL (.log file)
    // 4. Write into Memtable
    // 5. Update sequence of version set
    fn process_batch(&self) {
        let db = self.inner.clone();
        thread::spawn(move || {
            loop {
                let mut queue = db.batch_queue.lock().unwrap();
                while queue.is_empty() {
                    queue = db.process_batch_sem.wait(queue).unwrap();
                }
                let first = queue.pop_front().unwrap();
                let mut size = first.batch.approximate_size();

                // Allow the group to grow up to a maximum size, but if the
                // original write is small, limit the growth so we do not slow
                // down the small write too much
                let mut max_size = 1 << 20;
                if size <= 128 << 10 {
                    max_size = size + (128 << 10)
                }
                let mut signals = vec![];
                signals.push(first.signal.clone());
                let mut grouped = first;

                // Group several batches from queue
                while !queue.is_empty() {
                    let current = queue.pop_front().unwrap();
                    if current.options.sync && !grouped.options.sync {
                        // Do not include a sync write into a batch handled by a non-sync write.
                        break;
                    }
                    size += current.batch.approximate_size();
                    if size > max_size {
                        // Do not make batch too big
                        break;
                    }
                    grouped.batch.append(current.batch);
                    signals.push(current.signal.clone());
                }

                match db.make_room_for_write(false) {
                    Ok(mut versions) => {
                        let mut last_seq = versions.get_last_sequence();
                        grouped.batch.set_sequence(last_seq + 1);
                        last_seq += u64::from(grouped.batch.count());
                        // must initialize the WAL writer after `make_room_for_write`
                        let writer = versions.record_writer.as_mut().unwrap();
                        let mut status = writer.add_record(&Slice::from(grouped.batch.data()));
                        let mut sync_err = false;
                        if status.is_ok() && grouped.options.sync {
                            status = writer.sync();
                            if status.is_err() {
                                sync_err = true;
                            }
                        }
                        if status.is_ok() {
                            let memtable = db.mem.read().unwrap();
                            status = grouped.batch.insert_into(&*memtable);
                        }

                        for signal in signals.iter() {
                            if let Err(e) = signal.send(status.clone()) {
                                error!(
                                    "[process batch] Fail sending finshing signal to waiting batch: {}", e
                                )
                            }
                        }
                        if let Err(e) = status {
                            if sync_err {
                                // The state of the log file is indeterminate: the log record we
                                // just added may or may not show up when the DB is re-opened.
                                // So we force the DB into a mode where all future writes fail.
                                db.record_bg_error(e.clone());
                            }
                        }
                        versions.set_last_sequence(last_seq);
                    }
                    Err(e) => {
                        for signal in signals.iter() {
                            if let Err(e) = signal.send(Err(e.clone())) {
                                error!(
                                    "[process batch] Fail sending finishing signal to waiting batch: {}", e
                                )
                            }
                        }
                    }
                }
            }
        });
    }

    // Process a compaction work when receiving the signal.
    // The compaction might run recursively since we produce new table files.
    fn process_compaction(&self) {
        let db = self.inner.clone();
        thread::spawn(move || {
            while let Ok(()) = db.do_compaction.1.recv() {
                if db.is_shutting_down.load(Ordering::Acquire) {
                    // No more background work when shutting down
                    break;
                } else if db.bg_error.read().unwrap().is_some() {
                    // Non more background work after a background error
                } else {
                    db.background_compaction();
                }
                db.background_compaction_scheduled
                    .store(false, Ordering::Release);

                // Previous compaction may have produced too many files in a level,
                // so reschedule another compaction if needed
                db.maybe_schedule_compaction();
                db.background_work_finished_signal.notify_all();
            }
        });
    }
}

impl Clone for WickDB {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct DBImpl {
    env: Arc<dyn Storage>,
    internal_comparator: Arc<InternalKeyComparator>,
    options: Arc<Options>,
    // The physical path of wickdb
    db_name: String,
    db_lock: Option<Mutex<Box<dyn File>>>,

    /*
     * Fields for write batch scheduling
     */
    batch_queue: Mutex<VecDeque<BatchTask>>,
    process_batch_sem: Condvar,

    // the table cache
    table_cache: Arc<TableCache>,

    // The version set
    versions: Mutex<VersionSet>,

    // signal of compaction finished
    background_work_finished_signal: Condvar,
    // whether we have a compaction running
    background_compaction_scheduled: AtomicBool,
    // signal of schedule a compaction
    do_compaction: (Sender<()>, Receiver<()>),
    // Though Memtable is thread safe with multiple readers and single writers and
    // all relative methods are using immutable borrowing,
    // we still need to mutate the field `mem` and `im_mem` in few situations.
    mem: ShardedLock<MemTable>,
    im_mem: ShardedLock<Option<MemTable>>, // iff the memtable is compacted
    // Have we encountered a background error in paranoid mode
    bg_error: RwLock<Option<WickErr>>,
    // Whether the db is closing
    is_shutting_down: AtomicBool,
}

unsafe impl Sync for DBImpl {}
unsafe impl Send for DBImpl {}

// A wrapper struct for scheduling `WriteBatch`
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

impl Drop for DBImpl {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.is_shutting_down.store(true, Ordering::Release);
        if let Some(lock) = self.db_lock.as_ref() {
            lock.lock().unwrap().unlock();
        }
    }
}

impl DBImpl {
    fn new(options: Options, db_name: String) -> Self {
        let o = Arc::new(options);
        let icmp = Arc::new(InternalKeyComparator::new(o.comparator.clone()));
        Self {
            env: o.env.clone(),
            internal_comparator: icmp.clone(),
            options: o.clone(),
            db_name: db_name.clone(),
            db_lock: None,
            batch_queue: Mutex::new(VecDeque::new()),
            process_batch_sem: Condvar::new(),
            table_cache: Arc::new(TableCache::new(
                db_name.clone(),
                o.clone(),
                o.table_cache_size(),
            )),
            versions: Mutex::new(VersionSet::new(db_name.clone(), o.clone())),
            background_work_finished_signal: Condvar::new(),
            background_compaction_scheduled: AtomicBool::new(false),
            do_compaction: crossbeam_channel::unbounded(),
            mem: ShardedLock::new(MemTable::new(icmp)),
            im_mem: ShardedLock::new(None),
            bg_error: RwLock::new(None),
            is_shutting_down: AtomicBool::new(false),
        }
    }
    fn get_snapshot(&self) -> Arc<Snapshot> {
        self.versions.lock().unwrap().new_snapshot()
    }

    fn get(&self, options: ReadOptions, key: Slice) -> Result<Option<Slice>> {
        let snapshot = match &options.snapshot {
            Some(snapshot) => snapshot.sequence(),
            None => self.versions.lock().unwrap().get_last_sequence(),
        };
        let lookup_key = LookupKey::new(key.as_slice(), snapshot);
        // search the memtable
        if let Some(result) = self.mem.read().unwrap().get(&lookup_key) {
            match result {
                Ok(value) => return Ok(Some(value)),
                // mem.get only returns Err() when get an Deletion of the key
                Err(_) => return Ok(None),
            }
        }
        // search the immutable memtable
        if let Some(im_mem) = self.im_mem.read().unwrap().as_ref() {
            if let Some(result) = im_mem.get(&lookup_key) {
                match result {
                    Ok(value) => return Ok(Some(value)),
                    Err(_) => return Ok(None),
                }
            }
        }
        let current = self.versions.lock().unwrap().current();
        let (value, seek_stats) = current.get(options, lookup_key, self.table_cache.clone())?;
        if current.update_stats(seek_stats) {
            self.maybe_schedule_compaction()
        }
        Ok(value)
    }

    // Delete any unneeded files and stale in-memory entries.
    #[allow(unused_must_use)]
    fn delete_obsolete_files(&self, mut versions: MutexGuard<VersionSet>) {
        if self.bg_error.read().is_err() {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect
            return;
        }
        versions.lock_live_files();
        // ignore IO error on purpose
        if let Ok(files) = self.env.list(self.db_name.as_str()) {
            for file in files.iter() {
                if let Some((file_type, number)) = parse_filename(file) {
                    let mut keep = true;
                    match file_type {
                        FileType::Log => {
                            keep = number >= versions.get_log_number()
                                || number == versions.get_prev_log_number()
                        }
                        FileType::Manifest => keep = number >= versions.get_manifest_number(),
                        FileType::Table => keep = versions.pending_outputs.contains(&number),
                        // Any temp files that are currently being written to must
                        // be recorded in pending_outputs
                        FileType::Temp => keep = versions.pending_outputs.contains(&number),
                        _ => {}
                    }
                    if !keep {
                        if file_type == FileType::Table {
                            self.table_cache.evict(number)
                        }
                        info!("Delete type={:?} #{}", file_type, number);
                        // ignore the IO error here
                        self.env.remove(
                            format!("{}{}{:?}", self.db_name.as_str(), MAIN_SEPARATOR, file)
                                .as_str(),
                        );
                    }
                }
            }
        }
    }

    // Schedule the WriteBatch and wait for the result from the receiver.
    // This function wakes up the thread in `process_batch`.
    fn schedule_batch_and_wait(&self, options: WriteOptions, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let (send, recv) = crossbeam_channel::bounded(0);
        let task = BatchTask::new(batch, send, options);
        self.batch_queue.lock().unwrap().push_back(task);
        self.process_batch_sem.notify_all();
        match recv.recv() {
            Ok(m) => m,
            Err(e) => Err(WickErr::new_from_raw(Status::Unexpected, None, Box::new(e))),
        }
    }

    // Make sure there is enough space in memtable.
    // This method acquires the mutex of VersionSet and deliver it to the caller.
    fn make_room_for_write(&self, mut force: bool) -> Result<MutexGuard<VersionSet>> {
        let mut allow_delay = !force;
        let mut versions = self.versions.lock().unwrap();
        loop {
            if let Some(e) = { self.bg_error.write().unwrap().take() } {
                return Err(e);
            } else if allow_delay
                && versions.level_files_count(0) >= self.options.l0_slowdown_writes_threshold
            {
                // We are getting close to hitting a hard limit on the number of
                // L0 files.  Rather than delaying a single write by several
                // seconds when we hit the hard limit, start delaying each
                // individual write by 1ms to reduce latency variance.  Also,
                // this delay hands over some CPU to the compaction thread in
                // case it is sharing the same core as the writer.
                thread::sleep(Duration::from_micros(1000));
                allow_delay = false; // do not delay a single write more than once
            } else if !force
                && self.mem.read().unwrap().approximate_memory_usage()
                    <= self.options.write_buffer_size
            {
                // There is room in current memtable
                break;
            } else if self.im_mem.read().unwrap().is_some() {
                info!("Current memtable full; waiting...");
                versions = self.background_work_finished_signal.wait(versions).unwrap();
            } else if versions.level_files_count(0) >= self.options.l0_stop_writes_threshold {
                info!("Too many L0 files; waiting...");
                versions = self.background_work_finished_signal.wait(versions).unwrap();
            } else {
                // there must be no prev log
                let new_log_num = versions.get_next_file_number();
                let log_file = self.env.create(
                    generate_filename(self.db_name.as_str(), FileType::Log, new_log_num).as_str(),
                )?;
                versions.set_next_file_number(new_log_num + 1);
                versions.record_writer = Some(Writer::new(log_file));
                // rotate the mem to immutable mem
                let mut mem = self.mem.write().unwrap();
                let memtable =
                    mem::replace(&mut *mem, MemTable::new(self.internal_comparator.clone()));
                let mut im_mem = self.im_mem.write().unwrap();
                *im_mem = Some(memtable);
                force = false; // do not force another compaction if have room
                self.maybe_schedule_compaction();
            }
        }
        Ok(versions)
    }

    // Compact immutable memory table to level0 files
    fn compact_mem_table(&self) {
        let mut versions = self.versions.lock().unwrap();
        let base = versions.current();
        let mut edit = VersionEdit::new(self.options.max_levels);
        let mut im_mem = self.im_mem.write().unwrap();
        // Persistent given memtable into a single level file.
        match {
            let now = SystemTime::now();
            let mut meta = FileMetaData::default();
            meta.number = versions.inc_next_file_number();
            versions.pending_outputs.insert(meta.number);
            let mem_iter = im_mem.as_ref().unwrap().new_iterator();
            info!("Level-0 table #{} : started", meta.number);
            let build_result = self.build_table(mem_iter, &mut meta);
            info!(
                "Level-0 table #{} : {} bytes [{:?}]",
                meta.number, meta.file_size, &build_result
            );
            versions.pending_outputs.remove(&meta.number);
            let mut level = 0;

            // If `file_size` is zero, the file has been deleted and
            // should not be added to the manifest
            if build_result.is_ok() && meta.file_size > 0 {
                let smallest_ukey = Slice::from(meta.smallest.user_key());
                let largest_ukey = Slice::from(meta.largest.user_key());
                level = base.pick_level_for_memtable_output(&smallest_ukey, &largest_ukey);
                edit.add_file(
                    level,
                    meta.number,
                    meta.file_size,
                    meta.smallest.clone(),
                    meta.largest.clone(),
                );
            }
            versions.compaction_stats[level].accumulate(
                now.elapsed().unwrap().as_micros() as u64,
                0,
                meta.file_size,
            );
            build_result
        } {
            Ok(()) => {
                if self.is_shutting_down.load(Ordering::Acquire) {
                    self.record_bg_error(WickErr::new(
                        Status::IOError,
                        Some("Deleting DB during memtable compaction"),
                    ))
                } else {
                    edit.prev_log_number = Some(0);
                    edit.log_number = Some(versions.get_log_number());
                    match versions.log_and_apply(&mut edit) {
                        Ok(()) => {
                            *im_mem = None;
                            self.delete_obsolete_files(versions);
                        }
                        Err(e) => {
                            self.record_bg_error(e);
                        }
                    }
                }
            }
            Err(e) => {
                self.record_bg_error(e);
            }
        }
    }

    // The complete compaction process
    fn background_compaction(&self) {
        if self.im_mem.read().unwrap().is_some() {
            // minor compaction
            self.compact_mem_table();
        } else {
            let mut is_manual = false;
            let mut versions = self.versions.lock().unwrap();
            if let Some(mut compaction) = {
                match versions.manual_compaction.take() {
                    // manul compaction
                    Some(mut manual) => {
                        if manual.done {
                            versions.pick_compaction()
                        } else {
                            let compaction = versions.compact_range(
                                manual.level,
                                manual.begin.clone(),
                                manual.end.clone(),
                            );
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
                            let stop = if let Some(c) = &compaction {
                                format!(
                                    "{:?}",
                                    c.inputs[CompactionInputsRelation::Source as usize]
                                        .last()
                                        .unwrap()
                                        .largest
                                        .clone()
                                )
                            } else {
                                "(end)".to_owned()
                            };
                            info!(
                                "Manual compaction at level-{} from {} .. {}; will stop at {}",
                                manual.level, begin, end, stop
                            );
                            is_manual = true;
                            versions.manual_compaction = Some(manual);
                            compaction
                        }
                    }
                    None => versions.pick_compaction(),
                }
            } {
                if is_manual && compaction.is_trivial_move() {
                    // just move file to next level
                    let f = compaction.inputs[CompactionInputsRelation::Source as usize]
                        .first()
                        .unwrap();
                    compaction.edit.delete_file(compaction.level, f.number);
                    compaction.edit.add_file(
                        compaction.level + 1,
                        f.number,
                        f.file_size,
                        f.smallest.clone(),
                        f.largest.clone(),
                    );
                    if let Err(e) = versions.log_and_apply(&mut compaction.edit) {
                        debug!("Error in compaction: {:?}", &e);
                        self.record_bg_error(e);
                    }
                    let current_summary = versions.current().level_summary();
                    info!(
                        "Moved #{} to level-{} {} bytes, current level summary: {}",
                        f.number,
                        compaction.level + 1,
                        f.file_size,
                        current_summary
                    )
                } else {
                    let level = compaction.level;
                    info!(
                        "Compacting {}@{} + {}@{} files",
                        compaction.inputs[CompactionInputsRelation::Source as usize].len(),
                        level,
                        compaction.inputs[CompactionInputsRelation::Parent as usize].len(),
                        level + 1
                    );
                    {
                        let snapshots = &mut versions.snapshots;
                        // Cleanup all redundant snapshots first
                        snapshots.gc();
                        if snapshots.is_empty() {
                            compaction.oldest_snapshot_alive = versions.get_last_sequence();
                        } else {
                            compaction.oldest_snapshot_alive = snapshots.oldest().sequence();
                        }
                    }
                    self.delete_obsolete_files(self.do_compaction(&mut compaction));
                }
                if !self.is_shutting_down.load(Ordering::Acquire) {
                    if let Some(e) = self.bg_error.read().unwrap().as_ref() {
                        info!("Compaction error: {:?}", e)
                    }
                }
                if is_manual {
                    versions.manual_compaction.as_mut().unwrap().done = true;
                }
            }
        }
    }

    // Merging files in level n into file in level n + 1 and
    // keep the still-in-use files
    fn do_compaction(&self, c: &mut Compaction) -> MutexGuard<VersionSet> {
        let now = SystemTime::now();
        let mut input_iter =
            c.new_input_iterator(self.internal_comparator.clone(), self.table_cache.clone());
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
            if self.im_mem.read().unwrap().is_some() {
                let imm_start = SystemTime::now();
                self.compact_mem_table();
                mem_compaction_duration = imm_start.elapsed().unwrap().as_micros() as u64;
            }
            let ikey = input_iter.key();
            // Checkout whether we need rotate a new output file
            if c.should_stop_before(&ikey, icmp.clone()) && c.builder.is_some() {
                status = self.finish_output_file(c, input_iter.valid());
                if status.is_err() {
                    break;
                }
            }
            let mut drop = false;
            match ParsedInternalKey::decode_from(ikey.clone()) {
                Some(key) => {
                    if !has_current_ukey
                        || ucmp.compare(key.user_key.as_slice(), current_ukey.as_slice())
                            != CmpOrdering::Equal
                    {
                        // First occurrence of this user key
                        current_ukey = key.user_key.clone();
                        has_current_ukey = true;
                        last_sequence_for_key = u64::max_value();
                    }
                    // Keep the still-in-use old key or not
                    if last_sequence_for_key <= c.oldest_snapshot_alive
                        || (key.value_type == ValueType::Deletion
                            && key.seq <= c.oldest_snapshot_alive
                            && !c.key_exist_in_deeper_level(&key.user_key))
                    {
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
                            status = self.versions.lock().unwrap().open_compaction_output_file(c);
                            if status.is_err() {
                                break;
                            }
                        }
                        let last = c.outputs.len() - 1;
                        // TODO: InternalKey::decoded_from adds extra cost of copying
                        if c.builder.as_ref().unwrap().num_entries() == 0 {
                            // We have a brand new builder so use current key as smallest
                            c.outputs[last].smallest =
                                Rc::new(InternalKey::decoded_from(ikey.as_slice()));
                        }
                        // Keep updating the largest
                        c.outputs[last].largest =
                            Rc::new(InternalKey::decoded_from(ikey.as_slice()));
                        let _ = c
                            .builder
                            .as_mut()
                            .unwrap()
                            .add(ikey.as_slice(), input_iter.value().as_slice());
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
            status = Err(WickErr::new(
                Status::IOError,
                Some("Deleting DB during compaction"),
            ))
        }
        if status.is_ok() && c.builder.is_some() {
            status = self.finish_output_file(c, input_iter.valid())
        }

        if status.is_ok() {
            status = input_iter.status()
        }
        // Calculate the stats of this compaction
        let mut versions = self.versions.lock().unwrap();
        versions.compaction_stats[c.level + 1].accumulate(
            now.elapsed().unwrap().as_micros() as u64 - mem_compaction_duration,
            c.bytes_read(),
            c.bytes_written(),
        );
        if status.is_ok() {
            info!(
                "Compacted {}@{} + {}@{} files => {} bytes",
                c.inputs[CompactionInputsRelation::Source as usize].len(),
                c.level,
                c.inputs[CompactionInputsRelation::Parent as usize].len(),
                c.level + 1,
                c.total_bytes,
            );
            c.apply_to_edit();
            status = versions.log_and_apply(&mut c.edit);
        }
        if let Err(e) = status {
            self.record_bg_error(e)
        }

        let summary = versions.current().level_summary();
        info!("compacted to : {}", summary);

        // Close unclosed table builder and remove files in `pending_outputs`
        if let Some(builder) = c.builder.as_mut() {
            builder.close()
        }
        for output in c.outputs.iter() {
            versions.pending_outputs.remove(&output.number);
        }
        versions
    }

    // Replace the `bg_error` with new WickErr if it's None
    fn record_bg_error(&self, e: WickErr) {
        let old = self.bg_error.read().unwrap();
        if old.is_none() {
            mem::drop(old);
            let mut x = self.bg_error.write().unwrap();
            *x = Some(e);
            self.background_work_finished_signal.notify_all();
        }
    }

    // Check whether db needs to run a compaction. DB will run a compaction when:
    // 1. no background compaction is running
    // 2. DB is not shutting down
    // 3. no error has been encountered
    // 4. there is an immutable table or a manual compaction request or current version needs to be compacted
    fn maybe_schedule_compaction(&self) {
        if self.background_compaction_scheduled.load(Ordering::Acquire)
            // Already scheduled
        || self.is_shutting_down.load(Ordering::Acquire)
            // DB is being shutting down
        || self.bg_error.read().unwrap().is_some()
            // Got err
        ||  (self.im_mem.read().unwrap().is_none()
            && !self.versions.lock().unwrap().needs_compaction())
        {
            // No work needs to be done
        } else {
            self.background_compaction_scheduled
                .store(true, Ordering::Release);
            if let Err(e) = self.do_compaction.0.send(()) {
                error!(
                    "[schedule compaction] Fail sending signal to compaction channel: {}",
                    e
                )
            }
        }
    }

    // Build a Table file from the contents of `iter`.  The generated file
    // will be named according to `meta.number`.  On success, the rest of
    // meta will be filled with metadata about the generated table.
    // If no data is present in iter, `meta.file_size` will be set to
    // zero, and no Table file will be produced.
    fn build_table<'a>(
        &self,
        mut iter: Box<dyn Iterator + 'a>,
        meta: &mut FileMetaData,
    ) -> Result<()> {
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
                    break;
                }
                prev_key = key;
            }
            if status.is_ok() {
                meta.smallest = Rc::new(InternalKey::decoded_from(smallest_key.as_slice()));
                meta.largest = Rc::new(InternalKey::decoded_from(prev_key.as_slice()));
                status = builder.finish(true).and_then(|_| {
                    meta.file_size = builder.file_size();
                    // make sure that the new file is in the cache
                    let mut it = self.table_cache.new_iter(
                        Rc::new(ReadOptions::default()),
                        meta.number,
                        meta.file_size,
                    );
                    it.status()
                })
            }
        }

        let iter_status = iter.status();
        if iter_status.is_err() {
            status = iter_status;
        };
        if status.is_err() || meta.file_size == 0 {
            self.env.remove(file_name.as_str())?;
            status
        } else {
            Ok(())
        }
    }

    // Finish the current output file by calling `buidler.finish` and insert it into the table cache
    fn finish_output_file(&self, compact: &mut Compaction, input_iter_valid: bool) -> Result<()> {
        assert!(!compact.outputs.is_empty());
        assert!(compact.builder.is_some());
        let current_entries = compact.builder.as_ref().unwrap().num_entries();
        let status = if input_iter_valid {
            compact.builder.as_mut().unwrap().finish(true)
        } else {
            compact.builder.as_mut().unwrap().close();
            Ok(())
        };
        let current_bytes = compact.builder.as_ref().unwrap().file_size();
        // update current output
        let length = compact.outputs.len();
        compact.outputs[length - 1].file_size = current_bytes;
        compact.total_bytes += current_bytes;
        compact.builder = None;
        if status.is_ok() && current_entries > 0 {
            let output_number = compact.outputs[length - 1].number;
            // make sure that the new file is in the cache
            let mut it = self.table_cache.new_iter(
                Rc::new(ReadOptions::default()),
                output_number,
                current_bytes,
            );
            it.status()?;
            info!(
                "Generated table #{}@{}: {} keys, {} bytes",
                output_number, compact.level, current_entries, current_bytes
            );
        }
        status
    }
}
