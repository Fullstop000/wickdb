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
pub mod iterator;

use crate::batch::{WriteBatch, HEADER_SIZE};
use crate::compaction::{Compaction, CompactionStats, ManualCompaction};
use crate::db::filename::{generate_filename, parse_filename, update_current, FileType};
use crate::db::format::{
    InternalKey, InternalKeyComparator, LookupKey, ParsedInternalKey, ValueType, MAX_KEY_SEQUENCE,
    VALUE_TYPE_FOR_SEEK,
};
use crate::db::iterator::{DBIterator, DBIteratorCore};
use crate::iterator::{Iterator, KMergeIter};
use crate::mem::{MemTable, MemTableIterator};
use crate::options::{Options, ReadOptions, WriteOptions};
use crate::record::reader::Reader;
use crate::record::writer::Writer;
use crate::snapshot::Snapshot;
use crate::sstable::table::TableBuilder;
use crate::storage::{File, Storage};
use crate::table_cache::TableCache;
use crate::util::reporter::LogReporter;
use crate::version::version_edit::{FileMetaData, VersionEdit};
use crate::version::version_set::{SSTableIters, VersionSet};
use crate::version::Version;
use crate::Comparator;
use crate::{Error, Result};
use crossbeam_channel::{Receiver, Sender};
use crossbeam_utils::sync::ShardedLock;
use std::cmp::Ordering as CmpOrdering;
use std::collections::vec_deque::VecDeque;
use std::mem;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::thread;
use std::time::{Duration, Instant};

/// A `DB` is a persistent ordered map from keys to values.
/// A `DB` is safe for concurrent access from multiple threads without
/// any external synchronization.
pub trait DB {
    /// The iterator that can yield all the kv pairs in `DB`
    type Iterator;

    /// `put` sets the value for the given key. It overwrites any previous value
    /// for that key; a DB is not a multi-map.
    fn put(&self, write_opt: WriteOptions, key: &[u8], value: &[u8]) -> Result<()>;

    /// `get` gets the value for the given key. It returns `None` if the DB
    /// does not contain the key.
    fn get(&self, read_opt: ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Return an iterator over the contents of the database.
    fn iter(&self, read_opt: ReadOptions) -> Result<Self::Iterator>;

    /// `delete` deletes the value for the given key. It returns `Status::NotFound` if
    /// the DB does not contain the key.
    fn delete(&self, write_opt: WriteOptions, key: &[u8]) -> Result<()>;

    /// `write` applies the operations contained in the `WriteBatch` to the DB atomically.
    fn write(&self, write_opt: WriteOptions, batch: WriteBatch) -> Result<()>;

    /// `close` shuts down the current WickDB by waiting util all the background tasks are complete
    /// and then releases the file lock. A closed db should never be used again and is able to be
    /// dropped safely.
    fn close(&mut self) -> Result<()>;

    /// `destroy` shuts down the current WickDB and delete all relative files and the db directory.
    fn destroy(&mut self) -> Result<()>;

    /// Acquire a `Snapshot` for reading DB
    fn snapshot(&self) -> Arc<Snapshot>;
}

/// The wrapper of `DBImpl` for concurrency control.
/// `WickDB` is thread safe and is able to be shared by `clone()` in different threads.
#[derive(Clone)]
pub struct WickDB<S: Storage + Clone + 'static, C: Comparator> {
    inner: Arc<DBImpl<S, C>>,
    shutdown_batch_processing_thread: (Sender<()>, Receiver<()>),
    shutdown_compaction_thread: (Sender<()>, Receiver<()>),
}

/// The iterator yields all the user keys and user values in db
pub type WickDBIterator<S, C> = DBIterator<InternalIterator<S, C>, S, C>;

// The iterator yields all the internal keys and internal values in db
type InternalIterator<S, C> = KMergeIter<
    DBIteratorCore<InternalKeyComparator<C>, MemTableIterator<C>, KMergeIter<SSTableIters<S, C>>>,
>;

impl<S: Storage + Clone, C: Comparator + 'static> DB for WickDB<S, C> {
    type Iterator = WickDBIterator<S, C>;

    fn put(&self, options: WriteOptions, key: &[u8], value: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.put(key, value);
        self.write(options, batch)
    }

    fn get(&self, options: ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(options, key)
    }

    fn iter(&self, read_opt: ReadOptions) -> Result<Self::Iterator> {
        let internal_iter = self.internal_iter(read_opt)?;
        let ucmp = self.inner.internal_comparator.user_comparator.clone();
        let sequence = if let Some(snapshot) = &read_opt.snapshot {
            snapshot.sequence()
        } else {
            self.inner.versions.lock().unwrap().last_sequence()
        };
        Ok(DBIterator::new(
            internal_iter,
            self.inner.clone(),
            sequence,
            ucmp,
        ))
    }

    fn delete(&self, options: WriteOptions, key: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.delete(key);
        self.write(options, batch)
    }

    fn write(&self, options: WriteOptions, batch: WriteBatch) -> Result<()> {
        self.inner.schedule_batch_and_wait(options, batch, false)
    }

    fn close(&mut self) -> Result<()> {
        if self.inner.is_shutting_down.load(Ordering::Acquire) {
            return Ok(());
        }
        self.inner.is_shutting_down.store(true, Ordering::Relaxed);
        self.inner.schedule_close_batch();
        let _ = self.shutdown_batch_processing_thread.1.recv();
        // Send a signal to avoid blocking forever
        let _ = self.inner.do_compaction.0.send(());
        let _ = self.shutdown_compaction_thread.1.recv();
        self.inner.close()?;
        info!("DB {} closed", &self.inner.db_path);
        Ok(())
    }

    fn destroy(&mut self) -> Result<()> {
        info!("Start destroying: {}", &self.inner.db_path);
        let db = self.inner.clone();
        self.close()?;
        info!("Remove dir: {}", &self.inner.db_path);
        db.env.remove_dir(&db.db_path, true)
    }

    fn snapshot(&self) -> Arc<Snapshot> {
        self.inner.snapshot()
    }
}

impl<S: Storage + Clone, C: Comparator + 'static> WickDB<S, C> {
    /// Create a new WickDB
    pub fn open_db<P: AsRef<Path>>(
        mut options: Options<C>,
        db_path: P,
        storage: S,
    ) -> Result<Self> {
        let db_path = match db_path.as_ref().to_owned().into_os_string().into_string() {
            Ok(s) => s,
            Err(_) => {
                return Err(Error::Customized(
                    "Invalid db path. Expect to use Unicode db path.".to_owned(),
                ))
            }
        };
        options.initialize(&db_path, &storage);
        debug!("Open db: '{:?}'", &db_path);
        let mut db = DBImpl::new(options, db_path, storage);
        let (mut edit, should_save_manifest) = db.recover()?;
        let mut versions = db.versions.lock().unwrap();
        if versions.record_writer.is_none() {
            let new_log_number = versions.inc_next_file_number();
            let log_file = db.env.create(&generate_filename(
                &db.db_path,
                FileType::Log,
                new_log_number,
            ))?;
            versions.record_writer = Some(Writer::new(log_file));
            edit.set_log_number(new_log_number);
            versions.set_log_number(new_log_number);
        }
        if should_save_manifest {
            edit.set_prev_log_number(0);
            edit.set_log_number(versions.log_number());
            versions.log_and_apply(edit)?;
        }

        let current = versions.current();
        db.delete_obsolete_files(versions)?;
        let wick_db = WickDB {
            inner: Arc::new(db),
            shutdown_batch_processing_thread: crossbeam_channel::bounded(1),
            shutdown_compaction_thread: crossbeam_channel::bounded(1),
        };
        wick_db.process_compaction();
        wick_db.process_batch();
        // Schedule a compaction to current version for potential unfinished work
        debug!("Try to schedule a compaction on opening db");
        wick_db.inner.maybe_schedule_compaction(current);
        Ok(wick_db)
    }

    /// Schedule a compaction for the key range `[begin, end]`.
    pub fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        self.inner.compact_range(begin, end)
    }

    /// Schedue a manual compaction for the key range `[begin, end]` at level `level`
    pub fn compact_range_at(
        &self,
        level: usize,
        begin: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<()> {
        self.inner.manual_compact_range(level, begin, end)
    }

    /// Returns true if the given snapshot is removed
    pub fn release_snapshot(&self, s: Arc<Snapshot>) -> bool {
        let mut vset = self.inner.versions.lock().unwrap();
        vset.snapshots.release(s)
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
        let shutdown = self.shutdown_batch_processing_thread.0.clone();
        thread::Builder::new().name("batch process".to_owned()).spawn(move || {
            loop {
                if db.is_shutting_down.load(Ordering::Acquire) {
                    // Cleanup all the batch queue
                    let mut queue = db.batch_queue.lock().unwrap();
                    while let Some(batch) = queue.pop_front() {
                        let _ = batch.signal.send(Err(Error::DBClosed(
                            "DB is closing. Clean up all the batch in queue".to_owned(),
                        )));
                    }
                    break;
                }
                let first = {
                    let mut queue = db.batch_queue.lock().unwrap();
                    while queue.is_empty() {
                        // yields current thread and unlock queue
                        queue = db.process_batch_sem.wait(queue).unwrap();
                    }
                    queue.pop_front().unwrap()
                };
                if first.stop_process {
                    break;
                }
                let force = first.force_mem_compaction;
                match db.make_room_for_write(force) {
                    Ok(mut versions) => {
                        let (mut grouped, signals) = db.group_batches(first);
                        if !grouped.batch.is_empty() {
                            let mut last_seq = versions.last_sequence();
                            grouped.batch.set_sequence(last_seq + 1);
                            last_seq += u64::from(grouped.batch.get_count());
                            // `record_writer` must be initialized here
                            let writer = versions.record_writer.as_mut().unwrap();
                            let mut res = writer.add_record(grouped.batch.data());
                            let mut sync_err = false;
                            if res.is_ok() && grouped.options.sync {
                                res = writer.sync();
                                if res.is_err() {
                                    sync_err = true;
                                }
                            }
                            if res.is_ok() {
                                let memtable = db.mem.read().unwrap();
                                // Might encounter corruption err here
                                res = grouped.batch.insert_into(&*memtable);
                            }
                            match res {
                                Ok(_) => {
                                    for signal in signals {
                                        if let Err(e) = signal.send(Ok(())) {
                                            error!(
                                                "[process batch] Fail sending finshing signal to waiting batch: {}", e
                                            )
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("[process batch] write batch failed: {}", e);
                                    for signal in signals {
                                        if let Err(e) = signal.send(Err(Error::Customized(
                                            "[process batch] write batch failed".to_owned(),
                                        ))) {
                                            error!(
                                                "[process batch] Fail sending finshing signal to waiting batch: {}", e
                                            )
                                        }
                                    }
                                    if sync_err {
                                        // The state of the log file is indeterminate: the log record we
                                        // just added may or may not show up when the DB is re-opened.
                                        // So we force the DB into a mode where all future writes fail.
                                        db.record_bg_error(e);
                                    }
                                }
                            }
                            versions.set_last_sequence(last_seq);
                        } else {
                            // Notify waiting batches
                            for signal in signals {
                                if let Err(e) = signal.send(Ok(())) {
                                    error!(
                                        "[process batch] Fail sending finishing signal to waiting batch: {}", e
                                    )
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if let Err(e) = first.signal.send(Err(Error::Customized(format!(
                            "[process batch] Error making room for write requests: {}",
                            e
                        )))) {
                            error!(
                                "[process batch] fail to send finishing signal to waiting batch: {}", e
                            )
                        }
                    }
                }
            }
            shutdown.send(()).unwrap();
            info!("batch processing thread shut down");
        }).unwrap();
    }

    // Process a compaction work when receiving the signal.
    // The compaction might run recursively since we produce new table files.
    fn process_compaction(&self) {
        let db = self.inner.clone();
        let shutdown = self.shutdown_compaction_thread.0.clone();
        thread::Builder::new()
            .name("compaction".to_owned())
            .spawn(move || {
                let mut done_compaction = false;
                while let Ok(()) = db.do_compaction.1.recv() {
                    if db.is_shutting_down.load(Ordering::Acquire) {
                        // No more background work when shutting down
                        break;
                    } else if db.bg_error.read().unwrap().is_some() {
                        // Non more background work after a background error
                    } else {
                        done_compaction = db.background_compaction();
                        db.background_work_finished_signal.notify_all();
                    }
                    db.background_compaction_scheduled
                        .store(false, Ordering::Release);

                    if done_compaction {
                        // Previous compaction may have produced too many files in a level,
                        // so reschedule another compaction if needed
                        let current = db.versions.lock().unwrap().current();
                        db.maybe_schedule_compaction(current);
                    }
                }
                shutdown.send(()).unwrap();
                info!("compaction thread shut down");
            })
            .unwrap();
    }

    fn internal_iter(&self, read_opt: ReadOptions) -> Result<InternalIterator<S, C>> {
        let mut mem_iters = vec![self.inner.mem.read().unwrap().iter()];
        if let Some(im_mem) = self.inner.im_mem.read().unwrap().as_ref() {
            mem_iters.push(im_mem.iter());
        }
        let sst_iter = self
            .inner
            .versions
            .lock()
            .unwrap()
            .current_sst_iter(read_opt, self.inner.table_cache.clone())?;
        let iter_core = DBIteratorCore::new(
            self.inner.internal_comparator.clone(),
            mem_iters,
            vec![sst_iter],
        );
        Ok(KMergeIter::new(iter_core))
    }
}

pub struct DBImpl<S: Storage + Clone, C: Comparator> {
    env: S,
    internal_comparator: InternalKeyComparator<C>,
    options: Arc<Options<C>>,
    // The physical path of wickdb
    db_path: String,
    db_lock: Option<S::F>,

    /*
     * Fields for write batch scheduling
     */
    batch_queue: Mutex<VecDeque<BatchTask>>,
    process_batch_sem: Condvar,

    // the table cache
    table_cache: TableCache<S, C>,

    // The version set
    versions: Mutex<VersionSet<S, C>>,

    // The queue for ManualCompaction
    // All the compaction will be executed one by one once compaction is triggered
    manual_compaction_queue: Mutex<VecDeque<ManualCompaction>>,

    // signal whether the compaction finished
    background_work_finished_signal: Condvar,
    // whether we have scheduled and running a compaction
    background_compaction_scheduled: AtomicBool,
    // signal of schedule a compaction
    do_compaction: (Sender<()>, Receiver<()>),
    // Though Memtable is thread safe with multiple readers and single writers and
    // all relative methods are using immutable borrowing,
    // we still need to mutate the field `mem` and `im_mem` in some situations.
    mem: ShardedLock<MemTable<C>>,
    // There is a compacted immutable table or not
    im_mem: ShardedLock<Option<MemTable<C>>>,
    // Have we encountered a background error in paranoid mode
    bg_error: RwLock<Option<Error>>,
    // Whether the db is closing
    is_shutting_down: AtomicBool,
}

impl<S: Storage + Clone, C: Comparator> Drop for DBImpl<S, C> {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        if !self.is_shutting_down.load(Ordering::Acquire) {
            let _ = self.close();
        }
    }
}

impl<S: Storage + Clone, C: Comparator> DBImpl<S, C> {
    fn close(&self) -> Result<()> {
        self.is_shutting_down.store(true, Ordering::Release);
        match &self.db_lock {
            Some(lock) => lock.unlock(),
            None => Ok(()),
        }
    }
}

impl<S: Storage + Clone + 'static, C: Comparator + 'static> DBImpl<S, C> {
    fn new(options: Options<C>, db_path: String, storage: S) -> Self {
        let o = Arc::new(options);
        let icmp = InternalKeyComparator::new(o.comparator.clone());
        Self {
            env: storage.clone(),
            internal_comparator: icmp.clone(),
            options: o.clone(),
            db_path: db_path.clone(),
            db_lock: None,
            batch_queue: Mutex::new(VecDeque::new()),
            process_batch_sem: Condvar::new(),
            table_cache: TableCache::new(
                db_path.clone(),
                o.clone(),
                o.table_cache_size(),
                storage.clone(),
            ),
            versions: Mutex::new(VersionSet::new(db_path, o.clone(), storage)),
            manual_compaction_queue: Mutex::new(VecDeque::new()),
            background_work_finished_signal: Condvar::new(),
            background_compaction_scheduled: AtomicBool::new(false),
            do_compaction: crossbeam_channel::unbounded(),
            mem: ShardedLock::new(MemTable::new(o.write_buffer_size, icmp)),
            im_mem: ShardedLock::new(None),
            bg_error: RwLock::new(None),
            is_shutting_down: AtomicBool::new(false),
        }
    }
    fn snapshot(&self) -> Arc<Snapshot> {
        self.versions.lock().unwrap().new_snapshot()
    }

    fn get(&self, options: ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.is_shutting_down.load(Ordering::Acquire) {
            return Err(Error::DBClosed("get request".to_owned()));
        }
        let snapshot = match &options.snapshot {
            Some(snapshot) => snapshot.sequence(),
            None => self.versions.lock().unwrap().last_sequence(),
        };
        let lookup_key = LookupKey::new(key, snapshot);
        // search the memtable
        if let Some(result) = self.mem.read().unwrap().get(&lookup_key) {
            match result {
                Ok(value) => return Ok(Some(value.to_vec())),
                // mem.get only returns Err() when it get a Deletion of the key
                Err(_) => return Ok(None),
            }
        }
        // search the immutable memtable
        if let Some(im_mem) = self.im_mem.read().unwrap().as_ref() {
            if let Some(result) = im_mem.get(&lookup_key) {
                match result {
                    Ok(value) => return Ok(Some(value.to_vec())),
                    Err(_) => return Ok(None),
                }
            }
        }
        let current = self.versions.lock().unwrap().current();
        let (value, seek_stats) = current.get(options, lookup_key, &self.table_cache)?;
        if current.update_stats(seek_stats) {
            self.maybe_schedule_compaction(current);
        }
        Ok(value)
    }

    // Record a sample of bytes read at the specified internal key
    // Might schedule a background compaction.
    fn record_read_sample(&self, internal_key: &[u8]) {
        let current = self.versions.lock().unwrap().current();
        if current.record_read_sample(internal_key) {
            self.maybe_schedule_compaction(current);
        }
    }

    // Recover DB from `db_path`.
    // Returns the newest VersionEdit and whether we need to persistent VersionEdit to Manifest
    fn recover(&mut self) -> Result<(VersionEdit, bool)> {
        info!("Start recovering db : {}", &self.db_path);
        // Ignore error from `mkdir_all` since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        let _ = self.env.mkdir_all(&self.db_path);

        // Try acquire file lock
        let lock_file = self
            .env
            .create(&generate_filename(&self.db_path, FileType::Lock, 0))?;
        lock_file.lock()?;
        self.db_lock = Some(lock_file);
        if !self
            .env
            .exists(&generate_filename(&self.db_path, FileType::Current, 0))
        {
            if self.options.create_if_missing {
                // Create new necessary files for DB
                let mut new_db = VersionEdit::new(self.options.max_levels);
                new_db.set_comparator_name(self.options.comparator.name().to_owned());
                new_db.set_log_number(0);
                new_db.set_next_file(2);
                new_db.set_last_sequence(0);
                // Create manifest
                let manifest_filenum = 1;
                let manifest_filename =
                    generate_filename(&self.db_path, FileType::Manifest, manifest_filenum);
                debug!("Create manifest file: {}", &manifest_filename);
                let manifest = self.env.create(manifest_filename.as_str())?;
                let mut manifest_writer = Writer::new(manifest);
                let mut record = vec![];
                new_db.encode_to(&mut record);
                debug!("Append manifest record: {:?}", &new_db);
                match manifest_writer.add_record(&record) {
                    Ok(()) => update_current(&self.env, &self.db_path, manifest_filenum)?,
                    Err(e) => {
                        self.env.remove(manifest_filename.as_str())?;
                        return Err(e);
                    }
                }
            } else {
                return Err(Error::InvalidArgument(
                    self.db_path.to_owned() + " does not exist (create_if_missing is false)",
                ));
            }
        } else if self.options.error_if_exists {
            return Err(Error::InvalidArgument(
                self.db_path.to_owned() + " exists (error_if_exists is true)",
            ));
        }
        let mut versions = self.versions.lock().unwrap();
        let mut should_save_manifest = versions.recover()?;

        // Recover from all newer log files than the ones named in the
        // MANIFEST (new log files may have been added by the previous
        // incarnation without registering them in the MANIFEST).
        //
        // Note that PrevLogNumber() is no longer used, but we pay
        // attention to it in case we are recovering a database
        // produced by an older version of leveldb.
        let min_log = versions.log_number();
        let prev_log = versions.prev_log_number();
        let mut expected_files = versions.live_files();
        let all_files = self.env.list(&self.db_path)?;
        let mut logs_to_recover = vec![];
        for filename in all_files {
            if let Some((file_type, file_number)) = parse_filename(filename) {
                expected_files.remove(&file_number);
                if file_type == FileType::Log && (file_number >= min_log || file_number == prev_log)
                {
                    logs_to_recover.push(file_number);
                }
            }
        }
        if !expected_files.is_empty() && self.options.paranoid_checks {
            return Err(Error::Corruption(format!(
                "missing files {:?}",
                expected_files
            )));
        }

        // Recover in the order in which the logs were generated
        logs_to_recover.sort_unstable();
        let mut max_sequence = 0;
        let mut edit = VersionEdit::new(self.options.max_levels);
        for (i, log_number) in logs_to_recover.iter().enumerate() {
            let last_seq = self.replay_log_file(
                &mut versions,
                *log_number,
                i == logs_to_recover.len() - 1,
                &mut should_save_manifest,
                &mut edit,
            )?;
            if max_sequence < last_seq {
                max_sequence = last_seq
            }

            // The previous incarnation may not have written any MANIFEST
            // records after allocating this log number.  So we manually
            // update the file number allocation counter in VersionSet.
            versions.mark_file_number_used(*log_number);
        }
        if versions.last_sequence() < max_sequence {
            versions.set_last_sequence(max_sequence)
        }

        Ok((edit, should_save_manifest))
    }

    // Replays the edits in the named log file and returns the last sequence of insertions
    fn replay_log_file(
        &self,
        versions: &mut MutexGuard<VersionSet<S, C>>,
        log_number: u64,
        last_log: bool,
        save_manifest: &mut bool,
        edit: &mut VersionEdit,
    ) -> Result<u64> {
        let file_name = generate_filename(&self.db_path, FileType::Log, log_number);

        // Open the log file
        let log_file = match self.env.open(file_name.as_str()) {
            Ok(f) => f,
            Err(e) => {
                return if self.options.paranoid_checks {
                    Err(e)
                } else {
                    info!("ignore errors when replaying log file : {:?}", e);
                    Ok(0)
                }
            }
        };

        // We intentionally make Reader do checksumming even if
        // paranoid_checks is false so that corruptions cause entire commits
        // to be skipped instead of propagating bad information (like overly
        // large sequence numbers).
        let reporter = LogReporter::new();
        let mut reader = Reader::new(log_file, Some(Box::new(reporter.clone())), true, 0);
        info!("Recovering log #{}", log_number);

        // Read all the records and add to a memtable
        let mut mem = None;
        let mut record_buf = vec![];
        let mut batch = WriteBatch::default();
        let mut max_sequence = 0;
        let mut need_compaction = false; // indicates whether the memtable needs to be compacted
        let mut inserted_size = 0;
        while reader.read_record(&mut record_buf) {
            if let Err(e) = reporter.result() {
                return Err(e);
            }
            if record_buf.len() < HEADER_SIZE {
                return Err(Error::Corruption("log record too small".to_owned()));
            }
            if mem.is_none() {
                mem = Some(MemTable::new(
                    self.options.write_buffer_size,
                    self.internal_comparator.clone(),
                ))
            }
            let mem_ref = mem.as_ref().unwrap();
            batch.set_contents(&mut record_buf);
            let last_seq = batch.get_sequence() + u64::from(batch.get_count()) - 1;
            if let Err(e) = batch.insert_into(mem_ref) {
                if self.options.paranoid_checks {
                    return Err(e);
                } else {
                    info!("ignore errors when replaying log file : {:?}", e);
                }
            }
            inserted_size += batch.approximate_size();
            if last_seq > max_sequence {
                max_sequence = last_seq
            }
            if mem_ref.approximate_memory_usage() > self.options.write_buffer_size {
                need_compaction = true;
                *save_manifest = true;
                let mut iter = mem_ref.iter();
                versions.write_level0_files(
                    &self.db_path,
                    &self.table_cache,
                    &mut iter,
                    edit,
                    false,
                )?;
                mem = None;
            }
        }
        debug!(
            "{} bytes inserted into Memtable in recovering",
            inserted_size
        );
        // See if we should keep reusing the last log file.
        if self.options.reuse_logs && last_log && !need_compaction {
            let log_file = reader.into_file();
            debug!("Reusing old log file {}", file_name);
            versions.record_writer = Some(Writer::new(log_file));
            versions.set_log_number(log_number);
            if let Some(m) = mem {
                *self.mem.write().unwrap() = m;
                mem = None;
            } else {
                *self.mem.write().unwrap() = MemTable::new(
                    self.options.write_buffer_size,
                    self.internal_comparator.clone(),
                );
            }
        }
        if let Some(m) = &mem {
            debug!("Try to flush memtable into level 0 in recovering",);
            *save_manifest = true;
            let mut iter = m.iter();
            versions.write_level0_files(
                &self.db_path,
                &self.table_cache,
                &mut iter,
                edit,
                false,
            )?;
        }
        Ok(max_sequence)
    }

    // Delete any unneeded files and stale in-memory entries.
    // This func could delete generated compaction files when the compaction is failed due some reasons (e.g. block entry currupted)
    fn delete_obsolete_files(&self, mut versions: MutexGuard<VersionSet<S, C>>) -> Result<()> {
        versions.lock_live_files();
        // ignore IO error on purpose
        let files = self.env.list(&self.db_path)?;
        for file in files.iter() {
            if let Some((file_type, number)) = parse_filename(file) {
                let keep = match file_type {
                    FileType::Log => {
                        number >= versions.log_number() || number == versions.prev_log_number()
                    }
                    FileType::Manifest => number >= versions.manifest_number(),
                    FileType::Table => versions.pending_outputs.contains(&number),
                    // Any temp files that are currently being written to must
                    // be recorded in pending_outputs
                    FileType::Temp => versions.pending_outputs.contains(&number),
                    _ => true,
                };
                if !keep {
                    if file_type == FileType::Table {
                        self.table_cache.evict(number)
                    }
                    info!(
                        "Delete type={:?} #{} [filename {:?}]",
                        file_type, number, &file
                    );
                    // ignore the IO error here
                    if let Err(e) = self.env.remove(&file) {
                        error!("Delete file failed [filename {:?}]: {:?}", &file, e)
                    }
                }
            }
        }
        versions.pending_outputs.clear();
        Ok(())
    }

    // Schedule a WriteBatch to close batch processing thread for gracefully shutting down db
    fn schedule_close_batch(&self) {
        let (send, _) = crossbeam_channel::bounded(0);
        let task = BatchTask {
            stop_process: true,
            force_mem_compaction: false,
            batch: WriteBatch::default(),
            signal: send,
            options: WriteOptions::default(),
        };
        self.batch_queue.lock().unwrap().push_back(task);
        self.process_batch_sem.notify_all();
    }

    // Schedule the WriteBatch and wait for the result from the receiver.
    // This function wakes up the thread in `process_batch`.
    // An empty `WriteBatch` will trigger a force memtable compaction.
    fn schedule_batch_and_wait(
        &self,
        options: WriteOptions,
        batch: WriteBatch,
        force_mem_compaction: bool,
    ) -> Result<()> {
        if self.is_shutting_down.load(Ordering::Acquire) {
            return Err(Error::DBClosed("schedule WriteBatch".to_owned()));
        }
        if batch.is_empty() && !force_mem_compaction {
            return Ok(());
        }
        let (send, recv) = crossbeam_channel::bounded(0);
        let task = BatchTask {
            stop_process: false,
            force_mem_compaction,
            batch,
            signal: send,
            options,
        };
        self.batch_queue.lock().unwrap().push_back(task);
        self.process_batch_sem.notify_all();
        recv.recv().unwrap_or_else(|e| Err(Error::RecvError(e)))
    }

    // Group a bunch of batches in the waiting queue
    // This will ignore the task with `force_mem_compaction` after batched
    fn group_batches(&self, first: BatchTask) -> (BatchTask, Vec<Sender<Result<()>>>) {
        let mut size = first.batch.approximate_size();
        // Allow the group to grow up to a maximum size, but if the
        // original write is small, limit the growth so we do not slow
        // down the small write too much
        let mut max_size = 1 << 20;
        if size <= 128 << 10 {
            max_size = size + (128 << 10)
        }
        let mut signals = vec![first.signal.clone()];
        let mut grouped = first;

        let mut queue = self.batch_queue.lock().unwrap();
        // Group several batches from queue
        while !queue.is_empty() {
            let current = queue.pop_front().unwrap();
            if current.stop_process || (current.options.sync && !grouped.options.sync) {
                // Do not include a stop process batch
                // Do not include a sync write into a batch handled by a non-sync write.
                queue.push_front(current);
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
        (grouped, signals)
    }

    // Make sure there is enough space in memtable.
    // This method acquires the mutex of `VersionSet` and deliver it to the caller.
    // The `force` flag is used for forcing to compact current memtable into level 0
    // sst files
    fn make_room_for_write(&self, mut force: bool) -> Result<MutexGuard<VersionSet<S, C>>> {
        let mut allow_delay = !force;
        let mut versions = self.versions.lock().unwrap();
        loop {
            if let Some(e) = self.take_bg_error() {
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
                info!("Current memtable full; waiting...",);
                versions = self.background_work_finished_signal.wait(versions).unwrap();
            } else if versions.level_files_count(0) >= self.options.l0_stop_writes_threshold {
                info!(
                    "Too many L0 files {}; waiting...",
                    versions.level_files_count(0)
                );
                versions = self.background_work_finished_signal.wait(versions).unwrap();
            } else {
                let new_log_num = versions.get_next_file_number();
                let log_file = self.env.create(
                    &generate_filename(&self.db_path, FileType::Log, new_log_num).as_str(),
                )?;
                versions.set_next_file_number(new_log_num + 1);
                versions.set_log_number(new_log_num);
                versions.record_writer = Some(Writer::new(log_file));
                // rotate the mem to immutable mem
                {
                    let mut mem = self.mem.write().unwrap();
                    if mem.len() > 0 {
                        let memtable = mem::replace(
                            &mut *mem,
                            MemTable::new(
                                self.options.write_buffer_size,
                                self.internal_comparator.clone(),
                            ),
                        );
                        let mut im_mem = self.im_mem.write().unwrap();
                        *im_mem = Some(memtable);
                    }
                    force = false; // do not force another compaction if have room
                }
                self.maybe_schedule_compaction(versions.current());
            }
        }
        Ok(versions)
    }

    // Compact immutable memory table to level0 files
    fn compact_mem_table(&self) -> Result<()> {
        debug!("Compact memtable");
        let mut versions = self.versions.lock().unwrap();
        let mut edit = VersionEdit::new(self.options.max_levels);
        let mut im_mem = self.im_mem.write().unwrap();
        let mut iter = im_mem.as_ref().unwrap().iter();
        versions.write_level0_files(
            &self.db_path,
            &self.table_cache,
            &mut iter,
            &mut edit,
            true,
        )?;
        if self.is_shutting_down.load(Ordering::Acquire) {
            Err(Error::DBClosed("when compacting memory table".to_owned()))
        } else {
            edit.prev_log_number = Some(0);
            edit.log_number = Some(versions.log_number()); // earlier logs no longer needed
            let res = versions.log_and_apply(edit);
            *im_mem = None;
            self.delete_obsolete_files(versions)?;
            res
        }
    }

    // Force current memtable contents(even if the memtable is not full) to be compacted into sst files
    fn force_compact_mem_table(&self) -> Result<()> {
        let empty_batch = WriteBatch::default();
        // Schedule a force memory compaction
        self.schedule_batch_and_wait(WriteOptions::default(), empty_batch, true)?;
        // Waiting for memory compaction complete
        // TODO: This is not safe because there could be several compaction triggered continously
        thread::sleep(Duration::from_secs(1));
        if self.im_mem.read().unwrap().is_some() {
            return self.take_bg_error().map_or(Ok(()), Err);
        }
        assert_eq!(self.mem.read().unwrap().len(), 0);
        Ok(())
    }

    // Compact the underlying storage for the key range `[begin, end]`.
    //
    // In particular, deleted and overwritten versions are discarded,
    // and the data is rearranged to reduce the cost of operations
    // needed to access the data.
    //
    // This operation should typically only be invoked by users
    // who understand the underlying implementation.
    //
    // A `None` is treated as a key before all keys for `begin`
    // and a key after all keys for `end` in the database.
    fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        let mut max_level_with_files = 1;
        {
            let versions = self.versions.lock().unwrap();
            let current = versions.current();
            for l in 1..self.options.max_levels as usize {
                if current.overlap_in_level(l, begin, end) {
                    max_level_with_files = l;
                }
            }
        }
        self.force_compact_mem_table()?;
        for l in 0..max_level_with_files {
            self.manual_compact_range(l, begin, end)?
        }
        Ok(())
    }

    // Schedules a manual compaction for the key range `[begin, end]` and waits util the
    // compaction completes
    fn manual_compact_range(
        &self,
        level: usize,
        begin: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<()> {
        assert!(level + 1 < self.options.max_levels as usize);
        let (sender, finished) = crossbeam_channel::bounded(1);
        {
            let mut m_queue = self.manual_compaction_queue.lock().unwrap();
            m_queue.push_back(ManualCompaction {
                level,
                done: sender,
                begin: begin.map(|k| InternalKey::new(k, MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK)),
                end: end.map(|k| InternalKey::new(k, 0, ValueType::Value)),
            });
        }
        let v = self.versions.lock().unwrap().current();
        self.maybe_schedule_compaction(v);
        match finished.recv() {
            Ok(res) => res,
            Err(e) => Err(Error::RecvError(e)),
        }
    }

    // The complete compaction process
    // Returns true if a compaction is actually scheduled
    fn background_compaction(&self) -> bool {
        if self.im_mem.read().unwrap().is_some() {
            if let Err(e) = self.compact_mem_table() {
                warn!("Compact memtable error: {:?}", e);
            }
            true
        } else {
            let mut versions = self.versions.lock().unwrap();
            let mut is_manual = false;
            let (compaction, done) = {
                if let Some(manual) = self.manual_compaction_queue.lock().unwrap().pop_front() {
                    is_manual = true;
                    let begin = if let Some(begin) = &manual.begin {
                        format!("{:?}", begin)
                    } else {
                        "(-∞)".to_owned()
                    };
                    let end = if let Some(end) = &manual.end {
                        format!("{:?}", end)
                    } else {
                        "(+∞)".to_owned()
                    };
                    match versions.compact_range(
                        manual.level,
                        manual.begin.as_ref(),
                        manual.end.as_ref(),
                    ) {
                        Some(c) => {
                            info!(
                                "Received manual compaction at level {} from {} .. {}; will stop at {:?}",
                                manual.level, begin, end, &c.inputs.base.last().unwrap().largest
                            );
                            (Some(c), Some(manual.done))
                        }
                        None => {
                            info!("Received manual compaction at level {} from {} .. {}; No compaction needs to be done", manual.level, begin, end);
                            manual.done.send(Ok(())).unwrap();
                            (None, None)
                        }
                    }
                } else {
                    (versions.pick_compaction(), None)
                }
            };
            let has_compaction = compaction.is_some();
            if let Some(mut compaction) = compaction {
                let level = compaction.level;
                info!(
                    "[{:?}] Compacting [{}]@{} + [{}]@{} files",
                    compaction.reason,
                    compaction.inputs.desc_base_files(),
                    level,
                    compaction.inputs.desc_parent_files(),
                    level + 1
                );
                if !is_manual && compaction.is_trivial_move() {
                    // just move file to next level
                    let f = compaction.inputs.base.first().unwrap();
                    compaction.edit.delete_file(compaction.level, f.number);
                    compaction.edit.add_file(
                        compaction.level + 1,
                        f.number,
                        f.file_size,
                        f.smallest.clone(),
                        f.largest.clone(),
                    );
                    let res = versions.log_and_apply(compaction.edit);
                    if let Err(e) = res.as_ref() {
                        error!("Compaction error: {}", e);
                    }
                    let current_summary = versions.current().level_summary();
                    info!(
                        "Moved #{} to level-{} {} bytes, current level summary: {}",
                        f.number,
                        compaction.level + 1,
                        f.file_size,
                        current_summary
                    );
                    if let Some(done) = done {
                        done.send(res).unwrap();
                    }
                    if let Err(e) = self.delete_obsolete_files(versions) {
                        error!("Delete obsolete files error: {}", e);
                    }
                } else {
                    {
                        let snapshots = &mut versions.snapshots;
                        // Cleanup all redundant snapshots first
                        snapshots.gc();
                        if snapshots.is_empty() {
                            compaction.oldest_snapshot_alive = versions.last_sequence();
                        } else {
                            compaction.oldest_snapshot_alive = snapshots.oldest().sequence();
                        }
                    }
                    // Unlock VersionSet here to avoid dead lock
                    mem::drop(versions);
                    match self.do_compaction(compaction) {
                        Ok(versions) => {
                            let res = self.delete_obsolete_files(versions);
                            if let Some(done) = done {
                                done.send(res).unwrap();
                            }
                        }
                        Err(e) => {
                            {
                                let versions = self.versions.lock().unwrap();
                                let _ = self.delete_obsolete_files(versions);
                            }
                            error!("Compaction error: {:?}", &e);
                            if let Some(done) = done {
                                done.send(Err(e)).unwrap();
                            }
                        }
                    }
                };
            }
            has_compaction
        }
    }

    // Merging files in level n into file in level n + 1 and keep the still-in-use files
    // This func could compact memtable first if the writing is still on-going
    // `delete_obsolete_files` must be called even if this returns an error
    fn do_compaction(&self, mut c: Compaction<S::F, C>) -> Result<MutexGuard<VersionSet<S, C>>> {
        let now = Instant::now();
        let mut input_iter =
            c.new_input_iterator(self.internal_comparator.clone(), self.table_cache.clone())?;
        let mut mem_compaction_duration = 0;
        input_iter.seek_to_first();

        let mut last_sequence_for_key = u64::max_value();
        // TODO: Use Option<&[u8]> instead
        let mut current_ukey: Option<Vec<u8>> = None;
        while input_iter.valid() && !self.is_shutting_down.load(Ordering::Acquire) {
            if self.im_mem.read().unwrap().is_some() {
                let imm_start = Instant::now();
                self.compact_mem_table()?;
                mem_compaction_duration += imm_start.elapsed().as_micros() as u64;
            }
            let iter_status = input_iter.status();
            let ikey = input_iter.key();
            // Checkout whether we need rotate a new output file
            if c.should_stop_before(ikey, &self.internal_comparator) && c.builder.is_some() {
                self.finish_output_file(&mut c, iter_status)?
            }
            let mut drop = false;
            let ucmp = &self.internal_comparator.user_comparator;
            match ParsedInternalKey::decode_from(ikey) {
                Some(key) => {
                    if current_ukey.is_none()
                        || ucmp.compare(key.user_key, current_ukey.as_ref().unwrap())
                            != CmpOrdering::Equal
                    {
                        // First occurrence of this user key
                        current_ukey = Some(key.user_key.to_vec());
                        last_sequence_for_key = u64::max_value();
                    }
                    // Keep the still-in-use old key or not
                    if last_sequence_for_key <= c.oldest_snapshot_alive
                        || (key.value_type == ValueType::Deletion
                            && key.seq <= c.oldest_snapshot_alive
                            && !c.key_exist_in_deeper_level(key.user_key))
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
                            self.versions
                                .lock()
                                .unwrap()
                                .create_compaction_output_file(&mut c)?;
                        }
                        let last = c.outputs.len() - 1;
                        if c.builder.as_ref().unwrap().num_entries() == 0 {
                            // We have a brand new builder so use current key as smallest
                            c.outputs[last].smallest = InternalKey::decoded_from(ikey);
                        }
                        // Keep updating the largest
                        c.outputs[last].largest = InternalKey::decoded_from(ikey);
                        c.builder.as_mut().unwrap().add(ikey, input_iter.value())?;
                        let builder = c.builder.as_ref().unwrap();
                        // Rotate a new output file if the current one is big enough
                        if builder.file_size() >= self.options.max_file_size {
                            self.finish_output_file(&mut c, input_iter.status())?;
                        }
                    }
                }
                None => {
                    current_ukey = None;
                    last_sequence_for_key = u64::max_value();
                }
            }
            input_iter.next();
        }
        if self.is_shutting_down.load(Ordering::Acquire) {
            return Err(Error::DBClosed("major compaction".to_owned()));
        }
        if c.builder.is_some() {
            self.finish_output_file(&mut c, input_iter.status())?;
        }
        // Close unclosed table builder and remove files in `pending_outputs`
        if let Some(builder) = c.builder.as_mut() {
            builder.close()
        }
        info!(
            "Compactions stats for Level{}: {:?}",
            c.level,
            CompactionStats {
                micros: now.elapsed().as_micros() as u64 - mem_compaction_duration,
                bytes_read: c.bytes_read(),
                bytes_written: c.bytes_written(),
            }
        );
        let mut versions = self.versions.lock().unwrap();
        for output in c.outputs.iter() {
            versions.pending_outputs.remove(&output.number);
        }
        if let Ok(()) = input_iter.status() {
            info!(
                "Compacted {}@{} + {}@{} files => {} bytes",
                c.inputs.desc_base_files(),
                c.level,
                c.inputs.desc_parent_files(),
                c.level + 1,
                c.total_bytes,
            );
            c.apply_to_edit();
            mem::drop(c.input_version);
            versions.log_and_apply(c.edit)?;
        }
        Ok(versions)
    }

    // Replace the `bg_error` with new `Error` if it's `None`
    fn record_bg_error(&self, e: Error) {
        if !self.has_bg_error() {
            let mut x = self.bg_error.write().unwrap();
            *x = Some(e);
            self.background_work_finished_signal.notify_all();
        }
    }

    fn take_bg_error(&self) -> Option<Error> {
        self.bg_error.write().unwrap().take()
    }

    fn has_bg_error(&self) -> bool {
        self.bg_error.read().unwrap().is_some()
    }

    // Check whether db needs to run a compaction. DB will run a compaction when:
    // 1. no background compaction is running
    // 2. DB is not shutting down
    // 3. no error has been encountered
    // 4. there is an immutable table or a manual compaction request or current version needs to be compacted
    fn maybe_schedule_compaction(&self, version: Arc<Version<C>>) -> bool {
        if self.background_compaction_scheduled.load(Ordering::Acquire)
            // Already scheduled
            || self.is_shutting_down.load(Ordering::Acquire)
            // DB is being shutting down
            || self.has_bg_error()
            // Got err
            || (self.im_mem.read().unwrap().is_none()
            && self.manual_compaction_queue.lock().unwrap().is_empty() && !version.needs_compaction())
        {
            // No work needs to be done
            false
        } else {
            self.background_compaction_scheduled
                .store(true, Ordering::Release);
            if let Err(e) = self.do_compaction.0.send(()) {
                error!(
                    "[schedule compaction] Fail sending signal to compaction channel: {}",
                    e
                )
            }
            true
        }
    }

    // Finish the current output file by calling `builder.finish` and insert it into the table cache
    fn finish_output_file(
        &self,
        c: &mut Compaction<S::F, C>,
        input_iter_status: Result<()>,
    ) -> Result<()> {
        assert!(!c.outputs.is_empty());
        assert!(c.builder.is_some());
        let current_entries = c.builder.as_ref().unwrap().num_entries();
        let status = if input_iter_status.is_ok() {
            c.builder.as_mut().unwrap().finish(true)
        } else {
            c.builder.as_mut().unwrap().close();
            input_iter_status
        };
        let current_bytes = c.builder.as_ref().unwrap().file_size();
        // update current output
        c.outputs.last_mut().unwrap().file_size = current_bytes;
        c.total_bytes += current_bytes;
        c.builder = None;
        if status.is_ok() && current_entries > 0 {
            let f = c.outputs.last().unwrap();
            let _ = self.table_cache.new_iter(
                self.internal_comparator.clone(),
                ReadOptions::default(),
                f.number,
                f.file_size,
            )?;
            info!(
                "Compaction output table #{}@{}: {} keys, {} bytes, [{:?} ... {:?}]",
                f.number,
                c.level + 1,
                current_entries,
                f.file_size,
                f.smallest,
                f.largest,
            );
        }
        status
    }

    // Returns the approximate file system space used by keys in "[start .. end)"
    //
    // Note that the returned sizes measure file system space usage, so
    // if the user data compresses by a factor of ten, the returned
    // sizes will be one-tenth the size of the corresponding user data size.
    //
    // The results may not include the sizes of recently written data.
    pub fn get_approximate_size(&self, start: &[u8], end: &[u8]) -> u64 {
        let current = self.versions.lock().unwrap().current();
        let start_ikey = InternalKey::new(start, MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK);
        let end_ikey = InternalKey::new(end, MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK);
        let start = current.approximate_offset_of(&start_ikey, &self.table_cache);
        let limit = current.approximate_offset_of(&end_ikey, &self.table_cache);
        if limit >= start {
            limit - start
        } else {
            0
        }
    }
}

// A wrapper struct for scheduling `WriteBatch`
struct BatchTask {
    // flag for shutdown the batch processing thread gracefully
    stop_process: bool,
    force_mem_compaction: bool,
    batch: WriteBatch,
    signal: Sender<Result<()>>,
    options: WriteOptions,
}

// Build a Table file from the contents of `iter`.  The generated file
// will be named according to `meta.number`.  On success, the rest of
// meta will be filled with metadata about the generated table.
// If no data is present in iter, `meta.file_size` will be set to
// zero, and no Table file will be produced.
pub(crate) fn build_table<S: Storage + Clone, C: Comparator + 'static>(
    options: Arc<Options<C>>,
    storage: &S,
    db_path: &str,
    table_cache: &TableCache<S, C>,
    iter: &mut dyn Iterator,
    meta: &mut FileMetaData,
) -> Result<()> {
    meta.file_size = 0;
    iter.seek_to_first();
    let file_name = generate_filename(db_path, FileType::Table, meta.number);
    let mut status = Ok(());
    if iter.valid() {
        let file = storage.create(file_name.as_str())?;
        let icmp = InternalKeyComparator::new(options.comparator.clone());
        let mut builder = TableBuilder::new(file, icmp.clone(), &options);
        let mut prev_key = vec![];
        meta.smallest = InternalKey::decoded_from(iter.key());
        while iter.valid() {
            let key = iter.key().to_vec();
            let s = builder.add(&key, iter.value());
            if s.is_err() {
                status = s;
                break;
            }
            prev_key = key;
            iter.next();
        }
        if !prev_key.is_empty() {
            meta.largest = InternalKey::decoded_from(&prev_key);
        }
        if status.is_ok() {
            status = builder.finish(true).and_then(|_| {
                meta.file_size = builder.file_size();
                assert!(meta.file_size > 0);
                // make sure that the new file is in the cache
                let mut it = table_cache.new_iter(
                    icmp,
                    ReadOptions::default(),
                    meta.number,
                    meta.file_size,
                )?;
                it.status()
            });
        }
    }
    let iter_status = iter.status();
    if iter_status.is_err() {
        status = iter_status;
    };
    if status.is_err() || meta.file_size == 0 {
        storage.remove(file_name.as_str())?;
        status
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mem::MemStorage;
    use crate::{BloomFilter, BytewiseComparator, CompressionType, Options};
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use std::ops::{Deref, DerefMut};
    use std::str;
    use std::sync::atomic::AtomicUsize;

    impl<S: Storage + Clone, C: Comparator + 'static> WickDB<S, C> {
        fn options(&self) -> Arc<Options<C>> {
            self.inner.options.clone()
        }

        fn total_sst_files(&self) -> usize {
            let versions = self.inner.versions.lock().unwrap();
            let mut res = 0;
            for l in 0..self.options().max_levels as usize {
                res += versions.level_files_count(l);
            }
            res
        }

        fn file_count_per_level(&self) -> String {
            let mut res = String::new();
            let versions = self.inner.versions.lock().unwrap();
            for l in 0..self.options().max_levels as usize {
                let count = versions.level_files_count(l);
                res.push_str(&count.to_string());
                res.push(',');
            }
            res.trim_end_matches("0,").trim_end_matches(",").to_owned()
        }
    }

    #[derive(Debug, Clone, Copy, FromPrimitive)]
    enum TestOption {
        Default,
        // Enable `reuse_log`
        Reuse,
        // Use Bloom Filter as the filter policy
        FilterPolicy,
        // No compression enabled
        UnCompressed,
    }

    impl From<u8> for TestOption {
        fn from(src: u8) -> TestOption {
            num_traits::FromPrimitive::from_u8(src).unwrap()
        }
    }

    fn new_test_options(o: TestOption) -> Options<BytewiseComparator> {
        let opt = match o {
            TestOption::Default => Options::default(),
            TestOption::Reuse => {
                let mut o = Options::default();
                o.reuse_logs = true;
                o
            }
            TestOption::FilterPolicy => {
                let filter = BloomFilter::new(10);
                let mut o = Options::default();
                o.filter_policy = Some(Arc::new(filter));
                o
            }
            TestOption::UnCompressed => {
                let mut o = Options::default();
                o.compression = CompressionType::NoCompression;
                o
            }
        };
        opt
    }

    fn iter_to_string(iter: &dyn Iterator) -> String {
        if iter.valid() {
            format!(
                "{}->{}",
                str::from_utf8(iter.key()).unwrap(),
                str::from_utf8(iter.value()).unwrap()
            )
        } else {
            "(invalid)".to_owned()
        }
    }

    fn default_cases() -> Vec<DBTest> {
        cases(|opt| opt)
    }

    fn cases<F>(mut opt_hook: F) -> Vec<DBTest>
    where
        F: FnMut(Options<BytewiseComparator>) -> Options<BytewiseComparator>,
    {
        vec![
            TestOption::Default,
            TestOption::Reuse,
            TestOption::FilterPolicy,
            TestOption::UnCompressed,
        ]
        .into_iter()
        .map(|opt| {
            let options = opt_hook(new_test_options(opt));
            DBTest::new(options)
        })
        .collect()
    }

    struct DBTest {
        // Used as the db's inner storage
        store: MemStorage,
        // Used as the db's options
        opt: Options<BytewiseComparator>,
        db: WickDB<MemStorage, BytewiseComparator>,
    }

    impl DBTest {
        fn new(opt: Options<BytewiseComparator>) -> Self {
            let store = MemStorage::default();
            let name = "db_test";
            let db = WickDB::open_db(opt.clone(), name, store.clone()).unwrap();
            DBTest { store, opt, db }
        }

        // Close the inner db without destroy the contents and establish a new WickDB on same db path with same option
        fn reopen(&mut self) -> Result<()> {
            self.db.close()?;
            let db = WickDB::open_db(self.opt.clone(), &self.db.inner.db_path, self.store.clone())?;
            self.db = db;
            Ok(())
        }

        // Put entries with default `WriteOptions`
        fn put_entries(&self, entries: Vec<(&str, &str)>) {
            for (k, v) in entries {
                self.db
                    .put(WriteOptions::default(), k.as_bytes(), v.as_bytes())
                    .unwrap()
            }
        }

        fn put(&self, k: &str, v: &str) -> Result<()> {
            self.db
                .put(WriteOptions::default(), k.as_bytes(), v.as_bytes())
        }

        fn delete(&self, k: &str) -> Result<()> {
            self.db.delete(WriteOptions::default(), k.as_bytes())
        }

        fn get(&self, k: &str, snapshot: Option<Snapshot>) -> Option<String> {
            let mut read_opt = ReadOptions::default();
            read_opt.snapshot = snapshot;
            match self.db.get(read_opt, k.as_bytes()) {
                Ok(v) => v.and_then(|v| Some(unsafe { String::from_utf8_unchecked(v) })),
                Err(_) => None,
            }
        }
        fn assert_get(&self, k: &str, expect: Option<&str>) {
            match self.db.get(ReadOptions::default(), k.as_bytes()) {
                Ok(v) => match v {
                    Some(s) => {
                        let bytes = s.as_slice();
                        let expect = expect.map(|s| s.as_bytes());
                        if bytes.len() > 1000 {
                            if expect != Some(bytes) {
                                panic!("expect(len={}), but got(len={}), not equal contents, key: {}, got: {:?}..., expect: {:?}...", expect.map_or(0, |s| s.len()), bytes.len(), k, &bytes[..50], &expect.unwrap()[..50]);
                            }
                        } else {
                            assert_eq!(expect, Some(bytes), "key: {}", k);
                        }
                    }
                    None => assert_eq!(expect, None, "key: {}", k),
                },
                Err(e) => panic!("got error {:?}, key: {}", e, k),
            }
        }

        fn must_release_snapshot(&self, s: Arc<Snapshot>) {
            assert!(self.release_snapshot(s))
        }

        // Return a string that contains all key,value pairs in order,
        // formatted like "(k1->v1)(k2->v2)...".
        // Also checks the db iterator works fine in both forward and backward direction
        fn assert_contents(&self) -> String {
            let mut iter = self.db.iter(ReadOptions::default()).unwrap();
            iter.seek_to_first();
            let mut result = String::new();
            let mut backward = vec![];
            while iter.valid() {
                let s = iter_to_string(&iter);
                result.push('(');
                result.push_str(&s);
                result.push(')');
                backward.push(s);
                iter.next();
            }

            // Chech reverse iteration results are reverse of forward results
            backward.reverse();
            iter.seek_to_last();
            let mut matched = 0;
            while iter.valid() {
                assert!(matched < backward.len());
                assert_eq!(iter_to_string(&iter), backward[matched]);
                iter.prev();
                matched += 1
            }
            assert_eq!(matched, backward.len());
            result
        }

        // Return all the values for the given `user_key`
        fn all_entires_for(&self, user_key: &[u8]) -> String {
            let mut iter = self.db.internal_iter(ReadOptions::default()).unwrap();
            let ikey = InternalKey::new(user_key, MAX_KEY_SEQUENCE, ValueType::Value);
            iter.seek(ikey.data());
            let mut result = String::new();
            if iter.valid() {
                result.push_str("[ ");
                let mut first = true;
                while iter.valid() {
                    match ParsedInternalKey::decode_from(iter.key()) {
                        None => result.push_str("CORRUPTED"),
                        Some(pkey) => {
                            if self
                                .db
                                .options()
                                .comparator
                                .compare(&pkey.user_key, user_key)
                                != CmpOrdering::Equal
                            {
                                break;
                            }
                            if !first {
                                result.push_str(", ");
                            }
                            first = false;
                            match pkey.value_type {
                                ValueType::Value => {
                                    result.push_str(str::from_utf8(iter.value()).unwrap())
                                }
                                ValueType::Deletion => result.push_str("DEL"),
                                ValueType::Unknown => result.push_str("UNKNOWN"),
                            }
                        }
                    }
                    iter.next();
                }
                if !first {
                    result.push_str(" ");
                }
                result.push_str("]");
            } else {
                result = iter.status().unwrap_err().to_string();
            }
            result
        }

        fn compact(&self, begin: Option<&str>, end: Option<&str>) {
            self.db
                .inner
                .compact_range(begin.map(|s| s.as_bytes()), end.map(|s| s.as_bytes()))
                .unwrap()
        }

        // Do `n` memtable compactions, each of which produces an sstable
        // covering the key range `[begin,end]`.
        fn make_sst_files(&self, n: usize, begin: &str, end: &str) {
            for _ in 0..n {
                self.put(begin, "begin").unwrap();
                self.put(end, "end").unwrap();
                self.db.inner.force_compact_mem_table().unwrap();
            }
        }

        // Prevent pushing of new sstables into deeper levels by adding
        // tables that cover a specified range to all levels
        fn fill_levels(&self, begin: &str, end: &str) {
            self.make_sst_files(self.db.options().max_levels as usize, begin, end)
        }

        fn assert_put_get(&self, key: &str, value: &str) {
            self.put(key, value).unwrap();
            assert_eq!(value, self.get(key, None).unwrap());
        }

        fn num_sst_files_at_level(&self, level: usize) -> usize {
            self.inner.versions.lock().unwrap().level_files_count(level)
        }

        // Check the number of sst files at `level` in current version
        fn assert_file_num_at_level(&self, level: usize, expect: usize) {
            assert_eq!(self.num_sst_files_at_level(level), expect);
        }

        // Check all the number of sst files at each level in current version
        fn assert_file_num_at_each_level(&self, expect: Vec<usize>) {
            let current = self.inner.versions.lock().unwrap().current();
            let max_level = self.options().max_levels as usize;
            let mut got = Vec::with_capacity(max_level);
            for l in 0..max_level {
                got.push(current.get_level_files(l).len());
            }
            assert_eq!(got, expect);
        }

        // Print all sst files at current version
        fn print_sst_files(&self) {
            let current = self.inner.versions.lock().unwrap().current();
            println!("{:?}", current);
        }

        fn assert_approximate_size(&self, start: &str, end: &str, a: usize, b: usize) {
            let s = self
                .inner
                .get_approximate_size(start.as_bytes(), end.as_bytes());
            assert!(
                s <= b as u64 && s >= a as u64,
                "approximate size between '{}' - '{}' should be between [{}, {}], but got {}",
                start,
                end,
                a,
                b,
                s
            );
        }

        // Delete a sst file randomly
        fn delete_one_sst_file(&self) -> Result<bool> {
            let files = self.store.list(&self.inner.db_path)?;
            for f in files {
                if let Some((tp, _)) = parse_filename(&f) {
                    if tp == FileType::Table {
                        self.store.remove(&f)?;
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
    }

    impl Default for DBTest {
        fn default() -> Self {
            let store = MemStorage::default();
            let name = "db_test";
            let opt = new_test_options(TestOption::Default);
            let db = WickDB::open_db(opt.clone(), name, store.clone()).unwrap();
            DBTest { store, opt, db }
        }
    }

    impl Deref for DBTest {
        type Target = WickDB<MemStorage, BytewiseComparator>;
        fn deref(&self) -> &Self::Target {
            &self.db
        }
    }

    impl DerefMut for DBTest {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.db
        }
    }

    fn rand_string(n: usize) -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(n)
            .collect::<String>()
    }

    fn key(i: usize) -> String {
        format!("key{:06}", i)
    }

    #[test]
    fn test_empty_db() {
        for t in default_cases() {
            assert_eq!(None, t.get("foo", None))
        }
    }

    #[test]
    fn test_empty_key() {
        for t in default_cases() {
            t.assert_put_get("", "v1");
            t.assert_put_get("", "v2");
        }
    }

    #[test]
    fn test_empty_value() {
        for t in default_cases() {
            t.assert_put_get("key", "v1");
            t.assert_put_get("key", "");
            t.assert_put_get("key", "v2");
        }
    }

    #[test]
    fn test_read_write() {
        for t in default_cases() {
            t.assert_put_get("foo", "v1");
            t.put("bar", "v2").unwrap();
            t.put("foo", "v3").unwrap();
            assert_eq!("v3", t.get("foo", None).unwrap());
            assert_eq!("v2", t.get("bar", None).unwrap());
        }
    }

    #[test]
    fn test_put_delete_get() {
        for t in default_cases() {
            t.assert_put_get("foo", "v1");
            t.assert_put_get("foo", "v2");
            t.delete("foo").unwrap();
            assert_eq!(None, t.get("foo", None));
        }
    }

    #[test]
    // Test getting kv from immutable memtable and SSTable
    fn test_get_from_immutable_layer() {
        for t in cases(|mut opt| {
            opt.write_buffer_size = 100000; // Small write buffer
            opt
        }) {
            t.assert_put_get("foo", "v1");
            // block `flush()`
            t.store.delay_data_sync.store(true, Ordering::Release);
            t.put("k1", &"x".repeat(100000)).unwrap(); // fill memtable
            assert_eq!("v1", t.get("foo", None).unwrap()); // "v1" on immutable table
            t.put("k2", &"y".repeat(100000)).unwrap(); // trigger compaction
                                                       // Waiting for compaction finish
            thread::sleep(Duration::from_secs(2));
            t.assert_file_num_at_level(2, 1);
            // Try to retrieve key "foo" from level 0 files
            t.assert_get("k1", Some(&"x".repeat(100000)));
            assert_eq!("v1", t.get("foo", None).unwrap()); // "v1" on SST files
            t.assert_get("k2", Some(&"y".repeat(100000)));
        }
    }

    #[test]
    // Test `force_compact_mem_table` and kv look up after compaction
    fn test_get_from_versions() {
        for t in default_cases() {
            t.assert_put_get("foo", "v1");
            t.inner.force_compact_mem_table().unwrap();
            assert_eq!("v1", t.get("foo", None).unwrap());
        }
    }

    #[test]
    // Test look up key with snapshot
    fn test_get_with_snapshot() {
        for t in default_cases() {
            for key in vec![String::from("foo"), "x".repeat(20)] {
                t.assert_put_get(&key, "v1");
                let s = t.db.snapshot();
                t.put(&key, "v2").unwrap();
                assert_eq!(t.get(&key, None).unwrap(), "v2");
                assert_eq!(t.get(&key, Some(s.sequence().into())).unwrap(), "v1");
                t.inner.force_compact_mem_table().unwrap();
                assert_eq!(t.get(&key, None).unwrap(), "v2");
                assert_eq!(t.get(&key, Some(s.sequence().into())).unwrap(), "v1");
            }
        }
    }

    // Ensure `get` returns same result with the same snapshot and the same key
    #[test]
    fn test_get_with_identical_snapshots() {
        for t in default_cases() {
            let keys = vec![String::from("foo"), "x".repeat(200)];
            for key in keys {
                t.assert_put_get(&key, "v1");
                let s1 = t.snapshot();
                let s2 = t.snapshot();
                let s3 = t.snapshot();
                t.assert_put_get(&key, "v2");
                assert_eq!(t.get(&key, Some(s1.sequence().into())).unwrap(), "v1");
                assert_eq!(t.get(&key, Some(s2.sequence().into())).unwrap(), "v1");
                assert_eq!(t.get(&key, Some(s3.sequence().into())).unwrap(), "v1");
                mem::drop(s1);
                t.inner.force_compact_mem_table().unwrap();
                assert_eq!(t.get(&key, None).unwrap(), "v2");
                assert_eq!(t.get(&key, Some(s2.sequence().into())).unwrap(), "v1");
                mem::drop(s2);
                assert_eq!(t.get(&key, Some(s3.sequence().into())).unwrap(), "v1");
            }
        }
    }

    #[test]
    fn test_iterate_over_empty_snapshot() {
        for t in default_cases() {
            let s = t.snapshot();
            let mut read_opt = ReadOptions::default();
            read_opt.snapshot = Some(s.sequence().into());
            t.put("foo", "v1").unwrap();
            t.put("foo", "v2").unwrap();
            let mut iter = t.iter(read_opt).unwrap();
            iter.seek_to_first();
            // No entry at this snapshot
            assert!(!iter.valid());
            // flush entries into sst file
            t.inner.force_compact_mem_table().unwrap();
            let mut iter = t.iter(read_opt).unwrap();
            iter.seek_to_first();
            assert!(!iter.valid());
        }
    }

    // Test that "get" always retrieve entries from the right sst file
    #[test]
    fn test_get_level0_ordering() {
        for t in default_cases() {
            t.put("bar", "b").unwrap();
            t.put("foo", "v1").unwrap();
            t.inner.force_compact_mem_table().unwrap();
            t.assert_file_num_at_level(2, 1);
            t.put("foo", "v2").unwrap();
            t.inner.force_compact_mem_table().unwrap();
            // The 2nd sst file is placed at level1 because the key "foo" is overlapped with
            // sst file in level 2 (produced by last "force_compact_mem_table")
            t.assert_file_num_at_each_level(vec![0, 1, 1, 0, 0, 0, 0]);
            assert_eq!(t.get("foo", None).unwrap(), "v2");
        }
    }

    #[test]
    fn test_get_ordered_by_levels() {
        for t in default_cases() {
            t.put("foo", "v1").unwrap();
            t.compact(Some("a"), Some("z"));
            assert_eq!(t.get("foo", None).unwrap(), "v1");
            t.put("foo", "v2").unwrap();
            t.inner.force_compact_mem_table().unwrap();
            assert_eq!(t.get("foo", None).unwrap(), "v2");
        }
    }

    #[test]
    fn test_pick_correct_file() {
        for t in default_cases() {
            t.put("a", "va").unwrap();
            t.compact(Some("a"), Some("b"));
            t.put("x", "vx").unwrap();
            t.compact(Some("x"), Some("y"));
            t.put("f", "vf").unwrap();
            t.compact(Some("f"), Some("g"));
            // Each sst file's key range doesn't overlap. So all the sst files are
            // placed at level 2
            t.assert_file_num_at_level(2, 3);
            t.print_sst_files();
            assert_eq!(t.get("a", None).unwrap(), "va");
            assert_eq!(t.get("x", None).unwrap(), "vx");
            assert_eq!(t.get("f", None).unwrap(), "vf");
        }
    }

    #[test]
    fn test_get_encounters_empty_level() {
        for t in default_cases() {
            // Arrange for the following to happen:
            //   * sstable A in level 0
            //   * nothing in level 1
            //   * sstable B in level 2
            // Then do enough Get() calls to arrange for an automatic compaction
            // of sstable A.  A bug would cause the compaction to be marked as
            // occurring at level 1 (instead of the correct level 0).

            // Step 1: First place sstables in levels 0 and 2
            while t.num_sst_files_at_level(0) == 0 || t.num_sst_files_at_level(2) == 0 {
                t.put("a", "begin").unwrap();
                t.put("z", "end").unwrap();
                t.inner.force_compact_mem_table().unwrap();
            }
            t.assert_file_num_at_level(0, 1);
            t.assert_file_num_at_level(1, 1);
            t.assert_file_num_at_level(2, 1);

            // Read a bunch of times to trigger compaction (drain `allow_seek`)
            for _ in 0..1000 {
                assert_eq!(t.get("missing", None), None);
            }
            // Wait for compaction to finish
            thread::sleep(Duration::from_secs(1));
            t.assert_file_num_at_level(0, 0);
            t.assert_file_num_at_level(1, 0);
            t.assert_file_num_at_level(2, 1);
        }
    }

    #[test]
    fn test_iter_empty_db() {
        let t = DBTest::default();
        let mut iter = t.iter(ReadOptions::default()).unwrap();
        iter.seek_to_first();
        assert!(!iter.valid());
        iter.seek_to_last();
        assert!(!iter.valid());
        iter.seek(b"foo");
        assert!(!iter.valid());
    }

    fn assert_iter_entry(iter: &dyn Iterator, k: &str, v: &str) {
        assert_eq!(str::from_utf8(iter.key()).unwrap(), k);
        assert_eq!(str::from_utf8(iter.value()).unwrap(), v);
    }

    #[test]
    fn test_iter_single() {
        let t = DBTest::default();
        t.put("a", "va").unwrap();
        let mut iter = t.iter(ReadOptions::default()).unwrap();
        iter.seek_to_first();
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert!(!iter.valid());
        iter.seek_to_first();
        assert_iter_entry(&iter, "a", "va");
        iter.prev();
        assert!(!iter.valid());

        iter.seek_to_last();
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert!(!iter.valid());
        iter.seek_to_last();
        assert_iter_entry(&iter, "a", "va");
        iter.prev();
        assert!(!iter.valid());

        iter.seek(b"");
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert!(!iter.valid());

        iter.seek(b"a");
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert!(!iter.valid());

        iter.seek(b"b");
        assert!(!iter.valid());
    }

    #[test]
    fn test_iter_multi() {
        let t = DBTest::default();
        t.put_entries(vec![("a", "va"), ("b", "vb"), ("c", "vc")]);

        let mut iter = t.iter(ReadOptions::default()).unwrap();
        iter.seek_to_first();
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert_iter_entry(&iter, "b", "vb");
        iter.next();
        assert_iter_entry(&iter, "c", "vc");
        iter.next();
        assert!(!iter.valid());
        iter.seek_to_first();
        assert_iter_entry(&iter, "a", "va");
        iter.prev();
        assert!(!iter.valid());

        iter.seek_to_last();
        assert_iter_entry(&iter, "c", "vc");
        iter.prev();
        assert_iter_entry(&iter, "b", "vb");
        iter.prev();
        assert_iter_entry(&iter, "a", "va");
        iter.prev();
        assert!(!iter.valid());
        iter.seek_to_last();
        assert_iter_entry(&iter, "c", "vc");
        iter.next();
        assert!(!iter.valid());

        iter.seek(b"");
        assert_iter_entry(&iter, "a", "va");
        iter.seek(b"a");
        assert_iter_entry(&iter, "a", "va");
        iter.seek(b"ax");
        assert_iter_entry(&iter, "b", "vb");
        iter.seek(b"b");
        assert_iter_entry(&iter, "b", "vb");
        iter.seek(b"z");
        assert!(!iter.valid());

        // Switch from reverse to forward
        iter.seek_to_last();
        iter.prev();
        iter.prev();
        iter.next();
        assert_iter_entry(&iter, "b", "vb");

        // Switch from forward to reverse
        iter.seek_to_first();
        iter.next();
        iter.next();
        iter.prev();
        assert_iter_entry(&iter, "b", "vb");

        // Make sure iter stays at snapshot
        t.put_entries(vec![
            ("a", "va2"),
            ("a2", "va3"),
            ("b", "vb2"),
            ("c", "vc2"),
        ]);
        t.delete("b").unwrap();
        iter.seek_to_first();
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert_iter_entry(&iter, "b", "vb");
        iter.next();
        assert_iter_entry(&iter, "c", "vc");
        iter.next();
        assert!(!iter.valid());
        iter.seek_to_last();
        assert_iter_entry(&iter, "c", "vc");
        iter.prev();
        assert_iter_entry(&iter, "b", "vb");
        iter.prev();
        assert_iter_entry(&iter, "a", "va");
        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_iter_small_and_large_mix() {
        let t = DBTest::default();
        let count = 100_000;
        t.put_entries(vec![
            ("a", "va"),
            ("b", &"b".repeat(count)),
            ("c", "vc"),
            ("d", &"d".repeat(count)),
            ("e", &"e".repeat(count)),
        ]);
        let mut iter = t.iter(ReadOptions::default()).unwrap();

        iter.seek_to_first();
        assert_iter_entry(&iter, "a", "va");
        iter.next();
        assert_iter_entry(&iter, "b", &"b".repeat(count));
        iter.next();
        assert_iter_entry(&iter, "c", "vc");
        iter.next();
        assert_iter_entry(&iter, "d", &"d".repeat(count));
        iter.next();
        assert_iter_entry(&iter, "e", &"e".repeat(count));
        iter.next();
        assert!(!iter.valid());

        iter.seek_to_last();
        assert_iter_entry(&iter, "e", &"e".repeat(count));
        iter.prev();
        assert_iter_entry(&iter, "d", &"d".repeat(count));
        iter.prev();
        assert_iter_entry(&iter, "c", "vc");
        iter.prev();
        assert_iter_entry(&iter, "b", &"b".repeat(count));
        iter.prev();
        assert_iter_entry(&iter, "a", "va");
        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_iter_multi_with_delete() {
        for t in default_cases() {
            t.put_entries(vec![("a", "va"), ("b", "vb"), ("c", "vc")]);
            t.delete("b").unwrap();
            assert_eq!(t.get("b", None), None);
            let mut iter = t.iter(ReadOptions::default()).unwrap();
            iter.seek(b"c");
            assert_iter_entry(&iter, "c", "vc");
            iter.prev();
            assert_iter_entry(&iter, "a", "va");
        }
    }

    #[test]
    fn test_iter_pins_ref() {
        let t = DBTest::default();
        t.put("foo", "hello").unwrap();

        // Get iterator that will yield the current contents of the DB.
        let mut iter = t.iter(ReadOptions::default()).unwrap();

        // Wirte to force compactions
        t.put("foo", "newvalue1").unwrap();
        for i in 0..100 {
            t.put(&key(i), &(key(i) + "v".repeat(100_000).as_str()))
                .unwrap();
        }
        t.put("foo", "newvalue2").unwrap();
        iter.seek_to_first();
        assert!(iter.valid());
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "foo");
        assert_eq!(str::from_utf8(iter.value()).unwrap(), "hello");
        iter.next();
        // Iter should only contains entries before being created
        assert!(!iter.valid());
    }

    #[test]
    fn test_reopen_with_empty_db() {
        for mut t in default_cases() {
            t.reopen().unwrap();
            t.reopen().unwrap();

            t.put_entries(vec![("foo", "v1"), ("foo", "v2")]);
            t.reopen().unwrap();
            t.reopen().unwrap();
            t.put("foo", "v3").unwrap();
            t.reopen().unwrap();
            assert_eq!(t.get("foo", None).unwrap(), "v3");
        }
    }

    #[test]
    fn test_recover_with_entries() {
        for mut t in default_cases() {
            t.put_entries(vec![("foo", "v1"), ("baz", "v5")]);
            t.reopen().unwrap();
            assert_eq!(t.get("foo", None).unwrap(), "v1");
            assert_eq!(t.get("baz", None).unwrap(), "v5");

            t.put_entries(vec![("bar", "v2"), ("foo", "v3")]);
            t.reopen().unwrap();
            assert_eq!(t.get("foo", None).unwrap(), "v3");
            t.put("foo", "v4").unwrap();
            assert_eq!(t.get("bar", None).unwrap(), "v2");
            assert_eq!(t.get("foo", None).unwrap(), "v4");
            assert_eq!(t.get("baz", None).unwrap(), "v5");
        }
    }

    // Check that writes done during a memtable compaction are recovered
    // if the database is shutdown during the memtable compaction.
    #[test]
    fn test_recover_during_memtable_compaction() {
        for mut t in cases(|mut opt| {
            opt.write_buffer_size = 10000;
            opt
        }) {
            // Trigger a long memtable compaction and reopen the database during it
            t.put_entries(vec![
                ("foo", "v1"),                             // Goes to 1st log file
                ("big1", "x".repeat(10_000_000).as_str()), // Fills memtable
                ("big2", "y".repeat(1000).as_str()),       // Triggers compaction
                ("bar", "v2"),                             // Goes to new log file
            ]);
            t.reopen().unwrap();
            t.assert_get("foo", Some("v1"));
            t.assert_get("bar", Some("v2"));
            t.assert_get("big1", Some("x".repeat(10_000_000).as_str()));
            t.assert_get("big2", Some("y".repeat(1000).as_str()));
        }
    }

    #[test]
    fn test_minor_compactions_happend() {
        let mut opts = Options::default();
        opts.write_buffer_size = 10000;
        let mut t = DBTest::new(opts);
        let n = 500;
        let starting_num_tables = t.total_sst_files();
        for i in 0..n {
            t.put(&key(i), &(key(i) + "v".repeat(1000).as_str()))
                .unwrap();
        }
        let ending_num_tables = t.total_sst_files();
        assert!(starting_num_tables < ending_num_tables);
        for i in 0..n {
            t.assert_get(&key(i), Some(&(key(i) + "v".repeat(1000).as_str())))
        }
        t.reopen().unwrap();
        for i in 0..n {
            t.assert_get(&key(i), Some(&(key(i) + "v".repeat(1000).as_str())))
        }
    }

    #[test]
    fn test_recover_with_large_log() {
        let opts = Options::default();
        let mut t = DBTest::new(opts);
        t.put("big1", &"1".repeat(200_000)).unwrap();
        t.put("big2", &"2".repeat(200_000)).unwrap();
        t.put("small3", &"3".repeat(10)).unwrap();
        t.put("small4", &"4".repeat(10)).unwrap();
        assert_eq!(t.num_sst_files_at_level(0), 0);

        // Make sure that if we re-open with a small write buffer size that
        // we flush table files in the middle of a large log file.
        t.opt.write_buffer_size = 100_000;
        t.reopen().unwrap();
        assert_eq!(t.num_sst_files_at_level(0), 3);
        t.assert_get("big1", Some(&"1".repeat(200_000)));
        t.assert_get("big2", Some(&"2".repeat(200_000)));
        t.assert_get("small3", Some(&"3".repeat(10)));
        t.assert_get("small4", Some(&"4".repeat(10)));
    }

    #[test]
    fn test_compaction_generate_multiple_files() {
        let mut opt = Options::default();
        opt.write_buffer_size = 100_000_000;
        let mut t = DBTest::new(opt);
        t.assert_file_num_at_level(0, 0);
        let n = 80;
        // write 8MB (80 values, each 100k)
        let mut values = vec![];
        for i in 0..n {
            let v = rand_string(100_000);
            values.push(v.clone());
            t.put(&i.to_string(), &v).unwrap();
        }

        // As opt.reuse_log = false, reopening moves entries into level-0 after replaying the WAL
        t.reopen().unwrap();
        for i in 0..n {
            t.assert_get(&i.to_string(), Some(&values[i]));
        }
        t.compact_range_at(0, None, None).unwrap();
        t.assert_file_num_at_level(0, 0);
        let l1_count = t.inner.versions.lock().unwrap().level_files_count(1);
        assert!(
            l1_count > 1,
            "level 1 file numbers should > 1, but got {}",
            l1_count
        );
        for i in 0..n {
            t.assert_get(&i.to_string(), Some(&values[i]));
        }
    }

    #[test]
    fn test_repeated_write_to_same_key() {
        let mut opt = Options::default();
        opt.write_buffer_size = 100_000; // limit the size of memtable
        opt.logger_level = crate::LevelFilter::Trace;
        // We must have at most one file per level except for level-0,
        // which may have up to kL0_StopWritesTrigger files.
        let max_files = opt.l0_stop_writes_threshold + opt.max_levels;
        let t = DBTest::new(opt.clone());
        let v = rand_string(2 * opt.write_buffer_size);
        for i in 0..10 * max_files {
            t.put("key", &v).unwrap();
            assert!(
                t.total_sst_files() < max_files,
                "after {}: {} total files",
                i,
                t.total_sst_files()
            );
        }
    }

    #[test]
    #[ignore]
    fn test_sparse_merge() {
        let mut opt = Options::default();
        opt.compression = crate::CompressionType::NoCompression;
        opt.logger_level = crate::LevelFilter::Trace;
        let t = DBTest::new(opt.clone());
        t.fill_levels("A", "Z");
        // Suppose there is:
        //    small amount of data with prefix A
        //    large amount of data with prefix B
        //    small amount of data with prefix C
        // and that recent updates have made small changes to all three prefixes.
        // Check that we do not do a compaction that merges all of B in one shot.
        t.put("A", "va").unwrap();
        // Write approximately 100MB of "B" values
        for i in 0..100_000 {
            t.put(format!("B{}", i).as_str(), "x".repeat(1000).as_str())
                .unwrap();
        }
        t.put("C", "vc").unwrap();
        t.inner.force_compact_mem_table().unwrap();
        t.compact_range_at(0, None, None).unwrap();

        // Make sparse update
        t.put("A", "va2").unwrap();
        t.put("B100", "bvalue2").unwrap();
        t.put("C", "vc2").unwrap();
        t.inner.force_compact_mem_table().unwrap();

        // Compactions should not cause us to create a situation where
        // a file overlaps too much data at the next level.
        assert!(
            t.inner
                .versions
                .lock()
                .unwrap()
                .max_next_level_overlapping_bytes()
                < 20 * 1024 * 1024
        );
        t.compact_range_at(0, None, None).unwrap();
        assert!(
            t.inner
                .versions
                .lock()
                .unwrap()
                .max_next_level_overlapping_bytes()
                < 20 * 1024 * 1024
        );
        t.compact_range_at(1, None, None).unwrap();
        assert!(
            t.inner
                .versions
                .lock()
                .unwrap()
                .max_next_level_overlapping_bytes()
                < 20 * 1024 * 1024
        );
    }

    #[test]
    fn test_approximate_size() {
        for mut t in cases(|mut opt| {
            opt.write_buffer_size = 100_000_000;
            opt.compression = crate::CompressionType::NoCompression;
            opt
        }) {
            t.assert_approximate_size("", "xyz", 0, 0);
            t.assert_file_num_at_level(0, 0);
            let n = 80;
            let s1 = 100_000;
            let s2 = 105_000; // allow some expansion from metadata
            for i in 0..n {
                t.put(&key(i), &rand_string(s1)).unwrap();
            }
            // approximate_size does not account for memtable
            t.assert_approximate_size("", &key(50), 0, 0);
            if t.options().reuse_logs {
                t.reopen().unwrap();
                // Recovery will reuse memtable
                t.assert_approximate_size("", &key(50), 0, 0);
                continue;
            }
            // Check sizes across recovery by reopening a few times
            for _ in 0..3 {
                t.reopen().unwrap();
                for compact_start in (0..n).step_by(10) {
                    for i in (0..n).step_by(10) {
                        t.assert_approximate_size("", &key(i), s1 * i, s2 * i);
                        t.assert_approximate_size(
                            "",
                            &(key(i) + ".suffix"),
                            s1 * (i + 1),
                            s2 * (i + 1),
                        );
                        t.assert_approximate_size(&key(i), &key(i + 10), s1 * 10, s2 * 10);
                    }
                    t.assert_approximate_size("", &key(50), s1 * 50, s2 * 50);
                    t.assert_approximate_size("", &(key(50) + ".suffix"), s1 * 50, s2 * 50);
                    t.compact_range_at(
                        0,
                        Some(key(compact_start).as_bytes()),
                        Some(key(compact_start + 9).as_bytes()),
                    )
                    .unwrap();
                }
                t.assert_file_num_at_level(0, 0);
                assert!(t.num_sst_files_at_level(1) > 0);
            }
        }
    }

    #[test]
    fn test_approximiate_sizes_min_of_small_and_large() {
        for mut t in cases(|mut opt| {
            opt.compression = crate::CompressionType::NoCompression;
            opt
        }) {
            let big1 = rand_string(100_000);
            t.put(&key(0), &rand_string(10000)).unwrap();
            t.put(&key(1), &rand_string(10000)).unwrap();
            t.put(&key(2), &big1).unwrap();
            t.put(&key(3), &rand_string(10000)).unwrap();
            t.put(&key(4), &big1).unwrap();
            t.put(&key(5), &rand_string(10000)).unwrap();
            t.put(&key(6), &rand_string(300000)).unwrap();
            t.put(&key(7), &rand_string(10000)).unwrap();
            if t.opt.reuse_logs {
                t.inner.force_compact_mem_table().unwrap();
            }
            for _ in 0..3 {
                t.reopen().unwrap();
                t.assert_approximate_size("", &key(0), 0, 0);
                t.assert_approximate_size("", &key(1), 10000, 11000);
                t.assert_approximate_size("", &key(2), 20000, 21000);
                t.assert_approximate_size("", &key(3), 120000, 121000);
                t.assert_approximate_size("", &key(4), 130000, 131000);
                t.assert_approximate_size("", &key(5), 230000, 231000);
                t.assert_approximate_size("", &key(6), 240000, 241000);
                t.assert_approximate_size("", &key(7), 540000, 541000);
                t.assert_approximate_size("", &key(8), 550000, 560000);
                t.assert_approximate_size(&key(3), &key(5), 110000, 111000);
                t.compact_range_at(0, None, None).unwrap();
            }
        }
    }

    #[test]
    fn test_snapshot() {
        for t in default_cases() {
            t.put("foo", "v1").unwrap();
            let s1 = t.snapshot();
            t.put("foo", "v2").unwrap();
            let s2 = t.snapshot();
            t.put("foo", "v3").unwrap();
            let s3 = t.snapshot();
            t.put("foo", "v4").unwrap();

            assert_eq!(
                Some("v1".to_owned()),
                t.get("foo", Some(s1.sequence().into()))
            );
            assert_eq!(
                Some("v2".to_owned()),
                t.get("foo", Some(s2.sequence().into()))
            );
            assert_eq!(
                Some("v3".to_owned()),
                t.get("foo", Some(s3.sequence().into()))
            );
            assert_eq!(Some("v4".to_owned()), t.get("foo", None));
            let mut versions = t.inner.versions.lock().unwrap();
            versions.snapshots.gc();
            assert!(!versions.snapshots.is_empty());
            mem::drop(s1);
            mem::drop(s2);
            mem::drop(s3);
            versions.snapshots.gc();
            assert!(versions.snapshots.is_empty());
        }
    }

    #[test]
    fn test_hidden_values_are_removed() {
        for t in default_cases() {
            t.fill_levels("a", "z");
            let big = rand_string(50000);
            t.put("foo", &big).unwrap();
            t.put("pastfoo", "v").unwrap();
            let s = t.snapshot();
            t.put("foo", "tiny").unwrap();
            t.put("pastfoo2", "v2").unwrap();
            t.inner.force_compact_mem_table().unwrap();
            assert!(t.num_sst_files_at_level(0) > 0);

            assert_eq!(t.get("foo", Some(s.sequence().into())), Some(big.clone()));
            t.assert_approximate_size("", "pastfoo", 50000, 60000);
            t.must_release_snapshot(s);
            assert_eq!(format!("[ tiny, {} ]", big), t.all_entires_for(b"foo"));
            t.compact_range_at(0, None, Some(b"x")).unwrap();
            assert_eq!("[ tiny ]".to_owned(), t.all_entires_for(b"foo"));
            t.assert_file_num_at_level(0, 0);
            assert!(t.num_sst_files_at_level(1) >= 1);
            t.compact_range_at(1, None, Some(b"x")).unwrap();
            assert_eq!("[ tiny ]".to_owned(), t.all_entires_for(b"foo"));
            t.assert_approximate_size("", "pastfoo", 0, 1000);
        }
    }

    #[test]
    fn test_mem_compact_into_max_level() {
        let t = DBTest::default();
        t.put("foo", "v1").unwrap();
        t.inner.force_compact_mem_table().unwrap();
        t.assert_file_num_at_level(t.opt.max_mem_compact_level, 1);

        // Place a table at level last-1 to prevent merging with preceding mutation
        t.put("a", "begin").unwrap();
        t.put("z", "end").unwrap();
        t.inner.force_compact_mem_table().unwrap();
        t.assert_file_num_at_level(t.opt.max_mem_compact_level, 1);
        t.assert_file_num_at_level(t.opt.max_mem_compact_level - 1, 1);
    }

    #[test]
    fn test_deletion_marker1() {
        let t = DBTest::default();
        t.put("foo", "v1").unwrap();
        t.inner.force_compact_mem_table().unwrap();
        t.put("a", "begin").unwrap();
        t.put("z", "end").unwrap();
        t.inner.force_compact_mem_table().unwrap();
        t.delete("foo").unwrap();
        t.put("foo", "v2").unwrap();
        assert_eq!(t.all_entires_for(b"foo"), "[ v2, DEL, v1 ]");
        t.inner.force_compact_mem_table().unwrap();
        assert_eq!(t.all_entires_for(b"foo"), "[ v2, DEL, v1 ]");
        let level = t.opt.max_mem_compact_level; // default is 2
        t.compact_range_at(level - 2, None, Some(b"z")).unwrap();
        // DELE eliminated, but v1 remains because we aren't compaction that level
        assert_eq!(t.all_entires_for(b"foo"), "[ v2, v1 ]");
        t.compact_range_at(level - 1, None, None).unwrap();
        // Mergeing last-1 with last, so we are the base level for "foo"
        assert_eq!(t.all_entires_for(b"foo"), "[ v2 ]");
    }

    #[test]
    fn test_deletion_marker2() {
        let t = DBTest::default();
        t.put("foo", "v1").unwrap();
        t.inner.force_compact_mem_table().unwrap();
        t.put("a", "begin").unwrap();
        t.put("z", "end").unwrap();
        t.inner.force_compact_mem_table().unwrap();

        t.delete("foo").unwrap();
        assert_eq!(t.all_entires_for(b"foo"), "[ DEL, v1 ]");
        t.inner.force_compact_mem_table().unwrap();
        assert_eq!(t.all_entires_for(b"foo"), "[ DEL, v1 ]");
        let level = t.opt.max_mem_compact_level; // default is 2
        t.compact_range_at(level - 2, None, None).unwrap();
        assert_eq!(t.all_entires_for(b"foo"), "[ DEL, v1 ]");
        t.compact_range_at(level - 1, None, None).unwrap();
        assert_eq!(t.all_entires_for(b"foo"), "[ ]");
    }

    #[test]
    fn test_overlap_in_level0() {
        for t in default_cases() {
            // Fill levels 1 and 2 to disable the pushing or new memtables to levels > 0
            t.put("100", "v100").unwrap();
            t.put("999", "v999").unwrap();
            t.inner.force_compact_mem_table().unwrap();
            t.delete("100").unwrap();
            t.delete("999").unwrap();
            t.inner.force_compact_mem_table().unwrap();
            assert_eq!("0,1,1", t.file_count_per_level());

            // Make files spanning the following ranges in level-0:
            //  files[0]  200 .. 900
            //  files[1]  300 .. 500
            // Note that filtes are sorted by smallest key
            t.put("300", "v300").unwrap();
            t.put("500", "v500").unwrap();
            t.inner.force_compact_mem_table().unwrap();
            t.put("200", "v200").unwrap();
            t.put("600", "v600").unwrap();
            t.put("900", "v000").unwrap();
            t.inner.force_compact_mem_table().unwrap();
            assert_eq!("2,1,1", t.file_count_per_level());

            // Compact away the placeholder files we created initially
            t.compact_range_at(1, None, None).unwrap();
            t.compact_range_at(2, None, None).unwrap();
            assert_eq!("2", t.file_count_per_level());

            // Do a memtable compaction
            t.delete("600").unwrap();
            t.inner.force_compact_mem_table().unwrap();
            assert_eq!("3", t.file_count_per_level());
            t.assert_get("600", None);
        }
    }

    #[test]
    fn test_l0_compaction_when_reopen() {
        let mut t = DBTest::default();
        assert_eq!("", t.assert_contents());
        t.put("b", "v").unwrap();
        t.reopen().unwrap();
        t.delete("b").unwrap();
        t.delete("a").unwrap();
        t.reopen().unwrap();
        t.delete("a").unwrap();
        t.reopen().unwrap();
        t.put("a", "v").unwrap();
        t.reopen().unwrap();
        t.reopen().unwrap();
        assert_eq!("(a->v)", t.assert_contents());
        t.delete("a").unwrap();
        t.put("", "").unwrap();
        t.delete("e").unwrap();
        t.reopen().unwrap();
        t.put("c", "cv").unwrap();
        t.reopen().unwrap();
        t.put("", "").unwrap();
        t.reopen().unwrap();
        t.put("", "").unwrap();
        t.reopen().unwrap();
        t.put("d", "dv").unwrap();
        t.reopen().unwrap();
        t.delete("d").unwrap();
        t.delete("b").unwrap();
        t.reopen().unwrap();
        assert_eq!("(->)(c->cv)", t.assert_contents());
    }

    #[test]
    fn test_comparator_check() {
        use std::cmp::Ordering;
        #[derive(Clone, Default)]
        struct NewComparator(BytewiseComparator);
        impl Comparator for NewComparator {
            fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
                self.0.compare(a, b)
            }
            fn name(&self) -> &str {
                return "wickdb.NewComparator";
            }
            fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
                self.0.separator(a, b)
            }
            fn successor(&self, key: &[u8]) -> Vec<u8> {
                self.0.successor(key)
            }
        }
        let mut opts = Options::default();
        opts.comparator = NewComparator(BytewiseComparator {});
        let storage = MemStorage::default();
        let mut db = WickDB::open_db(opts, "test", storage.clone()).unwrap();
        db.close().unwrap();
        let opts = Options::<BytewiseComparator>::default();
        let res = WickDB::open_db(opts, "test", storage.clone());
        match res {
            Ok(_) => panic!("should panic"),
            Err(e) => assert!(e.to_string().contains("does not match existing compactor")),
        };
    }

    #[test]
    fn test_custom_comparator() {
        use std::cmp::Ordering;
        use std::str;
        use std::usize;
        #[derive(Clone, Default)]
        struct NumberComparator {}
        fn to_number(n: &[u8]) -> usize {
            usize::from_str_radix(str::from_utf8(n).unwrap(), 16).unwrap()
        }
        impl Comparator for NumberComparator {
            fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
                to_number(a).cmp(&to_number(b))
            }
            fn name(&self) -> &str {
                return "test.NumberComparator";
            }
            fn separator(&self, _a: &[u8], b: &[u8]) -> Vec<u8> {
                b.to_vec()
            }
            fn successor(&self, key: &[u8]) -> Vec<u8> {
                key.to_vec()
            }
        }
        let mut opts = Options::default();
        opts.comparator = NumberComparator {};
        opts.create_if_missing = true;
        opts.filter_policy = None;
        opts.write_buffer_size = 1000;
        let store = MemStorage::default();
        let db = WickDB::open_db(opts, "test", store).unwrap();
        db.put(WriteOptions::default(), b"a", b"ten").unwrap();
        db.put(WriteOptions::default(), b"14", b"twenty").unwrap();
        for _ in 0..2 {
            assert_eq!(
                Some("ten".as_bytes().to_vec()),
                db.get(ReadOptions::default(), b"a").unwrap()
            );
            assert_eq!(
                Some("twenty".as_bytes().to_vec()),
                db.get(ReadOptions::default(), b"14").unwrap()
            );
            assert_eq!(None, db.get(ReadOptions::default(), b"f").unwrap());
        }
    }

    #[test]
    fn test_manual_compaction() {
        let mut opts = Options::default();
        opts.logger_level = crate::LevelFilter::Debug;
        let t = DBTest::new(opts);
        t.make_sst_files(3, "p", "q");
        assert_eq!("1,1,1", t.file_count_per_level());

        // Compaction range falls before files
        t.compact(Some(""), Some("c"));
        assert_eq!("1,1,1", t.file_count_per_level());

        // Compaction range falls after files
        t.compact(Some("r"), Some("z"));
        assert_eq!("1,1,1", t.file_count_per_level());

        t.compact(Some("p1"), Some("p9"));
        assert_eq!("0,0,1", t.file_count_per_level());

        // Populate a different range
        t.make_sst_files(3, "c", "e");
        assert_eq!("1,1,2", t.file_count_per_level());

        t.compact(Some("b"), Some("f"));
        assert_eq!("0,0,2", t.file_count_per_level());

        // Compact all
        t.make_sst_files(1, "a", "z");
        assert_eq!("0,1,2", t.file_count_per_level());
        t.compact(None, None);
        assert_eq!("0,0,1", t.file_count_per_level());
    }

    #[test]
    fn test_dbopen_options() {
        let store = MemStorage::default();
        let mut opts = Options::<BytewiseComparator>::default();
        let dbname = "db_options_test";
        // Does not exist, and create_if_missing == false
        opts.create_if_missing = false;
        match WickDB::open_db(opts.clone(), dbname, store.clone()) {
            Ok(_) => panic!("create_if_missing false should return error"),
            Err(e) => assert!(e.to_string().contains("does not exist")),
        }

        // Does not exist, and create_if_missing == true
        opts.create_if_missing = true;
        let mut db = WickDB::open_db(opts.clone(), dbname, store.clone()).unwrap();
        db.close().unwrap();

        // Does exist, and error_if_exists == true
        opts.create_if_missing = false;
        opts.error_if_exists = true;
        match WickDB::open_db(opts.clone(), dbname, store.clone()) {
            Ok(_) => panic!("error_if_exists true should return error"),
            Err(e) => assert!(e.to_string().contains("exists")),
        }

        // Does exist, and error_if_exists == true
        opts.create_if_missing = true;
        opts.error_if_exists = false;
        let _ = WickDB::open_db(opts, dbname, store.clone()).unwrap();
    }

    #[test]
    fn test_destroy_empty_dir() {
        let store = MemStorage::default();
        let opts = Options::<BytewiseComparator>::default();
        let dbname = "db_empty_dir";
        let mut db = WickDB::open_db(opts, dbname, store.clone()).unwrap();
        assert_eq!(4, store.list(dbname).unwrap().len());
        // clean up dir
        db.destroy().unwrap();
        assert!(!store.exists(dbname));
        assert!(db.destroy().is_err());
        assert!(db.destroy().is_err());
    }

    #[test]
    fn test_db_file_lock() {
        let store = MemStorage::default();
        let opts = Options::<BytewiseComparator>::default();
        let dbname = "db_file_lock";
        let _ = WickDB::open_db(opts.clone(), dbname, store.clone()).unwrap();
        match WickDB::open_db(opts, dbname, store.clone()) {
            Ok(_) => panic!("should return error try to create an opened db"),
            Err(e) => assert!(e.to_string().contains("Already locked")),
        }
    }

    // ***** Fail Injection Tests Start ***** //
    #[test]
    fn test_no_storage_space() {
        // TODO(fullstop000)
    }

    #[test]
    fn test_fs_sync_error() {
        // TODO(fullstop000)
    }

    #[test]
    fn test_fs_non_writable() {
        // TODO(fullstop000)
    }

    #[test]
    fn test_manifest_write_error() {
        // TODO(fullstop000)
    }
    // ***** Fail Injection Tests End ***** //

    #[test]
    fn test_missing_sstfile() {
        let mut t = DBTest::default();
        t.put("foo", "bar").unwrap();
        t.inner.force_compact_mem_table().unwrap();
        t.assert_get("foo", Some("bar"));
        t.close().unwrap();
        assert!(t.delete_one_sst_file().unwrap());
        t.opt.paranoid_checks = true;
        match t.reopen() {
            Ok(_) => panic!("Should report missing files"),
            Err(e) => assert!(e.to_string().contains("missing files")),
        }
    }

    #[test]
    fn test_file_deleted_after_compaction() {
        let t = DBTest::default();
        t.put("foo", "v2").unwrap();
        t.compact(Some("a"), Some("z"));
        let file_counts = t.store.list(&t.inner.db_path).unwrap().len();
        for _ in 0..10 {
            t.put("foo", "v2").unwrap();
            t.compact(Some("a"), Some("z"))
        }
        assert_eq!(t.store.list(&t.inner.db_path).unwrap().len(), file_counts);
    }

    #[test]
    fn test_db_reads_using_bloom_filter() {
        use crate::cache::lru::LRUCache;
        let mut store = MemStorage::default();
        store.count_random_reads = true;
        let mut opts = Options::<BytewiseComparator>::default();
        opts.logger_level = crate::LevelFilter::Debug;
        opts.block_cache = Some(Arc::new(LRUCache::new(0)));
        let db = WickDB::open_db(opts, "bloom_filter_test", store.clone()).unwrap();
        // Populate multiple layers
        let n = 10000;
        for i in 0..n {
            db.put(
                WriteOptions::default(),
                key(i).as_bytes(),
                key(i).as_bytes(),
            )
            .unwrap();
        }
        db.compact_range(Some(b"a"), Some(b"z")).unwrap();
        for i in (0..n).into_iter().step_by(100) {
            db.put(
                WriteOptions::default(),
                key(i).as_bytes(),
                key(i).as_bytes(),
            )
            .unwrap();
        }
        db.inner.force_compact_mem_table().unwrap();
        store.delay_data_sync.store(true, Ordering::Release);
        for i in 0..n {
            let v = db.get(ReadOptions::default(), key(i).as_bytes()).unwrap();
            assert_eq!(v, Some(key(i).into_bytes()), "key {}", key(i));
        }
        let reads = store.random_read_counter.load(Ordering::Relaxed);
        assert!(reads >= n && reads <= n + 2 * n / 100);
        store.random_read_counter.store(0, Ordering::Relaxed);
        for i in 0..n {
            assert_eq!(
                None,
                db.get(ReadOptions::default(), (key(i) + ".missing").as_bytes())
                    .unwrap()
            )
        }
        let reads = store.random_read_counter.load(Ordering::Relaxed);
        assert!(reads <= 3 * n / 100);
    }

    const THREAD_COUNT: usize = 4;
    const TEST_SECONDS: usize = 10;
    const KEY_NUM: usize = 1000;

    impl DBTest {
        fn new_multi_thd_test(&self) -> MultiThreadTest {
            MultiThreadTest {
                db: self.db.clone(),
                // store: self.store.clone(),
                stop: Arc::new(AtomicBool::new(false)),
                // options: self.opt.clone(),
                states: Vec::with_capacity(THREAD_COUNT),
            }
        }
    }
    struct MultiThreadTest {
        stop: Arc<AtomicBool>,
        db: WickDB<MemStorage, BytewiseComparator>,
        states: Vec<Arc<ThreadState>>,
    }

    struct ThreadState {
        db: WickDB<MemStorage, BytewiseComparator>,
        stop: Arc<AtomicBool>,
        // The set-get runs
        counter: AtomicUsize,
        done: AtomicBool,
        werrs: Arc<Mutex<Vec<Error>>>,
        rerrs: Arc<Mutex<Vec<Error>>>,
    }

    unsafe impl Send for ThreadState {}
    unsafe impl Sync for ThreadState {}

    impl MultiThreadTest {
        fn start(&mut self, id: usize) {
            let state = Arc::new(ThreadState {
                db: self.db.clone(),
                stop: self.stop.clone(),
                counter: AtomicUsize::new(0),
                done: AtomicBool::new(false),
                werrs: Arc::new(Mutex::new(Vec::new())),
                rerrs: Arc::new(Mutex::new(Vec::new())),
            });
            self.states.push(state.clone());
            thread::Builder::new()
                .name(id.to_string())
                .spawn(move || {
                    println!("===== starting thread {}", id);
                    let mut counter = 0;
                    let mut rnd = rand::thread_rng();
                    while !state.stop.load(Ordering::Acquire) {
                        state.counter.store(counter, Ordering::Release);
                        let key = rand::thread_rng().gen_range(0, KEY_NUM);
                        if rnd.gen_range(1, 3) == 1 {
                            // Write values of the form <key, id, counter>
                            let value = format!("{}.{}.{}", key, id, counter);
                            match state.db.put(
                                WriteOptions::default(),
                                key.to_string().as_bytes(),
                                value.as_bytes(),
                            ) {
                                Ok(_) => continue,
                                Err(e) => {
                                    let mut guard = state.werrs.lock().unwrap();
                                    guard.push(e);
                                    break;
                                }
                            }
                        } else {
                            match state
                                .db
                                .get(ReadOptions::default(), key.to_string().as_bytes())
                            {
                                Ok(v) => {
                                    if let Some(value) = v {
                                        let s = String::from_utf8(value).unwrap();
                                        let ss = s.split('.').collect::<Vec<_>>();
                                        assert_eq!(3, ss.len());
                                        assert_eq!(ss[0], key.to_string());
                                    }
                                }
                                Err(e) => {
                                    let mut guard = state.rerrs.lock().unwrap();
                                    guard.push(e);
                                    break;
                                }
                            }
                        }
                        counter += 1;
                    }
                    state.done.store(true, Ordering::Release);
                    println!(
                        "===== stopping thread {} after {} opts: write error {}, read error {}",
                        id,
                        counter,
                        state.werrs.lock().unwrap().len(),
                        state.rerrs.lock().unwrap().len()
                    );
                })
                .unwrap();
        }
    }

    #[test]
    fn test_multi_thread() {
        for t in default_cases() {
            let mut mt = t.new_multi_thd_test();
            for id in 0..THREAD_COUNT {
                mt.start(id);
            }
            thread::sleep(Duration::from_secs(TEST_SECONDS as u64));
            mt.stop.store(true, Ordering::Release);
            for state in mt.states.iter() {
                while !state.done.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(100));
                }
                {
                    let werrs = state.werrs.lock().unwrap();
                    assert_eq!(0, werrs.len(), "{:?}", werrs);
                    let rerrs = state.rerrs.lock().unwrap();
                    assert_eq!(0, rerrs.len(), "{:?}", rerrs);
                }
            }
        }
    }
}
