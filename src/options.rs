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

// use crate::cache::lru::SharedLRUCache;
use crate::cache::lru::LRUCache;
use crate::cache::{Cache, ShardedCache};
use crate::db::format::InternalFilterPolicy;
use crate::filter::FilterPolicy;
use crate::logger::Logger;
use crate::snapshot::Snapshot;
use crate::sstable::block::Block;
use crate::storage::{File, Storage};
use crate::util::comparator::Comparator;
use crate::{BloomFilter, LevelFilter, Log};
use std::sync::Arc;

const DEFAULT_CACHE_SHARDS: usize = 8;

#[derive(Clone, Copy, Debug, FromPrimitive)]
pub enum CompressionType {
    NoCompression = 0,
    SnappyCompression = 1,
    Unknown,
}

impl From<u8> for CompressionType {
    fn from(i: u8) -> Self {
        num_traits::FromPrimitive::from_u8(i).unwrap()
    }
}

/// Options to control the behavior of a database (passed to `DB::Open`)
#[derive(Clone)]
pub struct Options<C: Comparator> {
    // -------------------
    // Parameters that affect behavior:
    /// Comparator used to define the order of keys in the table.
    /// Default: a comparator that uses lexicographic byte-wise ordering
    ///
    /// REQUIRES: The client must ensure that the comparator supplied
    /// here has the same name and orders keys *exactly* the same as the
    /// comparator provided to previous open calls on the same DB.
    pub comparator: C,

    /// If true, the database will be created if it is missing.
    pub create_if_missing: bool,

    /// If true, an error is raised if the database already exists.
    pub error_if_exists: bool,

    /// If true, the implementation will do aggressive checking of the
    /// data it is processing and will stop early if it detects any
    /// errors.  This may have unforeseen ramifications: for example, a
    /// corruption of one DB entry may cause a large number of entries to
    /// become unreadable or for the entire DB to become unopenable.
    pub paranoid_checks: bool,

    // -------------------
    // Parameters that affect compaction:
    /// The max number of levels except L0
    pub max_levels: usize,

    /// The number of files necessary to trigger an L0 compaction.
    pub l0_compaction_threshold: usize,

    /// Soft limit on the number of L0 files. Writes are slowed down when this
    /// threshold is reached.
    pub l0_slowdown_writes_threshold: usize,

    /// Hard limit on the number of L0 files. Writes are stopped when this
    /// threshold is reached.
    pub l0_stop_writes_threshold: usize,

    /// The maximum number of bytes for L1. The maximum number of bytes for other
    /// levels is computed dynamically based on this value. When the maximum
    /// number of bytes for a level is exceeded, compaction is requested.
    pub l1_max_bytes: u64,

    /// Maximum level to which a new compacted memtable is pushed if it
    /// does not create overlap.  We try to push to level 2 to avoid the
    /// relatively expensive level 0=>1 compactions and to avoid some
    /// expensive manifest file operations.  We do not push all the way to
    /// the largest level since that can generate a lot of wasted disk
    /// space if the same key space is being repeatedly overwritten.
    pub max_mem_compact_level: usize,

    /// Approximate gap in bytes between samples of data read during iteration
    pub read_bytes_period: u64,

    // -------------------
    // Parameters that affect performance:
    /// Amount of data to build up in memory (backed by an unsorted log
    /// on disk) before converting to a sorted on-disk file.
    ///
    /// Larger values increase performance, especially during bulk loads.
    /// Up to two write buffers may be held in memory at the same time,
    /// so you may wish to adjust this parameter to control memory usage.
    /// Also, a larger write buffer will result in a longer recovery time
    /// the next time the database is opened.
    pub write_buffer_size: usize,

    /// Number of open files that can be used by the DB.  You may need to
    /// increase this if your database has a large working set (budget
    /// one open file per 2MB of working set).
    pub max_open_files: usize,

    // -------------------
    // Control over blocks (user data is stored in a set of blocks, and
    // a block is the unit of reading from disk).
    /// If non-null, use the specified cache for blocks.
    /// If null, we will automatically create and use an 8MB internal cache.
    pub block_cache: Option<Arc<dyn Cache<Vec<u8>, Arc<Block>>>>,

    /// Number of sstables that remains out of table cache
    pub non_table_cache_files: usize,

    /// Approximate size of user data packed per block.  Note that the
    /// block size specified here corresponds to uncompressed data.  The
    /// actual size of the unit read from disk may be smaller if
    /// compression is enabled.  This parameter can be changed dynamically.
    pub block_size: usize,

    /// Number of keys between restart points for delta encoding of keys.
    /// This parameter can be changed dynamically.  Most clients should
    /// leave this parameter alone.
    pub block_restart_interval: usize,

    /// The DB will write up to this amount of bytes to a file before
    /// switching to a new one.
    /// Most clients should leave this parameter alone.  However if your
    /// filesystem is more efficient with larger files, you could
    /// consider increasing the value.  The downside will be longer
    /// compactions and hence longer latency/performance hiccups.
    /// Another reason to increase this parameter might be when you are
    /// initially populating a large database.
    pub max_file_size: u64,

    /// Compress blocks using the specified compression algorithm.  This
    /// parameter can be changed dynamically. Default is SnappyCompression.
    pub compression: CompressionType,

    /// If true, append to existing MANIFEST and log files when a database is opened.
    /// This can significantly speed up open.
    pub reuse_logs: bool,

    /// If non-null, use the specified filter policy to reduce disk reads.
    pub filter_policy: Option<Arc<dyn FilterPolicy>>,

    /// The underlying logger
    /// In dev mode, default using a std output
    /// In release mode, default using a file `LOG` for output
    pub logger: Option<slog::Logger>,

    /// The maximum log level
    pub logger_level: LevelFilter,
}

impl<C: Comparator> Options<C> {
    /// Maximum number of bytes in all compacted files.  We avoid expanding
    /// the lower level file set of a compaction if it would make the
    /// total compaction cover more than this many bytes.
    pub(crate) fn expanded_compaction_byte_size_limit(&self) -> u64 {
        25 * self.max_file_size
    }

    /// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
    /// stop building a single file in a level-> level+1 compaction.
    pub(crate) fn max_grandparent_overlap_bytes(&self) -> u64 {
        10 * self.max_file_size
    }

    /// Maximum bytes of total files in a given level
    pub(crate) fn max_bytes_for_level(&self, mut level: usize) -> u64 {
        // Note: the result for level zero is not really used since we set
        // the level-0 compaction threshold based on number of files.

        // Result for both level-0 and level-1
        let mut result = self.l1_max_bytes;
        while level > 1 {
            result *= 10;
            level -= 1;
        }
        result
    }

    /// Reserve `non_table_cache_files` files or so for other uses and give the rest to TableCache
    pub(crate) fn table_cache_size(&self) -> usize {
        self.max_open_files - self.non_table_cache_files
    }

    /// Initialize Options by limiting ranges of some flags, applying customized Logger and etc.
    pub(crate) fn initialize<O: File + 'static, S: Storage<F = O>>(
        &mut self,
        db_path: &str,
        storage: &S,
    ) {
        if self.max_mem_compact_level < 2 {
            self.max_mem_compact_level = 2
        }
        self.max_open_files =
            Self::clip_range(self.max_open_files, 64 + self.non_table_cache_files, 50000);
        self.write_buffer_size = Self::clip_range(self.write_buffer_size, 64 << 10, 1 << 30);
        self.max_file_size = Self::clip_range(self.max_file_size, 1 << 20, 1 << 30);
        self.block_size = Self::clip_range(self.block_size, 1 << 10, 4 << 20);
        self.apply_logger(storage, db_path);
        if self.block_cache.is_none() {
            let mut shards = vec![];
            for _ in 0..DEFAULT_CACHE_SHARDS {
                shards.push(LRUCache::new(8 << 20));
            }
            self.block_cache = Some(Arc::new(ShardedCache::new(shards)))
        }
        if let Some(fp) = std::mem::replace(&mut self.filter_policy, None) {
            self.filter_policy = Some(Arc::new(InternalFilterPolicy::new(fp)));
        } else {
            let bf = BloomFilter::new(10);
            self.filter_policy = Some(Arc::new(InternalFilterPolicy::new(Arc::new(bf))))
        }
    }

    fn apply_logger<S: Storage>(&mut self, storage: &S, db_path: &str) {
        let user_logger = std::mem::replace(&mut self.logger, None);
        let logger = Logger::new(user_logger, self.logger_level, storage, db_path);
        let static_logger: &'static dyn Log = Box::leak(Box::new(logger));
        let _ = log::set_logger(static_logger); // global logger could be set
        log::set_max_level(self.logger_level);
        info!("Logger initialized: [level {:?}]", &self.logger_level);
    }

    fn clip_range<N: PartialOrd + Eq + Copy>(n: N, min: N, max: N) -> N {
        let mut r = n;
        if n > max {
            r = max
        }
        if n < min {
            r = min
        }
        r
    }
}

impl<C: Comparator> Default for Options<C> {
    fn default() -> Self {
        Options {
            comparator: C::default(),
            create_if_missing: true,
            error_if_exists: false,
            paranoid_checks: false,
            max_levels: 7,
            l0_compaction_threshold: 4,
            l0_slowdown_writes_threshold: 8,
            l0_stop_writes_threshold: 12,
            l1_max_bytes: 64 * 1024 * 1024, // 64MB
            max_mem_compact_level: 2,
            read_bytes_period: 1048576,
            write_buffer_size: 4 * 1024 * 1024, // 4MB
            max_open_files: 500,
            block_cache: None,
            non_table_cache_files: 10,
            block_size: 4 * 1024, // 4KB
            block_restart_interval: 16,
            max_file_size: 2 * 1024 * 1024, // 2MB
            compression: CompressionType::SnappyCompression,
            reuse_logs: false,
            filter_policy: None,
            logger: None,
            logger_level: LevelFilter::Warn,
        }
    }
}

/// Options that control read operations
#[derive(Clone, Copy)]
pub struct ReadOptions {
    /// If true, all data read from underlying storage will be
    /// verified against corresponding checksums.
    pub verify_checksums: bool,

    /// Should the data read for this iteration be cached in memory?
    /// Callers may wish to set this field to false for bulk scans.
    pub fill_cache: bool,

    /// If `snapshot` is `None`, read as of the supplied snapshot
    /// (which must belong to the DB that is being read and which must
    /// not have been released).  If `snapshot` is `None`, use an implicit
    /// snapshot of the state at the beginning of this read operation.
    pub snapshot: Option<Snapshot>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        ReadOptions {
            verify_checksums: false,
            fill_cache: true,
            snapshot: None,
        }
    }
}

/// Options that control write operations
#[derive(Default)]
pub struct WriteOptions {
    /// If true, the write will be flushed from the operating system
    /// buffer cache before the write is considered complete.
    /// If this flag is true, writes will be slower.
    ///
    /// If this flag is false, and the machine crashes, some recent
    /// writes may be lost.  Note that if it is just the process that
    /// crashes (i.e., the machine does not reboot), no writes will be
    /// lost even if sync==false.
    ///
    /// In other words, a DB write with sync==false has similar
    /// crash semantics as the "write()" system call.  A DB write
    /// with sync==true has similar crash semantics to a "write()"
    /// system call followed by "fsync()".
    pub sync: bool,
}
