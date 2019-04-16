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

use crate::util::comparator::{Comparator, BytewiseComparator};
use crate::cache::Cache;
use crate::filter::FilterPolicy;
use crate::util::slice::Slice;
use crate::snapshot::Snapshot;
use crate::options::CompressionType::{SnappyCompression, NoCompression};
use std::rc::Rc;
use std::cell::RefCell;
use crate::cache::lru::SharedLRUCache;
use std::sync::Arc;
use crate::table::block::Block;

#[derive(Clone, Copy, Debug)]
pub enum CompressionType {
    NoCompression = 0,
    SnappyCompression = 1,
}

impl From<u8> for CompressionType {
    fn from(i: u8) -> Self {
        match i {
            0 => NoCompression,
            1 => SnappyCompression,
            _ => panic!("invalid CompressionType value {}", i),
        }
    }
}

/// Options to control the behavior of a database (passed to `DB::Open`)
pub struct Options {

    // -------------------
    // Parameters that affect behavior:

    /// Comparator used to define the order of keys in the table.
    /// Default: a comparator that uses lexicographic byte-wise ordering
    ///
    /// REQUIRES: The client must ensure that the comparator supplied
    /// here has the same name and orders keys *exactly* the same as the
    /// comparator provided to previous open calls on the same DB.
    pub comparator : Rc<Box<dyn Comparator>>,

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

    // TODO: implement env or use a more convenient way

    // -------------------
    // Parameters that affect compaction:

    /// The max number of levels except L)
    pub max_levels: i8,

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
    pub l1_max_bytes: usize,

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
    pub block_cache: Option<Arc<RefCell<dyn Cache<Block>>>>,

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
    pub max_file_size: usize,

    /// Compress blocks using the specified compression algorithm.  This
    /// parameter can be changed dynamically. Default is SnappyCompression.
    pub compression: CompressionType,

    /// If true, append to existing MANIFEST and log files when a database is opened.
    /// This can significantly speed up open.
    pub reuse_logs: bool,

    /// If non-null, use the specified filter policy to reduce disk reads.
    /// Many applications will benefit from passing the result of
    /// NewBloomFilterPolicy() here.
    pub filter_policy: Option<Rc<Box<dyn FilterPolicy>>>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            comparator: Rc::new(Box::new(BytewiseComparator::new())),
            create_if_missing: false,
            error_if_exists: false,
            paranoid_checks: false,
            max_levels: 7,
            l0_compaction_threshold: 4,
            l0_slowdown_writes_threshold: 8,
            l0_stop_writes_threshold: 12,
            l1_max_bytes: 64 * 1024 * 1024, // 64MB
            write_buffer_size: 4 * 1024 * 1024, // 4MB
            max_open_files: 500,
            block_cache: Some(Arc::new(RefCell::new(SharedLRUCache::new(8<<20)))),
            block_size: 4 * 1024, // 4KB
            block_restart_interval: 16,
            max_file_size: 2 * 1024 * 1024, // 2MB
            compression: SnappyCompression,
            reuse_logs: true,
            filter_policy: None,
        }
    }
}

/// Options that control read operations
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
    pub snapshot : Option<Snapshot>,
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
    pub sync : bool
}
