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

use crate::table::{BlockHandle, BLOCK_TRAILER_SIZE, Footer};
use crate::table::block::{Block, BlockBuilder};
use crate::options::{Options, ReadOptions, CompressionType};
use std::fs::File;
use crate::table::filter_block::{FilterBlockReader, FilterBlockBuilder};
use crate::util::status::{WickErr, Status};
use std::rc::Rc;
use crate::util::comparator::Comparator;
use crate::util::slice::Slice;
use std::cmp::Ordering;
use std::io::Write;
use std::error::Error;
use crate::util::crc32::{value, extend};
use crate::util::coding::{put_fixed_32};

/// A `Table` is a sorted map from strings to strings.  Tables are
/// immutable and persistent.  A Table may be safely accessed from
/// multiple threads without external synchronization.
pub struct Table<'a> {
    options: Options,
    file: File,
    cache_id: u64,
    filter_reader: FilterBlockReader<'a>,
    filter_data: &'a [u8],
    meta_index_handle: BlockHandle,
    index_block: Block,
}

/// Temperarily stores the contents of the table it is
/// building in .sst file but does not close the file. It is up to the
/// caller to close the file after calling `Finish()`.
pub struct TableBuilder {
    options: Rc<Options>,
    cmp: Rc<Box<dyn Comparator>>,
    // underlying sst file
    file: File,
    // the written data length
    offset: u64,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    // the last added key
    // can be used when adding a new entry into index block
    last_key: Vec<u8>,
    // number of key/value pairs in the file
    num_entries: usize,
    closed: bool,
    filter_block: Option<FilterBlockBuilder>,
    // indicates iff we have to add a index to index_block
    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block. This allows us to use shorter
    // keys in the index block.
    pending_index_entry: bool,
    // handle to add to index block
    pending_handle: BlockHandle,
    err: Option<WickErr>,
}

impl TableBuilder {
    pub fn new(file: File, options: Rc<Options>) -> Self {
        let opt = options.clone();
        let db_builder = BlockBuilder::new(options.block_restart_interval, options.comparator.clone());
        let ib_builder = BlockBuilder::new(options.block_restart_interval, options.comparator.clone());
        let fb = {
            if let Some(policy) = opt.filter_policy.clone() {
                let mut f = FilterBlockBuilder::new(policy.clone());
                f.start_block(0);
                Some(f)
            } else {
                None
            }
        };
        Self {
            options: opt,
            file,
            cmp: options.comparator.clone(),
            offset: 0,
            data_block: db_builder,
            index_block: ib_builder,
            last_key: vec![],
            num_entries: 0,
            closed: false,
            filter_block: fb,
            pending_index_entry: false,
            pending_handle: BlockHandle::new(0,0),
            err: None
        }
    }

    /// Adds a key/value pair to the table being constructed.
    /// If we just have flushed a new block data before, add a index entry into the index block.
    /// If the data block reaches the limit, it will be flushed
    ///
    /// # Panics
    ///
    /// * If key is after any previously added key according to comparator.
    /// * TableBuilder is closed
    ///
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<(), WickErr> {
        self.assert_not_closed();
        if self.num_entries > 0 {
            assert_ne!(self.cmp.compare(key, &self.last_key.as_slice()), Ordering::Greater,
                "[table builder] new key is inconsistent with the last key in sstable"
            )
        }
        // check iff we need to add a new entry into the index block
        self.maybe_append_index_block(Some(key));
        // write to filter block
        self.filter_block.as_mut().map(| fb| fb.add_key(&Slice::from(key)));
        // TODO: avoid the copy
        self.last_key.resize(key.len(), 0);
        self.last_key.copy_from_slice(key);
        self.num_entries += 1;
        // write to data block
        self.data_block.add(key, value);

        // flush the data to file block if reaching the block size limit
        if self.data_block.current_size_estimate() >= self.options.block_size {
            self.flush()?
        }
        Ok(())
    }

    /// Flushes any buffered key/value pairs to file.
    /// Can be used to ensure that two adjacent entries never live in
    /// the same data block. Most clients should not need to use this method.
    ///
    /// # Panics
    ///
    /// * The table builder is closed
    ///
    pub fn flush(&mut self) -> Result<(), WickErr> {
        self.assert_not_closed();
        if !self.data_block.is_empty() {
            assert!(!self.pending_index_entry, "[table builder] the index for the previous data block should never remain when flushing current block data");
            let data_block = self.data_block.finish();
            let (compressed, compression) = Self::compress_block(data_block, self.options.compression)?;
            write_raw_block(&mut self.file, compressed.as_slice(), compression, &mut self.pending_handle, &mut self.offset)?;
            self.pending_index_entry = true;
            if let Err(e) = self.file.flush() {
                return Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e)));
            }
            if let Some(fb) = &mut self.filter_block {
               fb.start_block(self.offset)
            }
        }
        Ok(())
    }

    /// Finishes building the table. Stops using the file passed to the
    /// constructor after this function returns.
    ///
    /// # Panics
    ///
    /// * The table builder is closed
    ///
    pub fn finish(&mut self) -> Result<(), WickErr> {
        self.flush()?;
        self.assert_not_closed();
        self.closed = true;
        // write filter block
        let mut filter_block_handler = BlockHandle::new(0,0);
        let mut has_filter_block = false;
        if let Some(fb) = &mut self.filter_block {
            let data = fb.finish();
            write_raw_block(&mut self.file, data, CompressionType::NoCompression, &mut filter_block_handler, &mut self.offset)?;
            has_filter_block = true;
        }

        // write meta block
        let mut meta_block_handle = BlockHandle::new(0,0);
        let mut meta_block_builder = BlockBuilder::new(self.options.block_restart_interval, self.cmp.clone());
        let meta_block = {
            if has_filter_block {
                let filter_key = if let Some(fp) = &self.options.filter_policy {
                    "filter.".to_owned() + fp.name()
                } else {
                    String::from("")
                };
                meta_block_builder.add(filter_key.as_bytes(), filter_block_handler.encoded().as_slice());
            }
            meta_block_builder.finish()
        };
        self.write_block(meta_block, &mut meta_block_handle)?;

        // write index block
        self.maybe_append_index_block(None);
        let index_block = self.index_block.finish();
        let mut index_block_handle = BlockHandle::new(0, 0);
        let (c_index_block, ct) = Self::compress_block(index_block, self.options.compression)?;
        write_raw_block(&mut self.file, c_index_block.as_slice(), ct, &mut index_block_handle, &mut self.offset)?;

        // write footer
        let footer = Footer::new(meta_block_handle,index_block_handle).encoded();
        if let Err(e) = self.file.write_all(footer.as_slice()) {
            return Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e)));
        } else {
            self.offset += footer.len() as u64;
        }
        Ok(())
    }

    /// Returns the number of key/value added so far.
    #[inline]
    pub fn num_entries(&self) -> usize {
        self.num_entries
    }

    /// Returns size of the file generated so far. If invoked after a successful
    /// `Finish` call, returns the size of the final generated file.
    #[inline]
    pub fn file_size(&self) -> u64 {
        self.offset
    }

    pub fn test_mut(&mut self) {
        self.offset = 0
    }

    #[inline]
    fn assert_not_closed(&self) {
        assert!(!self.closed, "[table builder] try to handle a closed TableBuilder");
    }

    fn maybe_append_index_block(&mut self, key: Option<&[u8]>) -> bool {
        if self.pending_index_entry {
            // We've flushed a data block to the file so omit a index entry to index block for it
            assert!(self.data_block.is_empty(), "[table builder] the data block buffer is not empty after flushed, something wrong must happen");
            let s = if let Some(k) = key {
                self.cmp.separator(self.last_key.as_slice(), k)
            } else {
                self.cmp.successor(self.last_key.as_slice())
            };
            // TODO: use a allocted buffer instead
            let mut handle_encoding = vec![];
            self.pending_handle.encoded_to(&mut handle_encoding);
            self.index_block.add(s.as_slice(), handle_encoding.as_slice());
            self.pending_index_entry = false;
            return true;
        }
        false
    }

    // Compresses the give raw block by configured compression algorithm.
    // Returns the compressed data and compression data.
    // Notice this method will create a buffer for it and copy the compressed data into the buffer.
    fn compress_block(raw_block: &[u8], compression: CompressionType) -> Result<(Vec<u8>, CompressionType),WickErr> {
        match compression {
            CompressionType::SnappyCompression => {
                let mut enc = snap::Encoder::new();
                // TODO: avoid this allocate ?
                let mut buffer = vec![];
                match enc.compress(raw_block, buffer.as_mut_slice()) {
                    Ok(_) => {},
                    Err(e) => return Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e))),
                }
                Ok((buffer, CompressionType::SnappyCompression))
            },
            CompressionType::NoCompression => Ok((Vec::from(raw_block), CompressionType::NoCompression)),
        }
    }

    fn write_block(&mut self, raw_block: &[u8], handle: &mut BlockHandle) -> Result<(), WickErr> {
        let (data, compression) = Self::compress_block(raw_block, self.options.compression)?;
        self.write_raw_block(data.as_slice(), compression, handle)?;
        Ok(())
    }

    // Write given block `data` with trailer to the file and update the 'handle'
    fn write_raw_block(&mut self, data: &[u8], compression: CompressionType, handle: &mut BlockHandle) -> Result<(), WickErr> {
        handle.set_offset(self.offset);
        handle.set_size(data.len() as u64);
        // write block data
        if let Err(e) = self.file.write_all(data) {
            return Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e)));
        };
        // write trailer
        let mut trailer = vec![0u8;BLOCK_TRAILER_SIZE];
        trailer[0] = compression as u8;
        let crc = extend(value(data), &[compression as u8]);
        put_fixed_32(&mut trailer, crc);
        if let Err(e) = self.file.write_all(trailer.as_slice()) {
            return Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e)));
        }
        self.offset += (data.len() + BLOCK_TRAILER_SIZE) as u64;
        Ok(())
    }
}

// This func is used to avoid multiple mutable borrows caused by write_raw_block above
fn write_raw_block(file: &mut File, data: &[u8], compression: CompressionType, handle: &mut BlockHandle,  offset: &mut u64) -> Result<(), WickErr> {
    // write block data
    if let Err(e) = file.write_all(data) {
        return Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e)));
    };
    // update the block handle
    handle.set_offset(*offset);
    handle.set_size(data.len() as u64);
    // write trailer
    let mut trailer = vec![0u8;BLOCK_TRAILER_SIZE];
    trailer[0] = compression as u8;
    let crc = extend(value(data), &[compression as u8]);
    put_fixed_32(&mut trailer, crc);
    if let Err(e) = file.write_all(trailer.as_slice()) {
        return Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e)));
    }
    // update offset
    *offset += (data.len() + BLOCK_TRAILER_SIZE) as u64;
    Ok(())
}

/// Read the block identified by "handle" from "file".  On failure
/// return non-OK.  On success fill *result and return OK.
pub fn read_block(file: File, options: ReadOptions, handle: BlockHandle ) -> Result<Vec<u8>, WickErr> {

    Ok(vec![])
}
