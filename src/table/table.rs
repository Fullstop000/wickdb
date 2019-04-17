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

use crate::table::{BlockHandle, BLOCK_TRAILER_SIZE, Footer, FOOTER_ENCODED_LENGTH};
use crate::table::block::{Block, BlockBuilder};
use crate::options::{Options, CompressionType, ReadOptions};
use crate::table::filter_block::{FilterBlockReader, FilterBlockBuilder};
use crate::util::status::{WickErr, Status};
use std::rc::Rc;
use std::mem;
use crate::util::comparator::Comparator;
use crate::util::slice::Slice;
use std::cmp::Ordering;
use crate::iterator::{Iterator, EmptyIterator};
use crate::util::crc32::{value, extend, unmask};
use crate::util::coding::{put_fixed_32, decode_fixed_32, put_fixed_64};
use crate::storage::File;
use crate::util::byte::compare;

/// A `Table` is a sorted map from strings to strings.  Tables are
/// immutable and persistent.  A Table may be safely accessed from
/// multiple threads without external synchronization.
pub struct Table {
    options: Rc<Options>,
    file: Box<dyn File>,
    cache_id: u64,
    filter_reader: Option<FilterBlockReader>,
    // None iff we fail to read meta block
    meta_block_handle: Option<BlockHandle>,
    index_block: Block,

    // iterating fields
    read_options: ReadOptions,
    index_iter: Box<dyn Iterator>,
    // None: a error happens in the index iterator
    // EmptyIterator with a WickErr: a error happens in previous data iterator
    data_block_iter: Option<Box<dyn Iterator>>,
    current_data_block_handle: Vec<u8>,
    err: Option<WickErr>,
}

// Common methods
impl Table {
    /// Attempt to open the table that is stored in bytes `[0..size)`
    /// of `file`, and read the metadata entries necessary to allow
    /// retrieving data from the table.
    pub fn open(file: Box<dyn File>, size: u64, options: Rc<Options>) -> Result<Self, WickErr> {
        if size < FOOTER_ENCODED_LENGTH as u64 {
            return Err(WickErr::new(Status::Corruption, Some("file is too short to be an sstable")));
        };
        // Read footer
        let mut footer_space = vec![0; FOOTER_ENCODED_LENGTH];
        if let Err(e) = file.read_exact_at(footer_space.as_mut_slice(), size - FOOTER_ENCODED_LENGTH as u64) {
            return Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e)));
        };
        let (footer, _) = Footer::decode_from(footer_space.as_slice())?;

        // Read the index block
        let index_block_contents = read_block(&file, &footer.index_handle, options.paranoid_checks)?;
        let index_block = Block::new(index_block_contents)?;

        let cache_id = if let Some(cache) = &options.block_cache {
            cache.borrow_mut().new_id()
        } else {
            0
        };
        let mut t = Self {
            options: options.clone(),
            file,
            cache_id,
            filter_reader: None,
            meta_block_handle: None,
            index_block,

            read_options: ReadOptions::default(),
            index_iter: EmptyIterator::new(),
            data_block_iter: None,
            current_data_block_handle: vec![],
            err: None,
        };
        // Read meta block
        if footer.meta_index_handle.size > 0 && options.filter_policy.is_some() {
            // ignore the reading errors since meta info is not needed for operation
            if let Ok(meta_block_contents) = read_block(&t.file, &footer.meta_index_handle, options.paranoid_checks) {
                if let Ok(meta_block) = Block::new(meta_block_contents) {
                    let mut iter = meta_block.iter(options.comparator.clone());
                    let filter_key = if let Some(fp) = &options.filter_policy {
                        "filter.".to_owned() + fp.name()
                    } else {
                        String::from("")
                    };
                    // Read filter block
                    iter.seek(&Slice::from(filter_key.as_bytes()));
                    if iter.valid() && iter.key().as_str() == filter_key.as_str() {
                        if let Ok((filter_handle, _)) = BlockHandle::decode_from(iter.value().to_slice()) {
                            if let Ok(filter_block) = read_block(&t.file, &filter_handle, options.paranoid_checks) {
                                t.filter_reader = Some(FilterBlockReader::new(t.options.filter_policy.clone().unwrap(), filter_block));
                            }
                        }
                    }
                }
            }
        }
        Ok(t)
    }

    /// Converts an BlockHandle into an iterator over the contents of the corresponding block.
    pub fn block_reader(&self, data_block_handle: BlockHandle, options: &ReadOptions) -> Result<Box<dyn Iterator>, WickErr> {
        let block = if let Some(cache) = &self.options.block_cache {
            let mut cache_key_buffer = vec![0;16];
            put_fixed_64(&mut cache_key_buffer, self.cache_id);
            put_fixed_64(&mut cache_key_buffer, data_block_handle.offset);
            if let Some(cache_handle) = cache.borrow().look_up(&cache_key_buffer.as_slice()) {
                // TODO: use Rc to avoid value copy ?
                cache_handle.borrow().get_value().clone()
            } else {
                let data = read_block(&self.file, &data_block_handle, options.verify_checksums)?;
                let charge = data.len();
                let new_block = Block::new(data)?;
                if options.fill_cache {
                    // TODO: avoid clone
                    cache.borrow_mut().insert(cache_key_buffer, new_block.clone(), charge, None);
                }
                new_block
            }
        } else {
            let data = read_block(&self.file, &data_block_handle, options.verify_checksums)?;
            Block::new(data)?
        };
        Ok(block.iter(self.options.comparator.clone()))
    }

    /// Creates index iterator and assigns given ReadOptions
    pub fn init_iter(&mut self, options: ReadOptions) {
        let cmp = self.options.comparator.clone();
        self.index_iter = self.index_block.iter(cmp.clone());
        self.read_options = options;
    }

    /// Given a key, return an approximate byte offset in the file where
    /// the data for that key begins (or would begin if the key were
    /// present in the file).  The returned value is in terms of file
    /// bytes, and so includes effects like compression of the underlying data.
    /// E.g., the approximate offset of the last key in the table will
    /// be close to the file length.
    pub fn approximate_offset_of(&self, key: &[u8]) -> u64 {
        let mut index_iter = self.index_block.iter(self.options.comparator.clone());
        index_iter.seek(&Slice::from(key));
        if index_iter.valid() {
            let val = index_iter.value();
            if let Ok((h, _)) = BlockHandle::decode_from(val.to_slice()) {
                return h.offset;
            }
        }
        if let Some(meta) = &self.meta_block_handle {
            return meta.offset;
        }
        0
    }

    // Gets the first entry with the key equal or greater than target, then calls the 'callback'
    fn internal_get(&self, options: ReadOptions, key: &[u8], mut callback: Box<FnMut(&[u8], &[u8])>) -> Result<(), WickErr> {
        let mut index_iter = self.index_block.iter(self.options.comparator.clone());
        // seek to the first 'last key' bigger than 'key'
        index_iter.seek(&Slice::from(key));
        if index_iter.valid() {

            // It's called 'maybe_contained' not only because the filter policy may report the falsy result,
            // but also even if we've find a block with the last key bigger than the target
            // the key may not be contained if the block is the first block of the sstable.
            let mut maybe_contained = true;

            let handle_val = index_iter.value();
            // check the filter block
            if let Some(filter) = &self.filter_reader {
                if let Ok((handle, _)) = BlockHandle::decode_from(handle_val.to_slice()) {
                    if !filter.key_may_match(handle.offset, &Slice::from(key)) {
                        maybe_contained = false;
                    }
                }
            }
            if maybe_contained {
                let (data_block_handle, _)  = BlockHandle::decode_from(handle_val.to_slice())?;
                let mut block_iter = self.block_reader(data_block_handle, &options)?;
                block_iter.seek(&Slice::from(key));
                if block_iter.valid() {
                    callback(block_iter.key().to_slice(), block_iter.value().to_slice());
                }
                block_iter.status()?
            }
        }
        index_iter.status()
    }

}

// Iterator private methods
// TODO: use a trait to describe behaviors below
impl Table {
    #[inline]
    fn maybe_save_err(old: &mut Option<WickErr>, new: Result<(), WickErr>) {
        if old.is_none() && new.is_err() {
            mem::replace::<Option<WickErr>>(old, Some(new.unwrap_err()));
        }
    }
    // Try to read data block according to the current value of index iterator.
    fn init_data_block(&mut self) {
        if !self.index_iter.valid() {
            self.data_block_iter = None;
        } else {
            let v = self.index_iter.value();
            if self.data_block_iter.is_none() || compare(self.current_data_block_handle.as_slice(), v.to_slice()) != Ordering::Greater {
                match BlockHandle::decode_from(v.to_slice()) {
                    Ok((handle, _)) => {
                        match self.block_reader(handle, &self.read_options) {
                            Ok(bi) => {
                                self.set_data_iter(Some(bi))
                            },
                            Err(e) => {
                                self.set_data_iter(Some(EmptyIterator::new_with_err(e)))
                            }
                        }
                    },
                    Err(e) => {
                        self.set_data_iter(Some(EmptyIterator::new_with_err(e)))
                    },
                }
            }
        }
    }

    fn set_data_iter(&mut self, iter: Option<Box<dyn Iterator>>) {
        if let Some(di) = &mut self.data_block_iter {
            Self::maybe_save_err(&mut self.err, di.status());
        }
        self.data_block_iter = iter
    }

    // Used to skip invalid data blocks util finding a valid data block by `next()`
    // If found, set data block to the first
    fn skip_forward(&mut self) {
        while let Some(di) = &self.data_block_iter {
            if !di.valid() {
                break
            }
            // Move to next data block
            if !self.index_iter.valid() {
                self.set_data_iter(None)
            } else {
                self.index_iter.next();
                self.init_data_block();
                if let Some(i) = &mut self.data_block_iter {
                    // init to the first
                    i.seek_to_first();
                }
            }
        }
    }

    // Used to skip invalid data blocks util finding a valid data block by `prev()`
    // If found, set data block to the last
    fn skip_backward(&mut self) {
        while let Some(di) = &self.data_block_iter {
            if !di.valid() {
                break
            }
            // Move to next data block
            if !self.index_iter.valid() {
                self.set_data_iter(None)
            } else {
                self.index_iter.prev();
                self.init_data_block();
                if let Some(i) = &mut self.data_block_iter {
                    // init to the first
                    i.seek_to_last();
                }
            }
        }
    }
    #[inline]
    fn valid_or_panic(&self) {
        assert!(self.valid(), "[table iterator] invalid data iterator")
    }
}
impl Iterator for Table {
    fn valid(&self) -> bool {
        if let Some(data_iter) = &self.data_block_iter {
            return data_iter.valid();
        } else {
            // we have a err in the index iterator
            return false;
        }
    }

    fn seek_to_first(&mut self) {
        self.index_iter.seek_to_first();
        self.init_data_block();
        if let Some(di) = &mut self.data_block_iter {
            di.seek_to_first();
        }
        // to the first valid
        self.skip_forward();
    }

    fn seek_to_last(&mut self) {
        self.index_iter.seek_to_last();
        self.init_data_block();
        if let Some(di) = &mut self.data_block_iter {
            di.seek_to_last();
        }
        // to the last valid
        self.skip_backward();
    }

    fn seek(&mut self, target: &Slice) {
        self.index_iter.seek(target);
        if let Some(di) = &mut self.data_block_iter {
            di.seek(target);
        }
        // to the first valid
        self.skip_forward();
    }

    fn next(&mut self) {
        self.valid_or_panic();
        self.data_block_iter.as_mut().map_or((), |di| di.next());
        self.skip_forward();
    }

    fn prev(&mut self) {
        self.valid_or_panic();
        self.data_block_iter.as_mut().map_or((), |di| di.prev());
        self.skip_backward();
    }

    fn key(&self) -> Slice {
        self.valid_or_panic();
        self.data_block_iter.as_ref().map_or(Slice::new_empty(), |di| di.key())
    }

    fn value(&self) -> Slice {
        self.valid_or_panic();
        self.data_block_iter.as_ref().map_or(Slice::new_empty(), |di| di.value())
    }

    fn status(&mut self) -> Result<(), WickErr> {
        self.index_iter.status()?;
        if let Some(di) = &mut self.data_block_iter {
            di.status()?
        };
        if let Some(e) = self.err.take() {
            Err(e)?
        }
        Ok(())
    }
}

/// Temporarily stores the contents of the table it is
/// building in .sst file but does not close the file. It is up to the
/// caller to close the file after calling `Finish()`.
pub struct TableBuilder {
    options: Rc<Options>,
    cmp: Rc<Box<dyn Comparator>>,
    // underlying sst file
    file: Box<dyn File>,
    // the written data length
    // updated only after the pending_handle is stored in the index block
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
    //
    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block. This allows us to use shorter
    // keys in the index block.
    pending_index_entry: bool,
    // handle for current block to add to index block
    pending_handle: BlockHandle,
    err: Option<WickErr>,
}

impl TableBuilder {
    pub fn new(file: Box<dyn File>, options: Rc<Options>) -> Self {
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
            let (compressed, compression) = compress_block(data_block, self.options.compression)?;
            write_raw_block(&mut self.file, compressed.as_slice(), compression, &mut self.pending_handle, &mut self.offset)?;
            self.pending_index_entry = true;
            if let Err(e) = self.file.f_flush() {
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
        let (c_index_block, ct) = compress_block(index_block, self.options.compression)?;
        write_raw_block(&mut self.file, c_index_block.as_slice(), ct, &mut index_block_handle, &mut self.offset)?;

        // write footer
        let footer = Footer::new(meta_block_handle,index_block_handle).encoded();
        if let Err(e) = self.file.f_write(footer.as_slice()) {
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


    fn write_block(&mut self, raw_block: &[u8], handle: &mut BlockHandle) -> Result<(), WickErr> {
        let (data, compression) = compress_block(raw_block, self.options.compression)?;
        self.write_raw_block(data.as_slice(), compression, handle)?;
        Ok(())
    }

    // Write given block `data` with trailer to the file and update the 'handle'
    fn write_raw_block(&mut self, data: &[u8], compression: CompressionType, handle: &mut BlockHandle) -> Result<(), WickErr> {
        handle.set_offset(self.offset);
        handle.set_size(data.len() as u64);
        // write block data
        if let Err(e) = self.file.f_write(data) {
            return Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e)));
        };
        // write trailer
        let mut trailer = vec![0u8;BLOCK_TRAILER_SIZE];
        trailer[0] = compression as u8;
        let crc = extend(value(data), &[compression as u8]);
        put_fixed_32(&mut trailer, crc);
        if let Err(e) = self.file.f_write(trailer.as_slice()) {
            return Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e)));
        }
        self.offset += (data.len() + BLOCK_TRAILER_SIZE) as u64;
        Ok(())
    }
}

// Compresses the give raw block by configured compression algorithm.
// Returns the compressed data and compression data.
fn compress_block(raw_block: &[u8], compression: CompressionType) -> Result<(Vec<u8>, CompressionType),WickErr> {
    match compression {
        CompressionType::SnappyCompression => {
            let mut enc = snap::Encoder::new();
            // TODO: avoid this allocation ?
            let mut buffer = vec![];
            match enc.compress(raw_block, buffer.as_mut_slice()) {
                Ok(_) => {},
                Err(e) => return Err(WickErr::new_from_raw(Status::CompressionError, None, Box::new(e))),
            }
            Ok((buffer, CompressionType::SnappyCompression))
        },
        CompressionType::NoCompression => Ok((Vec::from(raw_block), CompressionType::NoCompression)),
    }
}

// This func is used to avoid multiple mutable borrows caused by write_raw_block(&mut self..) above
fn write_raw_block(file: &mut Box<dyn File>, data: &[u8], compression: CompressionType, handle: &mut BlockHandle,  offset: &mut u64) -> Result<(), WickErr> {
    // write block data
    if let Err(e) = file.f_write(data) {
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
    if let Err(e) = file.f_write(trailer.as_slice()) {
        return Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e)));
    }
    // update offset
    *offset += (data.len() + BLOCK_TRAILER_SIZE) as u64;
    Ok(())
}

/// Read the block identified from `file` according to the given `handle`.
/// If the read data does not match the checksum, return a error marked as `Status::Corruption`
pub fn read_block(file: &Box<dyn File>, handle: &BlockHandle, verify_checksum: bool ) -> Result<Vec<u8>, WickErr> {
    let n = handle.size as usize;
    let mut buffer = vec![0; n + BLOCK_TRAILER_SIZE];
    if let Err(e) = file.f_read_at(buffer.as_mut_slice(), handle.offset) {
        return Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e)));
    }
    if verify_checksum {
        let crc = unmask(decode_fixed_32(&buffer.as_slice()[n + 1..]));
        let actual = value(&buffer.as_slice()[..n]);
        if crc != actual {
            return Err(WickErr::new(Status::Corruption, Some("block checksum mismatch")))
        }
    }

    let data = {
        match CompressionType::from(buffer[n]) {
            CompressionType::NoCompression => {
                buffer.truncate(BLOCK_TRAILER_SIZE);
                buffer
            },
            CompressionType::SnappyCompression => {
                let mut decompressed = vec![];
                match snap::decompress_len(&buffer.as_slice()[..n]) {
                    Ok(len) => {
                        decompressed.resize(len, 0u8);
                    },
                    Err(e) => {
                        return Err(WickErr::new_from_raw(Status::CompressionError, None, Box::new(e)));
                    },
                }
                let mut dec = snap::Decoder::new();
                if let Err(e) = dec.decompress(&buffer.as_slice()[..n], decompressed.as_mut_slice()) {
                    return Err(WickErr::new_from_raw(Status::CompressionError, None, Box::new(e)));
                }
                decompressed
            }
        }
    };
    Ok(data)
}

#[cfg(test)]
mod tests {
    // TODO: add tests case after finishing the storage
}