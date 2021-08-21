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

use crate::cache::Cache;
use crate::filter::FilterPolicy;
use crate::iterator::{ConcatenateIterator, DerivedIterFactory, Iterator};
use crate::options::{CompressionType, Options, ReadOptions};
use crate::sstable::block::{Block, BlockBuilder, BlockIterator};
use crate::sstable::filter_block::{FilterBlockBuilder, FilterBlockReader};
use crate::sstable::{BlockHandle, Footer, BLOCK_TRAILER_SIZE, FOOTER_ENCODED_LENGTH};
use crate::storage::File;
use crate::util::coding::{decode_fixed_32, put_fixed_32, put_fixed_64};
use crate::util::comparator::Comparator;
use crate::util::crc32::{extend, hash, mask, unmask};
use crate::{Error, Result};
use snap::raw::max_compress_len;
use std::cmp::Ordering;
use std::sync::Arc;

/// A `Table` is a sorted map from strings to strings, which must be immutable and persistent.
/// A `Table` may be safely accessed from multiple threads
/// without external synchronization.
pub struct Table<F: File> {
    file: F,
    file_number: u64,
    filter_reader: Option<FilterBlockReader>,
    meta_block_handle: Option<BlockHandle>,
    index_block: Block,
    block_cache: Option<Arc<dyn Cache<Vec<u8>, Arc<Block>>>>,
}

impl<F: File> Table<F> {
    /// Attempt to open the table that is stored in bytes `[0..size)`
    /// of `file`, and read the metadata entries necessary to allow
    /// retrieving data from the table.
    ///
    /// NOTE: `UC` for user comparator and `TC` for table comparator
    pub fn open<UC: Comparator, TC: Comparator>(
        file: F,
        file_number: u64,
        file_len: u64,
        options: Arc<Options<UC>>,
        cmp: TC,
    ) -> Result<Self> {
        if file_len < FOOTER_ENCODED_LENGTH as u64 {
            return Err(Error::Corruption(
                "file is too short to be an sstable".to_owned(),
            ));
        };
        // Read footer
        let mut footer_space = vec![0; FOOTER_ENCODED_LENGTH];
        file.read_exact_at(
            footer_space.as_mut_slice(),
            file_len - FOOTER_ENCODED_LENGTH as u64,
        )?;
        let (footer, _) = Footer::decode_from(footer_space.as_slice())?;
        // Read the index block
        let index_block_contents =
            read_block(&file, &footer.index_handle, options.paranoid_checks)?;
        let index_block = Block::new(index_block_contents)?;
        let mut t = Self {
            block_cache: options.block_cache.clone(),
            file,
            file_number,
            filter_reader: None,
            meta_block_handle: None,
            index_block,
        };
        // Read meta block
        if footer.meta_index_handle.size > 0 && options.filter_policy.is_some() {
            // ignore the reading errors since meta info is not needed for operation
            if let Ok(meta_block_contents) =
                read_block(&t.file, &footer.meta_index_handle, options.paranoid_checks)
            {
                if let Ok(meta_block) = Block::new(meta_block_contents) {
                    t.meta_block_handle = Some(footer.meta_index_handle);
                    let mut iter = meta_block.iter(cmp);
                    let filter_key = if let Some(fp) = &options.filter_policy {
                        "filter.".to_owned() + fp.name()
                    } else {
                        String::from("")
                    };
                    // Read filter block
                    iter.seek(filter_key.as_bytes());
                    if iter.valid() && iter.key() == filter_key.as_bytes() {
                        if let Ok((filter_handle, _)) = BlockHandle::decode_from(iter.value()) {
                            if let Ok(filter_block) =
                                read_block(&t.file, &filter_handle, options.paranoid_checks)
                            {
                                t.filter_reader = Some(FilterBlockReader::new(
                                    options.filter_policy.clone().unwrap(),
                                    filter_block,
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(t)
    }

    // Converts an BlockHandle into an iterator over the contents of the corresponding block.
    fn block_reader<CC: Comparator>(
        &self,
        cmp: CC,
        data_block_handle: BlockHandle,
        options: ReadOptions,
    ) -> Result<BlockIterator<CC>> {
        let iter = if let Some(cache) = &self.block_cache {
            let mut cache_key_buffer = vec![0; 16];
            put_fixed_64(&mut cache_key_buffer, self.file_number);
            put_fixed_64(&mut cache_key_buffer, data_block_handle.offset);
            if let Some(b) = cache.get(&cache_key_buffer) {
                b.iter(cmp)
            } else {
                let data = read_block(&self.file, &data_block_handle, options.verify_checksums)?;
                let charge = data.len();
                let new_block = Block::new(data)?;
                let b = Arc::new(new_block);
                let iter = b.iter(cmp);
                if options.fill_cache {
                    cache.insert(cache_key_buffer, b, charge);
                }
                iter
            }
        } else {
            let data = read_block(&self.file, &data_block_handle, options.verify_checksums)?;
            let b = Block::new(data)?;
            b.iter(cmp)
        };
        Ok(iter)
    }

    /// Finds the first entry with the key equal or greater than target and
    /// returns the block iterator direclty
    ///
    /// The given `key` is an internal key so the `cmp` must be a InternalKeyComparator
    ///
    pub fn internal_get<TC: Comparator>(
        &self,
        options: ReadOptions,
        cmp: TC,
        key: &[u8],
    ) -> Result<Option<BlockIterator<TC>>> {
        let mut index_iter = self.index_block.iter(cmp.clone());
        // seek to the first 'last key' bigger than 'key'
        index_iter.seek(key);
        if index_iter.valid() {
            // It's called 'maybe_contained' not only because the filter policy may report the falsy result,
            // but also even if we've found a block with the last key bigger than the target
            // the key may not be contained if the block is the first block of the sstable.
            let mut maybe_contained = true;

            let handle_val = index_iter.value();
            // check the filter block
            if let Some(filter) = &self.filter_reader {
                if let Ok((handle, _)) = BlockHandle::decode_from(handle_val) {
                    if !filter.key_may_match(handle.offset, key) {
                        maybe_contained = false;
                    }
                }
            }
            if maybe_contained {
                let (data_block_handle, _) = BlockHandle::decode_from(handle_val)?;
                let mut block_iter = self.block_reader(cmp, data_block_handle, options)?;
                block_iter.seek(key);
                if block_iter.valid() {
                    return Ok(Some(block_iter));
                }
                block_iter.seek_to_first();
                while block_iter.valid() {
                    block_iter.next();
                }
                block_iter.status()?;
            }
        }
        index_iter.status()?;
        Ok(None)
    }

    /// Given a key, return an approximate byte offset in the file where
    /// the data for that key begins (or would begin if the key were
    /// present in the file).  The returned value is in terms of file
    /// bytes, and so includes effects like compression of the underlying data.
    /// E.g., the approximate offset of the last key in the table will
    /// be close to the file length.
    pub(crate) fn approximate_offset_of<TC: Comparator>(&self, cmp: TC, key: &[u8]) -> u64 {
        let mut index_iter = self.index_block.iter(cmp);
        index_iter.seek(key);
        if index_iter.valid() {
            let val = index_iter.value();
            if let Ok((h, _)) = BlockHandle::decode_from(val) {
                return h.offset;
            }
        }
        if let Some(meta) = &self.meta_block_handle {
            return meta.offset;
        }
        0
    }
}

pub struct TableIterFactory<C: Comparator, F: File> {
    options: ReadOptions,
    table: Arc<Table<F>>,
    cmp: C,
}

impl<C: Comparator, F: File> DerivedIterFactory for TableIterFactory<C, F> {
    type Iter = BlockIterator<C>;
    fn derive(&self, value: &[u8]) -> Result<Self::Iter> {
        BlockHandle::decode_from(value).and_then(|(handle, _)| {
            self.table
                .block_reader(self.cmp.clone(), handle, self.options)
        })
    }
}

pub type TableIterator<C, F> = ConcatenateIterator<BlockIterator<C>, TableIterFactory<C, F>>;

/// Create a new `ConcatenateIterator` as table iterator.
/// This iterator is able to yield all the key/values in the given `table` file
///
/// Entry format:
///     key: internal key
///     value: value of user key
pub fn new_table_iterator<C: Comparator, F: File>(
    cmp: C,
    table: Arc<Table<F>>,
    options: ReadOptions,
) -> TableIterator<C, F> {
    let index_iter = table.index_block.iter(cmp.clone());
    let factory = TableIterFactory {
        options,
        table,
        cmp,
    };
    ConcatenateIterator::new(index_iter, factory)
}

/// Temporarily stores the contents of the table it is
/// building in .sst file but does not close the file. It is up to the
/// caller to close the file after calling `Finish()`.
pub struct TableBuilder<C: Comparator, F: File> {
    cmp: C,
    // underlying sst file
    file: F,
    // the written data length
    // updated only after the pending_handle is stored in the index block
    offset: u64,
    data_block: BlockBuilder<C>,
    index_block: BlockBuilder<C>,
    // the last added key
    // can be used when adding a new entry into index block
    last_key: Vec<u8>,
    // number of key/value pairs in the file
    num_entries: usize,
    closed: bool,
    filter_block: Option<FilterBlockBuilder>,
    // Indicates whether we have to add a index to index_block
    //
    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block. This allows us to use shorter
    // keys in the index block.
    pending_index_entry: bool,
    // handle for current block to add to index block
    pending_handle: BlockHandle,

    // Fields from `Options`
    block_size: usize,
    block_restart_interval: usize,
    compression: CompressionType,
    filter_policy: Option<Arc<dyn FilterPolicy>>,
}

impl<C: Comparator, F: File> TableBuilder<C, F> {
    pub fn new<UC: Comparator>(file: F, cmp: C, options: &Arc<Options<UC>>) -> Self {
        let opt = options.clone();
        let db_builder = BlockBuilder::new(options.block_restart_interval, cmp.clone());
        let ib_builder = BlockBuilder::new(options.block_restart_interval, cmp.clone());
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
            file,
            cmp,
            offset: 0,
            data_block: db_builder,
            index_block: ib_builder,
            last_key: vec![],
            num_entries: 0,
            closed: false,
            filter_block: fb,
            pending_index_entry: false,
            pending_handle: BlockHandle::new(0, 0),
            compression: opt.compression,
            block_size: opt.block_size,
            block_restart_interval: opt.block_restart_interval,
            filter_policy: opt.filter_policy.clone(),
        }
    }

    /// Adds a key/value pair to the table being constructed.
    /// If the data block reaches the limit, it will be flushed
    /// If we just have flushed a new block data before, add an index entry into the index block.
    ///
    /// # Panics
    ///
    /// * If key is after any previously added key according to comparator.
    /// * TableBuilder is closed
    ///
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.assert_not_closed();
        if self.num_entries > 0 {
            assert_eq!(
                self.cmp.compare(key, self.last_key.as_slice()),
                Ordering::Greater,
                "[table builder] new key is inconsistent with the last key in sstable"
            )
        }
        // Check whether we need to create a new index entry
        self.maybe_append_index_block(Some(key));
        // Update filter block
        if let Some(fb) = self.filter_block.as_mut() {
            fb.add_key(key)
        }
        // TODO: avoid the copy
        self.last_key.resize(key.len(), 0);
        self.last_key.copy_from_slice(key);
        self.num_entries += 1;
        // write to data block
        self.data_block.add(key, value);

        // flush the data to file block if reaching the block size limit
        if self.data_block.current_size_estimate() >= self.block_size {
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
    pub fn flush(&mut self) -> Result<()> {
        self.assert_not_closed();
        if !self.data_block.is_empty() {
            assert!(!self.pending_index_entry, "[table builder] the index for the previous data block should never remain when flushing current block data");
            let data_block = self.data_block.finish();
            let (compressed, compression) = compress_block(data_block, self.compression)?;
            write_raw_block(
                &mut self.file,
                compressed.as_slice(),
                compression,
                &mut self.pending_handle,
                &mut self.offset,
            )?;
            self.data_block.reset();
            self.pending_index_entry = true;
            self.file.flush()?;
            if let Some(fb) = &mut self.filter_block {
                fb.start_block(self.offset)
            }
        }
        Ok(())
    }

    /// Finishes building the table and close the relative file.
    /// If `sync` is true, the `File::flush` will be called.
    ///
    /// # Panics
    ///
    /// * The table builder is closed
    ///
    pub fn finish(&mut self, sync: bool) -> Result<()> {
        self.flush()?;
        self.assert_not_closed();
        self.closed = true;
        // write filter block
        let mut filter_block_handler = BlockHandle::new(0, 0);
        let mut has_filter_block = false;
        if let Some(fb) = &mut self.filter_block {
            let data = fb.finish();
            write_raw_block(
                &mut self.file,
                data,
                CompressionType::NoCompression,
                &mut filter_block_handler,
                &mut self.offset,
            )?;
            has_filter_block = true;
        }

        // write meta block
        let mut meta_block_handle = BlockHandle::new(0, 0);
        let mut meta_block_builder =
            BlockBuilder::new(self.block_restart_interval, self.cmp.clone());
        let meta_block = {
            if has_filter_block {
                let filter_key = if let Some(fp) = &self.filter_policy {
                    "filter.".to_owned() + fp.name()
                } else {
                    String::from("")
                };
                meta_block_builder.add(filter_key.as_bytes(), &filter_block_handler.encoded());
            }
            meta_block_builder.finish()
        };
        self.write_block(meta_block, &mut meta_block_handle)?;

        // Write index block
        self.maybe_append_index_block(None); // flush the last index first
        let index_block = self.index_block.finish();
        let mut index_block_handle = BlockHandle::new(0, 0);
        let (c_index_block, ct) = compress_block(index_block, self.compression)?;
        write_raw_block(
            &mut self.file,
            c_index_block.as_slice(),
            ct,
            &mut index_block_handle,
            &mut self.offset,
        )?;
        self.index_block.reset();
        // write footer
        let footer = Footer::new(meta_block_handle, index_block_handle).encoded();
        self.file.write(footer.as_slice())?;
        self.offset += footer.len() as u64;
        if sync {
            self.file.flush()?;
            self.file.close()?;
        }
        Ok(())
    }

    /// Mark this builder as closed
    #[inline]
    pub fn close(&mut self) {
        assert!(
            !self.closed,
            "[table builder] try to close a closed TableBuilder"
        );
        self.closed = true;
        // TODO: maybe return Result<()> ?
        let _ = self.file.close();
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
        assert!(
            !self.closed,
            "[table builder] try to handle a closed TableBuilder"
        );
    }

    // Add a key into the index block if neccessary
    fn maybe_append_index_block(&mut self, key: Option<&[u8]>) -> bool {
        if self.pending_index_entry {
            // We've flushed a data block to the file so adding an relate index entry into index block
            assert!(self.data_block.is_empty(), "[table builder] the data block buffer is not empty after flushed, something is wrong");
            let s = if let Some(k) = key {
                self.cmp.separator(&self.last_key, k)
            } else {
                self.cmp.successor(&self.last_key)
            };
            // TODO: use a allocted buffer instead
            let mut handle_encoding = vec![];
            self.pending_handle.encoded_to(&mut handle_encoding);
            self.index_block.add(&s, &handle_encoding);
            self.pending_index_entry = false;
            return true;
        }
        false
    }

    fn write_block(&mut self, raw_block: &[u8], handle: &mut BlockHandle) -> Result<()> {
        let (data, compression) = compress_block(raw_block, self.compression)?;
        write_raw_block(&mut self.file, &data, compression, handle, &mut self.offset)?;
        Ok(())
    }
}

// Compresses the give raw block by configured compression algorithm.
// Returns the compressed data and compression data.
fn compress_block(
    raw_block: &[u8],
    compression: CompressionType,
) -> Result<(Vec<u8>, CompressionType)> {
    match compression {
        CompressionType::SnappyCompression => {
            let mut enc = snap::raw::Encoder::new();
            // TODO: avoid this allocation ?
            let mut buffer = vec![0; max_compress_len(raw_block.len())];
            match enc.compress(raw_block, buffer.as_mut_slice()) {
                Ok(size) => buffer.truncate(size),
                Err(e) => return Err(Error::CompressionFailed(e)),
            }
            Ok((buffer, CompressionType::SnappyCompression))
        }
        CompressionType::NoCompression | CompressionType::Unknown => {
            Ok((Vec::from(raw_block), CompressionType::NoCompression))
        }
    }
}

// Write given block data into the file with block trailer
fn write_raw_block<F: File>(
    file: &mut F,
    data: &[u8],
    compression: CompressionType,
    handle: &mut BlockHandle,
    offset: &mut u64,
) -> Result<()> {
    // write block data
    file.write(data)?;
    // update the block handle
    handle.set_offset(*offset);
    handle.set_size(data.len() as u64);
    // write trailer
    // TODO: use pre-allocated buf
    let mut trailer = vec![compression as u8];
    let crc = mask(extend(hash(data), &[compression as u8]));
    put_fixed_32(&mut trailer, crc);
    assert_eq!(trailer.len(), BLOCK_TRAILER_SIZE);
    file.write(trailer.as_slice())?;
    // update offset
    *offset += (data.len() + BLOCK_TRAILER_SIZE) as u64;
    Ok(())
}

// Read the block identified from `file` according to the given `handle`.
// If the read data does not match the checksum, return a error marked as `Status::Corruption`
fn read_block<F: File>(file: &F, handle: &BlockHandle, verify_checksum: bool) -> Result<Vec<u8>> {
    let n = handle.size as usize;
    // TODO: use pre-allocated buf
    let mut buffer = vec![0; n + BLOCK_TRAILER_SIZE];
    file.read_exact_at(buffer.as_mut_slice(), handle.offset)?;
    if verify_checksum {
        let crc = unmask(decode_fixed_32(&buffer[n + 1..]));
        // Compression type is included in CRC checksum
        let actual = hash(&buffer[..=n]);
        if crc != actual {
            return Err(Error::Corruption("block checksum mismatch".to_owned()));
        }
    }
    let data = {
        match CompressionType::from(buffer[n]) {
            CompressionType::NoCompression => {
                buffer.truncate(buffer.len() - BLOCK_TRAILER_SIZE);
                buffer
            }
            CompressionType::SnappyCompression => {
                // TODO: use pre-allocated buf
                let mut decompressed = vec![];
                match snap::raw::decompress_len(&buffer[..n]) {
                    Ok(len) => {
                        decompressed.resize(len, 0u8);
                    }
                    Err(e) => {
                        return Err(Error::CompressionFailed(e));
                    }
                }
                let mut dec = snap::raw::Decoder::new();
                if let Err(e) = dec.decompress(&buffer[..n], decompressed.as_mut_slice()) {
                    return Err(Error::CompressionFailed(e));
                }
                decompressed
            }
            CompressionType::Unknown => {
                return Err(Error::Corruption("bad block compression type".to_owned()))
            }
        }
    };
    Ok(data)
}

#[cfg(test)]
mod tests {
    use crate::filter::bloom::BloomFilter;
    use crate::iterator::Iterator;
    use crate::sstable::block::Block;
    use crate::sstable::table::{read_block, Table, TableBuilder};
    use crate::sstable::BlockHandle;
    use crate::storage::mem::MemStorage;
    use crate::util::comparator::BytewiseComparator;
    use crate::{File, Options, ReadOptions, Storage};
    use std::sync::Arc;

    #[test]
    fn test_build_empty_table_with_meta_block() {
        let s = MemStorage::default();
        let mut o = Options::<BytewiseComparator>::default();
        let bf = BloomFilter::new(16);
        o.filter_policy = Some(Arc::new(bf));
        let opt = Arc::new(o);
        let new_file = s.create("test").unwrap();
        let cmp = BytewiseComparator::default();
        let mut tb = TableBuilder::new(new_file, cmp, &opt);
        tb.finish(false).unwrap();
        let file = s.open("test").unwrap();
        let file_len = file.len().unwrap();
        let table = Table::open(file, 0, file_len, opt.clone(), cmp).unwrap();
        assert!(table.filter_reader.is_some());
        assert!(table.meta_block_handle.is_some());
    }

    #[test]
    fn test_build_empty_table_without_meta_block() {
        let s = MemStorage::default();
        let new_file = s.create("test").unwrap();
        let opt = Arc::new(Options::<BytewiseComparator>::default()); // no filter block on default
        let cmp = BytewiseComparator::default();
        let mut tb = TableBuilder::new(new_file, cmp, &opt);
        tb.finish(false).unwrap();
        let file = s.open("test").unwrap();
        let file_len = file.len().unwrap();
        let cmp = BytewiseComparator::default();
        let table = Table::open(file, 0, file_len, opt, cmp).unwrap();
        assert!(table.filter_reader.is_none());
        assert!(table.meta_block_handle.is_none()); // no filter block means no meta block
        let read_opt = ReadOptions::default();
        let res = table.internal_get(read_opt, cmp, b"test").unwrap();
        assert!(res.is_none());
    }

    #[test]
    #[should_panic]
    fn test_table_add_consistency() {
        let s = MemStorage::default();
        let new_file = s.create("test").expect("file create should work");
        let opt = Arc::new(Options::<BytewiseComparator>::default());
        let mut tb = TableBuilder::new(new_file, BytewiseComparator::default(), &opt);
        tb.add(b"222", b"").unwrap();
        tb.add(b"1", b"").unwrap();
    }

    #[test]
    fn test_block_write_and_read() {
        let s = MemStorage::default();
        let new_file = s.create("test").expect("file create should work");
        let opt = Arc::new(Options::<BytewiseComparator>::default());
        let cmp = BytewiseComparator::default();
        let mut tb = TableBuilder::new(new_file, cmp, &opt);
        let test_pairs = vec![("", "test"), ("aaa", "123"), ("bbb", "456"), ("ccc", "789")];
        for (key, val) in test_pairs.clone().drain(..) {
            tb.data_block.add(key.as_bytes(), val.as_bytes());
        }
        let block = Vec::from(tb.data_block.finish());
        let mut bh = BlockHandle::new(0, 0);
        tb.write_block(&block, &mut bh).unwrap();
        let file = s.open("test").expect("file open should work");
        let res = read_block(&file, &bh, true).unwrap();
        assert_eq!(res, block);
        let block = Block::new(res).unwrap();
        let mut iter = block.iter(cmp);
        iter.seek_to_first();
        let mut result_pairs = vec![];
        while iter.valid() {
            result_pairs.push((iter.key().to_vec(), iter.value().to_vec()));
            iter.next();
        }
        assert_eq!(result_pairs.len(), test_pairs.len());
        for (p1, p2) in result_pairs.iter().zip(test_pairs) {
            assert_eq!(p1.0.as_slice(), p2.0.as_bytes());
            assert_eq!(p1.1.as_slice(), p2.1.as_bytes());
        }
    }

    #[test]
    fn test_table_write_and_read() {
        let s = MemStorage::default();
        let new_file = s.create("test").unwrap();
        let opt = Arc::new(Options::<BytewiseComparator>::default());
        let cmp = BytewiseComparator::default();
        let mut tb = TableBuilder::new(new_file, cmp, &opt);
        let tests = vec![("", "test"), ("a", "aa"), ("b", "bb")];
        for (key, val) in tests.clone().drain(..) {
            tb.add(key.as_bytes(), val.as_bytes()).unwrap();
        }
        tb.finish(false).unwrap();
        let file = s.open("test").unwrap();
        let file_len = file.len().unwrap();
        let table = Table::open(file, 0, file_len, opt.clone(), cmp).unwrap();
        let read_opt = ReadOptions {
            verify_checksums: true,
            fill_cache: true,
            snapshot: None,
        };
        for (key, val) in tests.clone().drain(..) {
            assert_eq!(
                val.as_bytes(),
                table
                    .internal_get(read_opt, cmp, key.as_bytes())
                    .unwrap()
                    .unwrap()
                    .value()
            );
        }
    }
}
