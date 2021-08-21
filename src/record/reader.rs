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

use crate::record::reader::ReaderError::{BadRecord, EOF};
use crate::record::{RecordType, BLOCK_SIZE, HEADER_SIZE};
use crate::storage::File;
use crate::util::coding::decode_fixed_32;
use crate::util::crc32::{hash, unmask};
use std::io::SeekFrom;

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug)]
enum ReaderError {
    // * We have an internal reading file error
    // * We reaches the end of a log block
    // * We get a record that larger than BLOCK_SIZE
    EOF,
    // Indicates that we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    // * The record is below constructor's initial_offset (No drop is reported)
    BadRecord,
}

// represent a record
#[derive(Debug, Clone)]
struct Record {
    t: RecordType,
    data: Vec<u8>,
}

/// Notified when log reader encounters corruption.
pub trait Reporter {
    /// Some corruption was detected.  "bytes" is the approximate number
    /// of bytes dropped due to the corruption.
    fn corruption(&mut self, bytes: u64, reason: &str);
}

/// A `Reader` is used for reading records from log file.
/// The `Reader` always starts reading the records at `initial_offset` of the `file`.
pub struct Reader<F: File> {
    // NOTICE: we probably mutate the underlying file in the FilePtr by calling `seek()` and this is not thread safe
    file: F,
    reporter: Option<Box<dyn Reporter>>,
    // We should check sum for the record or not
    checksum: bool,
    // Last Read() indicated EOF by returning < BLOCK_SIZE
    eof: bool,
    // Offset of the last record returned by `read_record`.
    last_record_offset: u64,
    // Offset of the first location past the end of buf.
    end_of_buffer_offset: u64,
    // cache for current reading block
    buf: Vec<u8>,
    // the valid data length in buf
    buf_length: usize,
    // Offset at which to start looking for the first record to return
    initial_offset: u64,
    // if true, the reader will fast forward to the first valid First record or Full record
    // see the test case 'test_skip_into_multi_record'
    resyncing: bool,
}

impl<F: File> Reader<F> {
    pub fn new(
        file: F,
        reporter: Option<Box<dyn Reporter>>,
        checksum: bool,
        initial_offset: u64,
    ) -> Self {
        Reader {
            file,
            reporter,
            checksum,
            buf: vec![0; BLOCK_SIZE],
            buf_length: 0,
            eof: false,
            last_record_offset: 0,
            end_of_buffer_offset: 0,
            initial_offset,
            resyncing: initial_offset > 0,
        }
    }

    /// Deliver the file's ownership
    #[inline]
    pub fn into_file(self) -> F {
        self.file
    }

    /// Read the next complete record into given `buf`.
    /// Returns true if read successfully, false if we hit end of the input.
    pub fn read_record(&mut self, buf: &mut Vec<u8>) -> bool {
        if self.last_record_offset < self.initial_offset && !self.skip_to_initial_block() {
            return false;
        }
        // indicates that a record has been spilt into fragments
        let mut in_fragmented_record = false;
        // Record offset of the logical record that we're reading
        let mut prospective_record_offset = 0;
        loop {
            match self.read_physical_record() {
                Ok(mut record) => {
                    if self.resyncing {
                        match record.t {
                            RecordType::Middle => continue,
                            RecordType::Last => {
                                self.resyncing = false;
                                continue;
                            }
                            _ => self.resyncing = false,
                        }
                    }
                    let fragment_size = record.data.len() as u64;
                    // the start offset of the current read record
                    let physical_record_offset = self.end_of_buffer_offset
                        - self.buf_length as u64
                        - HEADER_SIZE as u64
                        - fragment_size;
                    match record.t {
                        RecordType::Full => {
                            if in_fragmented_record {
                                self.report_drop(
                                    buf.len() as u64,
                                    "partial record without end(1) for reading a new Full record",
                                );
                            }
                            // update record offset
                            self.last_record_offset = physical_record_offset;
                            buf.clear();
                            buf.append(&mut record.data);
                            return true;
                        }
                        RecordType::First => {
                            if in_fragmented_record {
                                self.report_drop(
                                    buf.len() as u64,
                                    "partial record without end(2) for reading a new First record",
                                );
                            }
                            prospective_record_offset = physical_record_offset;

                            // clean the potential corruption
                            buf.clear();
                            buf.append(&mut record.data);
                            in_fragmented_record = true;
                        }
                        RecordType::Middle => {
                            if !in_fragmented_record {
                                self.report_drop(
                                    fragment_size,
                                    format!(
                                        "missing start of fragmented record({:?})",
                                        RecordType::Middle
                                    )
                                    .as_str(),
                                );
                            // continue reading until find a new first or full record
                            } else {
                                buf.append(&mut record.data);
                            }
                        }
                        RecordType::Last => {
                            if !in_fragmented_record {
                                self.report_drop(
                                    fragment_size,
                                    format!(
                                        "missing start of fragmented record({:?})",
                                        RecordType::Last
                                    )
                                    .as_str(),
                                );
                            // continue reading until find a new first or full record
                            } else {
                                buf.extend(record.data);
                                // notice that we update the last_record_offset after we get the Last part but not the First
                                self.last_record_offset = prospective_record_offset;
                                return true;
                            }
                        }
                        RecordType::Zero => {
                            /* zero type record is considered as irrelevant and should never be read out*/
                        }
                    }
                }
                Err(e) => {
                    match e {
                        ReaderError::EOF => {
                            if in_fragmented_record {
                                // This can be caused by the writer dying immediately after writing a
                                // physical record but before completing the next
                                // one; don't treat it as a corruption,
                                // just ignore the entire logical record.
                                buf.clear();
                            }
                            return false;
                        }
                        ReaderError::BadRecord => {
                            if in_fragmented_record {
                                self.report_drop(
                                    buf.len() as u64,
                                    "bad record read in middle of record",
                                );
                                in_fragmented_record = false;
                                buf.clear();
                            }
                        }
                    }
                }
            }
        }
    }

    // Returns the last_record_offset.
    // Temporary for test.
    #[inline]
    #[allow(dead_code)]
    pub(super) fn last_record_offset(&self) -> u64 {
        self.last_record_offset
    }

    fn read_physical_record(&mut self) -> Result<Record, ReaderError> {
        loop {
            // we've reached the end of a block and do not have a valid header
            if self.buf_length < HEADER_SIZE {
                self.clear_buf();
                if !self.eof {
                    // try to read a block into the buf
                    match self.file.read(&mut self.buf) {
                        Ok(read) => {
                            self.end_of_buffer_offset += read as u64; // update the end offset here
                            self.buf_length = read;
                            if read < BLOCK_SIZE as usize {
                                self.eof = true;
                            }
                        }
                        Err(e) => {
                            self.report_drop(BLOCK_SIZE as u64, &e.to_string());
                            self.eof = true;
                            return Err(ReaderError::EOF);
                        }
                    }
                    continue;
                } else {
                    // If buffer is non-empty, it means we have a truncated header at the
                    // end of the file, which may be caused by writer
                    // crashing in the middle of writing the header.
                    // Instead of considering this an error, just report EOF.
                    return Err(ReaderError::EOF);
                }
            }
            // parse the header
            let header = &self.buf[0..HEADER_SIZE];
            let record_type = *header.last().unwrap();
            let data_length =
                ((header[4] as usize & 0xff) | ((header[5] as usize & 0xff) << 8)) as usize;
            let record_length = HEADER_SIZE + data_length;
            // a record must be included in one block
            if record_length > self.buf_length {
                let drop_size = self.buf_length;
                self.clear_buf();
                if !self.eof {
                    self.report_drop(drop_size as u64, "bad record length");
                    return Err(BadRecord);
                }
                // If the end of the file has been reached without reading |length| bytes
                // of payload, assume the writer died in the middle of writing the record.
                // Don't report a corruption.
                return Err(EOF);
            }

            // handling empty record generated by mmap
            if record_type == 0 && data_length == 0 {
                self.clear_buf();
                self.report_drop(self.buf.len() as u64, "empty length record");
                return Err(BadRecord);
            }

            // check crc
            if self.checksum {
                let expected = unmask(decode_fixed_32(header));
                // HEADER_SIZE - 1 to included the record type
                let actual = hash(&self.buf[HEADER_SIZE - 1..record_length]);
                if expected != actual {
                    let drop_size = self.buf_length;
                    self.clear_buf();
                    self.report_drop(drop_size as u64, "checksum mismatch");
                    return Err(BadRecord);
                }
            }

            let mut data = self.buf.drain(0..record_length).collect::<Vec<u8>>();
            self.buf_length -= data.len();

            // skip physical record that started before initial_offset
            if self.end_of_buffer_offset
                < self.initial_offset + self.buf_length as u64 + record_length as u64
            {
                return Err(BadRecord);
            }

            // drop the head part
            data.drain(0..HEADER_SIZE);
            return Ok(Record {
                // TODO: avoid panic when we read a invalid record type
                t: RecordType::from(record_type as usize),
                data,
            });
        }
    }

    // report record dropping to the `reporter`
    fn report_drop(&mut self, bytes: u64, reason: &str) {
        if let Some(reporter) = self.reporter.as_mut() {
            // make sure the bytes not overflows 'the initial_offset'
            // and a special case is that we got a read error when we first read a block
            if self.end_of_buffer_offset == 0
                || self.end_of_buffer_offset - bytes >= self.initial_offset
            {
                reporter.corruption(bytes, reason);
            }
        }
    }

    // clear `buf` and reset `buf_length`
    fn clear_buf(&mut self) {
        self.buf = vec![0; BLOCK_SIZE];
        self.buf_length = 0;
    }

    /// Skips all blocks that are completely before `initial_offset`
    /// Returns true on success. Handles reporting.
    fn skip_to_initial_block(&mut self) -> bool {
        let offset_in_block = self.initial_offset % BLOCK_SIZE as u64;
        let mut block_start_location = self.initial_offset - offset_in_block;

        // skip to next block starting if we'd be in the trailer
        if offset_in_block > BLOCK_SIZE as u64 - 6 {
            block_start_location += BLOCK_SIZE as u64;
        }
        self.end_of_buffer_offset = block_start_location;
        if block_start_location > 0 {
            if let Err(e) = self.file.seek(SeekFrom::Start(block_start_location)) {
                self.report_drop(block_start_location, &e.to_string());
                return false;
            }
        }
        true
    }
}
