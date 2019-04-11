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

use std::fs::File;
use std::io::{Seek, SeekFrom, Read};
use crate::record::{BLOCK_SIZE, MAX_RECORD_TYPE, RecordType, HEADER_SIZE};
use core::borrow::BorrowMut;
use std::error::Error;
use crate::record::reader::ReaderError::{BadRecord, EOF};
use crate::util::crc32::{value, unmask};
use crate::util::coding::decode_fixed_32;

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

impl ReaderError {
    pub fn as_usize(&self) -> usize {
        match self {
            ReaderError::EOF => MAX_RECORD_TYPE + 1,
            ReaderError::BadRecord => MAX_RECORD_TYPE + 2,
        }
    }
}

// represent a record
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
pub struct Reader {
    file: File,
    reporter: Option<Box<dyn Reporter>>,
    // iff check sum for the record
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

    // buf[i..j] is the current record payload includes header and data
    i: usize,
    j: usize,

    // Offset at which to start looking for the first record to return
    initial_offset: u64,
}

impl Reader {
    pub fn new(file: File, reporter: Option<Box<dyn Reporter>>, checksum: bool, initial_offset: u64) -> Self {
        Reader {
            file,
            reporter,
            checksum,
            buf: vec![0;BLOCK_SIZE],
            buf_length: 0,
            i: 0,
            j: 0,
            eof: false,
            last_record_offset: 0,
            end_of_buffer_offset: 0,
            initial_offset,
        }
    }

    /// Read the next complete record and returns a Vec for it.
    pub fn read_record(&mut self) -> Option<Vec<u8>> {
        // move to the right block offset
        if self.last_record_offset < self.initial_offset {
            if !self.skip_to_initial_block() {
                return None;
            }
        }
        // indicates that a record has been spilt into fragments
        let mut in_fragmented_record = false;
        // Record offset of the logical record that we're reading
        let mut prospective_record_offset = 0;
        let mut result: Vec<u8> = vec![];
        loop {
            match self.read_physical_record() {
                Ok(record) => {
                    let fragment_size = record.data.len() as u64;
                    // The offset for the last record from `read_physical_record`
                    let physical_record_offset = self.end_of_buffer_offset - self.buf_length as u64 + self.i as u64;
                    match record.t {
                        RecordType::Full => {
                            if in_fragmented_record {
                                self.report_corruption(result.len() as u64, "partial record without end(1) for reading a new Full record");
                            }
                            // update record offset
                            prospective_record_offset = physical_record_offset;
                            self.last_record_offset = prospective_record_offset;
                            return Some(record.data);

                        },
                        RecordType::First => {
                            if in_fragmented_record {
                                self.report_corruption(result.len() as u64, "partial record without end(2) for reading a new First record");
                            }
                            prospective_record_offset = physical_record_offset;

                            // clean the potential corruption
                            result.clear();
                            result.extend(record.data);
                            in_fragmented_record = true;

                        },
                        RecordType::Middle => {
                            if !in_fragmented_record {
                                self.report_corruption(fragment_size, "missing start of fragmented record(1)");
                            } else {
                                result.extend( record.data);
                            }

                        },
                        RecordType::Last => {
                            if !in_fragmented_record {
                                self.report_corruption(fragment_size, "missing start of fragmented record(2)");
                            } else {
                                result.extend(record.data);
                                // notice that we update the last_record_offset after we get the Last part but not the First
                                self.last_record_offset = prospective_record_offset;
                                return Some(result);
                            }
                        },

                    }
                },
                Err(e) => {
                    match e {
                        ReaderError::EOF => {
                            if in_fragmented_record {
                                // This can be caused by the writer dying immediately after writing a
                                // physical record but before completing the next
                                // one; don't treat it as a corruption,
                                // just ignore the entire logical record.
                                result.clear();
                            }
                            return None;
                        },
                        ReaderError::BadRecord => {
                            if in_fragmented_record {
                                self.report_corruption(
                                    result.len() as u64, "bad record read in middle of record"
                                );
                                in_fragmented_record = false;
                                result.clear();
                            }
                        },
                    }
                }
            }
        }
    }

    fn read_physical_record(&mut self) -> Result<Record, ReaderError> {
        loop {
            // we've reached the end of a block and do not have a valid header
            if self.buf_length - self.j < HEADER_SIZE {
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
                        },
                        Err(e) => {
                            self.report_drop(BLOCK_SIZE as u64, e.description());
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
            let header = &self.buf[self.j..self.j+ 6];
            let record_type = header[6];
            let record_length = ((header[4] & 0xff) | ((header[5] & 0xff) << 8)) as usize;

            // update the pointer
            self.i = self.j;
            self.j = self.j + HEADER_SIZE + record_length;

            // for now a record must be included in one block
            // TODO: maybe support separate big record in multiple blocks?
            if HEADER_SIZE + record_length > self.buf.len() {
                let drop_size = self.buf_length;
                self.buf.clear();
                if !self.eof {
                    self.report_corruption(drop_size as u64, "bad record length");
                    return Err(BadRecord);
                }
                // If the end of the file has been reached without reading |length| bytes
                // of payload, assume the writer died in the middle of writing the record.
                // Don't report a corruption.
                return Err(EOF);
            }

            // handling empty record
            if record_length == 0 {
                self.clear_buf();
                self.report_drop(self.buf.len() as u64, "empty length record");
                return Err(BadRecord);
            }

            // check crc
            if self.checksum {
                let expected = unmask(decode_fixed_32(header));
                let actual = value(&self.buf[self.i+HEADER_SIZE..self.j]);
                if expected != actual {
                    let drop_size = self.buf_length;
                    self.clear_buf();
                    self.report_corruption(drop_size as u64, "checksum mismatch");
                }
            }

            return Ok(Record {
                t: RecordType::from(record_type as usize),
                data: Vec::from(&self.buf[self.i+HEADER_SIZE..self.j]),
            });
        }
    }

    fn report_corruption(&mut self, bytes: u64, reason: &str) {
        self.report_drop(bytes, reason);
    }

    // report record dropping to the `reporter`
    fn report_drop(&mut self, bytes: u64, reason: &str) {
        if let Some(ref mut reporter) = self.reporter.borrow_mut() {
            // make sure the bytes not overflows 'the initial_offset'
            if self.end_of_buffer_offset -  bytes > self.initial_offset {
                reporter.corruption(bytes, reason);
            }
        }
    }

    // clear `buf` and reset `buf_length`
    fn clear_buf(&mut self) {
        self.buf.clear();
        self.buf_length = 0;
        self.i = 0;
        self.j = 0;
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
            let res = self.file.seek(SeekFrom::Start(block_start_location));
            if !res.is_err() {
                self.report_drop(block_start_location, res.unwrap_err().description());
                return false;
            }
        }
        true
    }
}