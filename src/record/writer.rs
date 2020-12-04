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

use crate::record::{RecordType, BLOCK_SIZE, HEADER_SIZE};
use crate::storage::File;
use crate::util::coding::encode_fixed_32;
use crate::util::crc32;
use crate::Result;

/// Writer writes records to an underlying log `File`.
pub struct Writer<F: File> {
    dest: F,
    //Current offset in block
    block_offset: usize,
    // crc32c values for all supported record types.  These are
    // pre-computed to reduce the overhead of computing the crc of the
    // record type stored in the header.
    crc_cache: [u32; (RecordType::Last as usize + 1) as usize],
}

impl<F: File> Writer<F> {
    pub fn new(dest: F) -> Self {
        let n = RecordType::Last as usize;
        let mut cache = [0; RecordType::Last as usize + 1];
        for h in 1..=n {
            let v: [u8; 1] = [RecordType::from(h) as u8];
            cache[h as usize] = crc32::hash(&v);
        }
        Self {
            dest,
            block_offset: 0,
            crc_cache: cache,
        }
    }

    /// Appends a slice into the underlying log file
    pub fn add_record(&mut self, s: &[u8]) -> Result<()> {
        let mut left = s.len();
        let mut begin = true; // indicate the record is a First or Middle record
        while {
            assert!(
                BLOCK_SIZE >= self.block_offset,
                "[record writer] the 'block_offset' {} overflows the max BLOCK_SIZE {}",
                self.block_offset,
                BLOCK_SIZE,
            );
            let leftover = BLOCK_SIZE - self.block_offset;

            // switch to a new block if the left size is not enough
            // for a record header
            if leftover < HEADER_SIZE {
                if leftover != 0 {
                    // fill the rest of the block with zero
                    self.dest.write(&[0; 6][..leftover])?;
                }
                self.block_offset = 0; // use a new block
            };
            assert!(
                BLOCK_SIZE >= self.block_offset + HEADER_SIZE,
                "[record writer] the left space of block {} is less than header size {}",
                BLOCK_SIZE - self.block_offset,
                HEADER_SIZE,
            );
            let space = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let to_write = if left < space { left } else { space };
            let end = to_write == left; // indicates whether the data exhausts a record
            let t = {
                if begin && end {
                    RecordType::Full
                } else if begin {
                    RecordType::First
                } else if end {
                    RecordType::Last
                } else {
                    RecordType::Middle
                }
            };

            let start = s.len() - left;
            self.write(t, &s[start..start + to_write])?;
            left -= to_write;
            begin = false;
            left > 0
        } { /* empty here */ }
        Ok(())
    }

    /// Sync the underlying file
    #[inline]
    pub fn sync(&mut self) -> Result<()> {
        self.dest.flush()
    }

    // create formatted bytes and write into the file
    fn write(&mut self, rt: RecordType, data: &[u8]) -> Result<()> {
        let size = data.len();
        assert!(
            size <= 0xffff,
            "[record writer] the data length in a record must fit 2 bytes but got {}",
            size
        );
        assert!(
            self.block_offset + HEADER_SIZE + size <= BLOCK_SIZE,
            "[record writer] new record [{:?}] overflows the BLOCK_SIZE [{}]",
            rt,
            BLOCK_SIZE,
        );
        // encode header
        let mut buf: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
        buf[4] = (size & 0xff) as u8; // data length
        buf[5] = (size >> 8) as u8;
        buf[6] = rt as u8; // record type

        // encode crc
        let mut crc = crc32::extend(self.crc_cache[rt as usize], data);
        crc = crc32::mask(crc);
        encode_fixed_32(&mut buf, crc);

        // write the header and the data
        self.dest.write(&buf)?;
        self.dest.write(data)?;
        // self.dest.flush()?;
        // update block_offset
        self.block_offset += HEADER_SIZE + size;
        Ok(())
    }
}
