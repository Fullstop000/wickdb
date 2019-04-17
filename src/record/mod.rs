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

/// The log file contents are a sequence of 32KB blocks. The only exception is that the tail of the file may contain a partial block.

mod reader;
mod writer;

/// The max size of a log block
pub const BLOCK_SIZE: usize = 32768;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum RecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl From<usize> for RecordType {
    fn from(v: usize) -> Self {
        match v {
            1 => RecordType::Full,
            2 => RecordType::First,
            3 => RecordType::Middle,
            4 => RecordType::Last,
            _ => panic!("invalid value [{}] for RecordType", v)
        }
    }
}

/// The format of a record header :
///
/// ```shell
///
/// | ----- 4bytes ----- | -- 2bytes -- | - 1byte - |
///      CRC checksum         length     record type
///
/// ```
pub const HEADER_SIZE: usize = 7;

pub const MAX_RECORD_TYPE: usize = RecordType::Last as usize;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, ErrorKind};
    use std::io::{SeekFrom};
    use crate::storage::File;
    use crate::util::status::Result;

    // Construct a string of the specified length made out of the supplied
    // partial string.
    fn big_string(partial_str: &str, n: usize) -> String {
        let mut s = String::new();
        while s.len() < n {
            s.push_str(partial_str);
        }
        s.truncate(n);
        s
    }

    // Construct a String from a number
    fn num_to_string(n: usize) -> String {
        n.to_string()
    }

    // Return a skewed potentially long string
    fn random_skewed_string(i: usize) -> String {
        let r = rand::thread_rng().gen_range(0, 1<<17);
        big_string(&num_to_string(i),  r)
    }

    struct StringFile {
        contents: Vec<u8>,
        force_err: bool,
        returned_partial: bool,
    }

    impl StringFile {
        pub fn new() -> Self {
            Self {
                contents: vec![],
                force_err: false,
                returned_partial: false,
            }
        }
    }

    impl File for StringFile {
        fn f_write(&mut self, buf: &[u8]) -> Result<usize> {
            self.contents.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn f_flush(&mut self) -> Result<()> {
            Ok(())
        }

        fn f_close(&mut self) -> Result<()> {
            unimplemented!()
        }

        fn f_seek(&mut self, pos: SeekFrom) -> Result<u64> {
            unimplemented!()
        }

        fn f_read(&mut self, buf: &mut [u8]) -> Result<usize> {
            assert!(!self.returned_partial, "must not read() after eof/error");
//            if self.force_err {
//                self.force_err = false;
//                self.returned_partial = true;
//                return Err()
//            }
            Ok(0)
        }

        fn f_lock(&self) -> Result<()> {
            unimplemented!()
        }

        fn f_unlock(&self) -> Result<()> {
            unimplemented!()
        }

        fn f_read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
            unimplemented!()
        }
    }
    struct RecordTest {}
}