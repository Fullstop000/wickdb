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
    // TODO
}