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

mod file;

use std::fs::{File, Metadata};
use std::io::Result;
use std::io;

/// Storage is a namespace for files.
///
/// The names are filepath names: they may be / separated or \ separated,
/// depending on the underlying operating system.
pub trait Storage {
    // TODO: abstract File as a trait

    fn create(name: &str) -> Result<File>;

    fn open(name: &str) -> Result<File>;

    fn remove(name: &str) -> Result<()>;

    fn rename(old: &str, new: &str) -> Result<()>;

    fn mkdir_all(dir: &str) -> Result<()>;

    fn lock(name: &str) -> Result<()>;

    fn list(dir: &str) -> Result<Vec<&str>>;

    fn stat(name: &str) -> Result<Metadata>;
}

pub trait ReadAt {
    /// Reads bytes from an offset in this source into a buffer, returning how
    /// many bytes were read.
    ///
    /// This function may yield fewer bytes than the size of `buf`, if it was
    /// interrupted or hit the "EOF".
    ///
    /// See [`Read::read()`](https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read)
    /// for details.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;

    /// Reads the exact number of bytes required to fill `buf` from an `offset`.
    ///
    /// Errors if the "EOF" is encountered before filling the buffer.
    ///
    /// See [`Read::read_exact()`](https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact)
    /// for details.
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match self.read_at(buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer"))
        } else {
            Ok(())
        }
    }
}

impl ReadAt for File {
    #[cfg(unix)]
    fn read_at(&self,  buf: &mut [u8], offset: u64) -> io::Result<usize>{
        std::os::unix::prelude::FileExt::read_at(self, buf, offset)
    }
    #[cfg(windows)]
    fn read_at(&self,  buf: &mut [u8], offset: u64) -> io::Result<usize>{
        std::os::windows::prelude::FileExt::seek_read(buf, offset)
    }
}

mod tests {
    use super::*;
    use std::io::Write;
    use std::fs::{remove_file};

    #[test]
    fn test_read_exact_at() {
        let mut f = File::create("test").expect("");
        f.write_all("hello world".as_bytes()).expect("");
        f.sync_all().expect("");
        let mut tests = vec![
            (0, "hello world"),
            (0, ""),
            (1, "ello"),
            (4, "o world"),
            (100, ""),
        ];
        let rf = File::open("test").expect("");
        let mut buffer = vec![];
        for (offset, expect) in tests.drain(..) {
            buffer.clear();
            buffer.resize(expect.as_bytes().len(), 0u8);
            rf.read_exact_at(buffer.as_mut_slice(), offset).expect("");
            assert_eq!(buffer, Vec::from(String::from(expect)));
        }
        remove_file("test").expect("");
    }
}