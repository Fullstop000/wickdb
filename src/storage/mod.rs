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

pub mod file;
pub mod mem;

use crate::{Error, Result};
use std::io;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

/// `Storage` is a namespace for files.
///
/// The names are filepath names: they may be / separated or \ separated,
/// depending on the underlying operating system.
///
/// `Storage` should be thread safe
pub trait Storage: Send + Sync {
    type F: File + 'static;
    /// Create a file if it does not exist and truncates exist one.
    fn create<P: AsRef<Path>>(&self, name: P) -> Result<Self::F>;

    /// Open a file for writing and reading
    fn open<P: AsRef<Path>>(&self, name: P) -> Result<Self::F>;

    /// Delete the named file
    fn remove<P: AsRef<Path>>(&self, name: P) -> Result<()>;

    /// Removes a directory at this path. If `recursively`, removes all its contents.
    fn remove_dir<P: AsRef<Path>>(&self, dir: P, recursively: bool) -> Result<()>;

    /// Returns true iff the named file exists.
    fn exists<P: AsRef<Path>>(&self, name: P) -> bool;

    /// Rename a file or directory to a new name, replacing the original file if
    /// `new` already exists.
    fn rename<P: AsRef<Path>>(&self, old: P, new: P) -> Result<()>;

    /// Recursively create a directory and all of its parent components if they
    /// are missing.
    fn mkdir_all<P: AsRef<Path>>(&self, dir: P) -> Result<()>;

    /// Returns a list of the full-path to each file in given directory
    fn list<P: AsRef<Path>>(&self, dir: P) -> Result<Vec<PathBuf>>;
}

/// A file abstraction for IO operations
pub trait File: Send + Sync {
    fn write(&mut self, buf: &[u8]) -> Result<usize>;
    fn flush(&mut self) -> Result<()>;
    fn close(&mut self) -> Result<()>;
    fn seek(&mut self, pos: SeekFrom) -> Result<u64>;
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
    fn read_all(&mut self, buf: &mut Vec<u8>) -> Result<usize>;
    fn len(&self) -> Result<u64>;
    fn is_empty(&self) -> bool {
        if let Ok(length) = self.len() {
            return length == 0;
        }
        // Err is considered as empty
        false
    }
    /// Locks the file for exclusive usage, blocking if the file is currently
    /// locked.
    fn lock(&self) -> Result<()>;
    fn unlock(&self) -> Result<()>;

    /// Reads bytes from an offset in this source into a buffer, returning how
    /// many bytes were read.
    ///
    /// This function may yield fewer bytes than the size of `buf`, if it was
    /// interrupted or hit the "EOF".
    ///
    /// See [`Read::read()`](https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read)
    /// for details.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Reads the exact number of bytes required to fill `buf` from an `offset`.
    ///
    /// Errors if the "EOF" is encountered before filling the buffer.
    ///
    /// See [`Read::read_exact()`](https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact)
    /// for details.
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
        while !buf.is_empty() {
            match self.read_at(buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(e) => match e {
                    Error::IO(err) => {
                        if err.kind() != io::ErrorKind::Interrupted {
                            return Err(Error::IO(err));
                        }
                    }
                    _ => return Err(e),
                },
            }
        }
        if !buf.is_empty() {
            let e = io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer");
            Err(Error::IO(e))
        } else {
            Ok(())
        }
    }
}

/// Write given `data` into underlying `env` file and flush file iff `should_sync` is true
pub fn do_write_string_to_file<S: Storage, P: AsRef<Path>>(
    env: &S,
    data: String,
    file_name: P,
    should_sync: bool,
) -> Result<()> {
    let mut file = env.create(&file_name)?;
    file.write(data.as_bytes())?;
    if should_sync {
        file.flush()?;
    }
    if file.close().is_err() {
        env.remove(&file_name)?;
    }
    Ok(())
}
