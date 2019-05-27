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

use crate::util::status::{Result, Status, WickErr};
use std::cell::RefCell;
use std::io;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

/// `Storage` is a namespace for files.
///
/// The names are filepath names: they may be / separated or \ separated,
/// depending on the underlying operating system.
///
/// `Storage` should be thread safe
pub trait Storage: Send + Sync {
    /// Create a file with given name
    fn create(&self, name: &str) -> Result<Box<dyn File>>;

    /// Open a file with given name
    fn open(&self, name: &str) -> Result<Box<dyn File>>;

    /// Delete the named file
    fn remove(&self, name: &str) -> Result<()>;

    /// Returns true iff the named file exists.
    fn exists(&self, name: &str) -> bool;

    /// Rename a file or directory to a new name, replacing the original file if
    /// `new` already exists.
    fn rename(&self, old: &str, new: &str) -> Result<()>;

    /// Recursively create a directory and all of its parent components if they
    /// are missing.
    fn mkdir_all(&self, dir: &str) -> Result<()>;

    /// Returns a list of file names in given
    fn list(&self, dir: &str) -> Result<Vec<PathBuf>>;
}

/// A file abstraction for IO operations
pub trait File {
    fn f_write(&mut self, buf: &[u8]) -> Result<usize>;
    fn f_flush(&mut self) -> Result<()>;
    fn f_close(&mut self) -> Result<()>;
    fn f_seek(&mut self, pos: SeekFrom) -> Result<u64>;
    fn f_read(&mut self, buf: &mut [u8]) -> Result<usize>;

    /// Locks the file for exclusive usage, blocking if the file is currently
    /// locked.
    fn f_lock(&self) -> Result<()>;

    fn f_unlock(&self) -> Result<()>;

    /// Reads bytes from an offset in this source into a buffer, returning how
    /// many bytes were read.
    ///
    /// This function may yield fewer bytes than the size of `buf`, if it was
    /// interrupted or hit the "EOF".
    ///
    /// See [`Read::read()`](https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read)
    /// for details.
    fn f_read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Reads the exact number of bytes required to fill `buf` from an `offset`.
    ///
    /// Errors if the "EOF" is encountered before filling the buffer.
    ///
    /// See [`Read::read_exact()`](https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact)
    /// for details.
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
        while !buf.is_empty() {
            match self.f_read_at(buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(mut e) => {
                    match e.status() {
                        Status::IOError => {
                            if let Some(r) = e.take_raw() {
                                let raw = Rc::try_unwrap(r).unwrap();
                                // DANGER: the raw error must be a io::Error otherwise we got UB
                                #[allow(clippy::cast_ptr_alignment)]
                                let raw_ptr = Box::into_raw(raw) as *mut io::Error;
                                match (unsafe { &*raw_ptr }).kind() {
                                    io::ErrorKind::Interrupted => {}
                                    _ => return Err(e),
                                }
                            }
                        }
                        _ => return Err(e),
                    }
                }
            }
        }
        if !buf.is_empty() {
            let e = io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer");
            Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e)))
        } else {
            Ok(())
        }
    }
}

pub type FilePtr = Rc<RefCell<dyn File>>;

/// Write given `data` into underlying `env` file and flush file iff `should_sync` is true
pub fn do_write_string_to_file(env: Arc<dyn Storage>, data: String, file_name: &str, should_sync: bool ) -> Result<()> {
    let mut file = env.create(file_name)?;
    file.f_write(data.as_bytes())?;
    if should_sync {
        file.f_flush()?;
    }
    if file.f_close().is_err() {
        env.remove(file_name)?;
    }
    Ok(())
}
