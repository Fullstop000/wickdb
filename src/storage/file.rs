// Copyright 2019 Fullstop000 <fullstop1005@gmail.com>.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this SysFile except in compliance with the License.
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
// found in the LICENSE SysFile. See the AUTHORS SysFile for names of contributors.

use crate::storage::{File, Storage};
use crate::util::status::{Result, Status, WickErr};
use fs2::FileExt;
use std::fs::{create_dir_all, read_dir, remove_file, rename, File as SysFile};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct FileStorage;

impl Storage for FileStorage {
    fn create(&self, name: &str) -> Result<Box<dyn File>> {
        match SysFile::create(name) {
            Ok(f) => Ok(Box::new(f)),
            Err(e) => Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e))),
        }
    }

    fn open(&self, name: &str) -> Result<Box<dyn File>> {
        match SysFile::open(name) {
            Ok(f) => Ok(Box::new(f)),
            Err(e) => Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e))),
        }
    }

    fn remove(&self, name: &str) -> Result<()> {
        let r = remove_file(name);
        w_io_result!(r)
    }

    fn exists(&self, name: &str) -> bool {
        Path::new(name).exists()
    }

    fn rename(&self, old: &str, new: &str) -> Result<()> {
        w_io_result!(rename(old, new))
    }

    fn mkdir_all(&self, dir: &str) -> Result<()> {
        let r = create_dir_all(dir);
        w_io_result!(r)
    }

    fn list(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        if dir.is_dir() {
            let mut v = vec![];
            match read_dir(dir) {
                Ok(rd) => {
                    for entry in rd {
                        match entry {
                            Ok(p) => v.push(p.path()),
                            Err(e) => {
                                return Err(WickErr::new_from_raw(
                                    Status::IOError,
                                    None,
                                    Box::new(e),
                                ))
                            }
                        }
                    }
                    return Ok(v);
                }
                Err(e) => return Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e))),
            }
        }
        Ok(vec![])
    }
}

impl File for SysFile {
    fn f_write(&mut self, buf: &[u8]) -> Result<usize> {
        w_io_result!(SysFile::write(self, buf))
    }

    fn f_flush(&mut self) -> Result<()> {
        w_io_result!(SysFile::flush(self))
    }

    fn f_close(&mut self) -> Result<()> {
        Ok(())
    }

    fn f_seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let r = SysFile::seek(self, pos);
        w_io_result!(r)
    }

    fn f_read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let r = SysFile::read(self, buf);
        w_io_result!(r)
    }

    fn f_lock(&self) -> Result<()> {
        let r = SysFile::try_lock_exclusive(self);
        w_io_result!(r)
    }

    fn f_unlock(&self) -> Result<()> {
        let r = SysFile::unlock(self);
        w_io_result!(r)
    }

    #[cfg(unix)]
    fn f_read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let r = std::os::unix::prelude::FileExt::read_at(self, buf, offset);
        w_io_result!(r)
    }
    #[cfg(windows)]
    fn f_read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let r = std::os::windows::prelude::FileExt::seek_read(buf, offset);
        w_io_result!(r)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::remove_file;
    use std::io::Write;

    #[test]
    fn test_read_exact_at() {
        let mut f = SysFile::create("test").expect("");
        f.write_all("hello world".as_bytes()).expect("");
        f.sync_all().expect("");
        let mut tests = vec![
            (0, "hello world"),
            (0, ""),
            (1, "ello"),
            (4, "o world"),
            (100, ""),
        ];
        let rf = SysFile::open("test").expect("");
        let mut buffer = vec![];
        for (offset, expect) in tests.drain(..) {
            buffer.resize(expect.as_bytes().len(), 0u8);
            rf.read_exact_at(buffer.as_mut_slice(), offset).expect("");
            assert_eq!(buffer, Vec::from(String::from(expect)));
        }
        // EOF case
        buffer.resize(100, 0u8);
        rf.read_exact_at(buffer.as_mut_slice(), 2)
            .expect_err("failed to fill whole buffer");
        remove_file("test").expect("");
    }
}
