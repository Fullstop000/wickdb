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

use crate::storage::{Storage, File};
use std::io::{Result, SeekFrom, Write, Seek, Read};
use std::fs::{File as SysFile, remove_file, rename, create_dir_all, read_dir};
use fs2::FileExt;
use std::path::{Path, PathBuf};

pub struct FileStorage {
}

impl Storage for FileStorage {

    fn create(name: &str) -> Result<Box<dyn File>> {
        let f = SysFile::create(name)?;
        Ok(Box::new(f))

    }

    fn open(name: &str) -> Result<Box<dyn File>> {
        let f = SysFile::open(name)?;
        Ok(box f)
    }

    fn remove(name: &str) -> Result<()> {
        remove_file(name)
    }

    fn exists(name: &str) -> bool {
        Path::new(name).exists()
    }

    fn rename(old: &str, new: &str) -> Result<()> {
        rename(old, new)
    }

    fn mkdir_all(dir: &str) -> Result<()> {
        create_dir_all(dir)
    }

    fn list(dir: &Path) -> Result<Vec<PathBuf>> {
        if dir.is_dir() {
            let mut v = vec![];
            for d in read_dir(dir)? {
                let p = d?;
                v.push(p.path())
            }
            return Ok(v);
        }
        Ok(vec![])
    }
}

impl File for SysFile {
    fn f_write(&mut self, buf: &[u8]) -> Result<usize> {
        SysFile::write(self, buf)
    }

    fn f_flush(&mut self) -> Result<()> {
        SysFile::flush(self)
    }

    fn f_close(&mut self) -> Result<()> {
        Ok(())
    }

    fn f_seek(&mut self, pos: SeekFrom) -> Result<u64> {
        SysFile::seek(self, pos)
    }

    fn f_read(&mut self, buf: &mut [u8]) -> Result<usize> {
        SysFile::read(self, buf)
    }

    fn f_lock(&self) -> Result<()> {
        SysFile::try_lock_exclusive(self)
    }

    fn f_unlock(&self) -> Result<()> {
        SysFile::unlock(self)
    }

    #[cfg(unix)]
    fn f_read_at(&self,  buf: &mut [u8], offset: u64) -> Result<usize>{
        std::os::unix::prelude::FileExt::read_at(self, buf, offset)
    }
    #[cfg(windows)]
    fn f_read_at(&self,  buf: &mut [u8], offset: u64) -> Result<usize>{
        std::os::windows::prelude::FileExt::seek_read(buf, offset)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::fs::remove_file;

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
        rf.read_exact_at(buffer.as_mut_slice(), 2).expect_err("failed to fill whole buffer");
        remove_file("test").expect("");
    }
}

