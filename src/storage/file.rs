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
use crate::{Error, Result};
use fs2::FileExt;
use std::fs::{
    create_dir_all, read_dir, remove_dir, remove_dir_all, remove_file, rename, File as SysFile,
    OpenOptions,
};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Default)]
pub struct FileStorage;

impl Storage for FileStorage {
    type F = SysFile;
    fn create<P: AsRef<Path>>(&self, name: P) -> Result<Self::F> {
        match OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(name)
        {
            Ok(f) => Ok(f),
            Err(e) => Err(Error::IO(e)),
        }
    }

    fn open<P: AsRef<Path>>(&self, name: P) -> Result<Self::F> {
        match OpenOptions::new().write(true).read(true).open(name) {
            Ok(f) => Ok(f),
            Err(e) => Err(Error::IO(e)),
        }
    }

    fn remove<P: AsRef<Path>>(&self, name: P) -> Result<()> {
        let r = remove_file(name);
        map_io_res!(r)
    }

    fn remove_dir<P: AsRef<Path>>(&self, dir: P, recursively: bool) -> Result<()> {
        let r = if recursively {
            remove_dir_all(dir)
        } else {
            remove_dir(dir)
        };
        map_io_res!(r)
    }

    fn exists<P: AsRef<Path>>(&self, name: P) -> bool {
        name.as_ref().exists()
    }

    fn rename<P: AsRef<Path>>(&self, old: P, new: P) -> Result<()> {
        map_io_res!(rename(old, new))
    }

    fn mkdir_all<P: AsRef<Path>>(&self, dir: P) -> Result<()> {
        let r = create_dir_all(dir);
        map_io_res!(r)
    }

    fn list<P: AsRef<Path>>(&self, dir: P) -> Result<Vec<PathBuf>> {
        if dir.as_ref().is_dir() {
            let mut v = vec![];
            match read_dir(dir) {
                Ok(rd) => {
                    for entry in rd {
                        match entry {
                            Ok(p) => v.push(p.path()),
                            Err(e) => return Err(Error::IO(e)),
                        }
                    }
                    return Ok(v);
                }
                Err(e) => return Err(Error::IO(e)),
            }
        }
        Ok(vec![])
    }
}

impl File for SysFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        map_io_res!(Write::write(self, buf))
    }

    fn flush(&mut self) -> Result<()> {
        map_io_res!(Write::flush(self))
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        map_io_res!(Seek::seek(self, pos))
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut reader = BufReader::new(self);
        let r = reader.read(buf);
        map_io_res!(r)
    }

    fn read_all(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        let mut reader = BufReader::new(self);
        let r = reader.read_to_end(buf);
        map_io_res!(r)
    }

    fn len(&self) -> Result<u64> {
        match SysFile::metadata(self) {
            Ok(v) => Ok(v.len()),
            Err(e) => Err(Error::IO(e)),
        }
    }

    fn lock(&self) -> Result<()> {
        map_io_res!(SysFile::try_lock_exclusive(self))
    }

    fn unlock(&self) -> Result<()> {
        map_io_res!(FileExt::unlock(self))
    }

    #[cfg(unix)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let r = std::os::unix::prelude::FileExt::read_at(self, buf, offset);
        map_io_res!(r)
    }
    #[cfg(windows)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let r = std::os::windows::prelude::FileExt::seek_read(self, buf, offset);
        map_io_res!(r)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::remove_file;
    use std::io::Write;

    #[test]
    fn test_read_exact_at() {
        let mut f = SysFile::create("test").unwrap();
        f.write_all("hello world".as_bytes()).unwrap();
        f.sync_all().unwrap();
        let tests = vec![
            (0, "hello world"),
            (0, ""),
            (1, "ello"),
            (4, "o world"),
            (100, ""),
        ];
        let rf = SysFile::open("test").unwrap();
        let mut buffer = vec![];
        for (offset, expect) in tests {
            buffer.resize(expect.as_bytes().len(), 0u8);
            rf.read_exact_at(buffer.as_mut_slice(), offset).unwrap();
            assert_eq!(buffer, Vec::from(String::from(expect)));
        }
        // EOF case
        buffer.resize(100, 0u8);
        rf.read_exact_at(buffer.as_mut_slice(), 2)
            .expect_err("failed to fill whole buffer");
        remove_file("test").unwrap();
    }
}
