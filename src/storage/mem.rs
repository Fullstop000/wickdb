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

use crate::storage::File;
use crate::util::status::{Result, Status, WickErr};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, Ordering};

/// `File` implementation based on memory
/// This is handy for our tests.
pub struct InmemFile {
    name: String,
    lock: AtomicBool,
    contents: Cursor<Vec<u8>>,
}

impl InmemFile {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            lock: AtomicBool::new(false),
            contents: Cursor::new(vec![]),
        }
    }

    #[inline]
    pub fn get_name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn get_pos_and_data(&self) -> (u64, &[u8]) {
        (self.contents.position(), self.contents.get_ref().as_slice())
    }
}

impl File for InmemFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let r = self.contents.write(buf);
        w_io_result!(r)
    }

    fn flush(&mut self) -> Result<()> {
        let r = self.contents.flush();
        w_io_result!(r)
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let r = self.contents.seek(pos);
        w_io_result!(r)
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let r = self.contents.read(buf);
        w_io_result!(r)
    }

    fn read_all(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        let r = self.contents.read_to_end(buf);
        w_io_result!(r)
    }

    fn len(&self) -> Result<u64> {
        Ok(self.contents.get_ref().len() as u64)
    }

    fn lock(&self) -> Result<()> {
        // Unlike described in comments, returns Err instead of blocking if locked
        if self.lock.load(Ordering::Acquire) {
            Err(WickErr::new(Status::IOError, Some("Already locked")))
        } else {
            self.lock.store(true, Ordering::Release);
            Ok(())
        }
    }

    fn unlock(&self) -> Result<()> {
        self.lock.store(false, Ordering::Release);
        Ok(())
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if buf.is_empty() {
            Ok(0)
        } else {
            let inner = self.contents.get_ref();
            let length = inner.len() as u64;
            if offset > length - 1 {
                return Ok(0);
            }
            let exact = if buf.len() as u64 + offset > length - 1 {
                return Err(WickErr::new(Status::IOError, Some("EOF")));
            } else {
                buf.len()
            };
            buf.copy_from_slice(&inner.as_slice()[offset as usize..offset as usize + exact]);
            Ok(exact as usize)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::InmemFile;
    use crate::storage::File;
    use crate::util::coding::put_fixed_32;
    use crate::util::status::Status;
    use std::error::Error;

    #[test]
    fn test_mem_file_lock_unlock() {
        let f = InmemFile::new("test");
        assert!(f.lock().is_ok());
        assert!(f.unlock().is_ok());
        f.lock().expect("");
        assert_eq!(f.lock().unwrap_err().status(), Status::IOError);
        f.unlock().expect("");
        assert!(f.unlock().is_ok());
    }

    #[test]
    fn test_mem_file_read_at() {
        let mut f = InmemFile::new("test");
        let mut buf = vec![];
        for i in 0..100 {
            put_fixed_32(&mut buf, i);
        }
        f.write(&buf).expect("");

        for (offset, buf_len, is_ok) in vec![
            (0, 100, true),
            (300, 99, true),
            (300, 100, false),
            (340, 100, false),
        ]
        .drain(..)
        {
            let mut read_buf = vec![0u8; buf_len];
            let res = f.read_at(read_buf.as_mut_slice(), offset);
            assert_eq!(
                res.is_ok(),
                is_ok,
                "offset: {}, buf_len: {}",
                offset,
                buf_len
            );
            match res {
                Ok(size) => assert_eq!(buf_len, size),
                Err(e) => assert_eq!(e.description(), "EOF"),
            }
        }
    }
}
