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

use crate::storage::{File, Storage};
use crate::util::collection::HashMap;
use crate::util::status::{Result, Status, WickErr};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

/// An in memory file system based on a simple HashMap
// TODO: maybe use a trie tree instead
#[derive(Default, Clone)]
pub struct MemStorage {
    inner: Arc<RwLock<HashMap<String, FileNode>>>,
}

impl Storage for MemStorage {
    fn create(&self, name: &str) -> Result<Box<dyn File>> {
        let file_node = FileNode::new(name);
        self.inner
            .write()
            .unwrap()
            .insert(String::from(name), file_node.clone());
        Ok(Box::new(file_node))
    }

    fn open(&self, name: &str) -> Result<Box<dyn File>> {
        match self.inner.read().unwrap().get(name) {
            Some(f) => Ok(Box::new(f.clone())),
            None => Err(WickErr::new(Status::IOError, Some("Not Found"))),
        }
    }

    // If not found, still returns Ok
    fn remove(&self, name: &str) -> Result<()> {
        self.inner.write().unwrap().remove(name);
        Ok(())
    }

    // Should not be used
    fn remove_dir(&self, _dir: &str, _recursively: bool) -> Result<()> {
        Ok(())
    }

    fn exists(&self, name: &str) -> bool {
        self.inner.read().unwrap().contains_key(name)
    }

    fn rename(&self, old: &str, new: &str) -> Result<()> {
        let mut map = self.inner.write().unwrap();
        match map.remove(old) {
            Some(f) => {
                map.insert(new.to_owned(), f);
                Ok(())
            }
            None => Err(WickErr::new(Status::IOError, Some("Not Found"))),
        }
    }

    // Should not be used
    fn mkdir_all(&self, _dir: &str) -> Result<()> {
        Ok(())
    }

    // Just list all keys in HashMap
    fn list(&self, _dir: &str) -> Result<Vec<PathBuf>> {
        let mut result = vec![];
        for (key, _) in self.inner.read().unwrap().iter() {
            result.push(PathBuf::from(key.clone()))
        }
        Ok(result)
    }
}

#[derive(Clone)]
pub struct FileNode {
    inner: Arc<RwLock<InmemFile>>,
}

impl FileNode {
    fn new(name: &str) -> Self {
        FileNode {
            inner: Arc::new(RwLock::new(InmemFile::new(name))),
        }
    }
}

impl File for FileNode {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        // TODO: as we acquire a mutable ref, the lock shouldn't be needed
        self.inner.write().unwrap().write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.write().unwrap().flush()
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.inner.write().unwrap().seek(pos)
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.write().unwrap().read(buf)
    }

    fn read_all(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        self.inner.write().unwrap().read_all(buf)
    }

    fn len(&self) -> Result<u64> {
        self.inner.read().unwrap().len()
    }

    fn lock(&self) -> Result<()> {
        self.inner.read().unwrap().lock()
    }

    fn unlock(&self) -> Result<()> {
        self.inner.read().unwrap().unlock()
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.inner.read().unwrap().read_at(buf, offset)
    }
}

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
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn pos_and_data(&self) -> (u64, &[u8]) {
        (self.contents.position(), self.contents.get_ref().as_slice())
    }
}

impl File for InmemFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let pos = self.contents.position();
        // Set position to last to prevent overwritting
        self.contents
            .set_position(self.contents.get_ref().len() as u64);
        let r = self.contents.write(buf);
        // Prevent position from being modified
        self.contents.set_position(pos);
        w_io_result!(r)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
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
        self.contents.set_position(0);
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
            let exact = if buf.len() as u64 + offset > length {
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
    use super::{InmemFile, MemStorage};
    use crate::storage::{File, Storage};
    use crate::util::coding::put_fixed_32;
    use crate::util::collection::HashSet;
    use crate::util::status::Status;
    use std::error::Error;

    #[test]
    fn test_mem_file_read_write() {
        let mut f = InmemFile::new("test");
        let written1 = f.write(b"hello world").expect("write should work");
        assert_eq!(written1, 11);
        let written2 = f.write(b"|hello world").expect("write should work");
        assert_eq!(written2, 12);
        let (pos, data) = f.pos_and_data();
        assert_eq!(pos, 0);
        assert_eq!(
            String::from_utf8(Vec::from(data)).unwrap(),
            "hello world|hello world"
        );
        let mut read_buf = vec![0u8; 5];
        let read = f.read(read_buf.as_mut_slice()).expect("read should work");
        assert_eq!(read, 5);
        let (pos, _) = f.pos_and_data();
        assert_eq!(pos, 5);
        read_buf.clear();
        let all = f.read_all(&mut read_buf).expect("read_all should work");
        assert_eq!(all, written1 + written2);
        assert_eq!(
            String::from_utf8(read_buf.clone()).unwrap(),
            "hello world|hello world"
        );
    }

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
            (0, 0, true),
            (0, 400, true),
            (0, 100, true),
            (300, 100, true),
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
                Ok(size) => {
                    assert_eq!(buf_len, size);
                    assert_eq!(
                        read_buf.as_slice(),
                        &buf.as_slice()[offset as usize..offset as usize + buf_len]
                    )
                }
                Err(e) => assert_eq!(e.description(), "EOF"),
            }
        }
    }

    #[test]
    fn test_memory_storage_basic() {
        let env = MemStorage::default();
        let mut f = env.create("test1").expect("'create' should work");
        assert!(env.exists("test1"));
        f.write(b"hello world").expect("file write should work");

        let expected_not_found = env.open("not exist");
        assert!(expected_not_found.is_err());
        assert_eq!(expected_not_found.err().unwrap().description(), "Not Found");

        f = env.open("test1").expect("'open' should work");
        let mut read_buf = vec![];
        f.read_all(&mut read_buf)
            .expect("file read_all should work");
        assert_eq!(String::from_utf8(read_buf).unwrap(), "hello world");

        let expected_not_found = env.rename("not exist", "test3");
        assert!(expected_not_found.is_err());
        assert_eq!(expected_not_found.unwrap_err().description(), "Not Found");

        env.rename("test1", "test2").expect("'rename' should work");
        assert!(!env.exists("test1"));
        assert!(env.exists("test2"));
        f = env.open("test2").expect("'open' should work");
        let mut read_buf = vec![];
        f.read_all(&mut read_buf)
            .expect("file read_all should work");
        assert_eq!(String::from_utf8(read_buf).unwrap(), "hello world");

        env.remove("test2").expect("'remove' should work");
        assert!(!env.exists("test2"));
        assert!(env.list("").expect("'list' should work").is_empty());

        let mut tmp_names = HashSet::default();
        for i in 0..1000 {
            env.create(i.to_string().as_str())
                .expect("'create' should work");
            tmp_names.insert(i.to_string());
        }
        let list = env.list("").expect("'list' should work");
        for name in list.iter() {
            assert!(tmp_names.contains(name.to_str().unwrap()))
        }
    }
}
