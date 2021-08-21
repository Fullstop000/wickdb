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
use crate::{Error, Result};
use std::collections::hash_map::Entry;
use std::io::{Cursor, Error as IOError, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf, MAIN_SEPARATOR};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

/// An in memory file system based on a simple HashMap with fault injection
/// abilities.
/// Any newly created file or directory will be stored by enum `Node`.
///
/// The key format follows the rules below:
///   user/name -> /user/name
///   /user/name/ -> /user/name
///   ignore all the `CurDir` and `ParentDir`
///
/// #NOTICE
///
/// `MemStorage` do not support computing `.` or `..` in `Path` for convenience.
/// A `test/../test/a' will be treat as `test/test/a`
///
#[derive(Clone)]
pub struct MemStorage {
    inner: Arc<RwLock<HashMap<String, Node>>>,

    // ---- Parameters for fault injection
    /// sstable/log `flush()` calls are blocked.
    pub delay_data_sync: Arc<AtomicBool>,

    /// sstable/log `flush()` calls return an error
    pub data_sync_error: Arc<AtomicBool>,

    /// Simulate no-space errors
    pub no_space: Arc<AtomicBool>,

    /// Simulate non-writable file system
    pub non_writable: Arc<AtomicBool>,

    /// Force sync of manifest files to fail
    pub manifest_sync_error: Arc<AtomicBool>,

    /// Force write to manifest files to fail
    pub manifest_write_error: Arc<AtomicBool>,

    /// Whether enable to record the count of random reads to files
    pub count_random_reads: bool,

    pub random_read_counter: Arc<AtomicUsize>,
}

impl Default for MemStorage {
    fn default() -> Self {
        let mut map = HashMap::default();
        map.insert(MAIN_SEPARATOR.to_string(), Node::Dir);
        let inner = Arc::new(RwLock::new(map));
        Self {
            inner,
            delay_data_sync: Arc::new(AtomicBool::new(false)),
            data_sync_error: Arc::new(AtomicBool::new(false)),
            no_space: Arc::new(AtomicBool::new(false)),
            non_writable: Arc::new(AtomicBool::new(false)),
            manifest_sync_error: Arc::new(AtomicBool::new(false)),
            manifest_write_error: Arc::new(AtomicBool::new(false)),
            count_random_reads: false,
            random_read_counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl MemStorage {
    // Checking whether the path is good for create a file.
    // Return `Err` when the parent dir is not exist
    fn is_ok_to_create<P: AsRef<Path>>(&self, name: P) -> Result<()> {
        if let Some(p) = name.as_ref().parent() {
            if !self.is_exist_dir(p) {
                return Err(Error::IO(IOError::new(
                    ErrorKind::NotFound,
                    format!("{:?}: No directory or file exist", p),
                )));
            }
            if let Some(n) = self
                .inner
                .read()
                .unwrap()
                .get(name.as_ref().to_str().unwrap())
            {
                if n.is_file() {
                    // File can be truncated
                    return Ok(());
                } else {
                    // Exist dir
                    return Err(Error::IO(IOError::new(
                        ErrorKind::AlreadyExists,
                        format!("{:?}: File exists", p),
                    )));
                }
            }
            Ok(())
        } else {
            // root file
            Err(Error::IO(IOError::new(
                ErrorKind::AlreadyExists,
                "Unable to create root",
            )))
        }
    }

    // Whether the given `path` is a existed directory
    fn is_exist_dir<P: AsRef<Path>>(&self, path: P) -> bool {
        let map = self.inner.read().unwrap();
        map.get(path.as_ref().to_str().unwrap())
            .map_or(false, |n| n.is_dir())
    }
}

// Remove all the relative part (also the root prefix) and rebuild a new `PathBuf`
// by concatenating all normal components.
fn clean<P: AsRef<Path>>(path: P) -> PathBuf {
    path.as_ref()
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => Some(s),
            _ => None,
        })
        .fold(PathBuf::from(MAIN_SEPARATOR.to_string()), |mut pb, s| {
            pb.push(s);
            pb
        })
}

impl Storage for MemStorage {
    type F = FileNode;

    fn create<P: AsRef<Path>>(&self, name: P) -> Result<Self::F> {
        if self.non_writable.load(Ordering::Acquire) {
            return Err(Error::IO(IOError::new(
                ErrorKind::Other,
                "simulate non writable error",
            )));
        }
        let path = clean(name);
        self.is_ok_to_create(path.as_path())?;
        let name = path.to_str().unwrap().to_owned();
        let mut file_node = FileNode::new(&name);
        file_node.delay_data_sync = self.delay_data_sync.clone();
        file_node.data_sync_error = self.data_sync_error.clone();
        file_node.no_space = self.no_space.clone();
        file_node.manifest_sync_error = self.manifest_sync_error.clone();
        file_node.manifest_write_error = self.manifest_write_error.clone();
        file_node.count_random_reads = Arc::new(AtomicBool::new(self.count_random_reads));
        file_node.random_read_counter = self.random_read_counter.clone();
        match self.inner.write().unwrap().entry(name) {
            Entry::Occupied(n) => match n.get() {
                Node::File(f) => return Ok(f.clone()),
                Node::Dir => {
                    return Err(Error::IO(IOError::new(
                        ErrorKind::Other,
                        format!("{} is a directory", n.key()),
                    )))
                }
            },
            Entry::Vacant(v) => v.insert(Node::File(file_node.clone())),
        };
        Ok(file_node)
    }

    fn open<P: AsRef<Path>>(&self, name: P) -> Result<Self::F> {
        let path = clean(name).to_str().unwrap().to_owned();
        match self.inner.read().unwrap().get(&path) {
            Some(n) => match n {
                Node::Dir => Err(Error::IO(IOError::new(
                    ErrorKind::NotFound,
                    format!("{}: Try to open a directory", &path),
                ))),
                Node::File(f) => Ok(f.clone()),
            },
            None => Err(Error::IO(IOError::new(
                ErrorKind::NotFound,
                format!("{}: No such file", &path),
            ))),
        }
    }

    // Same as `remove_file`
    fn remove<P: AsRef<Path>>(&self, name: P) -> Result<()> {
        let key = clean(name).to_str().unwrap().to_owned();
        let mut map = self.inner.write().unwrap();
        if let Some(n) = map.get(&key) {
            match n {
                Node::Dir => Err(Error::IO(IOError::new(
                    ErrorKind::NotFound,
                    format!("{}: No such file", &key),
                ))),
                Node::File(_) => {
                    map.remove(&key);
                    Ok(())
                }
            }
        } else {
            Err(Error::IO(IOError::new(
                ErrorKind::NotFound,
                format!("{}: No such file", &key),
            )))
        }
    }

    // Same as `remove_dir` or `remove_dir_all`
    fn remove_dir<P: AsRef<Path>>(&self, dir: P, recursively: bool) -> Result<()> {
        let key = clean(dir).to_str().unwrap().to_owned();
        let mut map = self.inner.write().unwrap();
        if recursively {
            // Remove the dir and all its contents
            let mut to_delete = if let Some(n) = map.get(&key) {
                if n.is_dir() {
                    map.keys()
                        .filter(|k| *k != &key && k.starts_with(&key))
                        .map(|p| p.into())
                        .collect::<Vec<PathBuf>>()
                } else {
                    return Err(Error::IO(IOError::new(
                        ErrorKind::NotFound,
                        format!("{}: No such directory", &key),
                    )));
                }
            } else {
                return Err(Error::IO(IOError::new(
                    ErrorKind::NotFound,
                    format!("{}: No such directory", &key),
                )));
            };
            if key != MAIN_SEPARATOR.to_string() {
                to_delete.push(key.into());
            }
            for k in to_delete {
                map.remove(k.to_str().unwrap());
            }
            Ok(())
        } else {
            // Remove an empty dir
            if let Some(n) = map.get(&key) {
                match n {
                    Node::Dir => {
                        // Should be an empty dir
                        for k in map.keys().filter(|k| *k != &key) {
                            if k.starts_with(&key) {
                                return Err(Error::IO(IOError::new(
                                    ErrorKind::NotFound,
                                    format!("{}: is not an empty dir", &key),
                                )));
                            }
                        }
                        map.remove(&key);
                        Ok(())
                    }
                    Node::File(_) => Err(Error::IO(IOError::new(
                        ErrorKind::NotFound,
                        format!("{}: is a file not dir", &key),
                    ))),
                }
            } else {
                Err(Error::IO(IOError::new(
                    ErrorKind::NotFound,
                    format!("{}: is a file not dir", &key),
                )))
            }
        }
    }

    fn exists<P: AsRef<Path>>(&self, name: P) -> bool {
        let path = clean(name);
        self.inner
            .read()
            .unwrap()
            .contains_key(path.to_str().unwrap())
    }

    fn rename<P: AsRef<Path>>(&self, old: P, new: P) -> Result<()> {
        let old = clean(old).to_str().unwrap().to_owned();
        if old == MAIN_SEPARATOR.to_string() {
            return Err(Error::IO(IOError::new(
                ErrorKind::InvalidInput,
                "Unable to rename the root",
            )));
        }
        let new = clean(new).to_str().unwrap().to_owned();
        let mut map = self.inner.write().unwrap();
        match map.remove(&old) {
            Some(f) => {
                map.insert(new, f);
                Ok(())
            }
            None => Err(Error::IO(IOError::new(
                ErrorKind::NotFound,
                format!("{}: No such file or directory", old),
            ))),
        }
    }

    fn mkdir_all<P: AsRef<Path>>(&self, dir: P) -> Result<()> {
        let path = clean(dir);
        let mut map = self.inner.write().unwrap();
        let components = path
            .ancestors()
            .map(|p| p.to_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        // All the components must not be a file
        for c in components.iter() {
            if let Some(n) = map.get(c) {
                match n {
                    Node::Dir => { /* creating same dir is idempotent, so don't throw err */ }
                    Node::File(_) => {
                        return Err(Error::IO(IOError::new(
                            ErrorKind::AlreadyExists,
                            format!("{}: File exists", c),
                        )))
                    }
                }
            }
        }
        for c in components {
            map.insert(c, Node::Dir);
        }
        Ok(())
    }

    fn list<P: AsRef<Path>>(&self, dir: P) -> Result<Vec<PathBuf>> {
        let path = clean(dir).to_str().unwrap().to_owned();
        let map = self.inner.read().unwrap();
        if !map.contains_key(&path) {
            return Err(Error::IO(IOError::new(
                ErrorKind::NotFound,
                format!("{}: No such directory", &path),
            )));
        }
        Ok(map
            .keys()
            .filter(|k| *k != &path && k.starts_with(&path))
            .map(|p| p.into())
            .collect::<Vec<PathBuf>>())
    }
}

#[derive(Clone)]
enum Node {
    File(FileNode),
    Dir,
}

impl Node {
    fn is_file(&self) -> bool {
        match self {
            Node::File(_) => true,
            Node::Dir => false,
        }
    }

    fn is_dir(&self) -> bool {
        match self {
            Node::File(_) => false,
            Node::Dir => true,
        }
    }
}

/// The `File` abstraction for `MemStorage` with fault injection ability
#[derive(Clone, Default)]
pub struct FileNode {
    name: String,
    delay_data_sync: Arc<AtomicBool>,
    data_sync_error: Arc<AtomicBool>,
    no_space: Arc<AtomicBool>,
    // The manifest config has more priority than others if self is a MANIFEST file
    manifest_sync_error: Arc<AtomicBool>,
    manifest_write_error: Arc<AtomicBool>,

    count_random_reads: Arc<AtomicBool>,
    random_read_counter: Arc<AtomicUsize>,

    inner: Arc<RwLock<InmemFile>>,
}

impl FileNode {
    #[allow(clippy::field_reassign_with_default)]
    fn new(name: &str) -> Self {
        let mut f = FileNode::default();
        f.name = name.to_owned();
        f
    }

    fn is_manifest(&self) -> bool {
        self.name.contains("MANIFEST")
    }
}

impl Drop for FileNode {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl File for FileNode {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.is_manifest() && self.manifest_write_error.load(Ordering::Acquire) {
            return Err(Error::IO(IOError::new(
                ErrorKind::Other,
                "simulated writer error",
            )));
        }
        if self.no_space.load(Ordering::Acquire) {
            // drop writes
            Ok(0)
        } else {
            self.inner.write().unwrap().write(buf)
        }
    }

    fn flush(&mut self) -> Result<()> {
        if self.is_manifest() {
            if self.manifest_sync_error.load(Ordering::Acquire) {
                Err(Error::IO(IOError::new(
                    ErrorKind::Other,
                    "simulated sync error",
                )))
            } else {
                self.inner.write().unwrap().flush()
            }
        } else if self.data_sync_error.load(Ordering::Acquire) {
            Err(Error::IO(IOError::new(
                ErrorKind::Other,
                "simulated sync error",
            )))
        } else if self.delay_data_sync.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(100));
            self.inner.write().unwrap().flush()
        } else {
            self.inner.write().unwrap().flush()
        }
    }

    fn close(&mut self) -> Result<()> {
        self.inner.write().unwrap().close()
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
        if self.count_random_reads.load(Ordering::Acquire) {
            self.random_read_counter.fetch_add(1, Ordering::Release);
        }
        self.inner.read().unwrap().read_at(buf, offset)
    }
}

/// `File` implementation based on memory
/// This is handy for our tests.
struct InmemFile {
    lock: AtomicBool,
    contents: Cursor<Vec<u8>>,
}

impl Default for InmemFile {
    fn default() -> Self {
        Self {
            lock: AtomicBool::new(false),
            contents: Cursor::new(vec![]),
        }
    }
}

impl Drop for InmemFile {
    fn drop(&mut self) {
        let _ = self.close();
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
        map_io_res!(r)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        // Reset read cursor to 0
        self.contents.set_position(0);
        Ok(())
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let r = self.contents.seek(pos);
        map_io_res!(r)
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let r = self.contents.read(buf);
        map_io_res!(r)
    }

    fn read_all(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        self.contents.set_position(0);
        let r = self.contents.read_to_end(buf);
        map_io_res!(r)
    }

    fn len(&self) -> Result<u64> {
        Ok(self.contents.get_ref().len() as u64)
    }

    fn lock(&self) -> Result<()> {
        // Unlike described in comments, returns Err instead of blocking if locked
        if self.lock.load(Ordering::Acquire) {
            Err(Error::IO(IOError::new(ErrorKind::Other, "Already locked")))
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
                return Err(Error::IO(IOError::new(ErrorKind::UnexpectedEof, "EOF")));
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
    use super::*;
    use crate::storage::{File, Storage};
    use crate::util::coding::put_fixed_32;

    impl MemStorage {
        fn assert_node_exists<P: AsRef<Path>>(&self, target: P) -> Node {
            let path = clean(target);
            let map = self.inner.read().unwrap();
            let v = map.get(path.to_str().unwrap());
            assert!(v.is_some());
            v.unwrap().clone()
        }

        fn assert_file_exists<P: AsRef<Path>>(&self, target: P) {
            assert!(self.assert_node_exists(target).is_file());
        }

        fn assert_dir_exists<P: AsRef<Path>>(&self, target: P) {
            assert!(self.assert_node_exists(target).is_dir());
        }
    }

    impl InmemFile {
        fn pos_and_data(&self) -> (u64, &[u8]) {
            (self.contents.position(), self.contents.get_ref().as_slice())
        }
    }

    #[test]
    fn test_mem_file_read_write() {
        let mut f = InmemFile::default();
        let written1 = f.write(b"hello world").unwrap();
        assert_eq!(written1, 11);
        let written2 = f.write(b"|hello world").unwrap();
        assert_eq!(written2, 12);
        let (pos, data) = f.pos_and_data();
        assert_eq!(pos, 0);
        assert_eq!(
            String::from_utf8(Vec::from(data)).unwrap(),
            "hello world|hello world"
        );
        let mut read_buf = vec![0u8; 5];
        let read = f.read(read_buf.as_mut_slice()).unwrap();
        assert_eq!(read, 5);
        let (pos, _) = f.pos_and_data();
        assert_eq!(pos, 5);
        read_buf.clear();
        let all = f.read_all(&mut read_buf).unwrap();
        assert_eq!(all, written1 + written2);
        assert_eq!(
            String::from_utf8(read_buf.clone()).unwrap(),
            "hello world|hello world"
        );
    }

    #[test]
    fn test_mem_file_lock_unlock() {
        let f = InmemFile::default();
        f.lock().unwrap();
        f.unlock().unwrap();
        f.lock().unwrap();
        assert_eq!(
            f.lock().unwrap_err().to_string(),
            "I/O operation error: Already locked"
        );
    }

    #[test]
    fn test_mem_file_read_at() {
        let mut f = InmemFile::default();
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
                Err(e) => assert_eq!(e.to_string(), "I/O operation error: EOF"),
            }
        }
    }

    #[test]
    fn test_storage_basic() {
        let store = MemStorage::default();
        // Test `create`
        let mut f = store.create("test1").unwrap();
        assert!(store.exists("test1"));
        f.write(b"hello world").unwrap();

        // Test `open` a non-exist file
        let expected_not_found = store.open("not exist");
        assert!(expected_not_found.is_err());

        f = store.open("test1").unwrap();
        let mut read_buf = vec![];
        f.read_all(&mut read_buf).unwrap();
        assert_eq!(String::from_utf8(read_buf).unwrap(), "hello world");

        let expected_not_found = store.rename("not exist", "test3");
        assert!(expected_not_found.is_err());

        // Test `rename`
        store.rename("test1", "test2").unwrap();
        assert!(!store.exists("test1"));
        assert!(store.exists("test2"));

        f = store.open("test2").unwrap();
        let mut read_buf = vec![];
        f.read_all(&mut read_buf).unwrap();
        assert_eq!(String::from_utf8(read_buf).unwrap(), "hello world");

        // Test `remove`
        store.remove("test2").unwrap();
        assert!(!store.exists("test2"));
    }

    #[test]
    fn test_storage_create() {
        let store = MemStorage::default();
        store.mkdir_all("/a/b/c").unwrap();
        let f = Node::File(FileNode::new("/a/b/c/d"));
        store
            .inner
            .write()
            .unwrap()
            .insert("/a/b/c/d".to_owned(), f);
        let tests = vec![
            ("/", false),               // root file
            ("/a", false),              // exist dir
            ("/a/d", true),             // non exist file
            ("/a/b/c", false),          // exist dir
            ("/a/b/c/d", true),         // truncate file
            ("/a/b/c/e", true),         // new file
            ("/a/b/c/d/e", false),      // parent is a file
            ("/a/b/c/d/e/e/e/", false), // no exist parent dir
        ];
        for (i, (input, expected)) in tests.into_iter().enumerate() {
            let res = store.create(input);
            assert_eq!(res.is_ok(), expected, "{}", i);
            if expected {
                store.assert_file_exists(input);
            }
        }
    }

    #[test]
    fn test_storage_open() {
        let store = MemStorage::default();
        store.create("test").unwrap();
        store.mkdir_all("/a/b/c").unwrap();
        let tests = vec![
            ("/", false),
            ("/test", true),
            ("test", true),
            ("test/", true),
            ("/a", false),
            ("/a/b", false),
            ("/****", false),
        ];
        for (input, expected) in tests {
            assert_eq!(store.open(input).is_ok(), expected);
        }
    }

    #[test]
    fn test_storage_exists() {
        let store = MemStorage::default();
        store.mkdir_all("a/b/c").unwrap();
        store.create("/a/test").unwrap();
        let tests = vec![
            ("/", true),
            ("///", true),
            ("/a/b/c/", true),
            ("a/b/c/", true),
            ("/a/b/c", true),
            ("/a", true),
            ("/a/b", true),
            ("/a/b/c/d", false),
            ("/a/test", true),
            ("test", false),
        ];
        for (input, expected) in tests {
            assert_eq!(store.exists(input), expected);
        }
    }

    #[test]
    fn test_storage_remove() {
        let store = MemStorage::default();
        store.mkdir_all("a/b/c").unwrap();
        store.create("test").unwrap();
        store.create("/a/test").unwrap();
        store.create("/a/b/test").unwrap();
        let tests = vec![
            ("/", false),
            ("a", false),
            ("/a", false),
            ("test", true),
            ("a/test", true),
            ("a/b/test", true),
            ("hello world", false),
        ];
        for (input, expected) in tests {
            assert_eq!(store.remove(input).is_ok(), expected)
        }
    }

    #[test]
    fn test_storage_remove_dir() {
        let store = MemStorage::default();
        // |- a
        //   |- 1
        //   |- 2
        // |- b
        // |- c
        //   |- 1
        //   |- d
        //      |- 2
        store.mkdir_all("a").unwrap();
        store.mkdir_all("b").unwrap();
        store.mkdir_all("c/d").unwrap();
        store.create("a/1").unwrap();
        store.create("a/2").unwrap();
        store.create("c/1").unwrap();
        store.create("c/d/2").unwrap();
        let tests = vec![
            ("/", false, false),
            ("a", false, false),
            ("a", true, true),
            ("b", false, true),
            ("c/1", false, false),
            ("c/1", true, false),
            ("c/d", true, true),
            ("/", true, true),
        ];
        for (input, recursively, expected) in tests {
            assert_eq!(store.remove_dir(input, recursively).is_ok(), expected);
        }
    }

    #[test]
    fn test_storage_mkdir_all() {
        let store = MemStorage::default();
        store.assert_dir_exists("/");
        store.create("test").unwrap();
        let tests = vec![
            ("/", true),
            ("/test/a", false),
            ("a/b/c", true),
            ("a/b/c/", true), // mkdir is idempotent
        ];
        for (input, expected) in tests {
            assert_eq!(store.mkdir_all(input).is_ok(), expected);
            if expected {
                store.assert_dir_exists(input);
            }
        }
    }

    #[test]
    fn test_storage_list() {
        let store = MemStorage::default();
        for i in 0..1000 {
            store.create(i.to_string()).unwrap();
        }
        let list = store.list("/").unwrap();
        for name in list {
            store.assert_file_exists(name);
        }
    }
    #[test]
    fn test_path_clean() {
        let tests = if cfg!(windows) {
            vec![
                ("\\path\\..\\test\\", "\\path\\test"),
                ("\\path\\.\\test\\..", "\\path\\test"),
                ("path", "\\path"),
                ("\\", "\\"),
                (r#"\\\"#, "\\"),
            ]
        } else {
            vec![
                ("/path/../test/", "/path/test"),
                ("/path/./test/..", "/path/test"),
                ("path", "/path"),
                ("/", "/"),
                ("///", "/"),
            ]
        };
        for (input, expected) in tests {
            let res = clean(input);
            assert_eq!(res.to_str().unwrap(), expected);
        }
    }

    #[test]
    fn test_reopen_file_and_read() {
        let store = MemStorage::default();
        let mut f = store.create("test").unwrap();
        let contents = "a".repeat(1000);
        f.write(contents.as_bytes()).unwrap();
        let mut got = vec![];
        f.read_all(&mut got).unwrap();
        assert_eq!(&contents.as_bytes(), &got.as_slice());
        f.close().unwrap();
        let mut f = store.open("test").unwrap();
        let mut got = vec![];
        f.read_all(&mut got).unwrap();
        assert_eq!(&contents.as_bytes(), &got.as_slice());
    }
}
