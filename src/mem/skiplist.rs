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

use super::arena::*;
use crate::iterator::Iterator;
use crate::util::comparator::Comparator;
use crate::util::slice::Slice;
use crate::util::status::Result;
use rand::random;
use std::cmp::Ordering as CmpOrdering;
use std::intrinsics::copy_nonoverlapping;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
use std::{mem, slice};

const BRANCHING: u32 = 4;
pub const MAX_HEIGHT: usize = 12;

#[derive(Debug)]
#[repr(C)]
/// Node represents a skiplist node.
/// The memory layout of Node should be stable but the default `repr(Rust)` causes the issue
/// that fields can not be laid out in the order of their declaration.
/// And `repr(C)` avoid it but it may add some padding bits for alignment purpose ( using 8 bits for u32 ).
/// However, it seems we should not use `repr(C, packed)` by https://doc.rust-lang.org/nomicon/other-reprs.html#reprpacked
/// As we use `#[repr(C)]`, the size of `usize` is always 8 so
/// `MAX_NODE_SIZE = size of Node + MAX_HEIGHT * size_of(usize) (ptr size is same as usize) = 56 + 12 * 8 = 152`
pub struct Node {
    pub key: Slice,
    // The inner memory of slices will be allocated dynamically
    // and the length depends on the height of Node
    pub next_nodes: Box<[AtomicPtr<Node>]>,
}

impl Node {
    /// Allocates memory in the given arena for Node
    #[allow(clippy::cast_ptr_alignment)]
    pub fn new(key: Slice, height: usize, arena: &Arena) -> *mut Node {
        let size = mem::size_of::<Node>() + height * mem::size_of::<AtomicPtr<Node>>();
        let ptr = arena.allocate_aligned(size);
        unsafe {
            let (node_part, nexts_part) =
                slice::from_raw_parts_mut(ptr, size).split_at_mut(mem::size_of::<Node>());
            let node = node_part.as_mut_ptr() as *mut Node;
            let nexts = Vec::from_raw_parts(
                nexts_part.as_mut_ptr() as *mut AtomicPtr<Node>,
                height,
                height,
            );
            (*node).key = key;
            (*node).next_nodes = nexts.into_boxed_slice();
            node
        }
    }

    #[inline]
    pub fn get_next(&self, height: usize) -> *mut Node {
        self.next_nodes[height - 1].load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_next(&self, height: usize, node: *mut Node) {
        self.next_nodes[height - 1].store(node, Ordering::Release);
    }

    #[inline]
    pub fn key(&self) -> &Slice {
        &self.key
    }
}

/// A skiplist with an memory based arena. The skiplist
/// should be thread safe for reading
pub struct Skiplist {
    // current max height
    // Should be handled atomically
    pub max_height: AtomicUsize,
    // comparator is used to compare the key of node
    pub comparator: Arc<dyn Comparator>,
    // head node
    pub head: *mut Node,
    // arena contains all the nodes data
    pub arena: Box<dyn Arena>,
}

impl Skiplist {
    /// Create a new Skiplist with the given arena capacity
    pub fn new(cmp: Arc<dyn Comparator>, mut arena: Box<dyn Arena>) -> Self {
        let head = Node::new(Slice::default(), MAX_HEIGHT, arena.as_mut());
        Skiplist {
            comparator: cmp,
            // init height is 1 ( ignore the height of head )
            max_height: AtomicUsize::new(1),
            arena,
            head,
        }
    }

    /// Insert a node into the skiplist by given key.
    /// The key must be unique otherwise this method panic.
    ///
    /// # NOTICE:
    ///
    /// Concurrent insertion is not thread safe but concurrent reading with a
    /// single writer is safe.
    ///
    pub fn insert(&self, key: Slice) {
        let mut prev = [ptr::null_mut(); MAX_HEIGHT];
        let node = self.find_greater_or_equal(&key, Some(&mut prev));
        if !node.is_null() {
            unsafe {
                assert_ne!(
                    (&(*node)).key().compare(&key),
                    CmpOrdering::Equal,
                    "[skiplist] duplicate insertion [key={:?}] is not allowed",
                    key
                );
            }
        }
        let height = rand_height();
        let max_height = self.max_height.load(Ordering::Acquire);
        if height > max_height {
            #[allow(clippy::needless_range_loop)]
            for i in max_height..height {
                prev[i] = self.head;
            }
            self.max_height.store(height, Ordering::Release);
        }
        // allocate the key
        let k = self.arena.allocate(key.size());
        unsafe {
            copy_nonoverlapping(key.as_ptr(), k, key.size());
        }
        // allocate the node
        let new_node = Node::new(
            Slice::new(k as *const u8, key.size()),
            height,
            self.arena.as_ref(),
        );
        unsafe {
            for i in 1..=height {
                (*new_node).set_next(i, (*(prev[i - 1])).get_next(i));
                (*(prev[i - 1])).set_next(i, new_node);
            }
        }
    }

    /// Find the nearest node with a key >= the given key.
    /// Add prev node into `prev_nodes`
    /// which can be helpful for adding new node to the skiplist.
    pub fn find_greater_or_equal(
        &self,
        key: &Slice,
        mut prev_nodes: Option<&mut [*mut Node]>,
    ) -> *mut Node {
        let mut level = self.max_height.load(Ordering::Acquire);
        let mut node = self.head;
        loop {
            unsafe {
                let next = (*node).get_next(level);
                if self.key_is_less_than_or_equal(key, next) {
                    // given key <= next key
                    // record the prev node
                    if let Some(ref mut p) = prev_nodes {
                        p[level - 1] = node;
                    }
                    // already arrived the bottom return the current node's next
                    if level == 1 {
                        return next;
                    }
                    // move to next level
                    level -= 1;
                } else {
                    // so we keep searching in the same level
                    node = next;
                }
            }
        }
    }

    /// Find the nearest node with a key < the given key.
    /// Return head if there is no such node.
    pub fn find_less_than(&self, key: &Slice) -> *mut Node {
        let mut level = self.max_height.load(Ordering::Acquire);
        let mut node = self.head;
        loop {
            unsafe {
                let next = (*node).get_next(level);
                if next.is_null()
                    || self
                        .comparator
                        .compare(&((*next).key()).as_slice(), key.as_slice())
                        != CmpOrdering::Less
                {
                    // next is nullptr or next.key >= key
                    if level == 1 {
                        return node;
                    } else {
                        // move to next level
                        level -= 1;
                    }
                } else {
                    // next.key < key
                    // keep search in the same level
                    node = next;
                }
            }
        }
    }

    /// Find the last node
    pub fn find_last(&self) -> *mut Node {
        let mut level = self.max_height.load(Ordering::Acquire);
        let mut node = self.head;
        loop {
            unsafe {
                let next = (*node).get_next(level);
                if next.is_null() {
                    if level == 1 {
                        return node;
                    }
                    // move to next level
                    level -= 1;
                } else {
                    node = next;
                }
            }
        }
    }

    /// Return whether the give key is less than the given node's key.
    pub(super) fn key_is_less_than_or_equal(&self, key: &Slice, n: *mut Node) -> bool {
        if n.is_null() {
            // take nullptr as +infinite large
            true
        } else {
            let node_key = unsafe { (*n).key() };
            match self.comparator.compare(key.as_slice(), node_key.as_slice()) {
                CmpOrdering::Greater => false,
                _ => true,
            }
        }
    }
}

/// Iteration over the contents of a skip list
pub struct SkiplistIterator<'a> {
    skl: &'a Skiplist,
    pub(super) node: *mut Node,
}

impl<'a> Iterator for SkiplistIterator<'a> {
    /// Returns true whether the iterator is positioned at a valid node
    #[inline]
    fn valid(&self) -> bool {
        !self.node.is_null()
    }

    /// Position at the first node in list
    #[inline]
    fn seek_to_first(&mut self) {
        unsafe {
            self.node = (*(self.skl.head)).next_nodes[0].load(Ordering::Acquire);
        }
    }

    /// Position at the last node in list
    #[inline]
    fn seek_to_last(&mut self) {
        self.node = self.skl.find_last();
    }

    /// Advance to the first node with a key >= target
    #[inline]
    fn seek(&mut self, target_key: &Slice) {
        self.node = self.skl.find_greater_or_equal(target_key, None);
    }

    /// Advance to the next position
    #[inline]
    fn next(&mut self) {
        self.panic_valid();
        unsafe {
            self.node = (*(self.node)).get_next(1);
        }
    }

    /// Advance to the previous position
    #[inline]
    fn prev(&mut self) {
        let key = self.key();
        self.node = self.skl.find_less_than(&key);
    }

    /// Return the key of node in current position
    #[inline]
    fn key(&self) -> Slice {
        self.panic_valid();
        unsafe { (*(self.node)).key().clone() }
    }
    /// Should not be used
    #[inline]
    fn value(&self) -> Slice {
        unimplemented!()
    }

    fn status(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<'a> SkiplistIterator<'a> {
    pub fn new(skl: &'a Skiplist) -> Self {
        Self {
            skl,
            node: ptr::null_mut(),
        }
    }

    /// If the head is nullptr, this method will panic. Otherwise return true.
    #[inline]
    pub fn panic_valid(&self) -> bool {
        assert!(self.valid(), "[skl] invalid iterator head",);
        true
    }
}

/// Generate a random height < MAX_HEIGHT for node
fn rand_height() -> usize {
    let mut height = 1;
    loop {
        if height < MAX_HEIGHT && random::<u32>() % BRANCHING == 0 {
            height += 1;
        } else {
            break;
        }
    }
    height
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iterator::Iterator;
    use crate::util::coding::{decode_fixed_64, put_fixed_64};
    use crate::util::comparator::BytewiseComparator;
    use crate::util::hash::hash as do_hash;
    use rand::Rng;
    use rand::RngCore;
    use std::cmp::Ordering as CmpOrdering;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Condvar, Mutex};
    use std::{ptr, thread};

    fn new_test_skl() -> Skiplist {
        Skiplist::new(
            Arc::new(BytewiseComparator::new()),
            Box::new(BlockArena::new()),
        )
    }

    fn construct_skl_from_nodes(mut nodes: Vec<(Slice, usize)>) -> Skiplist {
        if nodes.is_empty() {
            return new_test_skl();
        }
        let mut skl = new_test_skl();
        // just use MAX_HEIGHT as capacity because it's the largest value that node.height can have
        let mut prev_nodes = vec![skl.head; MAX_HEIGHT];
        let mut max_height = 1;
        for (key, height) in nodes.drain(..) {
            let n = Node::new(key, height, skl.arena.as_mut());
            for (h, prev_node) in prev_nodes[0..height].iter().enumerate() {
                unsafe {
                    (**prev_node).set_next(h + 1, n);
                }
            }
            for i in 0..height {
                prev_nodes[i] = n;
            }
            if height > max_height {
                max_height = height;
            }
        }
        // must update max_height
        skl.max_height.store(max_height, Ordering::Release);
        skl
    }

    #[test]
    fn test_rand_height() {
        for _ in 0..100 {
            let height = rand_height();
            assert_eq!(height < MAX_HEIGHT, true);
        }
    }

    #[test]
    fn test_key_is_less_than_or_equal() {
        let skl = new_test_skl();
        let vec = vec![1u8, 2u8, 3u8];
        let key = Slice::from(vec.as_slice());

        let tests = vec![
            (vec![1u8, 2u8], false),
            (vec![1u8, 2u8, 4u8], true),
            (vec![1u8, 2u8, 3u8], true),
        ];
        // nullptr should be considered as the largest
        assert_eq!(true, skl.key_is_less_than_or_equal(&key, ptr::null_mut()));

        for (node_key, expected) in tests {
            let node = Node::new(Slice::from(node_key.as_slice()), 1, skl.arena.as_ref());
            assert_eq!(expected, skl.key_is_less_than_or_equal(&key, node))
        }
    }

    #[test]
    fn test_find_greater_or_equal() {
        let inputs = vec![
            ("key1", 5),
            ("key3", 1),
            ("key5", 2),
            ("key7", 4),
            ("key9", 3),
        ];
        let nodes: Vec<(Slice, usize)> = inputs
            .iter()
            .map(|(key, height)| (Slice::from(*key), *height))
            .collect();
        let skl = construct_skl_from_nodes(nodes);
        let mut prev_nodes = vec![ptr::null_mut(); 5];
        // test the scenario for un-inserted key
        let target_key = Slice::from("key4");
        let res = skl.find_greater_or_equal(&target_key, Some(&mut prev_nodes));
        unsafe {
            assert_eq!((*res).key().as_str(), "key5");
            // prev_nodes should be correct
            assert_eq!((*(prev_nodes[0])).key().as_str(), "key3");
            for node in prev_nodes[1..5].iter() {
                assert_eq!((**node).key().as_str(), "key1");
            }
        }
        prev_nodes = vec![ptr::null_mut(); 5];
        // test the scenario for inserted key
        let target_key2 = Slice::from("key5");
        let res2 = skl.find_greater_or_equal(&target_key2, Some(&mut prev_nodes));
        unsafe {
            assert_eq!((*res2).key().as_str(), "key5");
            // prev_nodes should be correct
            assert_eq!((*(prev_nodes[0])).key().as_str(), "key3");
            for node in prev_nodes[1..5].iter() {
                assert_eq!((**node).key().as_str(), "key1");
            }
        }
    }

    #[test]
    fn test_find_less_than() {
        let inputs = vec![
            ("key1", 5),
            ("key3", 1),
            ("key5", 2),
            ("key7", 4),
            ("key9", 3),
        ];
        let nodes: Vec<(Slice, usize)> = inputs
            .iter()
            .map(|(key, height)| (Slice::from(*key), *height))
            .collect();
        let skl = construct_skl_from_nodes(nodes);

        // test scenario for un-inserted key
        let target_key = Slice::from("key4");
        let res = skl.find_less_than(&target_key);
        unsafe {
            assert_eq!((*res).key().as_str(), "key3");
        }

        // test scenario for inserted key
        let target_key = Slice::from("key5");
        let res = skl.find_less_than(&target_key);
        unsafe {
            assert_eq!((*res).key().as_str(), "key3");
        }
    }

    #[test]
    fn test_find_last() {
        let inputs = vec![
            ("key1", 5),
            ("key3", 1),
            ("key5", 2),
            ("key7", 4),
            ("key9", 3),
        ];
        let nodes: Vec<(Slice, usize)> = inputs
            .iter()
            .map(|(key, height)| (Slice::from(*key), *height))
            .collect();
        let skl = construct_skl_from_nodes(nodes);
        let last = skl.find_last();
        unsafe {
            assert_eq!((*last).key().as_str(), "key9");
        }
    }

    #[test]
    fn test_insert() {
        let inputs = vec!["key1", "key3", "key5", "key7", "key9"];
        let skl = new_test_skl();
        for key in inputs.clone().drain(..) {
            skl.insert(Slice::from(key));
        }

        let mut node = skl.head;
        for input_key in inputs.clone().drain(..) {
            unsafe {
                let next = (*node).get_next(1);
                let key = (*next).key();
                assert_eq!(key.as_str(), input_key);
                node = next;
            }
        }
        unsafe {
            // should be the last node
            assert_eq!((*node).get_next(1), ptr::null_mut());
        }
    }

    #[test]
    #[should_panic]
    fn test_duplicate_insert_should_panic() {
        let mut inputs = vec!["key1", "key1"];
        let skl = new_test_skl();
        for key in inputs.drain(..) {
            skl.insert(Slice::from(key));
        }
    }

    // this is a e2e test for all methods in SkiplistIterator
    #[test]
    fn test_basic() {
        let skl = new_test_skl();
        let inputs = vec!["key1", "key11", "key13", "key3", "key5", "key7", "key9"];
        for key in inputs.clone().drain(..) {
            skl.insert(Slice::from(key))
        }
        let mut skl_iterator = SkiplistIterator::new(&skl);
        assert_eq!(ptr::null_mut(), skl_iterator.node,);

        skl_iterator.seek_to_first();
        assert_eq!("key1", skl_iterator.key().as_str());
        for key in inputs.clone().drain(..) {
            if !skl_iterator.valid() {
                break;
            }
            assert_eq!(key, skl_iterator.key().as_str());
            skl_iterator.next();
        }

        skl_iterator.seek_to_first();
        skl_iterator.next();
        skl_iterator.prev();
        assert_eq!(inputs[0], skl_iterator.key().as_str());
        skl_iterator.seek_to_first();
        skl_iterator.seek_to_last();
        assert_eq!(inputs[inputs.len() - 1], skl_iterator.key().as_str());
        skl_iterator.seek(&Slice::from("key7"));
        assert_eq!("key7", skl_iterator.key().as_str());
        skl_iterator.seek(&Slice::from("key4"));
        assert_eq!("key5", skl_iterator.key().as_str());
    }

    const K: usize = 4;

    // Per-key generation
    struct State {
        generation: [AtomicUsize; K],
    }

    impl State {
        fn new() -> Self {
            let mut generation = [
                AtomicUsize::new(0),
                AtomicUsize::new(0),
                AtomicUsize::new(0),
                AtomicUsize::new(0),
            ];
            for i in 0..K {
                generation[i] = AtomicUsize::new(0)
            }
            Self { generation }
        }

        fn set(&self, k: usize, v: usize) {
            self.generation[k].store(v, Ordering::Release)
        }

        fn get(&self, k: usize) -> usize {
            self.generation[k].load(Ordering::Acquire)
        }
    }

    // Extract key
    fn key(key: u64) -> u64 {
        key >> 40
    }

    // Extract gen
    fn gen(key: u64) -> u64 {
        (key >> 8) & 0xffffffff
    }

    // Extract hash
    fn hash(key: u64) -> u64 {
        key & 0xff
    }

    fn hash_numbers(k: u64, g: u64) -> u64 {
        let mut bytes = vec![];
        put_fixed_64(&mut bytes, k);
        put_fixed_64(&mut bytes, g);
        do_hash(bytes.as_slice(), 0) as u64
    }

    // Format of key:
    // key | gen | hash
    fn make_key(k: u64, g: u64) -> u64 {
        (k << 40) | (g << 8) | (hash_numbers(k, g) & 0xff)
    }

    fn is_valid_key(k: u64) -> bool {
        hash(k) == hash_numbers(key(k), gen(k)) & 0xff
    }

    fn random_target() -> u64 {
        let mut rand = rand::thread_rng();
        let r = rand.gen_range(0, 10);
        match r {
            0 => make_key(0, 0),
            1 => make_key(K as u64, 0),
            _ => make_key(rand.gen_range(0, K) as u64, 0),
        }
    }

    struct U64Comparator {}
    impl Comparator for U64Comparator {
        fn compare(&self, a: &[u8], b: &[u8]) -> CmpOrdering {
            let s1 = decode_fixed_64(a);
            let s2 = decode_fixed_64(b);
            s1.cmp(&s2)
        }

        fn name(&self) -> &str {
            unimplemented!()
        }

        fn separator(&self, _a: &[u8], _b: &[u8]) -> Vec<u8> {
            unimplemented!()
        }

        fn successor(&self, _key: &[u8]) -> Vec<u8> {
            unimplemented!()
        }
    }

    // We want to make sure that with a single writer and multiple
    // concurrent readers (with no synchronization other than when a
    // reader's iterator is created), the reader always observes all the
    // data that was present in the skip list when the iterator was
    // constructed.  Because insertions are happening concurrently, we may
    // also observe new values that were inserted since the iterator was
    // constructed, but we should never miss any values that were present
    // at iterator construction time.
    struct ConcurrencyTest {
        current: State,
        list: Skiplist,
    }

    unsafe impl Send for ConcurrencyTest {}
    unsafe impl Sync for ConcurrencyTest {}

    impl ConcurrencyTest {
        pub fn new() -> Self {
            let arena = BlockArena::new();
            Self {
                current: State::new(),
                list: Skiplist::new(Arc::new(U64Comparator {}), Box::new(arena)),
            }
        }

        fn write_step(&self) {
            let k = rand::thread_rng().gen_range(0, K);
            let g = self.current.get(k) + 1;
            let key = make_key(k as u64, g as u64);
            let mut bytes = vec![];
            put_fixed_64(&mut bytes, key);
            self.list.insert(Slice::from(&bytes));
            self.current.set(k, g);
        }

        fn read_step(&self) {
            // Remember the initial committed state of the skiplist.
            let initial_state = State::new();
            for i in 0..K {
                initial_state.set(i, self.current.get(i));
            }

            let mut pos = random_target();
            let mut pos_bytes = vec![];
            put_fixed_64(&mut pos_bytes, pos);
            let mut iter = SkiplistIterator::new(&self.list);
            iter.seek(&Slice::from(&pos_bytes));
            loop {
                let current = if !iter.valid() {
                    // Seek to end
                    make_key(K as u64, 0)
                } else {
                    let s = iter.key();
                    let k = decode_fixed_64(s.as_slice());
                    assert!(is_valid_key(k));
                    k
                };
                // Verify that everything in [pos,current) was not present in
                // initial_state
                while pos < current {
                    assert!(
                        pos <= current,
                        "should not go backwards. pos: {}, current: {}",
                        pos,
                        current
                    );
                    assert!(
                        gen(pos) == 0 || gen(pos) > initial_state.get(key(pos) as usize) as u64,
                        "key: {}; gen: {}; initgetn: {}",
                        key(pos),
                        gen(pos),
                        initial_state.get(key(pos) as usize)
                    );
                    // Advance to next key in the valid key space
                    if key(pos) < key(current) {
                        pos = make_key(key(pos) + 1, 0);
                    } else {
                        pos = make_key(key(pos), gen(pos) + 1);
                    }
                }
                if !iter.valid() {
                    break;
                }
                // To next or seek to random position
                if rand::thread_rng().next_u64() % 2 == 0 {
                    iter.next();
                    pos = make_key(key(pos), gen(pos) + 1);
                } else {
                    let new_target = random_target();
                    if new_target > pos {
                        pos = new_target;
                        let mut bytes = vec![];
                        put_fixed_64(&mut bytes, pos);
                        iter.seek(&Slice::from(&bytes));
                    }
                }
            }
        }
    }

    #[test]
    fn test_concurrent_without_threads() {
        let test = ConcurrencyTest::new();
        for _ in 0..1000 {
            test.read_step();
            test.write_step();
        }
    }

    struct TestState {
        quit_flag: AtomicBool,
        mu: (Mutex<ReaderState>, Condvar),
    }

    impl TestState {
        fn new() -> Self {
            Self {
                quit_flag: AtomicBool::new(false),
                mu: (Mutex::new(ReaderState::Starting), Condvar::new()),
            }
        }

        // Wait util ReaderState reaches given `s`
        fn wait(&self, s: ReaderState) {
            let (mu, cv) = &self.mu;
            let mut guard = mu.lock().unwrap();
            while *guard != s {
                guard = cv.wait(guard).unwrap()
            }
        }

        // Change the ReaderState to given `s` and wake up all the
        // threads waiting for this
        fn change(&self, s: ReaderState) {
            let (mu, cv) = &self.mu;
            let mut guard = mu.lock().unwrap();
            *guard = s;
            cv.notify_all();
        }
    }

    // Test concurrency read&write
    fn run_concurrent() {
        for _ in 0..100 {
            let test = Arc::new(ConcurrencyTest::new());
            let test2 = test.clone();
            let state = Arc::new(TestState::new());
            let state2 = state.clone();
            let read = thread::spawn(move || {
                state.change(ReaderState::Running);
                while !state.quit_flag.load(Ordering::Acquire) {
                    test.read_step();
                }
                // Wakeup writing thread to exit
                state.change(ReaderState::Done);
            });
            let write = thread::spawn(move || {
                state2.wait(ReaderState::Running);
                for _ in 0..1000 {
                    test2.write_step();
                }
                // Break the reading loop
                state2.quit_flag.store(true, Ordering::Release);
                state2.wait(ReaderState::Done);
            });
            read.join().expect("Read thread panics");
            write.join().expect("Write thread panics");
        }
    }

    #[derive(Eq, PartialEq)]
    enum ReaderState {
        Starting,
        Running,
        Done,
    }

    #[test]
    fn test_concurrency_read_write() {
        for _ in 0..5 {
            run_concurrent()
        }
    }
}
