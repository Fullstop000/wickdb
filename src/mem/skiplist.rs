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

use super::arena::*;
use crate::util::comparator::Comparator;
use crate::util::slice::Slice;
use rand::random;
use std::cmp::Ordering as CmpOrdering;
use std::mem;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const BRANCHING: u32 = 4;
pub const MAX_HEIGHT: usize = 12;

// As we use #[repr(C)], the size of usize is always 8
// MAX_NODE_SIZE = size of Node + MAX_HEIGHT * size_of(usize) (ptr size is same as usize) = 56 + 12 * 8 = 152
pub const MAX_NODE_SIZE: usize = mem::size_of::<Node>() + MAX_HEIGHT * mem::size_of::<*mut u8>();

#[derive(Debug)]
#[repr(C)]
/// Node represents a skiplist node.
/// The memory layout of Node should be stable but the default `repr(Rust)` causes the issue
/// that fields can not be laid out in the order of their declaration.
/// And `repr(C)` avoid it but it may add some padding bits for alignment purpose ( using 8 bits for u32 ).
/// However, it seems we should not use `repr(C, packed)` by https://doc.rust-lang.org/nomicon/other-reprs.html#reprpacked
pub struct Node {
    pub key_offset: u32,
    pub key_size: u64,
    pub value_offset: u32,
    pub value_size: u64,
    // the height
    pub height: usize,
    // The inner memory of slices will be allocated dynamically
    // and the length depends on the height of Node
    pub next_nodes: Box<[AtomicPtr<Node>]>,
}

impl Node {
    /// Allocates memory in the given arena for Node
    pub fn new<A: Arena>(key: &Slice, value: &Slice, height: usize, arena: &A) -> *mut Node {
        let node = arena.alloc_node(height);
        unsafe {
            (*node).key_size = key.size() as u64;
            (*node).key_offset = arena.alloc_bytes(key);
            (*node).value_size = value.size() as u64;
            (*node).value_offset = arena.alloc_bytes(value);
        }
        node
    }

    pub fn get_next(&self, height: usize) -> *mut Node {
        invarint!(
            height <= self.height && height > 0,
            "skiplist: try to get next node in height [{}] but the height of node is {}",
            height,
            self.height
        );
        self.next_nodes[height - 1].load(Ordering::Acquire)
    }

    pub fn set_next(&self, height: usize, node: *mut Node) {
        invarint!(
            height <= self.height && height > 0,
            "skiplist: try to set next node in height [{}] but the height of node is {}",
            height,
            self.height
        );
        self.next_nodes[height - 1].store(node, Ordering::Release);
    }

    #[inline]
    pub fn key<A: Arena>(&self, arena: &A) -> Slice {
        let raw = arena.get(self.key_offset as usize, self.key_size as usize);
        Slice::from(raw)
    }

    #[inline]
    pub fn value<A: Arena>(&self, arena: &A) -> Slice {
        let raw = arena.get(self.value_offset as usize, self.value_size as usize);
        Slice::from(raw)
    }
}

pub struct Skiplist<A: Arena> {
    // current max height
    // Should be handled atomically
    pub max_height: AtomicUsize,
    // comparator is used to compare the key of node
    pub comparator: Rc<Comparator<Slice>>,
    // references of this Skiplist
    // This not only represents in-memory refs but also 'refs' in read request
    refs: AtomicUsize,
    // head node
    pub head: *mut Node,
    // arena contains all the nodes data
    pub arena: A,
}

impl Skiplist<AggressiveArena> {
    /// Create a new Skiplist with the given arena capacity
    pub fn new(arena_cap: usize, cmp: Rc<Comparator<Slice>>) -> Self {
        let arena = AggressiveArena::new(arena_cap);
        let head = arena.alloc_node(MAX_HEIGHT);
        Skiplist {
            comparator: cmp,
            // init height is 1 ( ignore the height of head )
            max_height: AtomicUsize::new(1),
            arena,
            head,
            refs: AtomicUsize::new(1), // as created
        }
    }

    /// Insert a node into the skiplist by given key and value.
    /// The key must be unique otherwise this method panic.
    pub fn insert(&self, key: &Slice, value: &Slice) {
        let mut prev = [ptr::null_mut(); MAX_HEIGHT];
        let node = self.find_greater_or_equal(key, &mut prev);
        unsafe {
            invarint!(
                &(*node).key(&self.arena) != key,
                "[skiplist] duplicate insertion [key={:?}] is not allowed",
                key
            );
        }
        let height = rand_height();
        let max_height = self.max_height.load(Ordering::Acquire);
        if height > max_height {
            for i in max_height..height {
                prev[i] = self.head;
            }
            self.max_height.store(height, Ordering::Release);
        }
        let new_node = Node::new(key, value, height, &self.arena);
        unsafe {
            for i in 0..height {
                (*new_node).set_next(i, (*(prev[i])).get_next(i));
                (*(prev[i])).set_next(i, new_node);
            }
        }
    }

    /// Find the nearest node with a key >= the given key.
    /// Add prev node into `prev_nodes`
    /// which can be helpful for adding new node to the skiplist.
    pub fn find_greater_or_equal(&self, key: &Slice, prev_nodes: &mut [*mut Node]) -> *mut Node {
        let mut level = self.max_height.load(Ordering::Acquire);
        let mut node = self.head;
        loop {
            unsafe {
                let next = (*node).get_next(level);
                if self.key_is_less_than(key, next) {
                    // given key < next key
                    // record the prev node
                    prev_nodes[level - 1] = node;
                    // already arrived the bottom return the current node
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
        let arena = &self.arena;
        loop {
            unsafe {
                let next = (*node).get_next(level);
                if next.is_null()
                    || self.comparator.compare(&((*next).key(arena)), key) != CmpOrdering::Less
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
    pub(super) fn key_is_less_than(&self, key: &Slice, n: *mut Node) -> bool {
        if n.is_null() {
            // take nullptr as +infinite large
            true
        } else {
            let node_key = unsafe { (*n).key(&self.arena) };
            match self.comparator.compare(key, &node_key) {
                CmpOrdering::Less => true,
                _ => false,
            }
        }
    }
}

/// Generate a random height < MAX_HEIGHT for node
pub fn rand_height() -> usize {
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
    use crate::util::comparator::BytewiseComparator;
    use std::ptr;
    use std::rc::Rc;

    fn new_test_skl() -> Skiplist<AggressiveArena> {
        Skiplist::new(64 << 20, Rc::new(BytewiseComparator::new()))
    }

    fn construct_skl_from_nodes(
        mut nodes: Vec<(Slice, Slice, usize)>,
    ) -> Skiplist<AggressiveArena> {
        if nodes.is_empty() {
            return new_test_skl();
        }
        let skl = new_test_skl();
        // just use MAX_HEIGHT as capacity because it's the largest value that node.height can have
        let mut prev_nodes = vec![skl.head; MAX_HEIGHT];
        let mut max_height = 1;
        for (key, value, height) in nodes.drain(..) {
            let n = Node::new(&key, &value, height, &skl.arena);
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
    fn test_key_is_less_than() {
        let skl = new_test_skl();
        let vec = vec![1u8, 2u8, 3u8];
        let key = Slice::from(vec.as_slice());
        // return false if node is nullptr
        assert_eq!(false, skl.key_is_less_than(&key, ptr::null_mut()));

        let n = Node::new(
            &Slice::from(vec![1u8, 2u8].as_slice()),
            &Slice::from(""),
            1,
            &skl.arena,
        );
        assert_eq!(false, skl.key_is_less_than(&key, n));

        let n2 = Node::new(
            &Slice::from(vec![1u8, 2u8, 4u8].as_slice()),
            &Slice::from(""),
            1,
            &skl.arena,
        );
        assert_eq!(true, skl.key_is_less_than(&key, n2));
    }

    #[test]
    fn test_find_greater_or_equal() {
        let value = Slice::from("");
        let inputs = vec![
            ("key1", "", 5),
            ("key3", "", 1),
            ("key5", "", 2),
            ("key7", "", 4),
            ("key9", "", 3),
        ];
        let nodes: Vec<(Slice, Slice, usize)> = inputs
            .iter()
            .map(|(key, val, height)| (Slice::from(*key), Slice::from(*val), *height))
            .collect();
        let skl = construct_skl_from_nodes(nodes);
        let mut prev_nodes = vec![ptr::null_mut(); 5];
        let target_key = Slice::from("key4");
        let res = skl.find_greater_or_equal(&target_key, &mut prev_nodes);
        unsafe {
            assert_eq!((*res).key(&skl.arena).as_str(), "key5");
            // prev_nodes should be correct
            assert_eq!((*(prev_nodes[0])).key(&skl.arena).as_str(), "key3");
            for node in prev_nodes[1..5].iter() {
                assert_eq!((**node).key(&skl.arena).as_str(), "key1");
            }
        }
    }

    #[test]
    fn test_find_less_than() {
        let value = Slice::from("");
        let inputs = vec![
            ("key1", "", 5),
            ("key3", "", 1),
            ("key5", "", 2),
            ("key7", "", 4),
            ("key9", "", 3),
        ];
        let nodes: Vec<(Slice, Slice, usize)> = inputs
            .iter()
            .map(|(key, val, height)| (Slice::from(*key), Slice::from(*val), *height))
            .collect();
        let skl = construct_skl_from_nodes(nodes);
        let target_key = Slice::from("key4");
        let res = skl.find_less_than(&target_key);
        unsafe {
            assert_eq!((*res).key(&skl.arena).as_str(), "key3");
        }
    }

    #[test]
    fn test_find_last() {
        let value = Slice::from("");
        let inputs = vec![
            ("key1", "", 5),
            ("key3", "", 1),
            ("key5", "", 2),
            ("key7", "", 4),
            ("key9", "", 3),
        ];
        let nodes: Vec<(Slice, Slice, usize)> = inputs
            .iter()
            .map(|(key, val, height)| (Slice::from(*key), Slice::from(*val), *height))
            .collect();
        let skl = construct_skl_from_nodes(nodes);
        let last = skl.find_last();
        unsafe {
            assert_eq!((*last).key(&skl.arena).as_str(), "key9");
        }
    }

    #[test]
    fn test_basic() {}
}
