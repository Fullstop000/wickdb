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

use crate::util::comparator::Comparator;
use crate::util::slice::Slice;
use crate::util::status::{Result, WickErr};
use std::cmp::Ordering;
use std::mem;

/// A common trait for iterating all the key/value entries.
// TODO: use Relative Type or Generics instead of explicitly using Slice as the type of key and value
pub trait Iterator {
    /// An iterator is either positioned at a key/value pair, or
    /// not valid.  This method returns true iff the iterator is valid.
    fn valid(&self) -> bool;

    /// Position at the first key in the source.  The iterator is Valid()
    /// after this call iff the source is not empty.
    fn seek_to_first(&mut self);

    /// Position at the last key in the source.  The iterator is
    /// Valid() after this call iff the source is not empty.
    fn seek_to_last(&mut self);

    /// Position at the first key in the source that is at or past target.
    /// The iterator is valid after this call iff the source contains
    /// an entry that comes at or past target.
    fn seek(&mut self, target: &Slice);

    /// Moves to the next entry in the source.  After this call, the iterator is
    /// valid iff the iterator was not positioned at the last entry in the source.
    /// REQUIRES: `valid()`
    fn next(&mut self);

    /// Moves to the previous entry in the source.  After this call, the iterator
    /// is valid iff the iterator was not positioned at the first entry in source.
    /// REQUIRES: `valid()`
    fn prev(&mut self);

    /// Return the key for the current entry.  The underlying storage for
    /// the returned slice is valid only until the next modification of
    /// the iterator.
    /// REQUIRES: `valid()`
    fn key(&self) -> Slice;

    /// Return the value for the current entry.  The underlying storage for
    /// the returned slice is valid only until the next modification of
    /// the iterator.
    /// REQUIRES: `valid()`
    fn value(&self) -> Slice;

    /// If an error has occurred, return it.  Else return an ok status.
    fn status(&mut self) -> Result<()>;
}

/// An special iterator calls all `tasks` before dropping
pub struct IterWithCleanup<I: Iterator> {
    inner_iter: Option<I>,
    // Tasks to be executed when self is dropped
    tasks: Vec<Box<dyn FnMut()>>,
    err: Option<WickErr>,
}

impl<I: Iterator> IterWithCleanup<I> {
    pub fn new(iter: I) -> Self {
        Self {
            inner_iter: Some(iter),
            tasks: vec![],
            err: None,
        }
    }

    pub fn new_with_err(err: WickErr) -> Self {
        Self {
            inner_iter: None,
            tasks: vec![],
            err: Some(err),
        }
    }

    pub fn register_task(&mut self, task: Box<dyn FnMut()>) {
        self.tasks.push(task)
    }
}

impl<I: Iterator> Drop for IterWithCleanup<I> {
    fn drop(&mut self) {
        for mut t in self.tasks.drain(..) {
            t()
        }
    }
}

impl<I: Iterator> Iterator for IterWithCleanup<I> {
    fn valid(&self) -> bool {
        self.err.is_none() && self.inner_iter.as_ref().map_or(true, |i| i.valid())
    }

    fn seek_to_first(&mut self) {
        if let Some(iter) = self.inner_iter.as_mut() {
            iter.seek_to_first()
        }
    }

    fn seek_to_last(&mut self) {
        if let Some(iter) = self.inner_iter.as_mut() {
            iter.seek_to_last()
        }
    }

    fn seek(&mut self, target: &Slice) {
        if let Some(iter) = self.inner_iter.as_mut() {
            iter.seek(target)
        }
    }

    fn next(&mut self) {
        if let Some(iter) = self.inner_iter.as_mut() {
            iter.next()
        }
    }

    fn prev(&mut self) {
        if let Some(iter) = self.inner_iter.as_mut() {
            iter.prev()
        }
    }

    fn key(&self) -> Slice {
        self.inner_iter
            .as_ref()
            .map_or(Slice::default(), |i| i.key())
    }

    fn value(&self) -> Slice {
        self.inner_iter
            .as_ref()
            .map_or(Slice::default(), |i| i.value())
    }

    fn status(&mut self) -> Result<()> {
        self.inner_iter.as_mut().map_or(Ok(()), |i| i.status())
    }
}

/// A plain iterator used as default
///
/// # Notice
///
/// The `valid()` is always `false`
pub struct EmptyIterator {
    err: Option<WickErr>,
}

impl EmptyIterator {
    #[inline]
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self { err: None }
    }
}

impl Iterator for EmptyIterator {
    fn valid(&self) -> bool {
        false
    }

    fn seek_to_first(&mut self) {}

    fn seek_to_last(&mut self) {}

    fn seek(&mut self, _target: &Slice) {}

    fn next(&mut self) {}

    fn prev(&mut self) {}

    fn key(&self) -> Slice {
        Slice::default()
    }

    fn value(&self) -> Slice {
        Slice::default()
    }

    fn status(&mut self) -> Result<()> {
        match self.err.take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// A concatenated iterator contains an original iterator `origin` and a `DerivedIterFactory`.
/// New derived iterator is generated by `factory(origin.value())`.
/// The origin Iterator should yield out the last key but not the first.
/// This is just like a bucket iterator with lazy generator.
pub struct ConcatenateIterator<I: Iterator, F: DerivedIterFactory> {
    origin: I,
    factory: F,
    derived: Option<F::Iter>,
    prev_derived_value: Vec<u8>,
    err: Option<WickErr>,
}

/// A factory that takes value from the origin and
pub trait DerivedIterFactory {
    type Iter: Iterator;
    /// Create a new `Iterator` based on value yield by original `Iterator`
    fn derive(&self, value: &Slice) -> Result<Self::Iter>;
}

impl<I: Iterator, F: DerivedIterFactory> ConcatenateIterator<I, F> {
    pub fn new(origin: I, factory: F) -> Self {
        Self {
            origin,
            factory,
            derived: None,
            prev_derived_value: vec![],
            err: None,
        }
    }

    #[inline]
    fn maybe_save_err(old: &mut Option<WickErr>, new: Result<()>) {
        if old.is_none() {
            if let Err(e) = new {
                mem::replace::<Option<WickErr>>(old, Some(e));
            }
        }
    }

    // Create a derived iter from the current value of the origin iter.
    // Only works when current derived iter is `None` or the previous origin value has been changed.
    // Same as `InitDataBlock` in C++ implementation
    fn init_derived_iter(&mut self) {
        if !self.origin.valid() {
            self.derived = None
        } else {
            let v = self.origin.value();
            if self.derived.is_none()
                || v.compare(&Slice::from(self.prev_derived_value.as_slice())) != Ordering::Equal
            {
                match self.factory.derive(&v) {
                    Ok(derived) => {
                        if derived.valid() {
                            self.prev_derived_value = Vec::from(v.as_slice());
                        }
                        self.set_derived(Some(derived))
                    }
                    Err(e) => Self::maybe_save_err(&mut self.err, Err(e)),
                }
            }
        }
    }

    // Same as `SetDataIterator` in C++ implementation
    #[inline]
    fn set_derived(&mut self, iter: Option<F::Iter>) {
        if let Some(iter) = &mut self.derived {
            Self::maybe_save_err(&mut self.err, iter.status())
        }
        self.derived = iter
    }

    // Skip invalid results util finding a valid derived iter by `next()`
    // If found, set derived iter to the first
    fn skip_forward(&mut self) {
        while self.derived.is_none() || !self.derived.as_ref().unwrap().valid() {
            if !self.origin.valid() {
                self.set_derived(None);
                break;
            } else {
                self.origin.next();
                self.init_derived_iter();
                if let Some(i) = &mut self.derived {
                    // init to the first
                    i.seek_to_first();
                }
            }
        }
    }

    // Skip invalid results util finding a valid derived iter by `prev()`
    // If found, set derived iter to the last
    fn skip_backward(&mut self) {
        while self.derived.is_none() || !self.derived.as_ref().unwrap().valid() {
            if !self.origin.valid() {
                self.set_derived(None);
                break;
            } else {
                self.origin.prev();
                self.init_derived_iter();
                if let Some(i) = &mut self.derived {
                    // init to the last
                    i.seek_to_last();
                }
            }
        }
    }

    #[inline]
    fn valid_or_panic(&self) {
        assert!(
            self.valid(),
            "[concatenated iterator] invalid derived iterator"
        )
    }
}

impl<I: Iterator, F: DerivedIterFactory> Iterator for ConcatenateIterator<I, F> {
    fn valid(&self) -> bool {
        if let Some(di) = &self.derived {
            di.valid()
        } else {
            false
        }
    }

    fn seek_to_first(&mut self) {
        self.origin.seek_to_first();
        self.init_derived_iter();
        if let Some(di) = self.derived.as_mut() {
            di.seek_to_first()
        }
        // scan forward util finding the first valid entry
        self.skip_forward();
    }

    fn seek_to_last(&mut self) {
        self.origin.seek_to_last();
        self.init_derived_iter();
        if let Some(di) = self.derived.as_mut() {
            di.seek_to_last()
        }
        // scan backward util finding the first valid entry
        self.skip_backward();
    }

    fn seek(&mut self, target: &Slice) {
        self.origin.seek(target);
        self.init_derived_iter();
        if let Some(di) = self.derived.as_mut() {
            di.seek(target)
        }
        self.skip_forward();
    }

    fn next(&mut self) {
        self.valid_or_panic();
        self.derived.as_mut().map_or((), |di| di.next());
        self.skip_forward();
    }

    fn prev(&mut self) {
        self.valid_or_panic();
        self.derived.as_mut().map_or((), |di| di.prev());
        self.skip_backward();
    }

    fn key(&self) -> Slice {
        self.valid_or_panic();
        self.derived
            .as_ref()
            .map_or(Slice::default(), |di| di.key())
    }

    fn value(&self) -> Slice {
        self.valid_or_panic();
        self.derived
            .as_ref()
            .map_or(Slice::default(), |di| di.value())
    }

    fn status(&mut self) -> Result<()> {
        self.origin.status()?;
        if let Some(di) = self.derived.as_mut() {
            di.status()?
        };
        if let Some(e) = self.err.take() {
            return Err(e);
        }
        Ok(())
    }
}

#[derive(Eq, PartialEq)]
pub enum IterDirection {
    Forward,
    Reverse,
}

/// Return an iterator that provided the union of the data in
/// `children[0..n-1]` with the correct order.
/// This iterator performs just like a `merge sort` to its children.
/// The result does no duplicate suppression.  I.e., if a particular
/// key is present in K child iterators, it will be yielded K times.
pub struct MergingIterator<C: Comparator> {
    cmp: C,
    direction: IterDirection,
    children: Vec<Box<dyn Iterator>>,
    current_index: usize, // index in 'children' of current iterator
}

impl<C: Comparator> MergingIterator<C> {
    pub fn new(cmp: C, children: Vec<Box<dyn Iterator>>) -> Self {
        let len = children.len();
        Self {
            cmp,
            direction: IterDirection::Forward,
            children,
            current_index: len,
        }
    }

    fn valid_or_panic(&self) {
        assert!(self.current_index < self.children.len());
    }

    // Find the iterator with the smallest 'key' and set it as current
    fn find_smallest(&mut self) {
        let mut smallest: Option<Slice> = None;
        let mut index = self.current_index;
        for (i, child) in self.children.iter().enumerate() {
            if child.valid()
                && (smallest.is_none()
                    || self.cmp.compare(
                        child.key().as_slice(),
                        smallest.as_ref().unwrap().as_slice(),
                    ) == Ordering::Less)
            {
                smallest = Some(child.key());
                index = i
            }
        }
        self.current_index = index;
    }

    // Find the iterator with the largest 'key' and set it as current
    fn find_largest(&mut self) {
        let mut largest: Option<Slice> = None;
        let mut index = self.current_index;
        for (i, child) in self.children.iter().enumerate() {
            if child.valid()
                && (largest.is_none()
                    || self
                        .cmp
                        .compare(child.key().as_slice(), largest.as_ref().unwrap().as_slice())
                        == Ordering::Greater)
            {
                largest = Some(child.key());
                index = i
            }
        }
        self.current_index = index;
    }

    fn get_current(&self) -> &dyn Iterator {
        self.children.get(self.current_index).unwrap().as_ref()
    }

    fn get_current_mut(&mut self) -> &mut dyn Iterator {
        self.children.get_mut(self.current_index).unwrap().as_mut()
    }
}

impl<C: Comparator> Iterator for MergingIterator<C> {
    fn valid(&self) -> bool {
        let i = self.current_index;
        if i < self.children.len() {
            self.get_current().valid()
        } else {
            false
        }
    }

    fn seek_to_first(&mut self) {
        for child in self.children.iter_mut() {
            child.seek_to_first()
        }
        self.find_smallest();
        self.direction = IterDirection::Forward;
    }

    fn seek_to_last(&mut self) {
        for child in self.children.iter_mut() {
            child.seek_to_last()
        }
        self.find_largest();
        self.direction = IterDirection::Reverse;
    }

    fn seek(&mut self, target: &Slice) {
        for child in self.children.iter_mut() {
            child.seek(target)
        }
        self.find_smallest();
        self.direction = IterDirection::Forward;
    }

    fn next(&mut self) {
        self.valid_or_panic();
        if self.direction != IterDirection::Forward {
            let key = self.key();
            for (i, child) in self.children.iter_mut().enumerate() {
                if i != self.current_index {
                    child.seek(&key);
                    if child.valid()
                        && self.cmp.compare(key.as_slice(), child.key().as_slice())
                            == Ordering::Equal
                    {
                        child.next();
                    }
                }
            }
            self.direction = IterDirection::Forward;
        }
        self.get_current_mut().next();
        self.find_smallest();
    }

    fn prev(&mut self) {
        self.valid_or_panic();
        if self.direction != IterDirection::Reverse {
            let key = self.key();
            for (i, child) in self.children.iter_mut().enumerate() {
                if i != self.current_index {
                    child.seek(&key);
                    if child.valid() {
                        child.prev();
                    } else {
                        // Child has no key >= current key so point to the last
                        child.seek_to_last();
                    }
                }
            }
            self.direction = IterDirection::Reverse;
        }
        self.get_current_mut().prev();
        self.find_largest();
    }

    fn key(&self) -> Slice {
        self.valid_or_panic();
        self.get_current().key()
    }

    fn value(&self) -> Slice {
        self.valid_or_panic();
        self.get_current().value()
    }

    fn status(&mut self) -> Result<()> {
        for child in self.children.iter_mut() {
            let status = child.status();
            if status.is_err() {
                return status;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::iterator::*;
    use crate::rand::Rng;
    use crate::util::comparator::BytewiseComparator;
    use crate::util::slice::Slice;
    use crate::util::status::Result;
    use std::cell::RefCell;
    use std::cmp::Ordering;
    use std::mem;
    use std::rc::Rc;

    struct TestCleanup {
        results: Vec<usize>,
    }

    #[test]
    fn test_iter_with_cleanup() {
        let test_cleaned_up = Rc::new(RefCell::new(TestCleanup { results: vec![] }));

        let mut iter = IterWithCleanup::new(EmptyIterator::new());
        for i in 0..100 {
            let cloned = test_cleaned_up.clone();
            iter.register_task(Box::new(move || cloned.borrow_mut().results.push(i)));
        }
        mem::drop(iter);
        assert_eq!(100, test_cleaned_up.borrow().results.len());
        for i in 0..100 {
            assert_eq!(i, test_cleaned_up.borrow().results[i]);
        }
    }

    // Divide given ordered `src` into `n` lists and then construct a `MergingIterator` with them
    fn new_test_merging_iter(
        mut src: Vec<String>,
        n: usize,
    ) -> MergingIterator<BytewiseComparator> {
        let mut children = vec![];
        for _ in 0..n {
            children.push(vec![]);
        }
        src.sort();
        let mut rnd = rand::thread_rng();
        for v in src {
            let i = rnd.gen_range(0, n);
            let child = children.get_mut(i).unwrap();
            child.push(v);
        }
        let cmp = BytewiseComparator::default();
        let iters = children
            .drain(..)
            .map(|mut child| {
                child.sort();
                TestSimpleArrayIter::box_new(child)
            })
            .collect::<Vec<_>>();
        MergingIterator::new(cmp, iters)
    }

    struct SortedIterTestSuite<O: Iterator, S: Iterator> {
        origin: O, // A sorted array based iterator
        shadow: S, // The iterator to be tested
    }

    impl<O: Iterator, S: Iterator> SortedIterTestSuite<O, S> {
        fn new(origin: O, shadow: S) -> Self {
            Self { origin, shadow }
        }

        #[inline]
        fn assert_valid(&self, expect: bool) {
            assert_eq!(self.origin.valid(), expect);
            assert_eq!(self.origin.valid(), self.shadow.valid());
        }

        #[inline]
        fn assert_key_and_value(&self) {
            assert_eq!(self.origin.key(), self.shadow.key());
            assert_eq!(self.origin.value(), self.shadow.value());
        }
    }

    impl<O: Iterator, S: Iterator> Iterator for SortedIterTestSuite<O, S> {
        fn valid(&self) -> bool {
            self.origin.valid() && self.shadow.valid()
        }

        fn seek_to_first(&mut self) {
            self.origin.seek_to_first();
            self.shadow.seek_to_first();
        }
        fn seek_to_last(&mut self) {
            self.origin.seek_to_last();
            self.shadow.seek_to_last();
        }

        fn seek(&mut self, target: &Slice) {
            self.origin.seek(target);
            self.shadow.seek(target);
        }

        fn next(&mut self) {
            self.origin.next();
            self.shadow.next();
        }

        fn prev(&mut self) {
            self.origin.prev();
            self.shadow.prev();
        }

        fn key(&self) -> Slice {
            unimplemented!()
        }

        fn value(&self) -> Slice {
            unimplemented!()
        }

        fn status(&mut self) -> Result<()> {
            unimplemented!()
        }
    }

    #[derive(Debug)]
    struct TestSimpleArrayIter {
        inner: Vec<String>,
        current: usize,
    }

    impl TestSimpleArrayIter {
        fn new(inner: Vec<String>) -> Self {
            let current = inner.len();
            Self { inner, current }
        }

        fn box_new(inner: Vec<String>) -> Box<dyn Iterator> {
            Box::new(Self::new(inner))
        }

        fn valid_or_panic(&self) {
            if !self.valid() {
                panic!("Invalid iterator {:?}", &self)
            }
        }
    }

    impl Iterator for TestSimpleArrayIter {
        fn valid(&self) -> bool {
            self.current < self.inner.len() && self.inner.len() > 0
        }
        fn seek_to_first(&mut self) {
            self.current = 0;
        }
        fn seek_to_last(&mut self) {
            if self.inner.len() > 0 {
                self.current = self.inner.len() - 1
            }
        }

        fn seek(&mut self, target: &Slice) {
            let mut current = self.inner.len() + 1;
            for (i, s) in self.inner.iter().enumerate() {
                match s.as_bytes().cmp(target.as_slice()) {
                    Ordering::Equal | Ordering::Greater => {
                        current = i;
                        break;
                    }
                    _ => continue,
                }
            }
            self.current = current;
        }

        fn next(&mut self) {
            self.valid_or_panic();
            self.current += 1;
        }

        fn prev(&mut self) {
            self.valid_or_panic();
            if self.current > 0 {
                self.current -= 1
            } else {
                // marked as invalid
                self.current = self.inner.len()
            }
        }

        fn key(&self) -> Slice {
            self.valid_or_panic();
            Slice::from(self.inner[self.current].as_str())
        }

        fn value(&self) -> Slice {
            self.key()
        }

        fn status(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct SimpleDeriveFactory {}
    impl SimpleDeriveFactory {
        fn new() -> Self {
            Self {}
        }
    }

    impl DerivedIterFactory for SimpleDeriveFactory {
        type Iter = TestSimpleArrayIter;
        fn derive(&self, value: &Slice) -> Result<Self::Iter> {
            let c = value.as_str().chars().nth(0).unwrap().to_string();
            let inner = vec![c.clone(), c.as_str().repeat(2), c.as_str().repeat(3)];
            Ok(TestSimpleArrayIter::new(inner))
        }
    }

    #[test]
    fn test_concatenated_iterator() {
        // inner: [a, aa, aaa, b, bb, bbb, c, cc, ccc]
        let mut iter = ConcatenateIterator::new(
            TestSimpleArrayIter::new(vec!["aaa".to_owned(), "bbb".to_owned(), "ccc".to_owned()]),
            SimpleDeriveFactory::new(),
        );

        assert!(!iter.valid());
        iter.seek_to_first();
        assert_eq!(iter.key().as_str(), "a");
        assert_eq!(iter.value().as_str(), "a");

        iter.next();
        assert_eq!(iter.key().as_str(), "aa");

        iter.seek_to_last();
        assert_eq!(iter.key().as_str(), "ccc");

        iter.prev();
        assert_eq!(iter.key().as_str(), "cc");

        iter.seek_to_first();
        iter.seek(&Slice::from("b"));
        assert_eq!(iter.key().as_str(), "b");

        iter.seek(&Slice::from("bb"));
        assert_eq!(iter.key().as_str(), "bb");

        iter.seek(&Slice::from("bbbb"));
        assert_eq!(iter.key().as_str(), "c");
        // Test seeking out of range
        iter.seek(&Slice::from("1"));
        assert_eq!(iter.key().as_str(), "a");
        iter.seek(&Slice::from("d"));
        assert!(!iter.valid());
    }

    #[test]
    fn test_merging_iterator() {
        let mut input = vec![];
        for i in 1..100 {
            input.push(i.to_string());
        }
        input.sort();
        let mut tests = vec![1, 5, 10, 50];
        for t in tests.drain(..) {
            let merging_iter = new_test_merging_iter(input.clone(), t);
            let origin = TestSimpleArrayIter::new(input.clone());
            let mut suite = SortedIterTestSuite::new(origin, merging_iter);
            suite.assert_valid(false);
            suite.seek_to_first();
            suite.assert_key_and_value();
            suite.seek_to_last();
            suite.assert_key_and_value();
            suite.seek(&Slice::from("3"));
            suite.assert_key_and_value();
            suite.prev();
            suite.assert_key_and_value();
            suite.next();
            suite.assert_key_and_value();
            suite.seek(&Slice::from("0"));
            suite.assert_key_and_value();
            suite.seek(&Slice::from("9999"));
            suite.assert_valid(false);
        }
    }
}
