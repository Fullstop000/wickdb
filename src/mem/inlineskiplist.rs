use crate::mem::arena::Arena;
use crate::Comparator;
use crate::{Iterator, Result};
use bytes::Bytes;
use rand::random;
use std::cmp::Ordering as CmpOrdering;
use std::mem;
use std::ptr;
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;

const MAX_HEIGHT: usize = 20;
const HEIGHT_INCREASE: u32 = u32::MAX / 3;

#[derive(Debug)]
#[repr(C)]
pub struct Node {
    key: Bytes,
    height: usize,
    // The actual size will vary depending on the height that a node
    // was allocated with.
    next_nodes: [AtomicPtr<Self>; MAX_HEIGHT],
}

impl Node {
    fn new<A: Arena>(key: Bytes, height: usize, arena: &A) -> *mut Self {
        let size =
            mem::size_of::<Self>() - (MAX_HEIGHT - height) * mem::size_of::<AtomicPtr<Self>>();
        let algin = mem::align_of::<Self>();
        let p = arena.allocate::<Node>(size, algin);
        unsafe {
            ptr::write(&mut (*p).key, key);
            ptr::write(&mut (*p).height, height);
            ptr::write_bytes(&mut (*p).next_nodes, 0, height);
            p
        }
    }

    #[inline]
    // height, 0-based index
    fn get_next(&self, height: usize) -> *mut Node {
        self.next_nodes.get(height).unwrap().load(Ordering::SeqCst)
    }

    #[inline]
    // height, 0-based index
    unsafe fn set_next(&self, height: usize, node: *mut Node) {
        self.next_nodes
            .get(height)
            .unwrap()
            .store(node, Ordering::SeqCst);
    }

    #[inline]
    fn key(&self) -> &[u8] {
        self.key.as_ref()
    }
}

struct InlineSkipListInner<A: Arena> {
    // Current height. 1 <= height <= kMaxHeight. CAS.
    height: AtomicUsize,
    // TODO: use NonNull instead?
    head: *mut Node,
    arena: A,
}

unsafe impl<A: Arena + Send> Send for InlineSkipListInner<A> {}
unsafe impl<A: Arena + Sync> Sync for InlineSkipListInner<A> {}

impl<A: Arena> Drop for InlineSkipListInner<A> {
    fn drop(&mut self) {
        let mut node = self.head;
        loop {
            let next = unsafe { (&*node).get_next(0) };
            if !next.is_null() {
                unsafe {
                    ptr::drop_in_place(next);
                }
                node = next;
                continue;
            }
            unsafe { ptr::drop_in_place(node) };
            return;
        }
    }
}

#[derive(Clone)]
pub struct InlineSkipList<C: Comparator, A: Arena + Clone + Send + Sync> {
    inner: Arc<InlineSkipListInner<A>>,
    comparator: C,
}

impl<C, A> InlineSkipList<C, A>
where
    C: Comparator,
    A: Arena + Clone + Send + Sync,
{
    pub fn new(comparator: C, arena: A) -> Self {
        let head = Node::new(Bytes::new(), MAX_HEIGHT, &arena);
        Self {
            inner: Arc::new(InlineSkipListInner {
                height: AtomicUsize::new(1),
                head,
                arena,
            }),
            comparator,
        }
    }

    // findNear finds the node near to key.
    // If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
    // node.key <= key (if allowEqual=true).
    // If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
    // node.key >= key (if allowEqual=true).
    // Returns the node found. The bool returned is true if the node has key equal to given key.
    fn find_near(&self, key: &[u8], less: bool, allow_equal: bool) -> (*mut Node, bool) {
        let mut x = self.inner.head;
        let mut level = self.get_height() - 1;
        loop {
            unsafe {
                // Assume x.key < key
                let next = (*x).get_next(level);
                if next.is_null() {
                    // x.key < key < END OF LIST
                    if level > 0 {
                        level -= 1;
                        continue;
                    }
                    // height = 0
                    if !less {
                        return (null_mut(), false);
                    }
                    // Try to return x. Make sure it is not a head node.
                    if x == self.inner.head {
                        return (null_mut(), false);
                    }
                    return (x, false);
                }
                let nextkey = (*next).key();
                match self.comparator.compare(key, nextkey) {
                    CmpOrdering::Greater => {
                        // x.key < next.key < key. We can continue to move right.
                        x = next;
                        continue;
                    }
                    CmpOrdering::Equal => {
                        // x.key < key == next.key.
                        if allow_equal {
                            return (next, true);
                        }
                        if !less {
                            // We want >, so go to base level to grab the next bigger note.
                            return ((*next).get_next(0), false);
                        }
                        // We want <. If not base level, we should go closer in the next level.
                        if level > 0 {
                            level -= 1;
                            continue;
                        }
                        // On base level. Return x.
                        // Try to return x. Make sure it is not a head node.
                        if x == self.inner.head {
                            return (null_mut(), false);
                        }
                        return (x, false);
                    }
                    CmpOrdering::Less => {
                        // x.key < key < next.key.
                        if level > 0 {
                            level -= 1;
                            continue;
                        }
                        // At base level. Need to return something.
                        if !less {
                            return (next, false);
                        }
                        if x == self.inner.head {
                            return (null_mut(), false);
                        }
                        return (x, false);
                    }
                }
            }
        }
    }

    pub fn put(&self, key: &[u8]) {
        let height = self.get_height();
        let mut prev = vec![null_mut(); MAX_HEIGHT + 1];
        let mut next = vec![null_mut(); MAX_HEIGHT + 1];
        prev[height] = self.inner.head;
        for i in (0..height).rev() {
            // Use higher level to speed up for current level.
            let (p, n) = self.find_splice_for_level(key, prev[i + 1], i);
            prev[i] = p;
            next[i] = n;
            assert_ne!(prev[i], next[i]);
        }
        let height = random_height();
        let x = Node::new(Bytes::copy_from_slice(key), height, &self.inner.arena);

        let list_height = self.get_height();
        while height > list_height {
            self.inner
                .height
                .compare_and_swap(list_height, height, Ordering::SeqCst);
            if self.get_height() == height {
                break;
            }
        }

        // We always insert from the base level and up. After you add a node in base level, we cannot
        // create a node in the level above because it would have discovered the node in the base level.
        for i in 0..height {
            loop {
                if prev[i].is_null() {
                    assert!(i > 1);
                    // We haven't computed prev, next for this level because height exceeds old listHeight.
                    // For these levels, we expect the lists to be sparse, so we can just search from head.
                    let (p, n) =
                        unsafe { self.find_splice_for_level(&(*x).key, self.inner.head, i) };
                    // Someone adds the exact same key before we are able to do so. This can only happen on
                    // the base level. But we know we are not on the base level.
                    prev[i] = p;
                    next[i] = n;
                    assert_ne!(p, n);
                }
                unsafe {
                    (*x).set_next(i, next[i]);
                    match &(*prev[i]).next_nodes[i].compare_exchange(
                        next[i],
                        x,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => break,
                        Err(_) => {
                            let (p, n) = self.find_splice_for_level(key, prev[i], i);
                            if p == n {
                                assert_eq!(i, 0, "Equality can happen only on base level");
                                return;
                            }
                            prev[i] = p;
                            next[i] = n;
                        }
                    }
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.find_last().is_null()
    }

    pub fn len(&self) -> usize {
        let mut node = self.inner.head;
        let mut count = 0;
        loop {
            let next = unsafe { (&*node).get_next(0) };
            if next != ptr::null_mut() {
                count += 1;
                node = next;
                continue;
            }
            return count;
        }
    }

    fn find_last(&self) -> *mut Node {
        let mut x = self.inner.head;
        let mut height = self.get_height() - 1;
        loop {
            unsafe {
                let next = (*x).get_next(height);
                if next.is_null() {
                    if height == 0 {
                        if x == self.inner.head {
                            return null_mut();
                        } else {
                            return x;
                        }
                    } else {
                        height -= 1;
                    }
                } else {
                    x = next;
                }
            }
        }
    }

    fn find_splice_for_level(
        &self,
        key: &[u8],
        before: *mut Node,
        height: usize,
    ) -> (*mut Node, *mut Node) {
        let mut before = before;
        loop {
            unsafe {
                let next = (*before).get_next(height);
                if next.is_null() {
                    return (before, null_mut());
                } else {
                    let next_key = (*next).key();
                    match self.comparator.compare(key, next_key) {
                        std::cmp::Ordering::Equal => return (next, next),
                        std::cmp::Ordering::Less => return (before, next),
                        std::cmp::Ordering::Greater => {
                            before = next;
                        }
                    }
                }
            }
        }
    }

    fn get_height(&self) -> usize {
        self.inner.height.load(Ordering::Relaxed)
    }
}

pub struct InlineSkiplistIterator<C, A>
where
    C: Comparator,
    A: Arena + Clone + Send + Sync,
{
    list: InlineSkipList<C, A>,
    node: *const Node,
}

impl<C, A> Iterator for InlineSkiplistIterator<C, A>
where
    C: Comparator,
    A: Arena + Clone + Send + Sync,
{
    #[inline]
    fn valid(&self) -> bool {
        !self.node.is_null()
    }

    fn key(&self) -> &[u8] {
        unsafe { (*self.node).key() }
    }

    fn value(&self) -> &[u8] {
        unimplemented!()
    }

    fn next(&mut self) {
        assert!(self.valid());
        unsafe {
            self.node = (*self.node).get_next(0);
        }
    }

    // find previous node
    fn prev(&mut self) {
        assert!(self.valid());
        let (node, _) = self.list.find_near(self.key(), true, false);
        self.node = node;
    }

    // find first node, key <= node.key
    fn seek(&mut self, key: &[u8]) {
        let (node, _) = self.list.find_near(key, false, true);
        self.node = node;
    }

    fn seek_to_first(&mut self) {
        unsafe {
            self.node = (*self.list.inner.head)
                .next_nodes
                .get_unchecked(0)
                .load(Ordering::Acquire)
        }
    }

    fn seek_to_last(&mut self) {
        self.node = self.list.find_last();
    }

    fn status(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<C, A> InlineSkiplistIterator<C, A>
where
    C: Comparator,
    A: Arena + Clone + Send + Sync,
{
    pub fn new(list: InlineSkipList<C, A>) -> Self {
        Self { list, node: null() }
    }

    // find last node, node.key <= key
    fn seek_for_prev(&mut self, key: &[u8]) {
        let (node, _) = self.list.find_near(key, true, true);
        self.node = node;
    }
}

fn random_height() -> usize {
    let mut height = 1;
    while height < MAX_HEIGHT && random::<u32>() < HEIGHT_INCREASE {
        height += 1;
    }
    height
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::arena::ArenaV2;
    use crate::BytewiseComparator;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    fn new_test_skl() -> InlineSkipList<BytewiseComparator, ArenaV2> {
        InlineSkipList::new(
            BytewiseComparator::default(),
            ArenaV2::with_capacity(1 << 20),
        )
    }

    #[test]
    fn test_find_near() {
        let cmp = BytewiseComparator::default();
        let arena = ArenaV2::with_capacity(1 << 20);
        let l = InlineSkipList::new(cmp, arena);
        for i in 0..1000 {
            let key = format!("{:05}{:08}", i * 10 + 5, 0);
            l.put(key.as_bytes());
        }
        let cases = vec![
            ("00001", false, false, Some("00005")),
            ("00001", false, true, Some("00005")),
            ("00001", true, false, None),
            ("00001", true, true, None),
            ("00005", false, false, Some("00015")),
            ("00005", false, true, Some("00005")),
            ("00005", true, false, None),
            ("00005", true, true, Some("00005")),
            ("05555", false, false, Some("05565")),
            ("05555", false, true, Some("05555")),
            ("05555", true, false, Some("05545")),
            ("05555", true, true, Some("05555")),
            ("05558", false, false, Some("05565")),
            ("05558", false, true, Some("05565")),
            ("05558", true, false, Some("05555")),
            ("05558", true, true, Some("05555")),
            ("09995", false, false, None),
            ("09995", false, true, Some("09995")),
            ("09995", true, false, Some("09985")),
            ("09995", true, true, Some("09995")),
            ("59995", false, false, None),
            ("59995", false, true, None),
            ("59995", true, false, Some("09995")),
            ("59995", true, true, Some("09995")),
        ];
        for (i, (key, less, allow_equal, exp)) in cases.into_iter().enumerate() {
            let seek_key = format!("{}{:08}", key, 0);
            let (res, found) = l.find_near(seek_key.as_bytes(), less, allow_equal);
            if exp.is_none() {
                assert!(!found, "{}", i);
                continue;
            }
            let e = format!("{}{:08}", exp.unwrap(), 0);
            assert_eq!(&unsafe { &*res }.key, e.as_bytes(), "{}", i);
        }
    }

    #[test]
    fn test_empty() {
        let key = b"aaa";
        let skl = new_test_skl();
        for less in &[false, true] {
            for allow_equal in &[false, true] {
                let (node, found) = skl.find_near(key, *less, *allow_equal);
                assert!(node.is_null());
                assert!(!found);
            }
        }
        let mut iter = InlineSkiplistIterator::new(skl.clone());
        assert!(!iter.valid());
        iter.seek_to_first();
        assert!(!iter.valid());
        iter.seek_to_last();
        assert!(!iter.valid());
        iter.seek(key);
        assert!(!iter.valid());
    }

    #[test]
    fn test_basic() {
        let c = BytewiseComparator::default();
        let arena = ArenaV2::with_capacity(1 << 20);
        let list = InlineSkipList::new(c, arena);
        let table = vec!["key1", "key2", "key3", "key4", "key5"];

        for key in table.clone() {
            list.put(key.as_bytes());
        }
        assert_eq!(list.len(), 5);
        assert!(!list.is_empty());
        let mut iter = InlineSkiplistIterator::new(list);
        for key in &table {
            iter.seek(key.as_bytes());
            assert_eq!(iter.key(), key.as_bytes());
        }
        for key in table.iter().rev() {
            assert_eq!(iter.key(), key.as_bytes());
            iter.prev();
        }
        assert!(!iter.valid());
        iter.seek_to_first();
        for key in table.iter() {
            assert_eq!(iter.key(), key.as_bytes());
            iter.next();
        }
        assert!(!iter.valid());
        iter.seek_to_first();
        assert_eq!(iter.key(), table.first().unwrap().as_bytes());
        iter.seek_to_last();
        assert_eq!(iter.key(), table.last().unwrap().as_bytes());
    }

    fn test_concurrent_basic(n: usize, cap: usize, key_len: usize) {
        let cmp = BytewiseComparator::default();
        let arena = ArenaV2::with_capacity(cap);
        let skl = InlineSkipList::new(cmp, arena);
        let keys: Vec<_> = (0..n)
            .map(|i| format!("{1:00$}", key_len, i).to_owned())
            .collect();
        let (tx, rx) = mpsc::channel();
        for key in keys.clone() {
            let tx = tx.clone();
            let l = skl.clone();
            thread::Builder::new()
                .name("write thread".to_owned())
                .spawn(move || {
                    println!("insert {:?}", &key[..10]);
                    l.put(key.as_bytes());
                    tx.send(()).unwrap();
                })
                .unwrap();
        }
        for _ in 0..n {
            rx.recv_timeout(Duration::from_secs(3)).unwrap();
        }
        for key in keys {
            let tx = tx.clone();
            let l = skl.clone();
            thread::Builder::new()
                .name("read thread".to_owned())
                .spawn(move || {
                    let mut iter = InlineSkiplistIterator::new(l);
                    iter.seek(key.as_bytes());
                    assert_eq!(iter.key(), key.as_bytes());
                    tx.send(()).unwrap();
                })
                .unwrap();
        }
        for _ in 0..n {
            rx.recv_timeout(Duration::from_secs(3)).unwrap();
        }
        assert_eq!(skl.len(), n);
    }

    #[test]
    fn test_concurrent_basic_small_value() {
        test_concurrent_basic(1000, 1 << 20, 5);
    }
    #[test]
    fn test_concurrent_basic_big_value() {
        test_concurrent_basic(1, 120 << 20, 1048576);
    }
}
