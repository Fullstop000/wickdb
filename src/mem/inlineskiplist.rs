use crate::mem::arena::Arena;
use crate::Comparator;
use rand::random;
use std::cmp::Ordering as CmpOrdering;
use std::mem;
use std::ptr;
use std::ptr::{null, null_mut};
use std::rc::Rc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

trait Iterator {
    fn valid(&self) -> bool;
    fn key(&self) -> &[u8];
    fn next(&mut self);
    fn prev(&mut self);
    fn seek(&mut self, target: &[u8]);
    fn seek_for_prev(&mut self, target: &[u8]);
    fn seek_to_first(&mut self);
    fn seek_to_last(&mut self);
}

const MAX_HEIGHT: usize = 20;
const HEIGHT_INCREASE: u32 = u32::MAX / 3;

#[derive(Debug)]
#[repr(C)]
pub struct Node {
    key: Vec<u8>,
    height: usize,
    // The actual size will vary depending on the height that a node
    // was allocated with.
    next_nodes: [AtomicPtr<Node>; 0],
}

impl Node {
    fn new<A: Arena>(key: &[u8], height: usize, arena: &A) -> *mut Self {
        let pointers_size = height * mem::size_of::<AtomicPtr<Self>>();
        let size = mem::size_of::<Self>() + pointers_size;

        let p = arena.allocate_aligned(size) as *mut Self;
        unsafe {
            ptr::write(&mut (*p).key, key.to_vec());
            ptr::write(&mut (*p).height, height);
            ptr::write_bytes(&mut (*p).next_nodes, 0, height);
            p as *mut Self
        }
    }

    #[inline]
    // height, 0-based index
    fn get_next(&self, height: usize) -> *mut Node {
        unsafe {
            self.next_nodes
                .get_unchecked(height)
                .load(Ordering::Acquire)
        }
    }

    #[inline]
    // height, 0-based index
    unsafe fn set_next(&self, height: usize, node: *mut Node) {
        self.next_nodes
            .get_unchecked(height)
            .store(node, Ordering::Release);
    }

    #[inline]
    fn key(&self) -> &[u8] {
        self.key.as_slice()
    }
}

pub struct InlineSkipList<C: Comparator, A: Arena> {
    // Current height. 1 <= height <= kMaxHeight. CAS.
    height: AtomicUsize,

    head: *mut Node,

    pub(super) arena: A,

    comparator: C,
}

impl<C, A> InlineSkipList<C, A>
where
    C: Comparator,
    A: Arena,
{
    pub fn new(comparator: C, arena: A) -> Self {
        let head = Node::new(Vec::new().as_slice(), MAX_HEIGHT, &arena);
        Self {
            height: AtomicUsize::new(1),
            head,
            arena,
            comparator,
        }
    }

    // findNear finds the node near to key.
    // If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
    // node.key <= key (if allowEqual=true).
    // If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
    // node.key >= key (if allowEqual=true).
    // Returns the node found. The bool returned is true if the node has key equal to given key.
    pub fn find_near(&self, key: &[u8], less: bool, allow_equal: bool) -> (*mut Node, bool) {
        let mut x = self.head;
        let mut height = self.get_height() - 1;
        loop {
            unsafe {
                // Assume x.key < key
                let next = (*x).get_next(height);
                if next.is_null() {
                    // x.key < key < END OF LIST
                    if height > 0 {
                        height -= 1;
                        continue;
                    }
                    // height = 0
                    if !less {
                        return (null_mut(), false);
                    }
                    // Try to return x. Make sure it is not a head node.
                    if x == self.head {
                        return (null_mut(), false);
                    }
                    return (x, false);
                }
                let node_key = (*x).key();
                match self.comparator.compare(key, node_key) {
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
                        if height > 0 {
                            height -= 1;
                            continue;
                        }
                        // On base level. Return x.
                        // Try to return x. Make sure it is not a head node.
                        if x == self.head {
                            return (null_mut(), false);
                        }
                        return (x, false);
                    }
                    CmpOrdering::Less => {
                        // x.key < key < next.key.
                        if height > 0 {
                            height -= 1;
                            continue;
                        }
                        // At base level. Need to return something.
                        if !less {
                            return (next, false);
                        }
                        if x == self.head {
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
        prev[height] = self.head;
        for i in (0..height).rev() {
            // Use higher level to speed up for current level.
            let (p, n) = self.find_splice_for_level(key, prev[i + 1], i);
            prev[i] = p;
            next[i] = n;

            assert_ne!(prev[i], next[i]);
        }
        let height = random_height();
        let x = Node::new(key, height, &self.arena);

        let list_height = self.get_height();
        while height > list_height {
            // TODO(accelsao): SeqCst or others?
            self.height
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
                }
                unsafe {
                    (*x).set_next(i, next[i]);
                    // TODO: Relaxed?
                    let old =
                        (*prev[i]).next_nodes[i].compare_and_swap(next[i], x, Ordering::Relaxed);
                    if old == x {
                        break;
                    }
                }
                // CAS Failed.
                let (p, n) = self.find_splice_for_level(key, prev[i], i);
                prev[i] = p;
                next[i] = n;
            }
        }
    }

    fn empty() -> bool {
        unimplemented!()
    }

    fn find_last() {
        unimplemented!()
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
        self.height.load(Ordering::Relaxed)
    }

    // Return whether the give key is less than the given node's key.
    fn key_is_less_than_or_equal(&self, key: &[u8], n: *mut Node) -> bool {
        if n.is_null() {
            // take nullptr as +infinite large
            true
        } else {
            let node_key = unsafe { (*n).key() };
            match self.comparator.compare(key, node_key) {
                CmpOrdering::Greater => false,
                _ => true,
            }
        }
    }
}

struct InlineSkiplistIterator<C, A>
where
    C: Comparator,
    A: Arena,
{
    list: Rc<InlineSkipList<C, A>>,
    node: *const Node,
}

impl<C, A> Iterator for InlineSkiplistIterator<C, A>
where
    C: Comparator,
    A: Arena,
{
    fn valid(&self) -> bool {
        !self.node.is_null()
    }

    fn key(&self) -> &[u8] {
        unsafe { (*self.node).key() }
    }

    fn next(&mut self) {
        assert!(self.valid());
        unsafe {
            self.node = (*self.node).get_next(0);
        }
    }

    fn prev(&mut self) {
        unimplemented!()
    }

    fn seek(&mut self, target: &[u8]) {
        unimplemented!()
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        unimplemented!()
    }

    fn seek_to_first(&mut self) {
        unimplemented!()
    }

    fn seek_to_last(&mut self) {
        unimplemented!()
    }
}

impl<C, A> InlineSkiplistIterator<C, A>
where
    C: Comparator,
    A: Arena,
{
    fn new(list: Rc<InlineSkipList<C, A>>) -> Self {
        Self { list, node: null() }
    }
}

fn random_height() -> usize {
    let mut height = 1;
    while height < MAX_HEIGHT && random::<u32>() < HEIGHT_INCREASE {
        height += 1;
    }
    height
}
