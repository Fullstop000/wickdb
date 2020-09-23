use crate::mem::arena::Arena;
use crate::Comparator;
use rand::random;
use std::mem;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const MAX_HEIGHT: usize = 20;
const HEIGHT_INCREASE: u32 = u32::MAX / 3;

#[derive(Debug)]
#[repr(C)]
struct Node {
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
    fn get_next(&self, height: usize) -> Option<NonNull<Node>> {
        unsafe {
            let node = self
                .next_nodes
                .get_unchecked(height)
                .load(Ordering::Acquire);
            if node.is_null() {
                None
            } else {
                NonNull::new(node)
            }
        }
    }

    #[inline]
    fn key(&self) -> &[u8] {
        self.key.as_slice()
    }
}

pub struct InlineSkipList<C: Comparator, A: Arena> {
    // Current height. 1 <= height <= kMaxHeight. CAS.
    height: AtomicUsize,

    head: Option<NonNull<Node>>,

    pub(super) arena: A,

    comparator: C,
}

impl<C, A> InlineSkipList<C, A>
where
    C: Comparator,
    A: Arena,
{
    pub fn new(comparator: C, arena: A) -> Self {
        let head = Node::new(vec![0u8; 0].as_slice(), MAX_HEIGHT, &arena);
        Self {
            height: AtomicUsize::new(1),
            head: NonNull::new(head),
            arena,
            comparator,
        }
    }

    pub fn insert(&self, key: &[u8]) {
        let height = self.get_height();
        let mut prev: Vec<Option<NonNull<Node>>> = vec![None; MAX_HEIGHT + 1];
        let mut next: Vec<Option<NonNull<Node>>> = vec![None; MAX_HEIGHT + 1];
        prev[height] = self.head;
        for i in (0..height).rev() {
            // Use higher level to speed up for current level.
            let (prev, next) = self.find_splice_for_level(key, prev[i + 1], i);
            if prev.is_some() && next.is_some() && prev.unwrap() == next.unwrap() {
                // key is duplicated, do nothing.
                return;
            }
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
    }

    fn find_splice_for_level(
        &self,
        key: &[u8],
        before: Option<NonNull<Node>>,
        height: usize,
    ) -> (Option<NonNull<Node>>, Option<NonNull<Node>>) {
        if let Some(before) = before {
            let mut before = before;
            loop {
                unsafe {
                    let next = (*before.as_ptr()).get_next(height);

                    if let Some(next) = next {
                        let next_key = (*next.as_ptr()).key();
                        match self.comparator.compare(key, next_key) {
                            std::cmp::Ordering::Equal => return (Some(next), Some(next)),
                            std::cmp::Ordering::Less => return (Some(before), Some(next)),
                            std::cmp::Ordering::Greater => {
                                before = next;
                            }
                        }
                    } else {
                        return (Some(before), None);
                    }
                }
            }
        } else {
            (None, None)
        }
    }

    fn get_height(&self) -> usize {
        self.height.load(Ordering::Relaxed)
    }
}

fn random_height() -> usize {
    let mut height = 1;
    while height < MAX_HEIGHT && random::<u32>() < HEIGHT_INCREASE {
        height += 1;
    }
    height
}
