use crate::mem::arena::Arena;
use crate::util::slice::Slice;
use crate::Comparator;
use rand::random;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const K_MAX_POSSIBLE_HEIGHT: u16 = 32;

#[derive(Debug)]
#[repr(C)]
struct Node {
    key: Slice,
    height: usize,
    next_nodes: [AtomicPtr<Node>; 0],
}

impl Node {
    fn allocate_node<A: Arena>(key_size: usize, height: usize, arena: A) -> *const Self {
        let prefix = mem::size_of::<AtomicPtr<Self>>() * (height - 1);

        let raw = arena.allocate_aligned(prefix + mem::size_of::<Self>() + key_size) as *mut Self;
        unsafe {
            ptr::write(&mut (*raw).height, height);
            ptr::write_bytes(&mut (*raw).next_nodes, 0, height);
            raw as *const Self
        }
    }

    // fn allocate_key(key_size: usize) -> *const u8 {
    //     allocate
    // }
}

pub struct InlineSkipList<C: Comparator, A: Arena> {
    k_max_height: u16,
    k_branching: u16,
    k_scaled_inverse_branching: u32,

    // Allocator used for allocations of nodes
    pub(super) arena: A,
    // Immutable after construction
    compare: C,

    head: *const Node,
    max_height: AtomicUsize,
}

impl<C, A> InlineSkipList<C, A>
where
    C: Comparator,
    A: Arena,
{
    #[inline]
    fn get_max_height(&self) -> usize {
        self.max_height.load(Ordering::Relaxed)
    }

    fn random_height(&self) -> usize {
        let mut height = 1;
        while height < self.k_max_height
            && height < K_MAX_POSSIBLE_HEIGHT
            && random::<u32>() < self.k_scaled_inverse_branching
        {
            height += 1;
        }
        height as usize
    }
}
