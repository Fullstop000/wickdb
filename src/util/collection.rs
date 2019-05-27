use std::cell::RefCell;
use std::rc::{Rc, Weak};

pub type NodePtr<T> = Rc<RefCell<Node<T>>>;
#[derive(Debug)]
pub struct Node<T> {
    pub data: T,
    pub prev: Option<Weak<RefCell<Node<T>>>>,
    pub next: Option<Rc<RefCell<Node<T>>>>,
}
impl<T> Node<T> {
    pub fn new(data: T) -> Self {
        Self {
            data,
            prev: None,
            next: None,
        }
    }

    /// Iff returns false, we need call `remove_head()` on the DList where the node
    /// belongs
    pub fn try_unlink(&mut self) -> bool {
        match &self.prev {
            None => {
                match &self.next {
                    // consider as true for an isolate Node
                    None => true,
                    Some(next) => {
                        next.borrow_mut().prev = None;
                        false // unable to drop node because it's hold by the list as head
                    }
                }
            }
            Some(prev) => {
                if let Some(rc_ptr) = prev.upgrade() {
                    // prev.next = self.next
                    rc_ptr.borrow_mut().next = self.next.clone();
                }
                match &self.next {
                    // is the tail
                    None => true,
                    Some(next) => {
                        // next.prev = self.prev
                        next.borrow_mut().prev = Some(prev.clone());
                        true
                    }
                }
            }
        }
    }

    #[inline]
    pub fn value(&self) -> &T {
        &self.data
    }

    #[inline]
    pub fn is_isolated(&self) -> bool {
        self.next.is_none() && self.prev.is_none()
    }
}

/// A simple implementation of DoubleLinkedList using safe Rust
/// Notice: this is not thread safe
pub struct DoubleLinkedList<T> {
    head: Option<Rc<RefCell<Node<T>>>>,
}

impl<T> DoubleLinkedList<T> {
    pub fn new() -> Self {
        Self { head: None }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.is_some()
    }

    // Append a new node to the head
    pub fn append(&mut self, data: T) -> Rc<RefCell<Node<T>>> {
        let mut n = Node::new(data);
        n.next = self.head.clone();
        n.prev = {
            match &self.head {
                Some(head) => head.borrow().prev.clone(),
                None => None,
            }
        };
        let ptr = Rc::new(RefCell::new(n));
        let weak_ptr = Rc::downgrade(&ptr.clone());
        if let Some(head) = &self.head {
            head.borrow_mut().prev = Some(weak_ptr)
        }
        self.head = Some(ptr.clone());
        ptr
    }

    /// Returns the head
    #[inline]
    pub fn front(&self) -> Option<Rc<RefCell<Node<T>>>> {
        self.head.clone()
    }

    /// Pops the head
    #[inline]
    pub fn pop_front(&mut self) {
        match &self.head {
            None => {}
            Some(head) => {
                let next = head.borrow_mut().next.clone();
                head.borrow_mut().next = None;
                self.head = next
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::util::collection::{DoubleLinkedList, Node};
    use std::rc::Rc;

    fn list_len<T>(list: &DoubleLinkedList<T>) -> usize {
        let mut len = 0;
        let mut current = list.head.clone();
        loop {
            match current {
                Some(ptr) => {
                    len += 1;
                    current = ptr.borrow().next.clone();
                }
                None => break,
            }
        }
        len
    }

    #[test]
    fn test_append() {
        let mut list = DoubleLinkedList::<usize>::new();
        let n1 = list.append(0);
        list.append(1);
        list.append(2);
        let n4 = list.append(3);
        assert_eq!(4, list_len(&list));

        // head.prev is None
        assert!(n4.borrow().prev.is_none());
        // tail.next is None
        assert!(n1.borrow().next.is_none());

        let mut values = vec![];
        let mut current = list.head;
        // head to tail run
        while current.is_some() {
            let inner = current.unwrap();
            values.push(inner.borrow().value().clone());
            current = inner.borrow().next.clone();
        }
        assert_eq!(vec![3, 2, 1, 0], values);

        // tail to head run
        values.clear();
        // we push n1 first because `other_current` should be a Weak but n1 is not
        values.push(n1.borrow().value().clone());
        let mut other_current = n1.borrow().prev.clone();
        while other_current.is_some() {
            let inner = other_current.unwrap().upgrade().unwrap();
            values.push(inner.borrow().value().clone());
            other_current = inner.borrow().prev.clone();
        }
        assert_eq!(vec![0, 1, 2, 3], values);
    }

    #[test]
    fn test_try_unlink() {
        // isolated node
        let mut n = Node::new(0);
        assert!(n.try_unlink());

        let mut list = DoubleLinkedList::<usize>::new();
        let n1 = list.append(0);
        let n2 = list.append(1);
        let n3 = list.append(2);
        let n4 = list.append(3);
        // unlink the tail
        assert!(n1.borrow_mut().try_unlink());
        assert_eq!(3, list_len(&list));
        assert!(n2.borrow().next.is_none());

        // unlink a middle node
        assert!(n3.borrow_mut().try_unlink());
        assert_eq!(2, list_len(&list));
        // prev should be updated
        assert_eq!(
            3,
            *(n2.borrow()
                .prev
                .clone()
                .expect("")
                .upgrade()
                .expect("")
                .borrow()
                .value())
        );
        // next should be undated
        assert_eq!(1, *(n4.borrow().next.clone().expect("").borrow().value()));

        // unlink a head node
        assert!(!n4.borrow_mut().try_unlink());
        list.pop_front();
        assert!(n4.borrow().is_isolated());
        assert_eq!(1, Rc::strong_count(&n4));
        assert_eq!(list.front().unwrap().borrow_mut().value(), &1);
    }
}
