use std::cell::{Ref, RefCell, RefMut};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Bound, RangeBounds};
use std::rc::Rc;

use crate::fetchable::Fetchable;
use crate::fetchable::Fetchable::{Fetched, Unfetched};
use crate::node::Node;
use crate::Row;

#[derive(Debug, Clone)]
pub enum NodeType<K: Ord + Clone, V> {
    Internal(BTreeSet<Child<K>>),
    // Leaf Children (BTreeMap) and Next Leaf Node
    Leaf(BTreeMap<K, V>, Rc<RefCell<Fetchable<Node<K, V>>>>),
}

/// Child struct to represent internal node keys, and nodes to their left/right
/// Left/right are Option<T> to indicate whether they have been fetched or not. It is assumed that they exist
#[derive(Debug, Clone)]
pub struct Child<K: Ord + PartialEq + Eq> {
    key: K,
    left: RefCell<Fetchable<Node<usize, Row>>>,
    right: RefCell<Fetchable<Node<usize, Row>>>,
}

impl<K: Ord + PartialEq + Eq> Child<K> {
    pub fn new(key: K, left_page: usize, right_page: usize) -> Self {
        Self {
            key,
            left: RefCell::new(Unfetched(left_page)),
            right: RefCell::new(Unfetched(right_page)),
        }
    }

    pub fn left(&self) -> Ref<Fetchable<Node<usize, Row>>> {
        self.left.borrow()
    }

    pub fn left_mut(&self) -> RefMut<Fetchable<Node<usize, Row>>> {
        self.left.borrow_mut()
    }
    pub fn set_left(&self, n: Node<usize, Row>) {
        self.left.replace(Fetched(n));
    }
    pub fn right(&self) -> Ref<Fetchable<Node<usize, Row>>> {
        self.right.borrow()
    }
    pub fn right_mut(&self) -> RefMut<Fetchable<Node<usize, Row>>> {
        self.right.borrow_mut()
    }
    pub fn set_right(&self, n: Node<usize, Row>) {
        self.right.replace(Fetched(n));
    }
    pub fn key(&self) -> &K {
        &self.key
    }
}

impl<K: Ord + PartialEq + Eq> Eq for Child<K> {}

impl<K: Ord + PartialEq + Eq> PartialEq<Self> for Child<K> {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K: Ord + PartialEq + Eq> PartialOrd<Self> for Child<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<K: Ord + PartialEq + Eq> Ord for Child<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<K: Ord + PartialEq + Eq + RangeBounds<K>> RangeBounds<K> for Child<K> {
    fn start_bound(&self) -> Bound<&K> {
        self.key.start_bound()
    }

    fn end_bound(&self) -> Bound<&K> {
        self.key.end_bound()
    }

    fn contains<U>(&self, item: &U) -> bool
    where
        K: PartialOrd<U>,
        U: ?Sized + PartialOrd<K>,
    {
        self.key.contains(item)
    }
}
