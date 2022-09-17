use crate::btree::NodeLink;
use std::cell::{Ref, RefCell, RefMut};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::rc::Rc;

use crate::fetchable::Fetchable;
use crate::fetchable::Fetchable::{Fetched, Unfetched};
use crate::node::Node;
use crate::Row;

#[derive(Debug, Clone)]
pub enum NodeType<K, V> {
    Internal(InternalNode<K, V>),
    Leaf(LeafNode<K, V>),
}

#[derive(Debug, Clone)]
pub struct InternalNode<K, V> {
    pub(crate) separators: Vec<K>,
    pub(crate) children: Vec<NodeLink<K, V>>,
}

impl<K, V> InternalNode<K, V> {
    pub fn new() -> Self {
        Self {
            separators: Vec::new(),
            children: Vec::new(),
        }
    }

    pub fn new_with(separators: Vec<K>, children: Vec<NodeLink<K, V>>) -> Self {
        Self {
            separators,
            children,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LeafNode<K, V> {
    pub(crate) children: Vec<KeyValuePair<K, V>>,
    pub(crate) last_leaf: Option<NodeLink<K, V>>,
    pub(crate) next_leaf: Option<NodeLink<K, V>>,
}

impl<K, V> LeafNode<K, V> {
    pub fn new() -> Self {
        Self {
            children: Vec::new(),
            last_leaf: None,
            next_leaf: None,
        }
    }

    pub fn new_with(
        children: Vec<KeyValuePair<K, V>>,
        last_leaf: Option<NodeLink<K, V>>,
        next_leaf: Option<NodeLink<K, V>>,
    ) -> Self {
        Self {
            children,
            last_leaf,
            next_leaf,
        }
    }
}

impl<K: Ord + Clone, V> NodeType<K, V> {
    pub fn internal_new() -> Self {
        Self::Internal(InternalNode::new())
    }

    pub fn internal_with_separators(separators: Vec<K>, children: Vec<NodeLink<K, V>>) -> Self {
        Self::Internal(InternalNode::new_with(separators, children))
    }

    pub fn leaf_new() -> Self {
        Self::Leaf(LeafNode::new())
    }

    pub fn leaf_with_children(children: Vec<KeyValuePair<K, V>>) -> Self
    where
        V: Debug,
    {
        Self::Leaf(LeafNode::new_with(children, None, None))
    }
}

#[derive(Debug, Clone)]
pub struct KeyValuePair<K, V> {
    pub key: K,
    pub value: V,
}
