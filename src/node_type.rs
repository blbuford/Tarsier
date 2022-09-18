use std::fmt::Debug;

use crate::pager::Offset;

#[derive(Debug, Clone)]
pub enum NodeType<K, V> {
    Internal(InternalNode<K>),
    Leaf(LeafNode<K, V>),
}

#[derive(Debug, Clone)]
pub struct InternalNode<K> {
    pub(crate) separators: Vec<K>,
    pub(crate) children: Vec<Offset>,
}

impl<K> InternalNode<K> {
    pub fn new() -> Self {
        Self {
            separators: Vec::new(),
            children: Vec::new(),
        }
    }

    pub fn new_with(separators: Vec<K>, children: Vec<Offset>) -> Self {
        Self {
            separators,
            children,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LeafNode<K, V> {
    pub(crate) children: Vec<KeyValuePair<K, V>>,
    pub(crate) last_leaf: Option<Offset>,
    pub(crate) next_leaf: Option<Offset>,
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
        last_leaf: Option<Offset>,
        next_leaf: Option<Offset>,
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

    pub fn internal_with_separators(separators: Vec<K>, children: Vec<Offset>) -> Self {
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
