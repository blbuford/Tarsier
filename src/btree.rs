use crate::btree::Fetchable::{Fetched, Unfetched};
use crate::cursor::Cursor;
use crate::pager::Pager;
use crate::Row;
use std::cell::{Ref, RefCell, RefMut};
use std::cmp::Ordering;
use std::collections::{BTreeSet, Bound};
use std::ops::RangeBounds;

pub const NODE_SIZE: usize = 4096;
pub const NODE_TYPE_OFFSET: usize = 0;
pub const IS_ROOT_OFFSET: usize = 1;
pub const PARENT_OFFSET: usize = 2;
pub const NUM_CELLS_OFFSET: usize = 6;
pub const CELL_KEY_SIZE: usize = 4;
pub const CELL_VALUE_SIZE: usize = 291;
pub const CELL_OFFSET: usize = 10;
pub const CELL_SIZE: usize = CELL_VALUE_SIZE + CELL_KEY_SIZE;

#[derive(Debug, Clone)]
pub struct KeyValuePair<K, V> {
    pub key: K,
    pub value: V,
}

#[derive(Debug, Clone)]
pub enum Fetchable<T> {
    Fetched(T),
    Unfetched(usize),
}

impl<T> Fetchable<T> {
    pub fn map<U, F>(self, f: F) -> Fetchable<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Fetched(x) => Fetched(f(x)),
            Unfetched(x) => Unfetched(x),
        }
    }

    pub fn unwrap(self) -> T {
        match self {
            Fetched(x) => x,
            Unfetched(_) => panic!("called `Fetchable::unwrap()` on an `Unfetched` value"),
        }
    }
    pub fn unwrap_or(self, default: T) -> T {
        match self {
            Fetched(x) => x,
            Unfetched(_) => default,
        }
    }

    pub fn unwrap_with_or<U, F>(self, f: F, default: U) -> U
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Fetched(x) => f(x),
            Unfetched(_) => default,
        }
    }

    pub fn as_ref(&self) -> Fetchable<&T> {
        match *self {
            Fetched(ref x) => Fetched(x),
            Unfetched(p) => Unfetched(p),
        }
    }
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

#[derive(Debug, Clone)]
pub enum NodeType<K: Ord, V> {
    Internal(BTreeSet<Child<K>>),
    Leaf(Vec<KeyValuePair<K, V>>),
}

pub struct BTree {
    root: RefCell<Node<usize, Row>>,
    pager: Pager,
}

impl BTree {
    pub fn new(mut pager: Pager) -> Self {
        let root = if pager.num_pages() == 0 {
            let mut root_node = pager.get_page(0);
            root_node.is_root = true;
            pager.commit_page(&root_node);
            root_node
        } else {
            pager.get_page(0)
        };

        Self {
            root: RefCell::new(root),
            pager,
        }
    }

    pub fn get(&self, page_num: usize, cell_num: usize) -> Row {
        let node = self.pager.get_page(page_num);
        node.get(cell_num).as_ref().unwrap().value.clone()
    }

    pub fn insert(&mut self, cursor: &Cursor, value: Row) -> bool {
        let mut node = self.pager.get_page(cursor.page_num());
        // TODO: Check this node's parent offset for an internal node. If exists, split all of its children to maintain B+-Tree height property
        return if node.num_cells >= 12 {
            let mut new_node = node.split(self.pager.num_pages());
            let mut new_parent: Node<usize, Row> = Node::internal();
            new_parent.page_num = self.pager.num_pages() + 1;
            std::mem::swap(&mut node.page_num, &mut new_parent.page_num);
            std::mem::swap(&mut node.is_root, &mut new_parent.is_root);
            node.parent_offset = Some(new_parent.page_num);
            new_node.parent_offset = Some(new_parent.page_num);
            let lower_largest_key = node.largest_key().unwrap().clone();
            let child_record =
                Child::new(lower_largest_key.clone(), node.page_num, new_node.page_num);

            match value.id.cmp(&(lower_largest_key as u32)) {
                Ordering::Less => node.insert(0, value.id as usize, value),
                Ordering::Greater => new_node.insert(0, value.id as usize, value),
                _ => panic!(),
            };

            self.pager.commit_page(&node);
            child_record.set_left(node);
            self.pager.commit_page(&new_node);
            child_record.set_right(new_node);

            if !new_parent.insert_internal_child(child_record) {
                panic!()
            }
            self.pager.commit_page(&new_parent);

            if new_parent.is_root {
                self.root.replace(new_parent);
            }

            true
        } else {
            if node.insert(cursor.cell_num(), value.id as usize, value) {
                self.pager.commit_page(&node);
                if node.is_root {
                    self.root.replace(node);
                }
                true
            } else {
                false
            }
        };
    }

    pub fn root(&self) -> Ref<Node<usize, Row>> {
        self.root.borrow()
    }

    pub fn close(&mut self) {
        self.pager.close()
    }

    pub fn find(&self, k: usize) -> Result<Cursor, Cursor> {
        self._find(k, self.root.borrow())
    }
    fn _find(&self, k: usize, node: Ref<Node<usize, Row>>) -> Result<Cursor, Cursor> {
        if let NodeType::Internal(ref children) = node.node_type {
            let child = match children.range(..=Child::new(k, 0, 0)).next_back() {
                Some(c) => Some(c),
                None => children.iter().next(),
            }
            .unwrap();
            let mut uf = usize::MAX;
            let n = match k.cmp(child.key()) {
                Ordering::Greater => {
                    if let Unfetched(page_num) = *child.right() {
                        uf = page_num;
                    }
                    if uf != usize::MAX {
                        child.set_right(self.pager.get_page(uf))
                    }
                    child.right()
                }
                _ => {
                    if let Unfetched(page_num) = *child.left() {
                        uf = page_num;
                    }
                    if uf != usize::MAX {
                        child.set_left(self.pager.get_page(uf))
                    }
                    child.left()
                }
            };

            self._find(k, Ref::map(n, |f| f.as_ref().unwrap()))
        } else {
            node.find(k)
                .map(|(page_num, cell_num)| {
                    Cursor::new(
                        page_num,
                        cell_num,
                        (self.pager.num_pages() - 1) == page_num && cell_num >= 12,
                    )
                })
                .map_err(|(page_num, insert_cell_num)| {
                    Cursor::new(
                        page_num,
                        insert_cell_num,
                        (self.pager.num_pages() - 1) == page_num && insert_cell_num >= 12,
                    )
                })
        }
    }
}

#[derive(Debug, Clone)]
pub struct Node<K: Ord, V> {
    pub(crate) is_root: bool,
    pub(crate) node_type: NodeType<K, V>,
    pub(crate) parent_offset: Option<usize>,
    pub(crate) num_cells: usize,
    pub(crate) page_num: usize,
}

impl<K: Ord, V> Node<K, V> {
    pub fn leaf() -> Self {
        Self {
            is_root: false,
            node_type: NodeType::Leaf(Vec::new()),
            parent_offset: None,
            num_cells: 0,
            page_num: 0,
        }
    }

    pub fn leaf_with_children(children: impl Iterator<Item = KeyValuePair<K, V>>) -> Self {
        let children = Vec::from_iter(children);
        let num_cells = children.len();
        Self {
            is_root: false,
            node_type: NodeType::Leaf(children),
            parent_offset: None,
            num_cells,
            page_num: 0,
        }
    }

    pub fn internal() -> Self {
        Self {
            is_root: false,
            node_type: NodeType::Internal(BTreeSet::new()),
            parent_offset: None,
            num_cells: 0,
            page_num: 0,
        }
    }

    pub fn get(&self, cell_num: usize) -> Option<&KeyValuePair<K, V>> {
        if let NodeType::Leaf(ref cells) = self.node_type {
            return cells.get(cell_num);
        }
        None
    }

    pub fn insert(&mut self, cell_num: usize, key: K, value: V) -> bool {
        match self.node_type {
            NodeType::Leaf(ref mut cells) => {
                if cells.len() >= 12 {
                    return false;
                }
                cells.insert(cell_num, KeyValuePair { key, value });
                self.num_cells += 1;
                return true;
            }
            _ => panic!(),
        }
    }

    pub fn find(&self, k: K) -> Result<(usize, usize), (usize, usize)> {
        match self.node_type {
            NodeType::Leaf(ref cells) => {
                // if self.num_cells >= 12 {
                //     return Err((usize::MAX, 0));
                // }
                cells
                    .binary_search_by(|kv| kv.key.cmp(&k))
                    .map(|ok| (self.page_num, ok))
                    .map_err(|err| (self.page_num, err))
            }
            NodeType::Internal(_) => {
                panic!()
            }
        }
    }

    pub fn split(&mut self, new_page_num: usize) -> Node<K, V> {
        if let NodeType::Leaf(ref mut cells) = self.node_type {
            let upper = cells.split_off(cells.len() / 2);
            let mut new_node = Node::leaf_with_children(upper.into_iter());
            new_node.page_num = new_page_num;
            self.num_cells = cells.len();
            return new_node;
        } else {
            panic!()
        }
    }

    pub fn largest_key(&self) -> Option<&K> {
        if let NodeType::Leaf(ref cells) = self.node_type {
            cells.last().map(|kvp| &kvp.key)
        } else {
            None
        }
    }

    pub fn insert_internal_child(&mut self, record: Child<K>) -> bool {
        if let NodeType::Internal(ref mut children) = self.node_type {
            return children.insert(record);
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use crate::btree::Child;
    use std::collections::BTreeSet;

    #[test]
    fn child_tree_shananigans() {
        let mut bt = BTreeSet::new();
        bt.insert(Child::new(4 as usize, 1, 2));
        bt.insert(Child::new(9 as usize, 2, 3));
        let smallest = match bt.range(..=Child::new(5, 0, 0)).next_back() {
            Some(c) => Some(c),
            None => bt.iter().next(),
        };

        assert!(smallest.is_some());
        assert_eq!(*smallest.unwrap().key(), 4);

        let largest = match bt.range(..=Child::new(10, 0, 0)).next_back() {
            Some(c) => Some(c),
            None => bt.iter().next(),
        };

        assert!(largest.is_some());
        assert_eq!(*largest.unwrap().key(), 9);

        let equiv = match bt.range(..=Child::new(9, 0, 0)).next_back() {
            Some(c) => Some(c),
            None => bt.iter().next(),
        };
        assert!(equiv.is_some());
        assert_eq!(*equiv.unwrap().key(), 9);
    }
}
