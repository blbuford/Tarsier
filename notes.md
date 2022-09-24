# Problems
- We're not reaaally paging that well with this implementation (or at all). The actual code splits too much between `btree` and `node`.
- The idea of linkage between elements in the tree is not well-defined. In some places, its `Unfetched(usize)` and others are `Rc<RefCell<..>>`
- Cursors are miserable right now. We need to be able to fetch pages in one shot without traversing the tree foreach element.


# Pager API
`BTree` creates new nodes as it sees fit then `commit()`s them to the pager

Theoretically, The pager could keep a list of dirty nodes based on get_mut calls and periodically
fsync them 

| method   | purpose                                               |
|----------|-------------------------------------------------------|
| commit   | adds a an owned `Node` and `Offset` pair to the cache |
| new_page | returns an `Offset` that a new node can be paged to   |
| get      | returns a `&Node` to access data from                 |
| get_mut  | returns a `&mut Node` to mutate                       |
|