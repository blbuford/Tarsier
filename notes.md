# Problems
- We're not reaaally paging that well with this implementation (or at all). The actual code splits too much between `btree` and `node`.
- The idea of linkage between elements in the tree is not well-defined. In some places, its `Unfetched(usize)` and others are `Rc<RefCell<..>>`
- Cursors are miserable right now. We need to be able to fetch pages in one shot without traversing the tree foreach element.