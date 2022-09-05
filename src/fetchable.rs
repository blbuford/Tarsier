use crate::fetchable::Fetchable::{Fetched, Unfetched};

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
    pub fn unwrap_unfetched(&self) -> &usize {
        match self {
            Unfetched(x) => x,
            Fetched(_) => panic!(),
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

    pub fn as_mut(&mut self) -> Fetchable<&mut T> {
        match *self {
            Fetched(ref mut x) => Fetched(x),
            Unfetched(p) => Unfetched(p),
        }
    }
    pub fn is_fetched(&self) -> bool {
        if let Fetched(_) = self {
            return true;
        }
        return false;
    }
}
