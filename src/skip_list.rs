use std::{borrow::Borrow, ptr::NonNull};

use rand::Rng;
use std::fmt::Debug;

const MAX_LEVELS: usize = 32;

#[derive(Debug)]
pub struct SkipList<T> {
    head: Link<T>,
    max_level: usize,
    len: usize,
}

type Link<T> = NonNull<Node<T>>;

#[derive(Clone, Debug)]
struct Node<T> {
    value: Option<T>,
    forward: Vec<Option<Link<T>>>,
}

pub struct IntoIter<T> {
    list: SkipList<T>,
}

pub struct Iter<'a, T> {
    next: Option<&'a Node<T>>,
}

pub struct IterMut<'a, T> {
    next: Option<&'a mut Node<T>>,
}

impl<T> SkipList<T> {
    pub fn new() -> Self {
        let max_level = 4;
        let head = unsafe {
            NonNull::new_unchecked(Box::into_raw(Box::new(Node {
                value: None,
                forward: vec![None; max_level],
            })))
        };
        Self {
            head,
            max_level,
            len: 0,
        }
    }

    fn add_a_level(&mut self) {
        if self.max_level < MAX_LEVELS {
            unsafe {
                (*self.head.as_ptr()).forward.push(None);
                let mut current = self.forward(self.head)[0];
                while let Some(node) = current {
                    (*node.as_ptr()).forward.push(None);
                    current = (&(*node.as_ptr())).forward[0];
                }
            }
            self.max_level += 1;
        }
    }

    fn pop_first(&mut self) -> Option<T> {
        unsafe {
            self.forward(self.head)[0].and_then(|node| {
                let boxed_node = Box::from_raw(node.as_ptr());
                for i in 0..self.max_level {
                    if self.forward(self.head)[i] == Some(node) {
                        (&mut (*self.head.as_ptr())).forward[i] = boxed_node.forward[i];
                    }
                }
                self.len -= 1;
                boxed_node.value
            })
        }
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            next: unsafe {
                let first_real_node = (&*self.head.as_ptr()).forward[0];
                first_real_node.map(|node| node.as_ref())
            }
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut {
            next: unsafe {
                let first_real_node = (&*self.head.as_ptr()).forward[0];
                first_real_node.map(|mut node| node.as_mut())
            },
        }
    }

    fn value(&self, node: NonNull<Node<T>>) -> Option<&T> {
        unsafe { (*node.as_ptr()).value.as_ref() }
    }

    fn forward(&self, node: NonNull<Node<T>>) -> &Vec<Option<Link<T>>> {
        unsafe { (*node.as_ptr()).forward.as_ref() }
    }

    fn forward_mut(&self, link: &mut Link<T>) -> &mut [Option<Link<T>>] {
        unsafe { &mut (*link.as_ptr()).forward }
    }
}


impl<T: Ord> SkipList<T> {
    pub fn search(&self, target: &T) -> bool {
        let mut node = self.head;

        for i in (0..self.max_level).rev() {
            let mut next = self.forward(node)[i];
            while next.and_then(|n| self.value(n)).is_some_and(|v| v < target) {
                node = next.unwrap();
                next = self.forward(node)[i];
            }
        }

        let node = self.forward(node)[0];
        node.map(|n| self.value(n))
            .is_some_and(|v| v == Some(target))
    }

    pub fn insert(&mut self, to_insert: T) -> Option<&T> {
        if self.len >= 2usize.pow(self.max_level as u32 - 1) {
            self.add_a_level();
        }

        let mut update = vec![None; self.max_level];
        let mut node = self.head;

        for i in (0..self.max_level).rev() {
            let mut next = self.forward(node)[i];
            while next
                .and_then(|n| self.value(n))
                .is_some_and(|v| v < &to_insert)
            {
                node = next.unwrap();
                next = self.forward(node)[i];
            }
            update[i] = Some(node);
        }

        let node = self.forward(node)[0];
        if node
            .map(|n| self.value(n))
            .is_some_and(|v| v == Some(&to_insert))
        {
            return None;
        }

        let level = self.pick_a_level();
        let mut new_node = unsafe {
            NonNull::new_unchecked(Box::into_raw(Box::new(Node {
                value: Some(to_insert),
                forward: vec![None; self.max_level],
            })))
        };

        for i in 0..=level {
            if let Some(mut update_i) = update[i] {
                self.forward_mut(&mut new_node)[i] = self.forward(update_i)[i];
                self.forward_mut(&mut update_i)[i] = Some(new_node);
            } else {
                unsafe {
                    (&mut (*new_node.as_ptr())).forward[i] = self.forward(self.head)[i];
                    (&mut (*self.head.as_ptr())).forward[i] = Some(new_node);
                }
            }
        }
        self.len += 1;
        unsafe {
            new_node.as_ref().value.as_ref()
        }
    }

    pub fn delete<Q>(&mut self, to_delete: &Q)
    where T: Borrow<Q>,
    Q: Ord + ?Sized{
        let mut update = vec![self.head; self.max_level];
        let mut node = self.head;

        for i in (0..self.max_level).rev() {
            let mut next = self.forward(node)[i];
            while next
                .and_then(|n| self.value(n))
                .is_some_and(|v| v.borrow() < to_delete)
            {
                node = next.unwrap();
                next = self.forward(node)[i];
            }
            update[i] = node;
        }

        let node_to_delete = self.forward(node)[0];
        if node_to_delete
            .and_then(|n| self.value(n))
            .is_some_and(|v| v.borrow() == to_delete)
        {
            for i in 0..self.max_level {
                if self.forward(update[i])[i] == node_to_delete {
                    self.forward_mut(&mut update[i])[i] = self.forward(node_to_delete.unwrap())[i];
                } else {
                    break;
                }
            }
            unsafe {
                drop(Box::from_raw(node_to_delete.unwrap().as_ptr()));
            }
            self.len -= 1;
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl<T> Drop for SkipList<T> {
    fn drop(&mut self) {
        unsafe {
            let mut current = Some(self.head);
            while let Some(node) = current {
                let boxed_node = Box::from_raw(node.as_ptr());
                current = boxed_node.forward[0];
            }
        }
    }
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.list.pop_first()
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.next.and_then(|node| unsafe {
            self.next = node.forward[0].map(|n| &(*n.as_ptr()));
            node.value.as_ref()
        })
    }
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        self.next.take().and_then(|node| unsafe {
            self.next = node.forward[0].map(|n| &mut (*n.as_ptr()));
            node.value.as_mut()
        })
    }
}

impl<T> IntoIterator for SkipList<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter { list: self }
    }
}

impl<'a, T> IntoIterator for &'a SkipList<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut SkipList<T> {
    type Item = &'a mut T;
    type IntoIter = IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<T: Clone + Ord + Debug> Clone for SkipList<T> {
    fn clone(&self) -> Self {
        let mut res = SkipList::new();
        for v in self.iter() {
            res.insert(v.clone());
        }
        res
    }
}

unsafe impl<T: Send> Send for SkipList<T> {}
unsafe impl<T: Sync> Sync for SkipList<T> {}

unsafe impl<T: Send> Send for Iter<'_, T> {}
unsafe impl<T: Sync> Sync for Iter<'_, T> {}

unsafe impl<T: Send> Send for IterMut<'_, T> {}
unsafe impl<T: Sync> Sync for IterMut<'_, T> {}

fn flip_a_coin() -> bool {
    let mut rng = rand::rng();
    rng.random_range(0.0..1.0) < 0.5
}

impl<T> SkipList<T> {
    fn pick_a_level(&self) -> usize {
        let mut res = 0;
        while flip_a_coin() && res < self.max_level - 1 {
            res += 1;
        }
        res
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};

    use super::SkipList;

    #[test]
    fn test_basic() {
        let mut skip_list = SkipList::new();

        assert_eq!(skip_list.len(), 0);
        skip_list.insert(1);
        assert_eq!(skip_list.len(), 1);
        skip_list.insert(17);
        assert_eq!(skip_list.len(), 2);
        skip_list.insert(10);
        assert_eq!(skip_list.len(), 3);
        skip_list.insert(10);
        assert_eq!(skip_list.len(), 3);

        assert!(skip_list.search(&10));

        skip_list.delete(&10);
        assert_eq!(skip_list.len(), 2);

        assert!(!skip_list.search(&10));

        for i in 100..125 {
            skip_list.insert(i);
        }
    }

    #[test]
    fn test_into_iter() {
        let mut skip_list = SkipList::new();

        for i in 0..10 {
            skip_list.insert(i);
        }

        println!("Calling into_iter implicitly");
        let mut count = 0;
        for (index, value) in skip_list.into_iter().enumerate() {
            count += 1;
            assert!(index == value);
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_iter() {
        let mut skip_list = SkipList::new();

        for i in 0..10 {
            skip_list.insert(i);
        }

        println!("Calling iter implicitly");
        let mut count = 0;
        for (index, value) in skip_list.iter().enumerate() {
            count += 1;
            assert_eq!(index, *value);
            println!("Iter found {value}");
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_iter_mut() {
        let mut skip_list = SkipList::new();

        for i in 0..10 {
            skip_list.insert(i);
        }

        println!("Calling iter implicitly");
        let mut count = 0;
        for (index, value) in skip_list.iter_mut().enumerate() {
            *value += 1;
            count += 1;
            assert_eq!(index, *value - 1);
            println!("Iter found and incremented {value}");
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_double_drop_scenario() {
        // This test demonstrates potential double-drop issues
        let mut skip_list = SkipList::new();

        // Insert a value that will create a specific node structure
        skip_list.insert(42);

        // Create a scenario where the same node might be referenced
        // multiple times in the forward vectors due to the random level assignment
        for i in 0..100 {
            skip_list.insert(i);
        }

        // The Drop implementation iterates through level 0, but if there are
        // bugs in the level management, nodes might be dropped multiple times

        // Force drop
        drop(skip_list);

        // If there's a double-drop, this might cause a crash
        println!("Drop completed - if you see this, no immediate double-drop occurred");
    }

    #[test]
    fn test_memory_leak_detection() {
        // This test is designed to potentially reveal memory leaks
        // Run with a memory leak detector like Valgrind

        for iteration in 0..10 {
            let mut skip_list = SkipList::new();

            // Create a complex structure
            for i in 0..50 {
                skip_list.insert(i);
            }

            // Delete some elements
            for i in (0..25).step_by(2) {
                skip_list.delete(&i);
            }

            // Create and abandon iterators
            let _iter1 = skip_list.iter();
            let _iter2 = skip_list.iter_mut();

            // Let it drop
        }
    }

    #[test]
    fn test_concurrent_access() {
        let skip_list = Arc::new(Mutex::new(SkipList::new()));
        let handles = (0..10).map(|n| -> _ {
            let c = Arc::clone(&skip_list);
            std::thread::spawn(move || {
                println!("Inserting {n}");
                let mut lock = c.lock().unwrap();
                lock.insert(n);
            })
        });
        for handle in handles {
            handle.join().unwrap();
        }

        let mut lock = skip_list.lock().unwrap();
        println!("List las length {}", lock.len());
        for i in lock.iter_mut() {
            println!("{i}");
        }
    }
}
