use std::{borrow::Borrow, collections::VecDeque, ptr::NonNull};

use rand::Rng;
use std::fmt::Debug;

const MAX_LEVELS: usize = 32;

pub struct SkipList<T> {
    head: Link<T>,
    tail: Link<T>,
    max_level: usize,
    len: usize,
}

type Link<T> = NonNull<Node<T>>;

#[derive(Clone, Debug)]
struct Node<T> {
    value: Option<T>,
    forward: Vec<(Link<T>, usize)>,
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
        let tail = unsafe {
            NonNull::new_unchecked(Box::into_raw(Box::new(Node {
                value: None,
                forward: vec![],
            })))
        };
        let head = unsafe {
            NonNull::new_unchecked(Box::into_raw(Box::new(Node {
                value: None,
                forward: vec![(tail, 1); max_level],
            })))
        };
        Self {
            head,
            tail,
            max_level,
            len: 0,
        }
    }

    fn add_a_level(&mut self) {
        unsafe {
            if self.max_level < MAX_LEVELS {
                (&mut (*self.head.as_ptr()))
                    .forward
                    .push((self.tail, self.len + 1));
                self.max_level += 1;
            }
        }
    }

    fn pop_first(&mut self) -> Option<T> {
        unsafe {
            let (first_real_node, _) = (&(*self.head.as_ptr())).forward[0];
            if (*first_real_node.as_ptr()).value.is_some() {
                let boxed_node = Box::from_raw(first_real_node.as_ptr());
                for i in 0..boxed_node.forward.len() {
                    (&mut (*self.head.as_ptr())).forward[i] = boxed_node.forward[i];
                }
                self.len -= 1;
                boxed_node.value
            } else {
                None
            }
        }
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            next: unsafe { Some((&(*self.head.as_ptr())).forward[0].0.as_ref()) },
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut {
            next: unsafe { Some((&mut (*self.head.as_ptr())).forward[0].0.as_mut()) },
        }
    }

    fn value(&self, node: NonNull<Node<T>>) -> Option<&T> {
        unsafe { (*node.as_ptr()).value.as_ref() }
    }

    fn forward(&self, node: NonNull<Node<T>>) -> &Vec<(Link<T>, usize)> {
        unsafe { (*node.as_ptr()).forward.as_ref() }
    }

    fn forward_mut(&self, link: &mut Link<T>) -> &mut [(Link<T>, usize)] {
        unsafe { &mut (*link.as_ptr()).forward }
    }
}

impl<T: Ord + Debug> SkipList<T> {
    pub fn search(&self, target: &T) -> Option<usize> {
        let mut node = self.head;
        let mut rank = 0;

        for i in (0..self.max_level).rev() {
            while let Some((next, span)) = self.forward(node).get(i) {
                if self.value(*next).is_some_and(|v| v < target) {
                    rank += span;
                    node = *next;
                } else {
                    break;
                }
            }
        }

        let (node, _) = self.forward(node)[0];
        if Some(target) == self.value(node) {
            Some(rank)
        } else {
            None
        }
    }

    pub fn insert(&mut self, to_insert: T) -> Option<&T> {
        if self.len >= 2usize.pow(self.max_level as u32 - 1) {
            self.add_a_level();
        }
        unsafe {
            let mut update = VecDeque::new();
            let mut node = self.head;
            let mut rank = 0usize;

            for i in (0..self.max_level).rev() {
                while let Some((next, span)) = self.forward(node).get(i) {
                    if self.value(*next).is_some_and(|v| v < &to_insert) {
                        rank += span;
                        node = *next;
                    } else {
                        break;
                    }
                }
                update.push_front((node, rank));
            }

            let (next_node, _) = self.forward(node)[0];
            if (*next_node.as_ptr())
                .value
                .as_ref()
                .is_some_and(|v| v == &to_insert)
            {
                return None;
            }

            let new_node = NonNull::new_unchecked(Box::into_raw(Box::new(Node {
                value: Some(to_insert),
                forward: vec![],
            })));

            let new_forward = &mut (*new_node.as_ptr()).forward;
            let level = self.pick_a_level();
            for i in 0..=level {
                let (update_i, offset_i) = update[i];
                if let Some((next_i, old_span)) =
                    &mut (&mut (*update_i.as_ptr())).forward.get_mut(i)
                {
                    new_forward.push((*next_i, *old_span - (rank - offset_i)));
                    *next_i = new_node;
                    *old_span = 1 + rank - offset_i;
                } else {
                    new_forward.push((self.tail, self.len + 1 - rank));
                }
                (&mut (*update_i.as_ptr())).forward[i].0 = new_node;
            }
            for i in (level + 1)..self.max_level {
                let (update_i, _) = update[i];
                if let Some((_, span_i)) = (&mut (*update_i.as_ptr())).forward.get_mut(i) {
                    *span_i += 1;
                }
            }
            self.len += 1;
            new_node.as_ref().value.as_ref()
        }
    }

    pub fn delete<Q>(&mut self, to_delete: &Q) -> Option<T>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut update = VecDeque::new();
        let mut node = self.head;
        let mut rank = 0;

        unsafe {
            for i in (0..self.max_level).rev() {
                while let Some((next, span)) = self.forward(node).get(i) {
                    if self.value(*next).is_some_and(|v| v.borrow() < to_delete) {
                        rank += span;
                        node = *next;
                    } else {
                        break;
                    }
                }
                update.push_front((node, rank));
            }

            let (node_to_delete, _) = (&(*node.as_ptr())).forward[0];
            if (*node_to_delete.as_ptr())
                .value
                .as_ref()
                .is_some_and(|v| v.borrow() == to_delete)
            {
                let boxed_node = Box::from_raw(node_to_delete.as_ptr());
                for i in 0..self.max_level {
                    let (update_i, offset_i) = update[i];
                    let (forward_i, span_i) = &mut (&mut (*update_i.as_ptr())).forward[i];
                    if *forward_i == node_to_delete {
                        *forward_i = boxed_node.forward[i].0;
                        *span_i = (rank - offset_i) + boxed_node.forward[i].1 - 1;
                    }
                }
                self.len -= 1;
                boxed_node.value
            } else {
                None
            }
        }
    }

    pub fn get_starting_at(&'_ self, min: &T) -> Iter<'_, T> {
        let (update, _) = self.find(min);
        let (node, _) = update[0];
        unsafe {
            Iter {
                next: node.as_ptr().as_ref(),
            }
        }
    }

    pub fn from_nth<'a>(&'a self, index: usize) -> Iter<'a, T> {
        let mut node = self.head;
        let mut rank = 0;
        for i in (0..self.max_level).rev() {
            while let Some((next, span)) = self.forward(node).get(i) {
                if rank + span <= index + 1 {
                    node = *next;
                    rank += span;
                } else {
                    break;
                }
            }
        }
        unsafe {
            Iter {
                next: node.as_ptr().as_ref(),
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    fn find<Q>(&self, value: &Q) -> (Vec<(Link<T>, usize)>, usize)
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut update = VecDeque::new();
        let mut node = self.head;
        let mut rank = 0usize;

        for i in (0..self.max_level).rev() {
            while let Some((next, span)) = self.forward(node).get(i) {
                if self.value(*next).is_some_and(|v| v.borrow() < value) {
                    rank += span;
                    node = *next;
                } else {
                    break;
                }
            }
            update.push_front((node, rank));
        }
        (update.into(), rank)
    }
}

impl<T: Debug> Debug for SkipList<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<T> Drop for SkipList<T> {
    fn drop(&mut self) {
        while self.pop_first().is_some() {}
        unsafe {
            let _ = Box::from_raw(self.head.as_ptr());
            let _ = Box::from_raw(self.tail.as_ptr());
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
            self.next = node.forward.get(0).map(|(ptr, _)| &*ptr.as_ptr());
            node.value.as_ref()
        })
    }
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        self.next.take().and_then(|node| unsafe {
            self.next = node.forward.get(0).map(|(ptr, _)| &mut *ptr.as_ptr());
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

    use rand::seq::SliceRandom;

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

        assert_eq!(skip_list.search(&10), Some(1));

        skip_list.delete(&10);
        assert_eq!(skip_list.len(), 2);

        assert!(skip_list.search(&10).is_none());
        assert_eq!(skip_list.search(&17), Some(1));

        let mut rng = rand::rng();
        let mut indices = (100..200).collect::<Vec<_>>();
        indices.shuffle(&mut rng);
        for i in indices {
            skip_list.insert(i);
        }
    }

    #[test]
    fn test_into_iter() {
        let mut skip_list = SkipList::new();

        let mut indices: Vec<_> = (0..10).collect();
        let mut rng = rand::rng();
        indices.shuffle(&mut rng);
        for i in indices {
            skip_list.insert(i);
        }

        println!("Calling into_iter");
        let mut count = 0;
        for (index, value) in skip_list.into_iter().enumerate() {
            count += 1;
            assert_eq!(index, value);
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_iter() {
        let mut skip_list = SkipList::new();

        let mut items: Vec<_> = (0..10).collect();
        let mut rng = rand::rng();
        items.shuffle(&mut rng);
        for i in items {
            skip_list.insert(i);
        }

        println!("Calling iter");
        let mut count = 0;
        for (index, value) in skip_list.iter().enumerate() {
            count += 1;
            assert_eq!(index, *value);
            assert_eq!(skip_list.search(value), Some(index));
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_iter_mut() {
        let mut skip_list = SkipList::new();

        for i in 0..10 {
            skip_list.insert(i);
        }

        println!("Calling iter_mut");
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
        let mut indices: Vec<_> = (0..100).collect();
        let mut rng = rand::rng();
        indices.shuffle(&mut rng);
        for i in indices {
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
