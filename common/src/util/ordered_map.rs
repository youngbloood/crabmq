use dashmap::DashMap;
use parking_lot::RwLock;
use rand::Rng as _;
use std::borrow::Borrow;
use std::sync::atomic::Ordering::Relaxed;
use std::{hash::Hash, sync::atomic::AtomicUsize};

pub trait Weight {
    fn get_weight(&self) -> usize;
}

pub struct OrderedMap<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone + Weight,
{
    index: AtomicUsize,
    list: RwLock<Vec<(K, V)>>,
    set: DashMap<K, usize>,
}

impl<K, V> Default for OrderedMap<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone + Weight,
{
    fn default() -> Self {
        Self {
            index: Default::default(),
            list: Default::default(),
            set: Default::default(),
        }
    }
}
impl<K, V> OrderedMap<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone + Weight,
{
    pub fn pop(&self) -> Option<V> {
        None
    }

    pub fn seek(&self, k: &K) -> Option<V> {
        None
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    pub fn roundrobin_next(&self) -> Option<V> {
        if self.list.read().is_empty() {
            return None;
        }
        if self.index.load(Relaxed) >= self.list.read().len() {
            self.index.store(0, Relaxed);
        }
        let unit = self.list.read()[self.index.load(Relaxed)].clone();
        self.index.fetch_add(1, Relaxed);
        Some(unit.1.clone())
    }

    pub fn rand(&self) -> Option<V> {
        if let Some(pos) = self.set.iter().next() {
            let unit = &self.list.read()[*pos];
            return Some(unit.1.clone());
        }
        None
    }

    pub fn rand_weight(&self) -> Option<V> {
        let mut all_weight = 0_usize;
        self.list.read().iter().for_each(|unit| {
            all_weight += unit.1.get_weight();
        });
        let mut rng = rand::thread_rng();
        let mut factor = rng.gen_range(0..all_weight);
        for unit in self.list.read().iter() {
            if factor < unit.1.get_weight() {
                return Some(unit.1.clone());
            }
            factor -= unit.1.get_weight();
        }
        None
    }

    pub fn insert(&self, k: K, v: V) -> Option<V> {
        if let Some(pos) = self.set.get(&k) {
            self.list.write()[*pos] = (k, v.clone());
        } else {
            {
                self.list.write().push((k.clone(), v.clone()));
            }
            {
                self.set.insert(k, self.list.read().len() - 1);
            }
        }
        Some(v)
    }

    pub fn get(&self, k: &K) -> Option<V> {
        if let Some(pos) = self.set.get(k) {
            let unit = &self.list.read()[*pos];
            return Some(unit.1.clone());
        }
        None
    }

    pub fn contains(&self, k: &K) -> bool {
        self.set.contains_key(k)
    }

    pub fn values(&self) -> Vec<V> {
        let mut res = vec![];
        for unit in self.list.read().iter() {
            res.push(unit.1.clone());
        }
        res
    }

    pub fn get_or_create(&self, k: K, v: V) -> V {
        if let Some(val) = self.get(&k) {
            return val;
        }
        self.insert(k, v).unwrap()
    }

    pub fn remove(&self, k: &K) -> Option<V> {
        if let Some(pos) = self.set.get(k) {
            let unit = self.list.write().remove(*pos);
            self.set.remove(k);
            return Some(unit.1.clone());
        }
        None
    }
}
