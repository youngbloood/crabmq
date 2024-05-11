// use parking_lot::RwLock;
// use std::{borrow::BorrowMut, cell::UnsafeCell};

// pub struct AtomicValue<T> {
//     mutex: RwLock<()>,
//     inner: UnsafeCell<T>,
// }

// impl<T> AtomicValue<T>
// where
//     T: Default + Copy,
// {
//     pub fn new(t: T) -> Self {
//         AtomicValue {
//             mutex: RwLock::new(()),
//             inner: UnsafeCell::new(t),
//         }
//     }

//     pub fn load(&self) -> T {
//         self.mutex.read();
//         let c = *self.inner.get_mut().cl;
//         c
//     }

//     pub fn store(&mut self, v: T) -> T {
//         self.mutex.write();
//         let old_v = self.inner.into_inner();
//         self.inner = UnsafeCell::new(v);

//         old_v
//     }

//     pub fn action(&mut self, mut callback: impl FnMut(&mut T)) {
//         self.mutex.write();
//         callback(self.inner.get_mut());
//     }
// }
