use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

pub struct CloseableBuffer<T> {
    value: Mutex<Option<T>>,
    closed: AtomicBool,
}

impl<T> CloseableBuffer<T> {
    pub const fn new() -> Self {
        Self {
            value: Mutex::new(None),
            closed: AtomicBool::new(false),
        }
    }

    pub fn submit(&self, val: T) -> bool {
        if self.closed.load(Ordering::SeqCst) {
            return false;
        }
        let mut lock = self.value.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) || lock.is_some() {
            return false;
        }
        *lock = Some(val);
        true
    }

    pub fn close_and_get_result(&self) -> Result<T, ()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Err(()); // already closed
        }
        let mut lock = self.value.lock().unwrap();
        lock.take().ok_or(())
    }
}
