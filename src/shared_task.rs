use futures::{Async, Future, Poll};
use futures::executor::{Notify, with_notify};
use futures::task::AtomicTask;
use std::cell::UnsafeCell;
use std::ops;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
use tokio_executor::spawn;

pub struct SharedTask<T> {
    inner: Arc<Inner<T>>,
}

pub struct LockGuard<'a, T> {
    shared: &'a mut SharedTask<T>,
}

struct Worker<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    val: UnsafeCell<T>,
    task: AtomicTask,
    lock: AtomicUsize,
}

const IDLE: usize = 0;
const LOCKED: usize = 1;
const POLLED: usize = 2;

impl<T> SharedTask<T>
where
    T: Future<Item = (), Error = ()> + Send + 'static,
{
    pub fn spawn(val: T) -> SharedTask<T> {
        let task = SharedTask {
            inner: Arc::new(Inner {
                val: UnsafeCell::new(val),
                task: AtomicTask::new(),
                lock: AtomicUsize::new(IDLE),
            }),
        };

        spawn(Worker {
            inner: task.inner.clone(),
        });

        task
    }

    pub fn lock(&mut self) -> Option<LockGuard<T>> {
        let actual = self.inner.lock.compare_and_swap(IDLE, LOCKED, AcqRel);

        if actual != IDLE {
            return None;
        }

        Some(LockGuard { shared: self })
    }
}

impl<'a, T> LockGuard<'a, T>
where
    T: Send + 'static,
{
    pub fn enter<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R
    {
        use futures::future::lazy;

        lazy(|| {
            Ok::<_, ()>(f(unsafe { &mut *self.shared.inner.val.get() }))
        }).wait().unwrap()
        /*
        with_notify(&self.shared.inner, 0, || {
            f(unsafe { &mut *self.shared.inner.val.get() })
        })
        */
    }
}

impl<'a, T> Drop for LockGuard<'a, T> {
    fn drop(&mut self) {
        let act = self.shared.inner.lock.swap(0, AcqRel);

        if act == POLLED {
            unimplemented!();
        }
    }
}

impl<T> Future for Worker<T>
where
    T: Future<Item = (), Error = ()>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        // self.inner.task.register();

        // TODO: Handle panic
        let act = self.inner.lock.fetch_add(1, AcqRel);

        if act != IDLE {
            return Ok(Async::NotReady);
        }

        let ret = unsafe {
            (*self.inner.val.get()).poll()
        };

        let prev = self.inner.lock.swap(IDLE, AcqRel);
        assert_eq!(prev, LOCKED);
        ret
    }
}

impl<T: Send> Notify for Inner<T> {
    fn notify(&self, _: usize) {
        unimplemented!();
        // self.task.notify();
    }
}

unsafe impl<T: Send> Sync for Inner<T> {}
