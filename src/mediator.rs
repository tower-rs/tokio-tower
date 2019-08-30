use crossbeam::atomic::AtomicCell;
use futures::{task, Poll};
use std::sync::Arc;
use std::task::Context;

enum CellValue<T> {
    /// The sender has left a value.
    Some(T),

    /// If the receiver sees this, the sender has disconnected.
    /// If the sender sees this, the receiver has disconnected.
    ///
    /// Will be `Some` if the sender sent a value that wasn't handled before it disconnected.
    Fin(Option<T>),

    /// The sender has not left a value.
    None,
}

impl<T> CellValue<T> {
    fn is_none(&self) -> bool {
        if let CellValue::None = *self {
            true
        } else {
            false
        }
    }
}

struct Mediator<T> {
    value: AtomicCell<CellValue<T>>,
    tx_task: task::AtomicWaker,
    rx_task: task::AtomicWaker,
}

pub(crate) struct Receiver<T>(Arc<Mediator<T>>);

pub(crate) struct Sender<T> {
    inner: Arc<Mediator<T>>,
    checked_ready: bool,
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        match self.inner.value.swap(CellValue::None) {
            CellValue::Some(t) => {
                self.inner.value.swap(CellValue::Fin(Some(t)));
            }
            CellValue::Fin(_) => {
                // receiver has gone away too -- all good.
                return;
            }
            CellValue::None => {
                self.inner.value.swap(CellValue::Fin(None));
            }
        }
        self.inner.rx_task.wake();
    }
}

pub(crate) fn new<T>() -> (Sender<T>, Receiver<T>) {
    let m = Arc::new(Mediator {
        value: AtomicCell::new(CellValue::None),
        tx_task: task::AtomicWaker::new(),
        rx_task: task::AtomicWaker::new(),
    });

    (
        Sender {
            inner: m.clone(),
            checked_ready: false,
        },
        Receiver(m),
    )
}

pub(crate) enum TrySendError<T> {
    Pending(T),
    Closed(T),
}

impl<T> Sender<T> {
    /// Returns true if there is a free slot for a client request.
    ///
    /// This method errors if the receiver has disconnected.
    pub(crate) fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<(), ()>> {
        // register in case we can't send
        self.inner.tx_task.register(cx.waker());
        match self.inner.value.swap(CellValue::None) {
            CellValue::Some(t) => {
                // whoops -- put it back
                self.inner.value.swap(CellValue::Some(t));
                // notify in case the receiver just missed us
                self.inner.rx_task.wake();
                Poll::Pending
            }
            CellValue::None => {
                self.checked_ready = true;
                Poll::Ready(Ok(()))
            }
            f @ CellValue::Fin(_) => {
                // the receiver must have gone away (since we can't have gone away)
                // put the Fin marker back for ourselves to see again later
                self.inner.value.swap(f);
                Poll::Ready(Err(()))
            }
        }
    }

    /// Attempts to place `t` in a free client request slot.
    ///
    /// This method returns `NotReady` if `is_ready` has not previously returned `true`.
    /// This method errors if the receiver has disconnected since `poll_ready`.
    pub(crate) fn try_send(&mut self, t: T) -> Result<(), TrySendError<T>> {
        if !self.checked_ready {
            return Err(TrySendError::Pending(t));
        }

        // we're suppposed to _know_ that there is a slot here,
        // so no need to do a tx_task.register.
        match self.inner.value.swap(CellValue::Some(t)) {
            CellValue::None => {}
            CellValue::Some(_) => unreachable!("is_ready returned true, but slot occupied"),
            f @ CellValue::Fin(_) => {
                // the receiver must have gone away (since we can't have gone away)
                // put the Fin marker back for ourselves to see again later
                if let CellValue::Some(t) = self.inner.value.swap(f) {
                    return Err(TrySendError::Closed(t));
                } else {
                    unreachable!("where did it go?");
                }
            }
        }

        self.checked_ready = false;
        self.inner.rx_task.wake();
        Ok(())
    }
}

impl<T> Receiver<T> {
    /// Attempts to receive a value sent by the client.
    ///
    /// `Ready(None)` is returned if the client has disconnected.
    pub(crate) fn try_recv(&mut self, cx: &mut Context) -> Poll<Option<T>> {
        self.0.rx_task.register(cx.waker());
        match self.0.value.swap(CellValue::None) {
            CellValue::Some(v) => {
                // let the sender know there's room now
                self.0.tx_task.wake();
                Poll::Ready(Some(v))
            }
            CellValue::Fin(Some(v)) => {
                // leave a None in there so we know to close after
                if cfg!(debug_assertions) {
                    let old = self.0.value.swap(CellValue::Fin(None));
                    assert!(old.is_none());
                } else {
                    self.0.value.store(CellValue::Fin(None));
                }
                Poll::Ready(Some(v))
            }
            CellValue::Fin(None) => Poll::Ready(None),
            CellValue::None => Poll::Pending,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.0.value.swap(CellValue::Fin(None));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio_mock_task::MockTask;

    #[test]
    fn basic() {
        let mut mt = MockTask::new();

        let (mut tx, mut rx) = new::<usize>();
        assert_eq!(mt.enter(|| tx.poll_ready()), Poll::Ready(Ok(())));
        assert!(!mt.is_notified());
        assert_eq!(mt.enter(|| tx.try_send(42)), Ok(()));
        assert!(!mt.is_notified());
        assert_eq!(mt.enter(|| rx.try_recv()), Poll::Ready(Some(42)));

        assert_eq!(mt.enter(|| tx.poll_ready()), Poll::Ready(Ok(())));
        assert_eq!(mt.enter(|| tx.try_send(43)), Ok(()));
        assert!(mt.is_notified());
        assert_eq!(mt.enter(|| tx.poll_ready()), Poll::Pending);
        assert_eq!(mt.enter(|| tx.try_send(44)), Err(TrySendError::Pending(44)));
        assert_eq!(mt.enter(|| rx.try_recv()), Poll::Ready(Some(43)));
        assert!(mt.is_notified()); // sender is notified
        assert_eq!(mt.enter(|| tx.poll_ready()), Poll::Ready(Ok(())));
        assert_eq!(mt.enter(|| tx.try_send(44)), Ok(()));
        assert!(mt.is_notified());

        mt.enter(|| drop(tx));
        assert_eq!(mt.enter(|| rx.try_recv()), Poll::Ready(Some(44)));
        assert_eq!(mt.enter(|| rx.try_recv()), Poll::Ready(None));
    }

    #[test]
    fn notified_on_empty_drop() {
        let mut mt = MockTask::new();

        let (tx, mut rx) = new::<usize>();
        assert_eq!(mt.enter(|| rx.try_recv()), Poll::Pending);
        assert!(!mt.is_notified());
        mt.enter(|| drop(tx));
        assert!(mt.is_notified());
        assert_eq!(mt.enter(|| rx.try_recv()), Poll::Ready(None));
    }

    #[test]
    fn sender_sees_receiver_drop() {
        let mut mt = MockTask::new();

        let (mut tx, rx) = new::<usize>();
        assert_eq!(mt.enter(|| tx.poll_ready()), Poll::Ready(Ok(())));
        mt.enter(|| drop(rx));
        assert_eq!(mt.enter(|| tx.poll_ready()), Poll::Ready(Err(())));
        assert_eq!(mt.enter(|| tx.try_send(42)), Err(TrySendError::Closed(42)));
    }
}
