use std::{
    future::Future,
    marker::PhantomData,
    sync::{atomic::AtomicBool, Arc},
    task::{Context, Poll, Waker},
    thread::{self, available_parallelism, yield_now},
};

use crossbeam::channel::bounded;
use crossbeam::{channel::Sender, queue::SegQueue};
use task::{GeneralizedTask, Task, DONE};

mod buf;
mod task;

pub(super) static GLOBAL_TASK_POOL: SegQueue<GeneralizedTask> = SegQueue::new();
static IS_INITIALIZED: AtomicBool = AtomicBool::new(false);

pub fn initialize() {
    if IS_INITIALIZED
        .compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::Release,
            std::sync::atomic::Ordering::Relaxed,
        )
        .is_err()
    {
        return;
    }

    for _ in 0..available_parallelism().map(|x| x.get()).unwrap_or(2) {
        thread::spawn(|| {
            loop {
                let Some(task) = GLOBAL_TASK_POOL.pop() else {
                    yield_now();
                    continue;
                };

                let waker = task.new_waker();
                let mut context = Context::from_waker(&waker);
                let results = task.run(&mut context);
                match results {
                    task::RunResults::Done | task::RunResults::Pending => {
                        drop(task);
                    }
                    task::RunResults::CouldNotAcquireOwnership => {
                        //Return task to pool
                        GLOBAL_TASK_POOL.push(task);
                    }
                }
            }
        });
    }
}

pub fn spawn<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
    future: F,
) -> TaskHandle<R, F> {
    let task = Task::create(future);
    GLOBAL_TASK_POOL.push(task.clone_generalized());
    TaskHandle(task, false)
}

pub struct TaskHandle<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
    Task<R, F>,
    bool,
);

impl<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static> TaskHandle<R, F> {
    pub fn join_blocking(self) -> Option<R> {
        self.0.join_blocking();
        self.0.collect_output()
    }
}

impl<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static> Future
    for TaskHandle<R, F>
{
    type Output = Option<R>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.0.state().load(std::sync::atomic::Ordering::Relaxed) == DONE {
            return Poll::Ready(self.0.collect_output());
        } else {
            if !self.1 {
                self.1 = true;
                if !self.0.set_waker(cx.waker().clone()) {
                    return Poll::Ready(None);
                }
            }
            return Poll::Pending;
        }
    }
}
