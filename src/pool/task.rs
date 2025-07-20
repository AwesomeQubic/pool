use std::{
    alloc::{self, Layout},
    cell::{Ref, RefCell},
    future::Future,
    marker::PhantomPinned,
    mem::forget,
    ops::Deref,
    pin::Pin,
    ptr::{self, NonNull},
    sync::atomic::{fence, AtomicU32, AtomicUsize},
    task::{Context, RawWaker, RawWakerVTable, Waker},
};

use crossbeam::{channel::Receiver, queue::SegQueue, utils::Backoff};

use super::{buf::CloseableBuffer, GLOBAL_TASK_POOL};

pub const RUNNING: u32 = 0;
pub const FROZEN: u32 = 2;
pub const DONE: u32 = 1;

//Heap allocated task
pub struct TaskInner<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static> {
    waker_table: &'static RawWakerVTable,
    generic_table: &'static Vtable,

    //Amount of references to the task
    ref_count: AtomicUsize,

    wakers: CloseableBuffer<Waker>,

    //State of a task used also as a futex
    state: AtomicU32,

    //Mutable data
    finished: RefCell<(F, Option<R>)>,

    //Guarantee that memory is not moved
    _pin: PhantomPinned,
}

impl<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static> TaskInner<R, F> {
    pub fn run(&self, context: &mut Context<'_>) -> RunResults {
        let Ok(mut ref_mut) = self.finished.try_borrow_mut() else {
            return RunResults::CouldNotAcquireOwnership;
        };

        unsafe {
            //SAFETY: Its heap see Task docs so its Pin
            let future_ptr = Pin::new_unchecked(&mut ref_mut.0);

            match future_ptr.poll(context) {
                std::task::Poll::Ready(r) => {
                    ref_mut.1 = Some(r);
                    drop(ref_mut);

                    if let Ok(waker) = self.wakers.close_and_get_result() {
                        waker.wake();
                    }

                    self.state.store(DONE, std::sync::atomic::Ordering::Relaxed);
                    atomic_wait::wake_all(&self.state as *const _);
                    RunResults::Done
                }
                std::task::Poll::Pending => {
                    self.state
                        .store(FROZEN, std::sync::atomic::Ordering::Release);
                    RunResults::Pending
                }
            }
        }
    }

    fn count_down(&self) -> usize {
        self.ref_count
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn count_up(&self) -> usize {
        self.ref_count
            .fetch_add(1, std::sync::atomic::Ordering::Release)
    }

    pub fn collect_output(&self) -> Option<R> {
        let Ok(mut borrowed) = self.finished.try_borrow_mut() else {
            return None;
        };
        borrowed.1.take()
    }

    pub fn state(&self) -> &AtomicU32 {
        &self.state
    }

    pub fn join_blocking(&self) {
        loop {
            let load = self.state.load(std::sync::atomic::Ordering::Relaxed);
            if load == DONE {
                return;
            } else {
                atomic_wait::wait(&self.state, load);
            }
        }
    }

    pub fn set_waker(&self, waker: Waker) -> bool {
        self.wakers.submit(waker)
    }
}

pub enum RunResults {
    Done,
    Pending,
    CouldNotAcquireOwnership,
}

pub struct Task<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static> {
    content: *const TaskInner<R, F>,
}

impl<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static> Task<R, F> {
    pub fn create(future: F) -> Self {
        unsafe {
            let layout = Layout::new::<TaskInner<R, F>>();
            let allocated = alloc::alloc(layout);
            if allocated.is_null() {
                panic!("Could not allocate task");
            }

            let cast = allocated.cast::<TaskInner<R, F>>();

            //SAFETY: Allocated is valid
            ptr::write(
                cast,
                TaskInner {
                    ref_count: AtomicUsize::new(1),
                    state: AtomicU32::new(0),
                    finished: RefCell::new((future, None)),
                    _pin: PhantomPinned,
                    waker_table: walker_vtable::<R, F>(),
                    generic_table: vtable::<R, F>(),
                    wakers: CloseableBuffer::new(),
                },
            );

            //SAFETY: Its not null cause we checked it before
            Self { content: cast }
        }
    }

    pub fn clone_generalized(&self) -> GeneralizedTask {
        self.count_up();
        GeneralizedTask {
            content: self.content.cast::<()>(),
            table: vtable::<R, F>(),
            waker_vtable: walker_vtable::<R, F>(),
        }
    }
}

unsafe impl<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static> Send
    for Task<R, F>
{
}
unsafe impl<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static> Sync
    for Task<R, F>
{
}

impl<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static> Deref for Task<R, F> {
    type Target = TaskInner<R, F>;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.content }
    }
}

impl<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static> Drop for Task<R, F> {
    fn drop(&mut self) {
        let cast = self.content.cast::<()>();
        unsafe { on_drop::<R, F>(cast) };
    }
}

pub struct GeneralizedTask {
    content: *const (),
    table: &'static Vtable,
    waker_vtable: &'static RawWakerVTable,
}

unsafe impl Send for GeneralizedTask {}
unsafe impl Sync for GeneralizedTask {}

impl GeneralizedTask {
    pub fn run(&self, context: &mut Context<'_>) -> RunResults {
        unsafe { (self.table.run)(self.content, context) }
    }

    pub fn get_state(&self) -> &AtomicU32 {
        unsafe { &*(self.table.get_state)(self.content) }
    }

    pub fn into_waker(self) -> Waker {
        let waker = unsafe { Waker::new(self.content, self.waker_vtable) };
        forget(self);
        waker
    }

    pub fn new_waker(&self) -> Waker {
        unsafe { (self.table.count_up)(self.content) };
        unsafe { Waker::new(self.content, self.waker_vtable) }
    }
}

impl Clone for GeneralizedTask {
    fn clone(&self) -> Self {
        unsafe { (self.table.count_up)(self.content) };
        Self {
            content: self.content.clone(),
            table: self.table,
            waker_vtable: self.waker_vtable,
        }
    }
}

impl Drop for GeneralizedTask {
    fn drop(&mut self) {
        unsafe { (self.table.drop)(self.content) }
    }
}

//Generalized task vtable

struct Vtable {
    count_up: unsafe fn(*const ()) -> usize,
    run: unsafe fn(*const (), &mut Context<'_>) -> RunResults,
    join_blocking: unsafe fn(*const ()),
    drop: unsafe fn(*const ()),
    get_state: unsafe fn(*const ()) -> *const AtomicU32,
}

fn vtable<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
) -> &'static Vtable {
    &Vtable {
        run: run_inner::<R, F>,
        join_blocking: join_blocking_inner::<R, F>,
        count_up: count_up_inner::<R, F>,
        drop: on_drop::<R, F>,
        get_state: get_state_inner::<R, F>,
    }
}

fn count_up_inner<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
    instance: *const (),
) -> usize {
    unsafe {
        let cast = &*instance.cast::<TaskInner<R, F>>();
        cast.count_up()
    }
}

fn run_inner<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
    instance: *const (),
    context: &mut Context<'_>,
) -> RunResults {
    unsafe {
        let cast = &*instance.cast::<TaskInner<R, F>>();
        cast.run(context)
    }
}

fn join_blocking_inner<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
    instance: *const (),
) {
    unsafe {
        let cast = &*instance.cast::<TaskInner<R, F>>();
        cast.join_blocking()
    }
}

fn get_state_inner<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
    instance: *const (),
) -> *const AtomicU32 {
    unsafe {
        let cast = &*instance.cast::<TaskInner<R, F>>();
        (&cast.state) as *const AtomicU32
    }
}

//SAFETY: Caller must ensure that *const () is point to a real initialized task with this exact generic traits
fn run_impl<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
    task: *const (),
    context: &mut Context<'_>,
) -> RunResults {
    unsafe {
        let casted = &*task.cast::<TaskInner<R, F>>();
        casted.run(context)
    }
}

pub unsafe fn on_drop<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
    task: *const (),
) {
    unsafe {
        let ptr = task.cast::<TaskInner<R, F>>();
        if (&*ptr).count_down() != 1 {
            return;
        }

        fence(std::sync::atomic::Ordering::Acquire);

        //We know this is safe
        ptr::drop_in_place(ptr.cast_mut());
    }
}

//Walker vtable

fn walker_vtable<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
) -> &'static RawWakerVTable {
    &RawWakerVTable::new(
        walker_clone::<R, F>,
        walker_wake::<R, F>,
        walker_wake_by_ref::<R, F>,
        waker_drop::<R, F>,
    )
}

fn walker_clone<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
    content: *const (),
) -> RawWaker {
    //Increment instance thighy
    unsafe {
        (&*content.cast::<TaskInner<R, F>>())
            .ref_count
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }
    RawWaker::new(content, walker_vtable::<R, F>())
}

fn walker_wake<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
    content: *const (),
) {
    unsafe {
        let cast = &*content.cast::<TaskInner<R, F>>();
        if cast
            .state
            .compare_exchange(
                FROZEN,
                RUNNING,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
        {
            //We will turn this walker into a global handle and push it into requeue
            GLOBAL_TASK_POOL.push(GeneralizedTask {
                content: content,
                table: cast.generic_table,
                waker_vtable: cast.waker_table,
            });
        } else {
            //Drop waker
            on_drop::<R, F>(content);
        }
    }
}

fn walker_wake_by_ref<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
    content: *const (),
) {
    unsafe {
        let cast = &*content.cast::<TaskInner<R, F>>();
        if cast
            .state
            .compare_exchange(
                FROZEN,
                RUNNING,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
        {
            cast.count_up();
            GLOBAL_TASK_POOL.push(GeneralizedTask {
                content,
                table: cast.generic_table,
                waker_vtable: cast.waker_table,
            });
        }
    }
}

fn waker_drop<R: Send + Sync + 'static, F: Future<Output = R> + Send + Sync + 'static>(
    content: *const (),
) {
    unsafe { on_drop::<R, F>(content) };
}
