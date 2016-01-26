#![cfg_attr(test, deny(warnings))]
// #![deny(missing_docs)]

//! # scoped-pool
//!
//! A flexible thread pool providing scoped threads.
//!

extern crate variance;
extern crate crossbeam;

use variance::InvariantLifetime as Id;
use crossbeam::sync::MsQueue;

use std::{thread, mem};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Condvar};

#[derive(Clone)]
pub struct Pool {
    queue: Arc<MsQueue<PoolMessage>>
}

impl Pool {
    pub fn new(size: usize) -> Pool {
        let pool = Pool {
            queue: Arc::new(MsQueue::new())
        };

        for _ in 0..size {
            let pool = pool.clone();
            thread::spawn(move || pool.run_thread());
        }

        pool
    }

    pub fn spawn<F: FnOnce() + Send + 'static>(&self, job: F) {
        Scope::forever(self.clone()).execute(job)
    }

    pub fn scoped<'scope, F, R>(&self, scheduler: F) -> R
    where F: FnOnce(&Scope<'scope>) -> R {
        Scope::forever(self.clone()).zoom(scheduler)
    }

    /// Shutdown the Pool.
    ///
    /// WARNING: Extreme care should be taken to not call shutdown concurrently
    /// with any scoped calls, or deadlock can occur.
    pub fn shutdown(&self) {
        self.queue.push(PoolMessage::Quit)
    }

    fn run_thread(&self) {

        loop {
            match self.queue.pop() {
                // On Quit, repropogate and quit.
                PoolMessage::Quit => {
                    self.queue.push(PoolMessage::Quit);
                    break
                },
                // On Task, run the task then complete the WaitGroup.
                PoolMessage::Task(job, wait) => {
                    let sentinel = Sentinel(self.clone(), Some(wait.clone()));
                    job.run();
                    sentinel.cancel();
                }
            }
        }
    }
}

pub struct Scope<'scope> {
    pool: Pool,
    wait: Arc<WaitGroup>,
    _scope: Id<'scope>
}

impl<'scope> Scope<'scope> {
    pub fn forever(pool: Pool) -> Scope<'static> {
        Scope {
            pool: pool,
            wait: Arc::new(WaitGroup::new()),
            _scope: Id::default()
        }
    }

    pub fn execute<F>(&self, job: F)
    where F: FnOnce() + Send + 'scope {
        // Submit the job *before* submitting it to the queue.
        self.wait.submit();

        let task = unsafe {
            // Safe because we will ensure the task finishes executing before
            // 'scope via joining before the resolution of `'scope`.
            mem::transmute::<Box<Task + Send + 'scope>,
                             Box<Task + Send + 'static>>(Box::new(job))
        };

        // Submit the task to be executed.
        self.pool.queue.push(PoolMessage::Task(task, self.wait.clone()));
    }

    pub fn zoom<'smaller, F, R>(&self, scheduler: F) -> R
    where F: FnOnce(&Scope<'smaller>) -> R,
          'scope: 'smaller {
        let scope = unsafe { self.refine::<'smaller>() };

        // Schedule all tasks then join all tasks
        let res = scheduler(&scope);
        scope.join();

        res
    }

    /// Awaits all jobs submitted on this Scope to be completed.
    ///
    /// Only guaranteed to join jobs which where `execute`d logically
    /// prior to `join`. Jobs `execute`d concurrently with `join` may
    /// or may not be completed before `join` returns.
    pub fn join(&self) {
        self.wait.join()
    }

    // Create a new scope with a smaller lifetime on the same pool.
    unsafe fn refine<'other>(&self) -> Scope<'other> where 'scope: 'other {
        Scope {
            pool: self.pool.clone(),
            wait: Arc::new(WaitGroup::new()),
            _scope: Id::default()
        }
    }
}

enum PoolMessage {
    Quit,
    Task(Box<Task + Send>, Arc<WaitGroup>)
}

pub struct WaitGroup {
    // Count of currently active tasks.
    active: AtomicUsize,

    // The lock and condition variable the joining threads
    // use to wait for the active tasks to complete.
    //
    // The lock contains a flag set to true by default,
    // and which is set to false to poison the group, causing
    // joins to panic.
    lock: Mutex<bool>,
    cond: Condvar
}

impl WaitGroup {
    /// Create a new empty WaitGroup.
    pub fn new() -> Self {
        WaitGroup {
            active: AtomicUsize::new(0),
            lock: Mutex::new(true),
            cond: Condvar::new()
        }
    }

    /// Submit to this WaitGroup, causing `join` to wait
    /// for an additional `complete`.
    pub fn submit(&self) {
        self.active.fetch_add(1, Ordering::SeqCst);
    }

    /// Complete a previous `submit`.
    pub fn complete(&self) {
        if self.active.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.cond.notify_all()
        }
    }

    /// Poison the WaitGroup so all `join`ing threads panic.
    pub fn poison(&self) {
        // Set the poison flag to false.
        *self.lock.lock().unwrap() = false;

        // Wake all pending joiners so they panic.
        self.cond.notify_all()
    }

    /// Wait for `submit`s to this WaitGroup to be `complete`d.
    ///
    /// Submits occuring completely before joins will always be waited on.
    ///
    /// Submits occuring concurrently with a `join` may or may not
    /// be waited for.
    ///
    /// Before submitting, `join` will always return immediately.
    pub fn join(&self) {
        // Check optimistically once before loading the lock.
        if self.active.load(Ordering::SeqCst) <= 0 {
            return
        }

        let mut lock = self.lock.lock().unwrap();
        loop {
            if !*lock {
                panic!("WaitGroup explicitly poisoned!");
            }

            if self.active.load(Ordering::SeqCst) <= 0 {
                return
            } else {
                lock = self.cond.wait(lock).unwrap();
            }
        }
    }
}

struct Sentinel(Pool, Option<Arc<WaitGroup>>);

impl Sentinel {
    fn cancel(mut self) {
        self.1.take().map(|wait| wait.complete());
    }
}

impl Drop for Sentinel {
    fn drop(&mut self) {
        self.1.take().map(|wait| wait.poison());
    }
}


trait Task {
    fn run(self: Box<Self>);
}

impl<F: FnOnce()> Task for F {
    fn run(self: Box<Self>) { (*self)() }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, Ordering};

    use {Pool, Scope};

    #[test]
    fn test_simple_use() {
        let pool = Pool::new(4);

        let mut buf = [0, 0, 0, 0];

        pool.scoped(|scope| {
            for i in &mut buf {
                scope.execute(move || *i += 1);
            }
        });

        assert_eq!(&buf, &[1, 1, 1, 1]);
    }

    #[test]
    fn test_zoom() {
        let pool = Pool::new(4);

        let mut outer = 0;

        pool.scoped(|scope| {
            let mut inner = 0;
            scope.zoom(|scope2| scope2.execute(|| inner = 1));
            assert_eq!(inner, 1);

            outer = 1;
        });

        assert_eq!(outer, 1);
    }

    #[test]
    fn test_spawn_doesnt_hang() {
        let pool = Pool::new(1);
        pool.spawn(move || loop {});
    }

    #[test]
    fn test_forever_zoom() {
        let pool = Pool::new(16);
        let forever = Scope::forever(pool.clone());

        let ran = AtomicBool::new(false);

        forever.zoom(|scope| scope.execute(|| ran.store(true, Ordering::SeqCst)));

        assert!(ran.load(Ordering::SeqCst));
    }

    #[test]
    #[should_panic]
    fn test_scheduler_panic() {
        let pool = Pool::new(4);
        pool.scoped(|_| panic!());
    }

    #[test]
    #[should_panic]
    fn test_scoped_execute_panic() {
        let pool = Pool::new(4);
        pool.scoped(|scope| scope.execute(|| panic!()));
    }

    #[test]
    #[should_panic]
    fn test_pool_panic() {
        let _pool = Pool::new(1);
        panic!();
    }

    #[test]
    #[should_panic]
    fn test_zoomed_scoped_execute_panic() {
        let pool = Pool::new(4);
        pool.scoped(|scope| scope.zoom(|scope2| scope2.execute(|| panic!())));
    }
}

