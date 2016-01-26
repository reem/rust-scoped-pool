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
use std::sync::{Arc, Mutex, Condvar};

#[derive(Clone)]
pub struct Pool {
    threads: usize,
    queue: Arc<MsQueue<PoolMessage>>
}

impl Pool {
    pub fn new(size: usize) -> Pool {
        let pool = Pool {
            threads: size,
            queue: Arc::new(MsQueue::new())
        };

        for _ in 0..size {
            let pool = pool.clone();
            thread::spawn(move || pool.run_thread());
        }

        pool
    }

    pub fn scoped<'pool: 'scope, 'scope, F, R>(&'pool self, scheduler: F) -> R
    where F: FnOnce(&Scope<'scope>) -> R {
        let scope = unsafe { Scope::new(self) };

        // Schedule all tasks, then join all tasks.
        let res = scheduler(&scope);
        scope.join();

        res
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
                    job.run();
                    wait.complete();
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
    unsafe fn new(pool: &Pool) -> Self {
        Scope {
            pool: pool.clone(),
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
            // 'scope via calling `join` before the resolution of `'scope`.
            mem::transmute::<Box<Task + Send + 'scope>,
                             Box<Task + Send + 'static>>(Box::new(job))
        };

        // Submit the task to be executed.
        self.pool.queue.push(PoolMessage::Task(task, self.wait.clone()));
    }

    // Awaits all jobs submitted on this queue to be completed.
    fn join(self) {
        self.wait.join()
    }
}

enum PoolMessage {
    Quit,
    Task(Box<Task + Send>, Arc<WaitGroup>)
}

struct WaitGroup {
    active: Mutex<usize>,
    cond: Condvar
}

impl WaitGroup {
    /// Create a new empty WaitGroup.
    fn new() -> Self {
        WaitGroup {
            active: Mutex::new(0),
            cond: Condvar::new()
        }
    }

    /// Submit a job to this WaitGroup, causing `join` to wait
    /// for an additional `complete` call.
    fn submit(&self) {
        *self.active.lock().unwrap() += 1;
    }

    /// Complete a previously `submit`ted job.
    fn complete(&self) {
        *self.active.lock().unwrap() -= 1;
        self.cond.notify_one()
    }

    /// Wait for all jobs `submit`ted to this WaitGroup to be `complete`d.
    fn join(&self) {
        let mut lock = self.active.lock().unwrap();

        loop {
            if *lock == 0 { break }

            lock = self.cond.wait(lock).unwrap();
        }
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
    use Pool;

    #[test]
    fn simple_use() {
        let pool = Pool::new(4);

        let mut buf = [0, 0, 0, 0];

        pool.scoped(|scope| {
            for i in &mut buf {
                scope.execute(move || *i += 1);
            }
        });

        assert_eq!(&buf, &[1, 1, 1, 1]);
    }
}

