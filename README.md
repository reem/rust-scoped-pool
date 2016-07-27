# scoped-pool

> A flexible thread pool providing scoped and static threads.

## [Documentation](https://crates.fyi/crates/scoped-pool/0.1.8)

## Overview

Example of simple use (applying a transformation to every item in an array):

```rust
extern crate scoped_pool;

use scoped_pool::Pool;

let pool = Pool::new(4);

let mut buf = [0, 0, 0, 0];

pool.scoped(|scope| {
    for i in &mut buf {
        scope.execute(move || *i += 1);
    }
});

assert_eq!(&buf, &[1, 1, 1, 1]);
```

Besides the core API used above (`Pool::new`, `Pool::scoped`) this crate also
provides many extremely useful convenience functions for complex code using
scoped threads.

Also includes the raw `WaitGroup` type, which can be used to implement similar
"wait for a group of actions to complete" logic, and is used in `Pool` and
`Scope`.

See the generated documentation (linked above) for details.

## Unique Aspects

Unlike many other scoped threadpool crates, this crate is designed to be
maximally flexible: `Pool` and `Scope` are both `Send + Sync`, `Pool` is `Clone`,
and both have many useful conveniences such as:

`Pool::spawn` for spawning `'static` jobs.

`Pool::expand` for expanding the number of threads in the pool.

`Pool::shutdown` for shutting down the pool.

`Scope::forever` and `Scope::zoom` for externalizing `Scope` management and
allowing fine-grained control over when jobs are scheduled and waited on.

Nearly all methods on both types require only an immutable borrow, and thus are
safe to use concurrently without external synchronization.

In addition, the internal design of this crate is carefully constructed so that
all unsafety is encapsulated in the `Scope` type, which effectively just adds
lifetime scoping to the `WaitGroup` type for jobs scheduled on a `Pool`.

## Usage

Use the crates.io repository; add this to your `Cargo.toml` along
with the rest of your dependencies:

```toml
[dependencies]
scoped-pool = "1"
```

## Author

[Jonathan Reem](https://medium.com/@jreem) is the primary author and maintainer of scoped-pool.

## License

MIT

