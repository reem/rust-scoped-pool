# scoped-pool

> A flexible thread pool providing scoped and static threads.

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

See the generated documentation (`cargo doc`) for details.

## Usage

Use the crates.io repository; add this to your `Cargo.toml` along
with the rest of your dependencies:

```toml
[dependencies]
scoped-pool = "*"
```

## Author

[Jonathan Reem](https://medium.com/@jreem) is the primary author and maintainer of scoped-pool.

## License

MIT

