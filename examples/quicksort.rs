extern crate scoped_pool;
extern crate itertools;
extern crate rand;

use rand::Rng;

use scoped_pool::{Pool, Scope};

pub fn quicksort<T: Send + Sync + Ord>(pool: &Pool, data: &mut [T]) {
    pool.scoped(move |scoped| do_quicksort(scoped, data))
}

fn do_quicksort<'a, T: Send + Sync + Ord>(scope: &Scope<'a>, data: &'a mut [T]) {
    scope.recurse(move |scope| {
        if data.len() > 1 {
            // Choose a random pivot.
            let mut rng = rand::thread_rng();
            let len = data.len();
            let pivot_index = rng.gen_range(0, len); // Choose a random pivot

            // Swap the pivot to the end.
            data.swap(pivot_index, len - 1);

            let split = {
                // Retrieve the pivot.
                let mut iter = data.into_iter();
                let pivot = iter.next_back().unwrap();

                // Partition the array.
                itertools::partition(iter, |val| &*val <= &pivot)
            };

            // Swap the pivot back in at the split point by putting
            // the element currently there are at the end of the slice.
            data.swap(split, len - 1);

            // Sort both halves.
            let (left, right) = data.split_at_mut(split);
            do_quicksort(scope, left);
            do_quicksort(scope, &mut right[1..]);
        }
    })
}

pub fn main() {
    let mut rng = rand::thread_rng();
    let pool = Pool::new(8);

    for _ in 0..10 {
        let n = rng.gen_range(1, 10000000);

        println!("Generating {} random elements!", n);
        let mut elements = (0..n).map(|_| ::rand::random::<u64>()).collect::<Vec<_>>();


        println!("Sorting elements!");
        quicksort(&pool, &mut elements);
        println!("Finished sorting elements!");


        println!("Verifying the elements are sorted.");
        for (current, next) in elements.iter().zip(elements[1..].iter()) {
            assert!(next >= current, "{:?} < {:?}! elements: {:?}", next, current, elements);
        }
    }

    pool.shutdown();
    println!("Success.");
}

