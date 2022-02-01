#![allow(non_upper_case_globals)]
use ray_rs::*;

remote! {
pub fn add_two_vecs(a: Vec<u64>, b: Vec<u64>) -> Vec<u64> {
    assert_eq!(a.len(), b.len());
    let mut ret = vec![0u64; a.len()];
    for i in 0..a.len() {
        ret[i] = a[i] + b[i];
    }
    ret
}
}

// remote! {
// pub fn add_two_vecs_ref(a: &'static Vec<u64>, b: Vec<u64>) -> Vec<u64> {
//     assert_eq!(a.len(), b.len());
//     let mut ret = vec![0u64; a.len()];
//     for i in 0..a.len() {
//         ret[i] = a[i] + b[i];
//     }
//     ret
// }
// }

// remote! {
// pub fn put_and_get_nested(a: Vec<u64>) -> Vec<u64> {
//     let id = put::<Vec<u64>, _>(&a);
//     let a_get = get::<Vec<u64>>(id);
//     assert_eq!(a, a_get);
//     return a_get;
// }
// }

remote! {
pub fn add_two_vecs_nested(a: Vec<u64>, b: Vec<u64>) -> Vec<u64> {
    let objr = add_two_vecs.remote(&a, &b);
    let res = get(&objr);
    res
}
}

// This is currently not possible to do?
// The owning worker is already dead by the time this function ends...?

// remote! {
// pub fn add_two_vecs_nested_remote_outer_get(a: Vec<u64>, b: Vec<u64>) -> Vec<u8> {
//     let obj = add_two_vecs.remote(&a, &b);
//     util::add_local_ref(obj.clone());
//     obj.into_bytes()
// }
// }

remote! {
pub fn add_three_vecs(a: Vec<u64>, b: Vec<u64>, c: Vec<u64>) -> Vec<u64> {
    assert_eq!(a.len(), b.len());
    assert_eq!(a.len(), c.len());
    let mut ret = vec![0u64; a.len()];
    for i in 0..a.len() {
        ret[i] = a[i] + b[i] + c[i];
    }
    ret
}
}

// // TODO: this should probably be in a separate crate, not simple.
// pub trait PhysicalOperator<I, R> {
//     fn update(&mut self, key: String, val: I);
//     fn get(&self) -> R;
// }
//
// // We choose
// pub struct SumOperator<I: Integer> {
//     materialized_values: HashMap<String, I>,
//     count: I,
// }
//
// impl<I: Integer> PhysicalOperator<I, I> for SumOperator<I> {
//     fn update(&mut self, key: String, val: I) {
//         self.materialized_values.upsert(key, val);
//     }
// }
//
// remote_actor! {
// }
//
//
// // We choose
// #[derive(Default)]
// pub struct Average<I: Float> {
//     materialized_values: HashMap<String, I>,
//     sum: I,
// }
//
// impl<I: Float> Average<I> {
//     // #[ray::create_actor]
//     pub fn new() -> Self {
//         Self::default()
//     }
// }
//
// impl<I: Float> PhysicalOperator<I, I> for Average<I> {
//     fn update(&mut self, key: String, val: I) {
//         let num_values = self.materialized_values.len(); // TODO: cache this instead?
//         if let Some(old) = self.materialized_values.insert(key, val) {
//             self.sum = self.sum - old + val;
//         } else {
//             self.sum += val;
//         }
//     }
//
//     fn get(&self) -> I {
//         self.sum / self.materialized_values.len()
//     }
// }
//
// impl <I: Float> PhysicalOperator for InnerJoin {
//
// }
//
// // InnerJoin - column_name. Row_bitmap...?
//
//
// #[test]
// fn inner_join_and_avg() {
//     Streaming::Operator::new()
// }

// Idea for generics... dynamic dispatch + concrete instantiation
// declaration (with some hacks to get it to specialize?)
