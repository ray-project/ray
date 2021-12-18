#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

use serde::{Serialize, Deserialize};

mod function_manager;
use function_manager::{FunctionManager, CBuffer};

fn my_func(a: &[u64], b: &[u64]) -> Vec<u64> {
    assert_eq!(a.len(), b.len());
    let mut result = vec![0u64; a.len()];
    for i in 0..a.len() {
        result[i] = a[i] + b[i];
    }
    result
}

extern "C" fn invoke_my_func(len: u64, args: *const u8) -> CBuffer {
    let (a, b) = rmp_serde::from_read_ref::<_, (Vec<u64>, Vec<u64>)>(
        unsafe { std::slice::from_raw_parts(args, len as usize) }
    ).unwrap();
    let result = rmp_serde::to_vec(&my_func(&a, &b)).unwrap();
    CBuffer::from_vec(result)
}

fn submit_my_func(a: &[u64], b: &[u64], fm: &FunctionManager) {
    let args = rmp_serde::to_vec(&(a, b)).unwrap();
    let result = fm.get_function("f")(args.len() as u64, args.as_ptr());
    let c = rmp_serde::from_read_ref::<_, Vec<u64>>(
        result.as_slice()
    ).unwrap();
    println!("{:?}", c);
}

fn main() {
    let (a, b): (Vec<_>, Vec<_>) = ((0u64..100).collect(), (0u64..100).collect());

    let mut fm = FunctionManager::default();
    fm.register("f", invoke_my_func);

    submit_my_func(&a, &b, &fm);
}
