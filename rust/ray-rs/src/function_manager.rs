use std::collections::{HashMap, };

type InvokerFunction = extern "C" fn(u64, *const u8) -> CBuffer;

#[derive(Default)]
pub struct FunctionManager {
    func_name_to_invoker: HashMap<String, InvokerFunction>,
}

impl FunctionManager {
    pub fn register(&mut self, name: &str, f: InvokerFunction) {
        self.func_name_to_invoker.insert(String::from(name), f);
    }

    pub fn get_function(&self, name: &str) -> InvokerFunction {
        *self.func_name_to_invoker.get(&String::from(name)).unwrap()
    }
}

// lazy_static! {
//     [pub(crate)] static ref global_function_manager: Arc<Mutex<FunctionManager>> =
//
// }

#[repr(C)]
pub struct CBuffer {
    vec: Vec<u8>
}

impl CBuffer {
    pub fn as_slice<'a>(&'a self) -> &'a [u8] {
        self.vec.as_slice()
    }

    pub fn from_vec(vec: Vec<u8>) -> Self {
        Self {
            vec
        }
    }
}
