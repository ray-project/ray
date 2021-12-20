use std::collections::{HashMap};
use uniffi::ffi::RustBuffer;

type InvokerFunction = extern "C" fn(u64, *const u8) -> RustBuffer;

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
