//! Minimal PyO3 extension proving rules_rust_pyo3 produces an importable
//! native module inside Ray's tree under hybrid WORKSPACE + bzlmod.

use pyo3::prelude::*;

#[pymodule]
mod ray_probe_ext {

    use super::*;

    /// Sum two integers, returned as a string, to prove the FFI boundary works.
    #[pyfunction]
    fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
        Ok((a + b).to_string())
    }
}
