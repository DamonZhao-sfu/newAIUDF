pub mod expressions;
use pyo3::types::PyModule;
use pyo3::types::PyModuleMethods;
use pyo3::{pymodule, Bound, PyResult, Python};
use pyo3_polars::PolarsAllocator;
use expressions::QueryArgs;

#[global_allocator]
static ALLOC: PolarsAllocator = PolarsAllocator::new();

#[pymodule]
fn polars(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<QueryArgs>().unwrap();
    Ok(())
}