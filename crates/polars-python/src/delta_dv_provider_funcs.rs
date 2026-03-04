use polars_core::frame::DataFrame;
use polars_error::{PolarsError, PolarsResult};
use polars_plan::plans::python_df_to_rust;
use polars_utils::pl_str::PlSmallStr;
use polars_utils::python_function::PythonObject;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyAnyMethods;
use pyo3::{Python, intern};

use crate::dataframe::PyDataFrame;
use crate::prelude::Wrap;

// kdn TODO REVIEW
pub fn name(callback: &PythonObject) -> PlSmallStr {
    Python::attach(|py| {
        pyo3::PyResult::Ok(PlSmallStr::from_str(
            &callback
                .getattr(py, intern!(py, "__class__"))?
                .getattr(py, intern!(py, "__name__"))?
                .extract::<PyBackedStr>(py)?,
        ))
    })
    .unwrap()
}

pub fn call(callback: &PythonObject) -> PolarsResult<DataFrame> {
    Python::attach(|py| {
        let result_wrapped = callback.getattr(py, intern!(py, "__call__"))?.call0(py)?;

        // unpack the wrapper in a PyDataFrame
        let py_pydf = result_wrapped.getattr(py, "_df").map_err(|_| {
            let pytype = result_wrapped.bind(py).get_type();
            PolarsError::ComputeError(
                format!("Expected the call to deletion_vectors() to return a 'DataFrame', got a '{pytype}'",)
                    .into(),
            )
        })?;
        // Downcast to Rust
        match py_pydf.extract::<PyDataFrame>(py) {
            Ok(pydf) => {
                dbg!("match arm Ok(pydf)");
                Ok(pydf.df.into_inner())
            },
            Err(_) => {
                //kdn TODO TBD - should we try or simply propagate the error?
                dbg!("match arm Err(_)");
                python_df_to_rust(py, result_wrapped.into_bound(py))
            },
        }
        // Ok(pydf.df.into_inner()) //kdn TODO RM
    })
}
