use std::sync::Arc;

use polars::prelude::default_values::DefaultFieldValues;
use polars::prelude::deletion::{
    DeletionFilesList, DeltaDeletionVectorCallback, DeltaDeletionVectorProvider,
};
use polars::prelude::{
    CastColumnsPolicy, CloudScheme, ColumnMapping, DeletionVectors, ExtraColumnsPolicy,
    MissingColumnsPolicy, PlSmallStr, Schema, TableStatistics, UnifiedScanArgs,
};
use polars_error::PolarsError;
use polars_io::{HiveOptions, RowIndex};
use polars_plan::plans::python_df_to_rust;
use polars_utils::IdxSize;
use polars_utils::slice_enum::Slice;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;

use crate::PyDataFrame;
use crate::io::cloud_options::OptPyCloudOptions;
use crate::prelude::Wrap;

/// Interface to `class ScanOptions` on the Python side
pub struct PyScanOptions<'py>(Bound<'py, PyAny>);

impl<'a, 'py> FromPyObject<'a, 'py> for PyScanOptions<'py> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        Ok(Self(ob.to_owned()))
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for Wrap<TableStatistics> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let attr = ob.getattr(intern!(py, "_df"))?;
        Ok(Wrap(TableStatistics(Arc::new(
            PyDataFrame::extract(attr.as_borrowed())?.df.into_inner(),
        ))))
    }
}

//kdn TODO
impl<'a, 'py> FromPyObject<'a, 'py> for Wrap<DeletionVectors> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let attr = ob.getattr(intern!(py, "_df"))?;
        Ok(Wrap(DeletionVectors(Arc::new(
            PyDataFrame::extract(attr.as_borrowed())?.df.into_inner(),
        ))))
    }
}

impl PyScanOptions<'_> {
    pub fn extract_unified_scan_args(
        &self,
        cloud_scheme: Option<CloudScheme>,
    ) -> PyResult<UnifiedScanArgs> {
        dbg!("start PyScanOptions::extract_unified_scan_args"); //kdn
        #[derive(FromPyObject)]
        struct Extract<'a> {
            row_index: Option<(Wrap<PlSmallStr>, IdxSize)>,
            pre_slice: Option<(i64, usize)>,
            cast_options: Wrap<CastColumnsPolicy>,
            extra_columns: Wrap<ExtraColumnsPolicy>,
            missing_columns: Wrap<MissingColumnsPolicy>,
            include_file_paths: Option<Wrap<PlSmallStr>>,
            glob: bool,
            hidden_file_prefix: Option<Vec<PyBackedStr>>,
            column_mapping: Option<Wrap<ColumnMapping>>,
            default_values: Option<Wrap<DefaultFieldValues>>,
            hive_partitioning: Option<bool>,
            hive_schema: Option<Wrap<Schema>>,
            try_parse_hive_dates: bool,
            rechunk: bool,
            cache: bool,
            storage_options: OptPyCloudOptions<'a>,
            credential_provider: Option<Py<PyAny>>,
            deletion_files: Option<Wrap<DeletionFilesList>>,
            deletion_vectors: Option<Wrap<DeletionVectors>>,
            // deletion_vector_callback: Option<DeltaDeletionVectorCallback>, //kdn TODO RM
            deletion_vector_callback: Option<Py<PyAny>>,
            // deletion_vector_provider: Option<Py<PyAny>>,
            table_statistics: Option<Wrap<TableStatistics>>,
            row_count: Option<(u64, u64)>,
        }

        let Extract {
            row_index,
            pre_slice,
            cast_options,
            extra_columns,
            missing_columns,
            include_file_paths,
            column_mapping,
            default_values,
            glob,
            hidden_file_prefix,
            hive_partitioning,
            hive_schema,
            try_parse_hive_dates,
            rechunk,
            cache,
            storage_options,
            credential_provider,
            deletion_files,
            deletion_vectors,
            deletion_vector_callback, //kdn TODO RM
            // deletion_vector_provider, //kdn TODO
            table_statistics,
            row_count,
        } = self.0.extract()?;

        dbg!(deletion_vectors.as_ref().map(|dv| dv.0.as_ref()));
        // dbg!(&deletion_vector_callback.is_some());

        let cloud_options =
            storage_options.extract_opt_cloud_options(cloud_scheme, credential_provider)?;

        let hive_schema = hive_schema.map(|s| Arc::new(s.0));

        let row_index = row_index.map(|(name, offset)| RowIndex {
            name: name.0,
            offset,
        });

        let hive_options = HiveOptions {
            enabled: hive_partitioning,
            hive_start_idx: 0,
            schema: hive_schema,
            try_parse_dates: try_parse_hive_dates,
        };

        // kdn TODO MOVE THIS
        // let deletion_vector_callback = deletion_vector_callback.map(|py_obj| {
        //     let py_obj = Arc::new(py_obj);
        //     DeltaDeletionVectorCallback(Arc::new(move || {
        //         Python::attach(|py| {
        //             let result_df_wrapper = py_obj.call0(py)?;
        //             // unpack the wrapper in a PyDataFrame
        //             let py_pydf = result_df_wrapper.getattr(py, "_df").map_err(|_| {
        //                 let pytype = result_df_wrapper.bind(py).get_type();
        //                 PolarsError::ComputeError(
        //                     format!("Expected the call to deletion_vectors() to return a 'DataFrame', got a '{pytype}'",)
        //                         .into(),
        //                 )
        //             })?;
        //             // Downcast to Rust
        //             match py_pydf.extract::<PyDataFrame>(py) {
        //                 Ok(pydf) => {
        //                     dbg!("match arm Ok(pydf)");
        //                     Ok(pydf.df.into_inner())
        //                 },
        //                 Err(_) => {
        //                     //kdn TODO TBD - should we try or simply propagate the error?
        //                     dbg!("match arm Err(_)");
        //                     python_df_to_rust(py, result_df_wrapper.into_bound(py))
        //                 },
        //             }
        //         })
        //     }))
        // });

        let deletion_vector_provider =
            deletion_vector_callback.map(|obj| DeltaDeletionVectorProvider::new(obj.into()));

        let unified_scan_args = UnifiedScanArgs {
            // Schema is currently still stored inside the options per scan type, but we do eventually
            // want to put it here instead.
            schema: None,
            cloud_options,
            hive_options,
            rechunk,
            cache,
            glob,
            hidden_file_prefix: hidden_file_prefix
                .map(|x| x.into_iter().map(|x| (*x).into()).collect()),
            projection: None,
            column_mapping: column_mapping.map(|x| x.0),
            default_values: default_values
                .map(|x| x.0)
                .filter(|DefaultFieldValues::Iceberg(v)| !v.is_empty()),
            row_index,
            pre_slice: pre_slice.map(Slice::from),
            cast_columns_policy: cast_options.0,
            missing_columns_policy: missing_columns.0,
            extra_columns_policy: extra_columns.0,
            include_file_paths: include_file_paths.map(|x| x.0),
            deletion_files: DeletionFilesList::filter_empty(deletion_files.map(|x| x.0)),
            // deletion_vectors: deletion_vectors.map(|x| x.0),
            deletion_vectors: None, //kdn TODO BLOCK
            // deletion_vector_callback, //kdn TODO RM
            deletion_vector_provider,
            table_statistics: table_statistics.map(|x| x.0),
            row_count,
        };

        Ok(unified_scan_args)
    }
}
