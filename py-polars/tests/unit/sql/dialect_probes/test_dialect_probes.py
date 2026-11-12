"""
Differential test of Polars SQL against DuckDB, Postgres and Spark SQL.

Every probe of the corpus runs on Polars and its outcome is compared with a
baseline recorded in this file. The baseline is strict in both directions, so:

- when a probe starts to run, remove it from `UNSUPPORTED` (and from
  `PORTABLE_GAPS` if it is listed there);
- when a semantics probe gives another value, update `POLARS_SEMANTICS` (and
  `DIVERGES_FROM_REFERENCES` if it is listed there).

What the reference engines do with each probe is recorded in `reference.json`.
The reference engines are not needed to run this test.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import polars as pl
from polars.exceptions import PolarsError
from tests.unit.sql.dialect_probes.corpus import (
    DATA_PLACEHOLDER,
    FEATURES,
    SEMANTICS,
    canonical,
    tables,
    write_data_files,
)

if TYPE_CHECKING:
    from tests.unit.sql.dialect_probes.corpus import Probe

REFERENCE = json.loads(
    (Path(__file__).parent / "reference.json").read_text(encoding="utf-8")
)
ERROR = "<error>"
NO_ROWS = "<no rows>"

# Feature probes that do not run on Polars.
UNSUPPORTED: set[str] = {
    # Common SQL.
    "lateral_subquery",
    "window_range_frame",
    "regexp_extract",
    "bool_and_or",
    "approx_count_distinct",
    "max_by_min_by",
    "count_if",
    "any_value",
    "lateral_column_alias",
    "create_temp_view",
    "describe_table",
    "information_schema",
    "md5_hash",
    "typeof",
    "pivot_std",
    "unpivot_std",
    # DuckDB idioms.
    "file_as_table",
    "read_csv_option",
    "columns_agg",
    "struct_literal",
    "list_comprehension",
    "list_transform_lambda",
    "list_transform_lambda_kw",
    "list_filter_lambda",
    "list_fns",
    "string_slice",
    "unnest_struct",
    "pivot_duckdb",
    "unpivot_duckdb",
    "asof_join",
    "positional_join",
    "summarize",
    "date_diff_part",
    "arg_max",
    "list_agg_fn",
    "regexp_matches",
    "string_split",
    "json_arrow",
    "using_sample",
    "limit_percent",
    "copy_to",
    "create_macro",
    "where_select_alias",
    # Spark SQL idioms.
    "lateral_view_explode",
    "explode_select",
    "posexplode",
    "named_struct",
    "array_fn",
    "map_fn",
    "hof_transform",
    "hof_filter",
    "hof_aggregate",
    "hof_exists",
    "array_fns_spark",
    "sequence_fn",
    "collect_list_set",
    "if_nvl",
    "date_fns_spark",
    "to_date_pattern",
    "date_format",
    "unix_time",
    "string_fns_spark",
    "div_operator",
    "percentile_approx",
    "stack_fn",
    "distribute_sort_by",
    "pipe_syntax",
    "identifier_clause",
    "tablesample_percent",
    "show_columns",
    "describe_table_kw",
}

# The unsupported probes that DuckDB, Postgres and Spark all accept: portable SQL
# that Polars lacks.
PORTABLE_GAPS: set[str] = {
    "lateral_subquery",
    "window_range_frame",
    "bool_and_or",
    "any_value",
    "create_temp_view",
    "md5_hash",
}

# The value each semantics probe gives on Polars, as `canonical` text.
POLARS_SEMANTICS: dict[str, str] = {
    "int_division": "3.5",
    "nulls_order_asc": "1",
    "nulls_order_desc": "null",
    "concat_with_null": "a",
    "pipe_concat_null": "null",
    "int_div_by_zero": "inf",
    "float_div_by_zero": "inf",
    "bigint_overflow": "-9223372036854775808",
    "int32_literal_overflow": "2147483648",
    "bad_cast": ERROR,
    "column_case": ERROR,
    "table_case": "1",
    "round_half_pos": "3",
    "round_half_neg": "-3",
    "round_decimal_literal": "0.13",
    "decimal_literal_eq": "true",
    "string_order_binary": "false",
    "implicit_string_vs_int": ERROR,
    "date_minus_date": "28 days, 0:00:00",
    "extract_dow_sunday": "0",
    "array_index_1": "1",
    "substring_pos_0": "h",
    "like_case": "false",
    "mod_negative": "-1",
    "union_int_string": "1",
    "where_int_as_bool": "6",
    "nan_max": ERROR,
    "avg_int_type": "3",
    "count_star_vs_col": "5",
    "empty_string_vs_null": "0",
}

# The semantics probes on which DuckDB, Postgres and Spark agree with each other
# and Polars gives something else.
DIVERGES_FROM_REFERENCES: set[str] = {
    "bigint_overflow",
    "int32_literal_overflow",
    "column_case",
    "implicit_string_vs_int",
}


# Probes whose lazy schema differs from the schema of the collected result.
# TODO: `2147483647 + 1` resolves to Int32 in the lazy schema but collects as Int64.
LAZY_SCHEMA_MISMATCH = {"int32_literal_overflow"}


@pytest.fixture(scope="module")
def data_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp("dialect_probes")
    write_data_files(directory)
    return directory


def _params(probes: list[Probe]) -> list[pytest.ParameterSet]:
    params = []
    for probe in probes:
        marks = []
        if probe.reads_files:
            marks.append(pytest.mark.write_disk)
        if probe.id in LAZY_SCHEMA_MISMATCH:
            marks.append(pytest.mark.may_fail_lazy_schema)
        params.append(pytest.param(probe, id=probe.id, marks=marks))
    return params


def _run(probe: Probe, request: pytest.FixtureRequest) -> pl.DataFrame:
    sql = probe.sql
    if probe.reads_files:
        directory: Path = request.getfixturevalue("data_dir")
        sql = sql.replace(DATA_PLACEHOLDER, directory.as_posix())
    with pl.SQLContext(tables(), eager=True) as ctx:
        return ctx.execute(sql)


@pytest.mark.parametrize("probe", _params(FEATURES))
def test_feature_probe(probe: Probe, request: pytest.FixtureRequest) -> None:
    if probe.id not in UNSUPPORTED:
        _run(probe, request)
        return
    try:
        _run(probe, request)
    except PolarsError:
        return
    pytest.fail(f"`{probe.id}` runs now: remove it from `UNSUPPORTED`")


@pytest.mark.parametrize("probe", _params(SEMANTICS))
def test_semantics_probe(probe: Probe, request: pytest.FixtureRequest) -> None:
    try:
        result = _run(probe, request)
    except PolarsError:
        value = ERROR
    else:
        value = NO_ROWS if result.is_empty() else canonical(result.row(0)[0])
    assert value == POLARS_SEMANTICS[probe.id]


def test_portable_gaps() -> None:
    accepted_by_all_references = {
        probe_id
        for probe_id, accepted in REFERENCE["features"].items()
        if all(accepted.values())
    }
    assert UNSUPPORTED & accepted_by_all_references == PORTABLE_GAPS


def test_divergences_from_references() -> None:
    diverges = set()
    for probe_id, values in REFERENCE["semantics"].items():
        reference_values = set(values.values())
        if len(reference_values) == 1:
            if POLARS_SEMANTICS[probe_id] not in reference_values:
                diverges.add(probe_id)
    assert diverges == DIVERGES_FROM_REFERENCES


def test_baseline_matches_corpus() -> None:
    feature_ids = {probe.id for probe in FEATURES}
    semantics_ids = {probe.id for probe in SEMANTICS}
    assert len(feature_ids) == len(FEATURES)
    assert len(semantics_ids) == len(SEMANTICS)
    assert feature_ids >= UNSUPPORTED
    assert set(POLARS_SEMANTICS) == semantics_ids
    assert set(REFERENCE["features"]) == feature_ids
    assert set(REFERENCE["semantics"]) == semantics_ids
