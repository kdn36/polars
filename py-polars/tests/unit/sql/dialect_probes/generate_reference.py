"""
Regenerate `reference.json`: what DuckDB, Postgres and Spark do with each probe.

This is run by hand, not by the test suite. It needs `duckdb`, `pyspark` (and a
Java runtime) and `psycopg`, plus a Postgres server with an empty, throwaway
database, because the probe tables are created in it. From `py-polars`::

    python -m tests.unit.sql.dialect_probes.generate_reference \
        --postgres "host=localhost port=5432 dbname=probes user=..."

Engines can be selected with `--engines`; results of the other engines are kept
from the existing file.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from tests.unit.sql.dialect_probes.corpus import (
    DATA_PLACEHOLDER,
    FEATURES,
    PROBES,
    SEMANTICS,
    canonical,
    tables,
    write_data_files,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from polars._typing import PolarsDataType

REFERENCE_PATH = Path(__file__).parent / "reference.json"
ENGINES = ("duckdb", "postgres", "spark")
ERROR = "<error>"
NO_ROWS = "<no rows>"

# The outcome of one probe: `None` if the statement failed, otherwise the first
# row of the result (an empty tuple if there are no rows).
Outcome = tuple[Any, ...] | None


def run_duckdb(data: Path) -> Iterator[tuple[str, Outcome]]:
    import duckdb

    con = duckdb.connect()
    for name in tables():
        path = (data / f"{name}.parquet").as_posix()
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{path}')")
    yield "_version", (duckdb.__version__,)
    for probe in PROBES:
        timer = threading.Timer(30, con.interrupt)
        timer.start()
        try:
            relation = con.sql(probe.sql.replace(DATA_PLACEHOLDER, data.as_posix()))
            rows = [] if relation is None else relation.fetchall()
            yield probe.id, (rows[0] if rows else ())
        except Exception:
            yield probe.id, None
        finally:
            timer.cancel()


def run_spark(data: Path) -> Iterator[tuple[str, Outcome]]:
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    spark = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", (data / "spark-warehouse").as_posix())
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        for name in tables():
            path = (data / f"{name}.parquet").as_posix()
            spark.read.parquet(path).createOrReplaceTempView(name)
        yield "_version", (spark.version,)
        for probe in PROBES:
            try:
                sql = probe.sql.replace(DATA_PLACEHOLDER, data.as_posix())
                rows = spark.sql(sql).take(1)
                yield probe.id, (tuple(rows[0]) if rows else ())
            except Exception:
                yield probe.id, None
    finally:
        spark.stop()


def _postgres_type(dtype: PolarsDataType, composite_name: str) -> str:
    if isinstance(dtype, pl.List):
        return _postgres_type(dtype.inner, composite_name) + "[]"
    if isinstance(dtype, pl.Struct):
        return composite_name
    if isinstance(dtype, pl.Datetime):
        return "TIMESTAMP"
    return {
        "Int64": "BIGINT",
        "Float64": "DOUBLE PRECISION",
        "String": "TEXT",
        "Boolean": "BOOLEAN",
        "Date": "DATE",
    }[str(dtype)]


def run_postgres(data: Path, conninfo: str) -> Iterator[tuple[str, Outcome]]:
    import psycopg  # type: ignore[import-not-found]

    with psycopg.connect(conninfo, autocommit=True) as con:
        for name, df in tables().items():
            columns, placeholders = [], []
            for column, dtype in df.schema.items():
                composite = f"{name}_{column}_t"
                if isinstance(dtype, pl.Struct):
                    fields = ", ".join(
                        f"{f.name} {_postgres_type(f.dtype, '')}" for f in dtype.fields
                    )
                    con.execute(f"CREATE TYPE {composite} AS ({fields})")
                    values = ", ".join(["%s"] * len(dtype.fields))
                    placeholders.append(f"ROW({values})::{composite}")
                else:
                    placeholders.append("%s")
                columns.append(f"{column} {_postgres_type(dtype, composite)}")
            con.execute(f"CREATE TABLE {name} ({', '.join(columns)})")
            rows = []
            for row in df.iter_rows():
                flat: list[Any] = []
                for value, dtype in zip(row, df.schema.values(), strict=True):
                    if isinstance(dtype, pl.Struct):
                        flat.extend(value[f.name] for f in dtype.fields)
                    else:
                        flat.append(value)
                rows.append(flat)
            with con.cursor() as cursor:
                cursor.executemany(
                    f"INSERT INTO {name} VALUES ({', '.join(placeholders)})", rows
                )
        con.execute("SET statement_timeout = '30s'")
        version = con.execute("SHOW server_version").fetchone()
        yield "_version", (str(version[0]).split()[0],) if version else ("unknown",)
        for probe in PROBES:
            try:
                with con.cursor() as cursor:
                    # No parameters are passed, so `%` in the SQL is taken literally.
                    cursor.execute(probe.sql.replace(DATA_PLACEHOLDER, data.as_posix()))
                    if cursor.description is None:
                        yield probe.id, ()
                    else:
                        row = cursor.fetchone()
                        yield probe.id, (() if row is None else row)
            except Exception:
                yield probe.id, None


def _semantic_value(outcome: Outcome) -> str:
    if outcome is None:
        return ERROR
    if not outcome:
        return NO_ROWS
    return canonical(outcome[0])


def _inline(mapping: dict[str, Any]) -> str:
    items = ", ".join(f"{json.dumps(k)}: {json.dumps(v)}" for k, v in mapping.items())
    return f"{{ {items} }}"


def _format(reference: dict[str, Any]) -> str:
    """One probe per line (in the layout `dprint` keeps), for readable diffs."""
    lines = ["{", f'  "engines": {_inline(reference["engines"])},']
    for section in ("features", "semantics"):
        lines.append(f'  "{section}": {{')
        entries = [
            f"    {json.dumps(probe_id)}: {_inline(result)}"
            for probe_id, result in reference[section].items()
        ]
        lines.append(",\n".join(entries))
        lines.append("  }," if section == "features" else "  }")
    lines.append("}")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--engines", default=",".join(ENGINES))
    parser.add_argument(
        "--postgres",
        metavar="CONNINFO",
        help="libpq connection string of an empty, throwaway database",
    )
    args = parser.parse_args()
    engines = args.engines.split(",")
    if unknown := set(engines) - set(ENGINES):
        parser.error(f"unknown engines: {sorted(unknown)}")
    # Never fall back to libpq's defaults: they could reach a database that is in use.
    if "postgres" in engines and "dbname=" not in (args.postgres or ""):
        parser.error("--postgres must name the throwaway database (dbname=...)")

    reference: dict[str, Any] = {"engines": {}, "features": {}, "semantics": {}}
    if REFERENCE_PATH.exists():
        reference = json.loads(REFERENCE_PATH.read_text(encoding="utf-8"))

    with tempfile.TemporaryDirectory() as tmp:
        data = Path(tmp)
        write_data_files(data)
        runners: dict[str, Callable[[], Iterator[tuple[str, Outcome]]]] = {
            "duckdb": lambda: run_duckdb(data),
            "postgres": lambda: run_postgres(data, args.postgres),
            "spark": lambda: run_spark(data),
        }
        outcomes = {engine: dict(runners[engine]()) for engine in engines}

    for engine, by_probe in outcomes.items():
        version = by_probe.pop("_version")
        reference["engines"][engine] = version[0] if version else "unknown"
        for probe in FEATURES:
            result = reference["features"].setdefault(probe.id, {})
            result[engine] = by_probe[probe.id] is not None
        for probe in SEMANTICS:
            result = reference["semantics"].setdefault(probe.id, {})
            result[engine] = _semantic_value(by_probe[probe.id])
        accepted = sum(by_probe[probe.id] is not None for probe in FEATURES)
        print(f"{engine}: accepts {accepted} of {len(FEATURES)} feature probes")

    # Keep the corpus order, and drop probes that no longer exist.
    reference = {
        "engines": dict(sorted(reference["engines"].items())),
        "features": {p.id: reference["features"][p.id] for p in FEATURES},
        "semantics": {p.id: reference["semantics"][p.id] for p in SEMANTICS},
    }
    for section in ("features", "semantics"):
        for probe_id, result in reference[section].items():
            reference[section][probe_id] = dict(sorted(result.items()))
    REFERENCE_PATH.write_text(_format(reference), encoding="utf-8")


if __name__ == "__main__":
    main()
