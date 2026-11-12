"""
SQL dialect probe corpus.

`probes.sql` holds short statements that users coming from DuckDB, Postgres or
Spark SQL commonly write. `test_dialect_probes.py` runs every probe on Polars and
checks the outcome against a recorded baseline. `reference.json` records what the
reference engines do with the same statements; `generate_reference.py`
regenerates it.

A feature probe only records whether the statement runs. A semantics probe also
records the first value of the first result column.
"""

from __future__ import annotations

from datetime import date, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any, NamedTuple

import polars as pl

# Probes that read files use this placeholder for the data directory.
DATA_PLACEHOLDER = "{DATA}"


class Probe(NamedTuple):
    """One statement of the corpus."""

    id: str
    kind: str  # "common", "duckdb", "spark" or "semantics"
    sql: str

    @property
    def reads_files(self) -> bool:
        return DATA_PLACEHOLDER in self.sql


def tables() -> dict[str, pl.DataFrame]:
    """The tables every probe can refer to."""
    return {
        "t": pl.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, None],
                "b": ["x", "y", "x", "z", None, "y"],
                "c": [1.5, 2.5, None, 4.0, 5.5, 6.0],
                "d": [date(2026, 1, i) for i in range(1, 7)],
                "ts": [datetime(2026, 1, 1, i, 30) for i in range(6)],
                "flag": [True, False, True, None, False, True],
            }
        ),
        "u": pl.DataFrame({"b": ["x", "y", "w"], "w": [10, 20, 30]}),
        "emp": pl.DataFrame(
            {
                "dept": ["eng", "eng", "ops", "ops", "ops"],
                "name": ["ann", "bob", "cat", "dan", "eve"],
                "salary": [100, 120, 90, 95, 90],
                "year": [2025, 2026, 2025, 2026, 2026],
            }
        ),
        "nested": pl.DataFrame(
            {
                "id": [1, 2, 3],
                "arr": [[1, 2, 3], [4], []],
                "tags": [["a", "b"], ["c"], []],
                "st": [
                    {"p": 1, "q": "u"},
                    {"p": 2, "q": "v"},
                    {"p": 3, "q": "w"},
                ],
            }
        ),
        "quotes": pl.DataFrame(
            {
                "sym": ["A", "A", "B"],
                "ts": [
                    datetime(2026, 1, 1, 9, 0),
                    datetime(2026, 1, 1, 10, 0),
                    datetime(2026, 1, 1, 9, 30),
                ],
                "price": [10.0, 11.0, 20.0],
            }
        ),
        "trades": pl.DataFrame(
            {
                "sym": ["A", "B"],
                "ts": [datetime(2026, 1, 1, 10, 15), datetime(2026, 1, 1, 9, 45)],
                "qty": [5, 7],
            }
        ),
    }


def write_data_files(directory: Path) -> None:
    """Write the files that the file-reading probes expect into `directory`."""
    frames = tables()
    for name, df in frames.items():
        df.write_parquet(directory / f"{name}.parquet")
    frames["t"].select("a", "b", "c").write_csv(directory / "t_csv.csv")
    (directory / "glob").mkdir(exist_ok=True)
    for i in range(3):
        pl.DataFrame({"g": [i, i]}).write_parquet(
            directory / "glob" / f"part{i}.parquet"
        )


def canonical(value: Any) -> str:
    """
    A value as text, so results of different engines can be compared.

    Numbers lose their type (`3`, `3.0` and `Decimal("3.00")` all give `3`), so
    only a difference in value shows up.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:.12g}"


def load_probes() -> list[Probe]:
    """Read `probes.sql`: a `-- probe: <id> <kind>` line, then the statement."""
    probes = []
    lines = (
        (Path(__file__).parent / "probes.sql").read_text(encoding="utf-8").splitlines()
    )
    for header, statement in pairwise(lines):
        if header.startswith("-- probe: "):
            probe_id, kind = header.removeprefix("-- probe: ").split()
            probes.append(Probe(probe_id, kind, statement.removesuffix(";")))
    return probes


PROBES = load_probes()
FEATURES = [p for p in PROBES if p.kind != "semantics"]
SEMANTICS = [p for p in PROBES if p.kind == "semantics"]
