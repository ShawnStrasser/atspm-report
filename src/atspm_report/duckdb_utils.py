"""Small helpers for running the report's SQL on DuckDB.

Every input the package accepts is either a pandas DataFrame or a DuckDB
relation (``duckdb.DuckDBPyRelation``). Relations are read into pandas once,
then each step runs as plain SQL on a private in-memory DuckDB connection, so
nothing here touches the caller's connection beyond reading the relation.
"""

from typing import Optional, Union

import duckdb
import pandas as pd

TableLike = Union[pd.DataFrame, duckdb.DuckDBPyRelation]


def is_relation(data) -> bool:
    return isinstance(data, duckdb.DuckDBPyRelation)


def to_pandas(data) -> Optional[pd.DataFrame]:
    """Return a pandas DataFrame for a DataFrame or DuckDB relation (None stays None)."""
    if data is None or isinstance(data, pd.DataFrame):
        return data
    if is_relation(data):
        return data.df()
    return data


def is_empty(data) -> bool:
    """True when data is None, an empty DataFrame, or a relation with no rows."""
    if data is None:
        return True
    if isinstance(data, pd.DataFrame):
        return data.empty
    if is_relation(data):
        # fetchall() reads the whole (one-row) result, so the connection is left idle.
        return len(data.limit(1).fetchall()) == 0
    return True


def row_count(data) -> int:
    if isinstance(data, pd.DataFrame):
        return len(data)
    if is_relation(data):
        return data.aggregate('count(*)').fetchall()[0][0]
    return 0


def columns(data) -> list:
    if isinstance(data, pd.DataFrame):
        return list(data.columns)
    if is_relation(data):
        return list(data.columns)
    return []


def run_sql(sql: str, params: Optional[dict] = None, single_thread: bool = False,
            **tables) -> pd.DataFrame:
    """Run sql on a fresh in-memory connection with each keyword registered as a table.

    params fills $name placeholders in sql. single_thread makes floating-point
    aggregates (AVG, STDDEV, windowed SUM) add up in the same order every run,
    so results are repeatable to the last bit; use it for small inputs.
    """
    con = duckdb.connect()
    try:
        if single_thread:
            con.execute("SET threads = 1")
        for name, data in tables.items():
            con.register(name, to_pandas(data))
        return con.execute(sql, params or None).df()
    finally:
        con.close()


def quote(name: str) -> str:
    """Quote a SQL identifier."""
    return '"' + str(name).replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """Quote a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def query_table(data, sql: str, name: str = 'raw') -> pd.DataFrame:
    """Run sql with data (a DataFrame or DuckDB relation) as the table called name: a relation on its own connection, so
    only the result is read; a DataFrame copied into a table of a private in-memory connection first (a query that reads
    the frame many times runs a hundred times slower on the frame itself)."""
    if is_relation(data):
        return data.query(name, sql).df()
    con = duckdb.connect()
    try:
        con.execute("SET threads = 4")
        con.register('_frame', data)
        con.execute(f"CREATE TABLE {quote(name)} AS SELECT * FROM _frame")
        con.unregister('_frame')
        return con.execute(sql).df()
    finally:
        con.close()
