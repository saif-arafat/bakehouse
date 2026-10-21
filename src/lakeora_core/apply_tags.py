from __future__ import annotations

import argparse
import builtins
import importlib
import sys
import types
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from lakeora_core.table_schema import (
    TableSchema,
    _quote,
    schema_prefix,
    set_schema_prefix,
)

LAYERS = ("bronze", "silver", "gold")
PACKAGE = "lakeora"

# information_schema.tables.table_type -> the object type ALTER expects
_ALTER_KIND = {
    "STREAMING_TABLE": "STREAMING TABLE",
    "MATERIALIZED_VIEW": "MATERIALIZED VIEW",
    "MANAGED": "TABLE",
    "EXTERNAL": "TABLE",
}


def _noop(*args, **kwargs):
    """Stands in for every pipelines API: @dp.table, @dp.table(...), dp.create_streaming_table(...), ..."""
    if len(args) == 1 and callable(args[0]) and not kwargs:
        return args[0]
    return lambda f: f


class _NoopPipelines(types.ModuleType):
    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        return _noop


@contextmanager
def _without_pipelines(spark=None) -> Iterator[None]:
    """Make `from pyspark import pipelines as dp` and `import dlt` importable outside a pipeline."""
    import pyspark

    stub = _NoopPipelines("pyspark.pipelines")
    saved_modules = {name: sys.modules.get(name) for name in ("pyspark.pipelines", "dlt")}
    saved_attr = pyspark.__dict__.get("pipelines")
    had_spark = hasattr(builtins, "spark")
    sys.modules["pyspark.pipelines"] = sys.modules["dlt"] = stub
    pyspark.pipelines = stub
    if spark is not None and not had_spark:
        builtins.spark = spark  # pipelines provide a global `spark`; some sources use it at import time
    try:
        yield
    finally:
        for name, module in saved_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module
        if saved_attr is None:
            pyspark.__dict__.pop("pipelines", None)
        else:
            pyspark.pipelines = saved_attr
        if spark is not None and not had_spark:
            del builtins.spark


def _source_modules(layers: tuple[str, ...]) -> list[str]:
    root = Path(importlib.import_module(PACKAGE).__file__).parent
    names = []
    for layer in layers:
        for path in sorted((root / layer).rglob("*.py")):
            rel = path.relative_to(root).with_suffix("")
            if "__pycache__" in rel.parts:
                continue
            parts = [p for p in rel.parts if p != "__init__"]
            names.append(".".join([PACKAGE, *parts]))
    return names


def collect_schemas(layers: tuple[str, ...] = LAYERS, spark=None) -> list[TableSchema]:
    """Every TableSchema defined at module level in the pipeline sources, in file order."""
    found: dict[int, TableSchema] = {}
    with _without_pipelines(spark):
        for name in _source_modules(layers):
            module = importlib.import_module(name)
            for value in vars(module).values():
                if isinstance(value, TableSchema):
                    found.setdefault(id(value), value)
    return list(found.values())


def _table_kind(spark, t: TableSchema) -> str | None:
    """The ALTER object type of the existing table, or None if it doesn't exist."""
    rows = spark.sql(
        f"SELECT table_type FROM {_quote(t.catalog)}.information_schema.tables "
        "WHERE table_schema = :schema AND table_name = :table",
        args={"schema": schema_prefix() + t.schema, "table": t.table},
    ).collect()
    if not rows:
        return None
    return _ALTER_KIND.get(rows[0].table_type, "TABLE")


def apply_tags(spark, schemas: list[TableSchema], dry_run: bool = False) -> int:
    """Run tag_sql() for each schema; returns the number of statements run."""
    count = 0
    for t in schemas:
        if not (t.tags or any(c.tags for c in t.columns)):
            continue
        if not (t.catalog and t.schema):
            print(f"skip {t.full_name}: tagging needs a catalog and schema")
            continue
        kind = _table_kind(spark, t)
        if kind is None:
            print(f"skip {t.full_name}: table doesn't exist (has the pipeline run?)")
            continue
        for stmt in t.tag_sql(kind):
            print(stmt)
            if not dry_run:
                spark.sql(stmt)
            count += 1
    return count


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--schema-prefix",
        default="",
        help='dev schema prefix, e.g. "user_"; "" for prod',
    )
    parser.add_argument("--layers", nargs="+", default=list(LAYERS))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the statements without running them",
    )
    args = parser.parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    set_schema_prefix(args.schema_prefix)
    schemas = collect_schemas(tuple(args.layers), spark)
    print(f"found {len(schemas)} table schemas")
    n = apply_tags(spark, schemas, dry_run=args.dry_run)
    print(f"{'would run' if args.dry_run else 'ran'} {n} tag statements")


if __name__ == "__main__":
    main()
