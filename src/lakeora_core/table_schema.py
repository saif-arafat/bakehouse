"""
Define Lakeflow pipeline table schemas (columns, PKs, FKs) as Python objects
and render them to the DDL string expected by `schema=` in
@dp.table / @dp.materialized_view.

Column types are PySpark DataTypes (StringType(), DecimalType(18, 2), ...),
and table names are given as separate catalog / schema / table parts.

Example
-------
    from pyspark import pipelines as dp
    from pyspark.sql.types import LongType, StringType, DecimalType
    from table_schema import Column, ForeignKey, TableSchema

    dim_customer = TableSchema(
        catalog="main", schema="gold", table="dim_customer",
        columns=[
            Column("customer_key", LongType()),
            Column("customer_id", StringType()),
            Column("customer_name", StringType(), comment="Display name",
                   tags={"pii": "name"}),
        ],
        primary_key="customer_key",
        tags={"domain": "sales"},
    )

    fact_orders = TableSchema(
        catalog="main", schema="gold", table="fact_orders",
        columns=[
            Column("order_id", StringType()),
            Column("customer_key", LongType()),
            Column("amount", DecimalType(18, 2)),
        ],
        primary_key="order_id",
        foreign_keys=[ForeignKey.to(dim_customer, "customer_key")],
    )

    @dp.materialized_view(name=fact_orders.full_name, schema=fact_orders.ddl())
    def fact_orders_mv():
        return fact_orders.conform(spark.read.table("orders_silver"))

Tags (Column(tags=...), TableSchema(tags=...)) are Unity Catalog tags. They
aren't part of the DDL; apply them after the pipeline runs with tag_sql():

    for stmt in dim_customer.tag_sql("MATERIALIZED VIEW"):
        spark.sql(stmt)
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pyspark.sql.types import DataType, StructField, StructType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

_SIMPLE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote(ident: str) -> str:
    """Backtick-quote an identifier only when it needs it."""
    if _SIMPLE_IDENT.match(ident):
        return ident
    return "`" + ident.replace("`", "``") + "`"


def _as_tuple(cols: str | Sequence[str] | None) -> tuple[str, ...]:
    if cols is None:
        return ()
    if isinstance(cols, str):
        return (cols,)
    return tuple(cols)


def _sql_string(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _check_name_part(label: str, value: str | None) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string, got {value!r}")
    if "." in value:
        raise ValueError(f"{label}={value!r} contains a dot; pass catalog, schema and table as separate arguments")


def _check_tags(owner: str, tags: object) -> None:
    if not isinstance(tags, dict):
        raise TypeError(f"{owner}: tags must be a dict of str -> str, got {tags!r}")
    for k, v in tags.items():
        if not isinstance(k, str) or not k.strip():
            raise ValueError(f"{owner}: tag names must be non-empty strings, got {k!r}")
        if not isinstance(v, str):
            raise TypeError(f"{owner}: tag {k!r} value must be a string, got {v!r}")


def _tags_clause(tags: dict[str, str]) -> str:
    pairs = ", ".join(f"{_sql_string(k)} = {_sql_string(v)}" for k, v in tags.items())
    return f"SET TAGS ({pairs})"


_TAGGABLE_KINDS = ("TABLE", "MATERIALIZED VIEW", "STREAMING TABLE")

SCHEMA_PREFIX_CONF = "lakeora.schema_prefix"

_schema_prefix_override: str | None = None


def set_schema_prefix(prefix: str | None) -> None:
    """Set the schema prefix outside a pipeline (e.g. in a job); None goes back to the Spark conf."""
    global _schema_prefix_override
    _schema_prefix_override = prefix


def schema_prefix() -> str:
    """Prefix put in front of every schema name, e.g. "user_" in dev, "" in prod.

    Read from the `lakeora.schema_prefix` pipeline configuration (see
    resources/lakeora_pipeline.py), so a dev pipeline writes to its own
    <user>_<schema> schemas instead of the prod ones. set_schema_prefix()
    takes precedence. "" outside a Spark session.
    """
    if _schema_prefix_override is not None:
        return _schema_prefix_override

    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        return ""
    return spark.conf.get(SCHEMA_PREFIX_CONF, "") or ""


def _sql_type(dtype: DataType) -> str:
    """SQL type name for a PySpark DataType, e.g. DecimalType(18,2) -> decimal(18,2)."""
    if not isinstance(dtype, DataType):
        raise TypeError(f"Column type must be a pyspark.sql.types.DataType instance (e.g. StringType()), got {dtype!r}")
    return dtype.simpleString()


@dataclass(frozen=True)
class Column:
    """A single column definition."""

    name: str
    dtype: DataType
    nullable: bool = True
    comment: str | None = None
    generated_as: str | None = None  # SQL expression, e.g. "CAST(order_ts AS DATE)"
    tags: dict[str, str] = field(default_factory=dict, hash=False)  # Unity Catalog tags, see TableSchema.tag_sql

    def __post_init__(self):
        _sql_type(self.dtype)  # fail fast on a bad type
        _check_tags(f"column {self.name!r}", self.tags)

    @classmethod
    def from_struct_field(cls, f: StructField, comment: str | None = None) -> Column:
        return cls(f.name, f.dataType, f.nullable, comment or (f.metadata or {}).get("comment"))

    def to_struct_field(self) -> StructField:
        meta = {"comment": self.comment} if self.comment else {}
        return StructField(self.name, self.dtype, self.nullable, meta)

    def ddl(self, force_not_null: bool = False) -> str:
        parts = [_quote(self.name), _sql_type(self.dtype)]
        if self.generated_as:
            parts.append(f"GENERATED ALWAYS AS ({self.generated_as})")
        if force_not_null or not self.nullable:
            parts.append("NOT NULL")
        if self.comment:
            parts.append(f"COMMENT {_sql_string(self.comment)}")
        return " ".join(parts)


@dataclass(frozen=True)
class ForeignKey:
    """A foreign key from this table's columns to another table's primary key."""

    columns: tuple[str, ...]
    ref: TableSchema
    ref_columns: tuple[str, ...]
    name: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "columns", _as_tuple(self.columns))
        object.__setattr__(self, "ref_columns", _as_tuple(self.ref_columns))
        if len(self.columns) != len(self.ref_columns):
            raise ValueError(f"FK column count mismatch: {self.columns} -> {self.ref_columns}")

    @classmethod
    def to(
        cls,
        ref: TableSchema,
        columns: str | Sequence[str],
        ref_columns: str | Sequence[str] | None = None,
        name: str | None = None,
    ) -> ForeignKey:
        """Build an FK pointing at another TableSchema.

        If ref_columns is omitted, the referenced table's primary key is used.
        """
        target = _as_tuple(ref_columns) or ref.primary_key
        if not target:
            raise ValueError(f"{ref.full_name} has no primary key to reference")
        missing = [c for c in target if c not in ref.column_names]
        if missing:
            raise ValueError(f"{ref.full_name} has no column(s) {missing}")
        if tuple(target) != ref.primary_key:
            raise ValueError(
                f"FK must reference the primary key of {ref.full_name} {ref.primary_key}, got {tuple(target)}"
            )
        return cls(_as_tuple(columns), ref, tuple(target), name)

    def ddl(self, default_name: str) -> str:
        cols = ", ".join(_quote(c) for c in self.columns)
        ref_cols = ", ".join(_quote(c) for c in self.ref_columns)
        return (
            f"CONSTRAINT {_quote(self.name or default_name)} "
            f"FOREIGN KEY ({cols}) "
            f"REFERENCES {self.ref.quoted_name}({ref_cols})"
        )


@dataclass(eq=False)
class TableSchema:
    """A table definition that renders to a Lakeflow `schema=` DDL string.

    catalog and schema are optional; leave them out to use the pipeline's
    default catalog/schema. Foreign keys that point at a table without a
    catalog and schema resolve against the pipeline default. schema is the
    prod schema name; full_name adds the dev prefix (see schema_prefix).
    """

    table: str
    columns: list[Column]
    catalog: str | None = None
    schema: str | None = None
    primary_key: str | Sequence[str] | None = None
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    pk_name: str | None = None
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)  # Unity Catalog tags, see tag_sql

    def __post_init__(self):
        _check_name_part("catalog", self.catalog)
        _check_name_part("schema", self.schema)
        _check_name_part("table", self.table)
        _check_tags(f"table {self.table!r}", self.tags)
        if self.catalog and not self.schema:
            raise ValueError("catalog given without schema")
        self.primary_key = _as_tuple(self.primary_key)
        self._validate()

    # ---- names ----------------------------------------------------------
    @property
    def _parts(self) -> list[str]:
        schema = schema_prefix() + self.schema if self.schema else None
        return [p for p in (self.catalog, schema, self.table) if p]

    @property
    def full_name(self) -> str:
        """catalog.schema.table (unquoted), for name= in the dp decorators.

        The schema gets the schema_prefix() of the running pipeline, if any.
        """
        return ".".join(self._parts)

    @property
    def quoted_name(self) -> str:
        """Same as full_name with each part backtick-quoted when needed."""
        return ".".join(_quote(p) for p in self._parts)

    # ---- columns --------------------------------------------------------
    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"{self.full_name} has no column {name!r}")

    @property
    def struct_type(self) -> StructType:
        """The columns as a PySpark StructType (PK columns marked non-nullable)."""
        return StructType(
            [
                StructField(
                    c.name,
                    c.dtype,
                    c.nullable and c.name not in self.primary_key,
                    {"comment": c.comment} if c.comment else {},
                )
                for c in self.columns
            ]
        )

    @classmethod
    def from_struct_type(cls, table: str, struct: StructType, **kwargs) -> TableSchema:
        """Build a TableSchema from an existing StructType."""
        return cls(
            table=table,
            columns=[Column.from_struct_field(f) for f in struct.fields],
            **kwargs,
        )

    def conform(self, df: DataFrame) -> DataFrame:
        """Select the declared columns, in order, cast to the declared types.

        Generated columns are skipped, since the table computes them.
        """
        from pyspark.sql import functions as F

        return df.select(*[F.col(c.name).cast(c.dtype).alias(c.name) for c in self.columns if not c.generated_as])

    # ---- keys -----------------------------------------------------------
    def add_foreign_key(
        self,
        ref: TableSchema,
        columns: str | Sequence[str],
        ref_columns: str | Sequence[str] | None = None,
        name: str | None = None,
    ) -> TableSchema:
        self.foreign_keys.append(ForeignKey.to(ref, columns, ref_columns, name))
        self._validate()
        return self

    def _validate(self) -> None:
        names = self.column_names
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"{self.full_name}: duplicate columns {sorted(dupes)}")
        missing_pk = [c for c in self.primary_key if c not in names]
        if missing_pk:
            raise ValueError(f"{self.full_name}: PK column(s) not defined {missing_pk}")
        for fk in self.foreign_keys:
            missing = [c for c in fk.columns if c not in names]
            if missing:
                raise ValueError(f"{self.full_name}: FK column(s) not defined {missing}")
            for mine, theirs in zip(fk.columns, fk.ref_columns):
                a, b = self.column(mine).dtype, fk.ref.column(theirs).dtype
                if a != b:
                    raise ValueError(
                        f"{self.full_name}.{mine} is {a.simpleString()} but "
                        f"{fk.ref.full_name}.{theirs} is {b.simpleString()}"
                    )

    # ---- rendering ------------------------------------------------------
    def ddl(self, pretty: bool = True) -> str:
        """Render the string to pass as `schema=`.

        Pipelines require NOT NULL alongside PRIMARY KEY, so PK columns are
        always rendered NOT NULL. A single-column PK is written inline; a
        composite PK is written as a table-level constraint.
        """
        pk = self.primary_key
        lines: list[str] = []

        for col in self.columns:
            line = col.ddl(force_not_null=col.name in pk)
            if len(pk) == 1 and col.name == pk[0]:
                line += " PRIMARY KEY"
            lines.append(line)

        if len(pk) > 1:
            pk_cols = ", ".join(_quote(c) for c in pk)
            pk_name = self.pk_name or f"pk_{self.table}"
            lines.append(f"CONSTRAINT {_quote(pk_name)} PRIMARY KEY ({pk_cols})")

        for fk in self.foreign_keys:
            default = f"fk_{self.table}_{fk.ref.table}_{'_'.join(fk.columns)}"
            lines.append(fk.ddl(default))

        sep = ",\n  " if pretty else ", "
        body = sep.join(lines)
        return f"\n  {body}\n" if pretty else body

    def tag_sql(self, kind: str = "MATERIALIZED VIEW") -> list[str]:
        """ALTER statements that set this table's and its columns' Unity Catalog tags.

        Tags can't go in the `schema=` DDL, so run these after the pipeline has
        created the table, e.g. from a job task: `for s in t.tag_sql(): spark.sql(s)`.
        kind is the object type: "MATERIALIZED VIEW", "STREAMING TABLE" or "TABLE".
        Without a catalog and schema the name resolves against the session's
        current catalog/schema. Tags are only set, never unset.
        """
        kind = kind.upper()
        if kind not in _TAGGABLE_KINDS:
            raise ValueError(f"kind must be one of {_TAGGABLE_KINDS}, got {kind!r}")
        prefix = f"ALTER {kind} {self.quoted_name}"
        stmts = [f"{prefix} {_tags_clause(self.tags)}"] if self.tags else []
        stmts += [f"{prefix} ALTER COLUMN {_quote(c.name)} {_tags_clause(c.tags)}" for c in self.columns if c.tags]
        return stmts

    def __str__(self) -> str:
        return self.ddl()


def schemas_in_dependency_order(tables: Iterable[TableSchema]) -> list[TableSchema]:
    """Order tables so every FK target comes before the table that references it.

    Raises on FK cycles.
    """
    by_name = {t.full_name: t for t in tables}
    ordered: list[TableSchema] = []
    state: dict[str, int] = {}  # 1 = visiting, 2 = done

    def visit(t: TableSchema) -> None:
        s = state.get(t.full_name)
        if s == 2:
            return
        if s == 1:
            raise ValueError(f"Foreign key cycle involving {t.full_name}")
        state[t.full_name] = 1
        for fk in t.foreign_keys:
            if fk.ref.full_name in by_name:
                visit(by_name[fk.ref.full_name])
        state[t.full_name] = 2
        ordered.append(t)

    for t in by_name.values():
        visit(t)
    return ordered
