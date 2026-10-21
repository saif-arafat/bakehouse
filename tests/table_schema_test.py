"""Unit tests for lakeora_core.table_schema.

These tests don't need Spark compute. Run them on their own with:

    uv run pytest --noconftest tests/table_schema_test.py
"""

import pytest
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakeora_core import table_schema
from lakeora_core.table_schema import (
    Column,
    ForeignKey,
    TableSchema,
    _quote,
    _sql_string,
    schemas_in_dependency_order,
)

# ---- fixtures -----------------------------------------------------------


@pytest.fixture
def dim_customer() -> TableSchema:
    return TableSchema(
        catalog="main",
        schema="gold",
        table="dim_customer",
        columns=[
            Column("customer_key", LongType()),
            Column("customer_id", StringType()),
            Column("customer_name", StringType(), comment="Display name"),
        ],
        primary_key="customer_key",
    )


@pytest.fixture
def dim_date() -> TableSchema:
    return TableSchema(
        catalog="main",
        schema="gold",
        table="dim_date",
        columns=[Column("date_key", IntegerType()), Column("date", DateType())],
        primary_key="date_key",
    )


@pytest.fixture
def fact_orders(dim_customer) -> TableSchema:
    return TableSchema(
        catalog="main",
        schema="gold",
        table="fact_orders",
        columns=[
            Column("order_id", StringType()),
            Column("customer_key", LongType()),
            Column("amount", DecimalType(18, 2)),
        ],
        primary_key="order_id",
        foreign_keys=[ForeignKey.to(dim_customer, "customer_key")],
    )


def _table(name, columns, **kwargs) -> TableSchema:
    return TableSchema(table=name, columns=columns, **kwargs)


# ---- helpers ------------------------------------------------------------


class TestQuote:
    @pytest.mark.parametrize("ident", ["a", "_a", "customer_key", "A1_b2"])
    def test_simple_identifiers_are_not_quoted(self, ident):
        assert _quote(ident) == ident

    @pytest.mark.parametrize(
        "ident, expected",
        [
            ("my-table", "`my-table`"),
            ("1abc", "`1abc`"),
            ("has space", "`has space`"),
            ("weird`name", "`weird``name`"),
        ],
    )
    def test_other_identifiers_are_backtick_quoted(self, ident, expected):
        assert _quote(ident) == expected


class TestSqlString:
    def test_plain(self):
        assert _sql_string("hello") == "'hello'"

    def test_escapes_single_quote(self):
        assert _sql_string("it's") == r"'it\'s'"

    def test_escapes_backslash_before_quote(self):
        # The backslash must be escaped first, or it would swallow the quote escape.
        assert _sql_string("a\\'b") == r"'a\\\'b'"


# ---- Column -------------------------------------------------------------


class TestColumn:
    def test_minimal_ddl(self):
        assert Column("id", LongType()).ddl() == "id bigint"

    def test_decimal_type(self):
        assert Column("amount", DecimalType(18, 2)).ddl() == "amount decimal(18,2)"

    def test_not_null(self):
        assert Column("id", LongType(), nullable=False).ddl() == "id bigint NOT NULL"

    def test_force_not_null(self):
        assert Column("id", LongType()).ddl(force_not_null=True) == "id bigint NOT NULL"

    def test_comment_is_escaped(self):
        assert Column("n", StringType(), comment="it's").ddl() == r"n string COMMENT 'it\'s'"

    def test_generated_column(self):
        col = Column("order_date", DateType(), generated_as="CAST(order_ts AS DATE)")
        assert col.ddl() == "order_date date GENERATED ALWAYS AS (CAST(order_ts AS DATE))"

    def test_clause_order(self):
        col = Column("d", DateType(), nullable=False, comment="c", generated_as="x")
        assert col.ddl() == "d date GENERATED ALWAYS AS (x) NOT NULL COMMENT 'c'"

    def test_name_needing_quotes(self):
        assert Column("order-id", StringType()).ddl() == "`order-id` string"

    def test_rejects_type_class_instead_of_instance(self):
        with pytest.raises(TypeError, match="DataType instance"):
            Column("id", StringType)

    def test_rejects_string_type_name(self):
        with pytest.raises(TypeError):
            Column("id", "string")

    def test_is_frozen(self):
        col = Column("id", LongType())
        with pytest.raises(AttributeError):
            col.name = "other"

    def test_to_struct_field(self):
        f = Column("n", StringType(), nullable=False, comment="c").to_struct_field()
        assert f == StructField("n", StringType(), False, {"comment": "c"})

    def test_to_struct_field_without_comment_has_empty_metadata(self):
        assert Column("n", StringType()).to_struct_field().metadata == {}

    def test_from_struct_field_reads_comment_from_metadata(self):
        col = Column.from_struct_field(StructField("n", StringType(), False, {"comment": "c"}))
        assert col == Column("n", StringType(), nullable=False, comment="c")

    def test_from_struct_field_explicit_comment_wins(self):
        f = StructField("n", StringType(), True, {"comment": "old"})
        assert Column.from_struct_field(f, comment="new").comment == "new"

    def test_struct_field_round_trip(self):
        col = Column("amount", DecimalType(18, 2), nullable=False, comment="Total")
        assert Column.from_struct_field(col.to_struct_field()) == col


# ---- ForeignKey ---------------------------------------------------------


class TestForeignKey:
    def test_to_defaults_to_primary_key(self, dim_customer):
        fk = ForeignKey.to(dim_customer, "customer_key")
        assert fk.columns == ("customer_key",)
        assert fk.ref_columns == ("customer_key",)
        assert fk.ref is dim_customer

    def test_to_with_explicit_ref_columns(self, dim_customer):
        fk = ForeignKey.to(dim_customer, "cust_key", "customer_key")
        assert fk.columns == ("cust_key",)
        assert fk.ref_columns == ("customer_key",)

    def test_to_requires_ref_primary_key(self):
        ref = _table("no_pk", [Column("id", LongType())])
        with pytest.raises(ValueError, match="no primary key"):
            ForeignKey.to(ref, "id")

    def test_to_rejects_unknown_ref_column(self, dim_customer):
        with pytest.raises(ValueError, match="no column"):
            ForeignKey.to(dim_customer, "x", "nope")

    def test_to_rejects_non_pk_ref_column(self, dim_customer):
        with pytest.raises(ValueError, match="must reference the primary key"):
            ForeignKey.to(dim_customer, "customer_id", "customer_id")

    def test_to_rejects_pk_columns_in_wrong_order(self):
        ref = _table("r", [Column("a", LongType()), Column("b", LongType())], primary_key=["a", "b"])
        with pytest.raises(ValueError, match="must reference the primary key"):
            ForeignKey.to(ref, ["x", "y"], ["b", "a"])

    def test_column_count_mismatch(self, dim_customer):
        with pytest.raises(ValueError, match="column count mismatch"):
            ForeignKey(("a", "b"), dim_customer, ("customer_key",))

    def test_direct_construction_normalises_strings_to_tuples(self, dim_customer):
        fk = ForeignKey("customer_key", dim_customer, "customer_key")
        assert fk.columns == ("customer_key",)
        assert fk.ref_columns == ("customer_key",)

    def test_ddl_uses_default_name(self, dim_customer):
        fk = ForeignKey.to(dim_customer, "customer_key")
        assert fk.ddl("fk_default") == (
            "CONSTRAINT fk_default FOREIGN KEY (customer_key) REFERENCES main.gold.dim_customer(customer_key)"
        )

    def test_ddl_explicit_name_wins(self, dim_customer):
        fk = ForeignKey.to(dim_customer, "customer_key", name="my-fk")
        assert fk.ddl("fk_default").startswith("CONSTRAINT `my-fk` FOREIGN KEY")


# ---- TableSchema: construction & validation -----------------------------


class TestTableSchemaNames:
    def test_full_name(self, dim_customer):
        assert dim_customer.full_name == "main.gold.dim_customer"

    def test_full_name_without_catalog(self):
        t = _table("t", [Column("id", LongType())], schema="gold")
        assert t.full_name == "gold.t"

    def test_full_name_table_only(self):
        assert _table("t", [Column("id", LongType())]).full_name == "t"

    def test_quoted_name(self):
        t = _table("my-table", [Column("id", LongType())], catalog="main", schema="my schema")
        assert t.quoted_name == "main.`my schema`.`my-table`"
        assert t.full_name == "main.my schema.my-table"

    def test_catalog_without_schema(self):
        with pytest.raises(ValueError, match="catalog given without schema"):
            _table("t", [Column("id", LongType())], catalog="main")

    @pytest.mark.parametrize("part", ["catalog", "schema", "table"])
    def test_rejects_dotted_name_parts(self, part):
        kwargs = {"catalog": "c", "schema": "s", "table": "t", part: "a.b"}
        with pytest.raises(ValueError, match="contains a dot"):
            TableSchema(columns=[Column("id", LongType())], **kwargs)

    @pytest.mark.parametrize("bad", ["", "   ", 123])
    def test_rejects_empty_or_non_string_name_parts(self, bad):
        with pytest.raises(ValueError, match="non-empty string"):
            _table(bad, [Column("id", LongType())])


class TestTableSchemaValidation:
    def test_primary_key_normalised_to_tuple(self, dim_customer):
        assert dim_customer.primary_key == ("customer_key",)

    def test_composite_primary_key_list_normalised_to_tuple(self):
        t = _table("t", [Column("a", LongType()), Column("b", LongType())], primary_key=["a", "b"])
        assert t.primary_key == ("a", "b")

    def test_no_primary_key(self):
        assert _table("t", [Column("id", LongType())]).primary_key == ()

    def test_duplicate_columns(self):
        with pytest.raises(ValueError, match=r"duplicate columns \['id'\]"):
            _table("t", [Column("id", LongType()), Column("id", StringType())])

    def test_pk_column_missing(self):
        with pytest.raises(ValueError, match="PK column"):
            _table("t", [Column("id", LongType())], primary_key="nope")

    def test_fk_column_missing(self, dim_customer):
        with pytest.raises(ValueError, match="FK column"):
            _table(
                "t",
                [Column("id", LongType())],
                foreign_keys=[ForeignKey.to(dim_customer, "customer_key")],
            )

    def test_fk_type_mismatch(self, dim_customer):
        with pytest.raises(ValueError, match="t.customer_key is int but main.gold.dim_customer.customer_key is bigint"):
            _table(
                "t",
                [Column("customer_key", IntegerType())],
                foreign_keys=[ForeignKey.to(dim_customer, "customer_key")],
            )

    def test_fk_decimal_precision_mismatch(self):
        ref = _table("ref", [Column("k", DecimalType(18, 2))], primary_key="k")
        with pytest.raises(ValueError, match="decimal"):
            _table("t", [Column("k", DecimalType(10, 2))], foreign_keys=[ForeignKey.to(ref, "k")])


class TestTableSchemaColumns:
    def test_column_names(self, fact_orders):
        assert fact_orders.column_names == ["order_id", "customer_key", "amount"]

    def test_column_lookup(self, fact_orders):
        assert fact_orders.column("amount").dtype == DecimalType(18, 2)

    def test_column_lookup_missing(self, fact_orders):
        with pytest.raises(KeyError, match="no column 'nope'"):
            fact_orders.column("nope")

    def test_struct_type_marks_pk_non_nullable(self, dim_customer):
        assert dim_customer.struct_type == StructType(
            [
                StructField("customer_key", LongType(), False),
                StructField("customer_id", StringType(), True),
                StructField("customer_name", StringType(), True, {"comment": "Display name"}),
            ]
        )

    def test_from_struct_type(self):
        struct = StructType(
            [
                StructField("id", LongType(), False),
                StructField("name", StringType(), True, {"comment": "c"}),
            ]
        )
        t = TableSchema.from_struct_type("t", struct, schema="gold", primary_key="id")
        assert t.full_name == "gold.t"
        assert t.primary_key == ("id",)
        assert t.columns == [
            Column("id", LongType(), nullable=False),
            Column("name", StringType(), comment="c"),
        ]

    def test_struct_type_round_trip(self, dim_customer):
        t = TableSchema.from_struct_type("t", dim_customer.struct_type)
        assert t.struct_type == dim_customer.struct_type


class TestConform:
    class FakeDataFrame:
        def select(self, *cols):
            self.selected = cols
            return self

    def test_selects_and_casts_declared_columns_in_order(self, fact_orders):
        df = fact_orders.conform(self.FakeDataFrame())
        assert [str(c) for c in df.selected] == [
            "Column<'CAST(order_id AS STRING) AS order_id'>",
            "Column<'CAST(customer_key AS BIGINT) AS customer_key'>",
            "Column<'CAST(amount AS DECIMAL(18,2)) AS amount'>",
        ]

    def test_skips_generated_columns(self):
        t = _table(
            "t",
            [
                Column("order_ts", TimestampType()),
                Column("order_date", DateType(), generated_as="CAST(order_ts AS DATE)"),
            ],
        )
        df = t.conform(self.FakeDataFrame())
        assert len(df.selected) == 1
        assert "order_ts" in str(df.selected[0])


# ---- TableSchema: foreign keys ------------------------------------------


class TestAddForeignKey:
    def test_adds_and_returns_self(self, dim_customer):
        t = _table("t", [Column("customer_key", LongType())])
        assert t.add_foreign_key(dim_customer, "customer_key") is t
        assert len(t.foreign_keys) == 1
        assert t.foreign_keys[0].ref is dim_customer

    def test_chaining(self, dim_customer, dim_date):
        t = (
            _table("t", [Column("customer_key", LongType()), Column("date_key", IntegerType())])
            .add_foreign_key(dim_customer, "customer_key")
            .add_foreign_key(dim_date, "date_key")
        )
        assert [fk.ref.table for fk in t.foreign_keys] == ["dim_customer", "dim_date"]

    def test_rejects_type_mismatch(self, dim_customer):
        t = _table("t", [Column("customer_key", StringType())])
        with pytest.raises(ValueError, match="string but"):
            t.add_foreign_key(dim_customer, "customer_key")

    @pytest.mark.xfail(strict=True, reason="add_foreign_key appends before validating, so a rejected FK is kept")
    def test_rejected_fk_is_not_kept(self, dim_customer):
        t = _table("t", [Column("customer_key", StringType())])
        with pytest.raises(ValueError):
            t.add_foreign_key(dim_customer, "customer_key")
        assert t.foreign_keys == []

    def test_instances_do_not_share_default_fk_list(self, dim_customer):
        a = _table("a", [Column("customer_key", LongType())])
        b = _table("b", [Column("customer_key", LongType())])
        a.add_foreign_key(dim_customer, "customer_key")
        assert b.foreign_keys == []


# ---- TableSchema: DDL rendering -----------------------------------------


class TestDdl:
    def test_single_pk_inline_and_not_null(self, dim_customer):
        assert dim_customer.ddl() == (
            "\n"
            "  customer_key bigint NOT NULL PRIMARY KEY,\n"
            "  customer_id string,\n"
            "  customer_name string COMMENT 'Display name'\n"
        )

    def test_compact(self, dim_customer):
        assert dim_customer.ddl(pretty=False) == (
            "customer_key bigint NOT NULL PRIMARY KEY, customer_id string, customer_name string COMMENT 'Display name'"
        )

    def test_str_is_pretty_ddl(self, dim_customer):
        assert str(dim_customer) == dim_customer.ddl()

    def test_no_pk(self):
        t = _table("t", [Column("a", LongType()), Column("b", StringType(), nullable=False)])
        assert t.ddl(pretty=False) == "a bigint, b string NOT NULL"

    def test_pk_on_non_nullable_column_has_single_not_null(self):
        t = _table("t", [Column("id", LongType(), nullable=False)], primary_key="id")
        assert t.ddl(pretty=False) == "id bigint NOT NULL PRIMARY KEY"

    def test_composite_pk_is_table_constraint(self):
        t = _table(
            "order_lines",
            [Column("order_id", StringType()), Column("line_no", IntegerType()), Column("qty", IntegerType())],
            primary_key=["order_id", "line_no"],
        )
        assert t.ddl(pretty=False) == (
            "order_id string NOT NULL, "
            "line_no int NOT NULL, "
            "qty int, "
            "CONSTRAINT pk_order_lines PRIMARY KEY (order_id, line_no)"
        )

    def test_composite_pk_custom_name(self):
        t = _table(
            "t",
            [Column("a", LongType()), Column("b", LongType())],
            primary_key=["a", "b"],
            pk_name="my-pk",
        )
        assert t.ddl(pretty=False).endswith("CONSTRAINT `my-pk` PRIMARY KEY (a, b)")

    def test_fk_constraint_with_default_name(self, fact_orders):
        assert fact_orders.ddl(pretty=False) == (
            "order_id string NOT NULL PRIMARY KEY, "
            "customer_key bigint, "
            "amount decimal(18,2), "
            "CONSTRAINT fk_fact_orders_dim_customer_customer_key FOREIGN KEY (customer_key) "
            "REFERENCES main.gold.dim_customer(customer_key)"
        )

    def test_composite_fk(self):
        ref = _table(
            "order_lines",
            [Column("order_id", StringType()), Column("line_no", IntegerType())],
            schema="gold",
            primary_key=["order_id", "line_no"],
        )
        t = _table(
            "shipments",
            [Column("order_id", StringType()), Column("line_no", IntegerType())],
            foreign_keys=[ForeignKey.to(ref, ["order_id", "line_no"])],
        )
        assert t.ddl(pretty=False) == (
            "order_id string, line_no int, "
            "CONSTRAINT fk_shipments_order_lines_order_id_line_no "
            "FOREIGN KEY (order_id, line_no) REFERENCES gold.order_lines(order_id, line_no)"
        )

    def test_fk_to_table_without_catalog_uses_bare_name(self):
        ref = _table("dim", [Column("k", LongType())], primary_key="k")
        t = _table("fact", [Column("k", LongType())], foreign_keys=[ForeignKey.to(ref, "k")])
        assert t.ddl(pretty=False).endswith("REFERENCES dim(k)")

    def test_quoted_identifiers_everywhere(self):
        ref = _table("dim-x", [Column("key-col", LongType())], catalog="c", schema="s", primary_key="key-col")
        t = _table(
            "fact",
            [Column("key-col", LongType())],
            foreign_keys=[ForeignKey.to(ref, "key-col", name="fk")],
        )
        assert t.ddl(pretty=False) == (
            "`key-col` bigint, CONSTRAINT fk FOREIGN KEY (`key-col`) REFERENCES c.s.`dim-x`(`key-col`)"
        )

    def test_section_order_columns_then_pk_then_fks(self, dim_customer, dim_date):
        t = _table(
            "t",
            [
                Column("customer_key", LongType()),
                Column("date_key", IntegerType()),
                Column("line", IntegerType()),
            ],
            primary_key=["customer_key", "date_key", "line"],
        )
        t.add_foreign_key(dim_customer, "customer_key").add_foreign_key(dim_date, "date_key")
        lines = t.ddl().strip().split(",\n  ")
        assert [line.split()[0] for line in lines] == [
            "customer_key",
            "date_key",
            "line",
            "CONSTRAINT",
            "CONSTRAINT",
            "CONSTRAINT",
        ]
        assert "PRIMARY KEY" in lines[3]
        assert "dim_customer" in lines[4]
        assert "dim_date" in lines[5]


# ---- schemas_in_dependency_order ----------------------------------------


class TestDependencyOrder:
    def test_targets_come_before_referencing_tables(self, dim_customer, dim_date, fact_orders):
        ordered = schemas_in_dependency_order([fact_orders, dim_date, dim_customer])
        names = [t.table for t in ordered]
        assert names.index("dim_customer") < names.index("fact_orders")
        assert set(names) == {"fact_orders", "dim_date", "dim_customer"}

    def test_chain(self):
        a = _table("a", [Column("id", LongType())], primary_key="id")
        b = _table("b", [Column("id", LongType())], primary_key="id", foreign_keys=[ForeignKey.to(a, "id")])
        c = _table("c", [Column("id", LongType())], foreign_keys=[ForeignKey.to(b, "id")])
        assert schemas_in_dependency_order([c, b, a]) == [a, b, c]

    def test_independent_tables_keep_input_order(self):
        x = _table("x", [Column("id", LongType())])
        y = _table("y", [Column("id", LongType())])
        assert schemas_in_dependency_order([y, x]) == [y, x]

    def test_ignores_targets_not_in_input(self, dim_customer, fact_orders):
        assert schemas_in_dependency_order([fact_orders]) == [fact_orders]

    def test_accepts_any_iterable(self, dim_customer, fact_orders):
        ordered = schemas_in_dependency_order(t for t in [fact_orders, dim_customer])
        assert ordered == [dim_customer, fact_orders]

    def test_empty(self):
        assert schemas_in_dependency_order([]) == []

    def test_cycle_raises(self):
        a = _table("a", [Column("id", LongType())], primary_key="id")
        b = _table("b", [Column("id", LongType())], primary_key="id", foreign_keys=[ForeignKey.to(a, "id")])
        a.add_foreign_key(b, "id")
        with pytest.raises(ValueError, match="cycle"):
            schemas_in_dependency_order([a, b])

    @pytest.mark.xfail(
        strict=True, reason="a self-referencing FK (e.g. manager_id -> employee_id) is reported as a cycle"
    )
    def test_self_reference_is_not_a_cycle(self):
        emp = _table(
            "employee",
            [Column("employee_id", LongType()), Column("manager_id", LongType())],
            primary_key="employee_id",
        )
        emp.add_foreign_key(emp, "manager_id")
        assert schemas_in_dependency_order([emp]) == [emp]


# ---- tags ---------------------------------------------------------------


class TestTags:
    @pytest.fixture
    def tagged(self) -> TableSchema:
        return TableSchema(
            catalog="main",
            schema="gold",
            table="dim_customer",
            columns=[
                Column("customer_key", LongType()),
                Column("email", StringType(), tags={"pii": "email", "owner": "crm"}),
                Column("customer_name", StringType(), tags={"pii": "name"}),
            ],
            primary_key="customer_key",
            tags={"domain": "sales", "tier": "gold"},
        )

    def test_defaults_to_no_tags(self, dim_customer):
        assert dim_customer.tags == {}
        assert all(c.tags == {} for c in dim_customer.columns)
        assert dim_customer.tag_sql() == []

    def test_tags_are_not_in_ddl(self, tagged):
        assert "TAGS" not in tagged.ddl()
        assert "pii" not in tagged.ddl()

    def test_tag_sql_table_then_columns_in_column_order(self, tagged):
        assert tagged.tag_sql() == [
            "ALTER MATERIALIZED VIEW main.gold.dim_customer SET TAGS ('domain' = 'sales', 'tier' = 'gold')",
            (
                "ALTER MATERIALIZED VIEW main.gold.dim_customer ALTER COLUMN email "
                "SET TAGS ('pii' = 'email', 'owner' = 'crm')"
            ),
            "ALTER MATERIALIZED VIEW main.gold.dim_customer ALTER COLUMN customer_name SET TAGS ('pii' = 'name')",
        ]

    @pytest.mark.parametrize("kind", ["STREAMING TABLE", "TABLE", "streaming table"])
    def test_tag_sql_kind(self, tagged, kind):
        assert all(s.startswith(f"ALTER {kind.upper()} main.gold.dim_customer ") for s in tagged.tag_sql(kind))

    def test_tag_sql_rejects_unknown_kind(self, tagged):
        with pytest.raises(ValueError, match="kind must be one of"):
            tagged.tag_sql("VIEW")

    def test_only_column_tags(self):
        t = _table("t", [Column("a", LongType()), Column("b", LongType(), tags={"k": "v"})])
        assert t.tag_sql("TABLE") == ["ALTER TABLE t ALTER COLUMN b SET TAGS ('k' = 'v')"]

    def test_only_table_tags(self):
        t = _table("t", [Column("a", LongType())], tags={"k": "v"})
        assert t.tag_sql("TABLE") == ["ALTER TABLE t SET TAGS ('k' = 'v')"]

    def test_empty_tag_value(self):
        t = _table("t", [Column("a", LongType())], tags={"certified": ""})
        assert t.tag_sql("TABLE") == ["ALTER TABLE t SET TAGS ('certified' = '')"]

    def test_quoting_and_escaping(self):
        t = _table(
            "my-table",
            [Column("e-mail", StringType(), tags={"note": "it's"})],
            catalog="c",
            schema="s",
        )
        assert t.tag_sql("TABLE") == [r"ALTER TABLE c.s.`my-table` ALTER COLUMN `e-mail` SET TAGS ('note' = 'it\'s')"]

    @pytest.mark.parametrize("bad", [{"": "v"}, {"  ": "v"}, {1: "v"}])
    def test_rejects_bad_tag_names(self, bad):
        with pytest.raises(ValueError, match="tag names"):
            Column("a", LongType(), tags=bad)
        with pytest.raises(ValueError, match="tag names"):
            _table("t", [Column("a", LongType())], tags=bad)

    def test_rejects_non_string_tag_value(self):
        with pytest.raises(TypeError, match="value must be a string"):
            Column("a", LongType(), tags={"k": 1})
        with pytest.raises(TypeError, match="value must be a string"):
            _table("t", [Column("a", LongType())], tags={"k": None})

    @pytest.mark.parametrize("bad", [["pii"], "pii", None])
    def test_rejects_non_dict_tags(self, bad):
        with pytest.raises(TypeError, match="tags must be a dict"):
            Column("a", LongType(), tags=bad)

    def test_column_with_tags_is_hashable(self):
        assert hash(Column("a", LongType(), tags={"k": "v"})) == hash(Column("a", LongType()))

    def test_tags_take_part_in_equality(self):
        assert Column("a", LongType(), tags={"k": "v"}) != Column("a", LongType())

    def test_instances_do_not_share_default_tags(self):
        a, b = Column("a", LongType()), Column("b", LongType())
        a.tags["k"] = "v"
        assert b.tags == {}


# ---- dev schema prefix --------------------------------------------------


class TestSchemaPrefix:
    @pytest.fixture
    def dev(self, monkeypatch):
        monkeypatch.setattr(table_schema, "schema_prefix", lambda: "user_")

    def test_no_prefix_outside_a_pipeline(self, dim_customer):
        assert table_schema.schema_prefix() == ""
        assert dim_customer.full_name == "main.gold.dim_customer"

    def test_prefix_applies_to_schema(self, dev, dim_customer):
        assert dim_customer.full_name == "main.user_gold.dim_customer"
        assert dim_customer.quoted_name == "main.user_gold.dim_customer"
        assert dim_customer.schema == "gold"

    def test_prefix_skipped_without_schema(self, dev):
        assert _table("t", [Column("a", LongType())]).full_name == "t"

    def test_prefix_applies_to_fk_references(self, dev, fact_orders):
        assert "REFERENCES main.user_gold.dim_customer(customer_key)" in fact_orders.ddl()

    def test_prefix_applies_to_tag_sql(self, dev, dim_customer):
        dim_customer.tags["k"] = "v"
        assert dim_customer.tag_sql("TABLE") == ["ALTER TABLE main.user_gold.dim_customer SET TAGS ('k' = 'v')"]

    def test_quoted_when_prefix_needs_it(self, monkeypatch, dim_customer):
        monkeypatch.setattr(table_schema, "schema_prefix", lambda: "first.last-")
        assert dim_customer.quoted_name == "main.`first.last-gold`.dim_customer"

    def test_dependency_order_with_prefix(self, dev, dim_customer, fact_orders):
        assert schemas_in_dependency_order([fact_orders, dim_customer]) == [dim_customer, fact_orders]
