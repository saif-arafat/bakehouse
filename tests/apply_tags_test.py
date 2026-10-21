"""Unit tests for lakeora_core.apply_tags. No Spark compute needed:

uv run pytest --noconftest tests/apply_tags_test.py
"""

import sys
from types import SimpleNamespace

import pytest
from pyspark.sql.types import LongType, StringType

from lakeora_core import apply_tags as at
from lakeora_core import table_schema
from lakeora_core.table_schema import Column, TableSchema


class FakeSpark:
    """Records spark.sql calls; information_schema lookups answer from `table_types`."""

    def __init__(self, table_types=None):
        self.table_types = table_types or {}
        self.statements = []

    def sql(self, stmt, args=None):
        if "information_schema.tables" in stmt:
            catalog = stmt.split("FROM ")[1].split(".")[0]
            kind = self.table_types.get((catalog, args["schema"], args["table"]))
            rows = [SimpleNamespace(table_type=kind)] if kind else []
            return SimpleNamespace(collect=lambda: rows)
        self.statements.append(stmt)
        return SimpleNamespace(collect=list)


@pytest.fixture(autouse=True)
def no_prefix():
    table_schema.set_schema_prefix(None)
    yield
    table_schema.set_schema_prefix(None)


@pytest.fixture
def tagged() -> TableSchema:
    return TableSchema(
        catalog="bronze",
        schema="erp",
        table="customer",
        columns=[Column("id", LongType()), Column("email", StringType(), tags={"pii": "email"})],
        primary_key="id",
        tags={"domain": "customer"},
    )


class TestNoop:
    def test_bare_decorator(self):
        def f():
            return 1

        assert at._noop(f) is f

    def test_decorator_with_arguments(self):
        def f():
            return 1

        assert at._noop(name="x", schema="a int")(f) is f

    def test_plain_call(self):
        at._noop("bronze.erp.customer")  # e.g. dp.create_streaming_table(...)

    def test_stub_module_returns_noop_for_any_api(self):
        stub = at._NoopPipelines("pyspark.pipelines")
        assert stub.table is at._noop
        assert stub.materialized_view is at._noop
        assert stub.expect_or_drop is at._noop


class TestWithoutPipelines:
    def test_from_import_gets_stub_and_is_restored(self):
        before = sys.modules.get("pyspark.pipelines")
        with at._without_pipelines():
            from pyspark import pipelines as dp

            assert isinstance(dp, at._NoopPipelines)
            import dlt

            assert dlt is dp
        assert sys.modules.get("pyspark.pipelines") is before

    def test_spark_global_only_inside(self):
        import builtins

        spark = object()
        with at._without_pipelines(spark):
            assert builtins.spark is spark
        assert not hasattr(builtins, "spark")


class TestCollectSchemas:
    def test_finds_bronze_customer(self):
        names = [t.full_name for t in at.collect_schemas(("bronze",))]
        assert "bronze.erp.customer" in names

    def test_source_modules_skip_pycache(self):
        mods = at._source_modules(("bronze",))
        assert "lakeora.bronze.erp.customer" in mods
        assert not any("__pycache__" in m for m in mods)

    def test_missing_layer_folder_is_empty(self):
        assert at._source_modules(("nope",)) == []


class TestApplyTags:
    def test_uses_object_type_of_existing_table(self, tagged):
        spark = FakeSpark({("bronze", "erp", "customer"): "STREAMING_TABLE"})
        assert at.apply_tags(spark, [tagged]) == 2
        assert spark.statements == [
            "ALTER STREAMING TABLE bronze.erp.customer SET TAGS ('domain' = 'customer')",
            "ALTER STREAMING TABLE bronze.erp.customer ALTER COLUMN email SET TAGS ('pii' = 'email')",
        ]

    @pytest.mark.parametrize(
        "table_type, kind",
        [("MATERIALIZED_VIEW", "MATERIALIZED VIEW"), ("MANAGED", "TABLE"), ("SOMETHING_NEW", "TABLE")],
    )
    def test_kind_mapping(self, tagged, table_type, kind):
        spark = FakeSpark({("bronze", "erp", "customer"): table_type})
        at.apply_tags(spark, [tagged])
        assert spark.statements[0].startswith(f"ALTER {kind} ")

    def test_looks_up_prefixed_schema(self, tagged):
        table_schema.set_schema_prefix("user_")
        spark = FakeSpark({("bronze", "user_erp", "customer"): "STREAMING_TABLE"})
        assert at.apply_tags(spark, [tagged]) == 2
        assert all("bronze.user_erp.customer" in s for s in spark.statements)

    def test_skips_missing_table(self, tagged, capsys):
        spark = FakeSpark()
        assert at.apply_tags(spark, [tagged]) == 0
        assert spark.statements == []
        assert "doesn't exist" in capsys.readouterr().out

    def test_skips_untagged_without_lookup(self):
        t = TableSchema(catalog="c", schema="s", table="t", columns=[Column("a", LongType())])
        assert at.apply_tags(FakeSpark(), [t]) == 0

    def test_skips_unqualified(self, capsys):
        t = TableSchema(table="t", columns=[Column("a", LongType())], tags={"k": "v"})
        assert at.apply_tags(FakeSpark(), [t]) == 0
        assert "needs a catalog and schema" in capsys.readouterr().out

    def test_dry_run_runs_nothing(self, tagged):
        spark = FakeSpark({("bronze", "erp", "customer"): "STREAMING_TABLE"})
        assert at.apply_tags(spark, [tagged], dry_run=True) == 2
        assert spark.statements == []


class TestSetSchemaPrefix:
    def test_override_and_reset(self):
        table_schema.set_schema_prefix("x_")
        assert table_schema.schema_prefix() == "x_"
        table_schema.set_schema_prefix(None)
        assert table_schema.schema_prefix() == ""

    def test_empty_override_wins(self):
        table_schema.set_schema_prefix("")
        assert table_schema.schema_prefix() == ""
