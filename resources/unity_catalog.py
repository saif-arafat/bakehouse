"""
Unity Catalog layout for the medallion layers.

Mirrors the dbt placement macros (src/macros/generate_database_name.sql and
generate_schema_name.sql): models/<layer>/<folder>/model.sql lands in catalog
<layer>, schema <folder>, with ordering prefixes dropped (01_bronze -> bronze,
A_conformed -> conformed). Schemas are derived from the folders on disk, so a
new model folder gets its schema on the next deploy.

The catalogs API cannot create catalogs on Default Storage, so each catalog is created
once with SQL by the prod service principal and then bound to its resource here:

    CREATE CATALOG gold;  -- as the service principal
    databricks bundle deployment bind gold_catalog gold -t prod
"""

from pathlib import Path

from databricks.bundles.catalogs import Catalog, CatalogGrant
from databricks.bundles.catalogs import Lifecycle as CatalogLifecycle
from databricks.bundles.core import Bundle, Resources
from databricks.bundles.schemas import Lifecycle as SchemaLifecycle
from databricks.bundles.schemas import Schema

MODELS_DIR = Path(__file__).parent.parent / "src" / "models"

# Prod is deployed by a service principal, which creates and therefore owns every catalog and
# schema here, so it needs no grant of its own. Other principals come from bundle variables
# (databricks.yml).
DEVELOPERS = "${var.uc_developers}"
READERS = "${var.uc_readers}"

LAYERS = {
    "bronze": "Raw source data, loaded as-is from ERP and web sources.",
    "silver": "Cleaned, typed and deduplicated entities.",
    "gold": "Conformed dimensions, facts, marts and metric views for consumption.",
}

# Grants on a catalog inherit to its schemas and tables. Developers can read prod and create
# their own dev schemas (dbt's <user>_<folder>, see generate_schema_name), which they then own,
# but cannot write to the prod schemas. Readers only see gold.
READ = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "EXECUTE"]
DEVELOP = [*READ, "CREATE_SCHEMA"]

CATALOG_GRANTS = {
    "bronze": [CatalogGrant(principal=DEVELOPERS, privileges=DEVELOP)],
    "silver": [CatalogGrant(principal=DEVELOPERS, privileges=DEVELOP)],
    "gold": [
        CatalogGrant(principal=DEVELOPERS, privileges=DEVELOP),
        CatalogGrant(principal=READERS, privileges=READ),
    ],
}


def clean_folder_name(folder: str) -> str:
    """Python twin of the clean_folder_name dbt macro."""
    prefix, sep, rest = folder.partition("_")
    if sep and (prefix.isdigit() or len(prefix) == 1):
        return rest
    return folder


def layer_folders() -> dict[str, list[str]]:
    """Map each catalog to the schema names found under its layer folder."""
    layers: dict[str, list[str]] = {}
    for layer_dir in sorted(p for p in MODELS_DIR.iterdir() if p.is_dir()):
        catalog = clean_folder_name(layer_dir.name)
        if catalog not in LAYERS:
            continue
        layers[catalog] = [
            clean_folder_name(p.name) for p in sorted(layer_dir.iterdir()) if p.is_dir()
        ]
    # seeds aren't under a layer folder; dbt_project.yml places them in bronze.seeds
    layers["bronze"].append("seeds")
    return layers


def load_unity_catalog(bundle: Bundle) -> Resources:
    resources = Resources()

    # the catalogs and prod schemas belong to the prod service principal; dev deploys only need
    # the CREATE_SCHEMA grant above, since dbt creates each developer's schemas itself
    if bundle.target != "prod":
        return resources

    for catalog, schemas in layer_folders().items():
        resources.add_resource(
            f"{catalog}_catalog",
            Catalog(
                name=catalog,
                comment=LAYERS[catalog],
                grants=CATALOG_GRANTS[catalog],
                lifecycle=CatalogLifecycle(prevent_destroy=True),
            ),
        )

        for schema in schemas:
            resources.add_resource(
                f"{catalog}_{schema}_schema",
                Schema(
                    # reference the catalog resource so deploy creates it first
                    catalog_name=f"${{resources.catalogs.{catalog}_catalog.name}}",
                    name=schema,
                    comment=f"{catalog.capitalize()} layer, {schema} models.",
                    lifecycle=SchemaLifecycle(prevent_destroy=True),
                ),
            )

    return resources
