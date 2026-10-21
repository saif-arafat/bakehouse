from databricks.bundles.core import Bundle, Resources
from databricks.bundles.jobs import (
    Environment,
    Job,
    JobEnvironment,
    PeriodicTriggerConfiguration,
    PeriodicTriggerConfigurationTimeUnit,
    PipelineTask,
    PythonWheelTask,
    Task,
    TaskDependency,
    TriggerSettings,
)
from databricks.bundles.pipelines import PathPattern, Pipeline, PipelineLibrary

from lakeora_core.table_schema import SCHEMA_PREFIX_CONF

LAYERS = ["bronze", "silver", "gold"]


def load_lakeora_pipeline(bundle: Bundle) -> Resources:
    prefix = "" if bundle.target == "prod" else "${workspace.current_user.short_name}_"

    resources = Resources()
    resources.add_resource(
        "lakeora_pipeline",
        Pipeline(
            name="lakeora_pipeline",
            serverless=True,
            # default catalog/schema for unqualified names and the event log; every TableSchema is qualified
            catalog="bronze",
            schema=f"{prefix}erp",
            # added to sys.path, so the pipeline sources can import lakeora_core
            root_path="src",
            libraries=[PipelineLibrary(glob=PathPattern(include=f"src/lakeora/{layer}/**")) for layer in LAYERS],
            configuration={SCHEMA_PREFIX_CONF: prefix},
        ),
    )
    # Tags can't go in the pipeline's schema DDL, so a job refreshes the pipeline and then applies them
    resources.add_resource(
        "lakeora_job",
        Job(
            name="lakeora_job",
            # Run this job every day, exactly one day from the last run; see https://docs.databricks.com/api/workspace/jobs/create#trigger
            trigger=TriggerSettings(
                periodic=PeriodicTriggerConfiguration(
                    interval=1,
                    unit=PeriodicTriggerConfigurationTimeUnit.DAYS,
                ),
            ),
            tasks=[
                Task(
                    task_key="refresh",
                    pipeline_task=PipelineTask(pipeline_id="${resources.pipelines.lakeora_pipeline.id}"),
                ),
                Task(
                    task_key="apply_tags",
                    depends_on=[TaskDependency(task_key="refresh")],
                    environment_key="default",
                    python_wheel_task=PythonWheelTask(
                        package_name="lakeora",
                        entry_point="apply_tags",
                        parameters=["--schema-prefix", prefix, "--layers", *LAYERS],
                    ),
                ),
            ],
            environments=[
                JobEnvironment(
                    environment_key="default",
                    spec=Environment(environment_version="4", dependencies=["dist/*.whl"]),
                ),
            ],
        ),
    )
    return resources
