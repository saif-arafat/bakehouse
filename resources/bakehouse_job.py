from databricks.bundles.jobs import (
    DbtTask,
    Environment,
    Job,
    JobEnvironment,
    PeriodicTriggerConfiguration,
    PeriodicTriggerConfigurationTimeUnit,
    Task,
    TriggerSettings,
)

DBT_VARS = '--vars "{ dev_schema: ${workspace.current_user.short_name}, warehouse_id: ${var.warehouse_id} }"'

bakehouse_job = Job(
    name="bakehouse_job",
    # Run this job every day, exactly one day from the last run; see https://docs.databricks.com/api/workspace/jobs/create#trigger
    trigger=TriggerSettings(
        periodic=PeriodicTriggerConfiguration(
            interval=1,
            unit=PeriodicTriggerConfigurationTimeUnit.DAYS,
        ),
    ),
    # email_notifications=JobEmailNotifications(
    #     on_failure=["your_email@example.com"],
    # ),
    tasks=[
        Task(
            task_key="dbt",
            environment_key="default",
            dbt_task=DbtTask(
                # Relative to the bundle root (databricks.yml)
                project_directory=".",
                # The default schema, catalog, etc. are defined in ../dbt_profiles/profiles.yml
                profiles_directory="dbt_profiles/",
                # The dbt commands to run (see also dbt_profiles/profiles.yml; dev_schema is used in the dev profile)
                commands=[
                    "dbt deps --target=${bundle.target}",
                    f"dbt build --target=${{bundle.target}} {DBT_VARS}",
                ],
            ),
        ),
    ],
    environments=[
        JobEnvironment(
            environment_key="default",
            spec=Environment(
                environment_version="4",
                dependencies=["dbt>=2"],
            ),
        ),
    ],
)
