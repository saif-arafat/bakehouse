from pyspark import pipelines as dp

from lakeora.bronze.erp.models import brz_erp__customer, brz_erp__customer_history

dp.create_streaming_table(
    name=brz_erp__customer_history.full_name,
    comment=brz_erp__customer_history.comment,
    schema=brz_erp__customer_history.ddl(),
)

# replaces the dbt snapshot snp_erp__customer (check strategy on the same columns)
dp.create_auto_cdc_flow(
    target=brz_erp__customer_history.full_name,
    source=brz_erp__customer.full_name,
    keys=["customerid", "created_date"],
    sequence_by="md_ingested_ts",
    stored_as_scd_type=2,
    track_history_column_list=[
        "first_name",
        "last_name",
        "email_address",
        "phone_number",
        "address",
        "city",
        "state",
        "country",
        "continent",
        "postal_zip_code",
        "gender",
    ],
)
