from datetime import datetime, timedelta
from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.utils.dates import days_ago

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")

default_args = {
    "start_date": datetime(2021, 3, 25),
    "end_date": datetime(2021, 3, 30),
    "depends_on_past": True,
    "dataflow_default_options": {
        "project": project_id,
        "temp_location": bucket_path + "/tmp/",
        "numWorkers": 1,
    },
}
with models.DAG(
    "Task2-Querying-BigQuery-Table",
    default_args=default_args,
    schedule_interval=timedelta(days=3),
) as dag:
    bq_important_column = BigQueryOperator(
        task_id="bq_important_column",
        sql="""SELECT
        MAX(CASE 
        WHEN ep.key = 'transaction_id' 
        THEN ep.value.int_value 
        ELSE NULL 
        END)
        AS transaction_id,
        MAX(CASE 
        WHEN ep.key = 'transaction_detail_id' 
        THEN ep.value.int_value 
        ELSE NULL 
        END)
        AS transaction_detail_id,
        MAX(CASE 
        WHEN ep.key = 'transaction_number' 
        THEN ep.value.int_value
        ELSE NULL 
        END)
        AS transaction_number,
        MAX(CASE 
        WHEN ep.key = 'transaction_datetime' 
        THEN ep.value.string_value 
        ELSE NULL 
        END)
        AS transaction_datetime,
        MAX(CASE 
        WHEN ep.key = 'purchase_quantity' 
        THEN ep.value.int_value
        ELSE NULL 
        END)
        AS purchase_quantity,
        MAX(CASE 
        WHEN ep.key = 'purchase_amount' 
        THEN ep.value.float_value 
        ELSE NULL 
        END)
        AS purchase_amount,
        MAX(CASE 
        WHEN ep.key = 'purchase_payment_method' 
        THEN ep.value.string_value
        ELSE NULL 
        END)
        AS purchase_payment_method,
        MAX(CASE 
        WHEN ep.key = 'purchase_source' 
        THEN ep.value.string_value
        ELSE NULL 
        END)
        AS purchase_source,
        MAX(CASE 
        WHEN ep.key = 'product_id' 
        THEN ep.value.string_value
        ELSE NULL 
        END)
        AS product_id,
          user_id,
          state,
          city,
          created_at,
        MAX(CASE 
        WHEN ep.key = 'ext_created_at' 
        THEN ep.value.string_value
        ELSE NULL 
        END)
        AS ext_created_at,
      FROM `week-2-de-blank-space-johan.trial1_johan_week2_DE.nomer2`, UNNEST(event_params) AS ep
      GROUP BY 
          user_id,
          state,
          city,
          created_at""",
        use_legacy_sql=False,
        destination_dataset_table="week-2-de-blank-space-johan.searched_keyword.task2",
        write_disposition='WRITE_APPEND'
    )
    create_partition_table = BigQueryOperator(
        task_id="create_partition_table",
        sql="""CREATE OR REPLACE TABLE 
        `week-2-de-blank-space-johan.searched_keyword.task2_partitioned`
        partition by DATE(created_at) AS
        SELECT * FROM `week-2-de-blank-space-johan.searched_keyword.task2`""",
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND'
    )
    transfer_bq_to_gcs = BigQueryToGCSOperator(
        task_id="transfer_bq_to_gcs",
        source_project_dataset_table="week-2-de-blank-space-johan.searched_keyword.task2_partitioned",
        destination_cloud_storage_uris=f'{bucket_path}'+'/final_partitioned'
    )
    bq_important_column >> create_partition_table >> transfer_bq_to_gcs
