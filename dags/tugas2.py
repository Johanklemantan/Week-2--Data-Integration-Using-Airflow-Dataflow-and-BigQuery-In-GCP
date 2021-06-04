from datetime import datetime, timedelta

from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id") 

default_args = {
    "start_date": datetime(2021,3,25),
    "end_date"  : datetime(2021,3,30), 
    "depends_on_past": True,
    "dataflow_default_options": {
        "project": project_id,  
        "temp_location": bucket_path + "/tmp/",
        "numWorkers": 1,
    },
}


with models.DAG(
    "Tugas2-Querying-BigQuery-Table",
    default_args=default_args,
    schedule_interval=timedelta(days=3),
) as dag:
    

    bq_important_column = BigQueryOperator(
        task_id = "bq_important_column",
        sql =  """SELECT 
        state,
        city,
        created_at,
        value.string_value,
        event_datetime,
        user_id,
        event_name,
        event_id
        FROM `week-2-de-blank-space-johan.trial1_johan_week2_DE.nomer2`
        CROSS JOIN UNNEST (event_params)
        WHERE (key = 'product_id' 
        OR  key = 'product_name')
        AND value.string_value is not null  ;""",
        use_legacy_sql = False,
        destination_dataset_table = "week-2-de-blank-space-johan:searched_keyword.tugas2",
        write_disposition = 'WRITE_APPEND'
    )
    bq_important_column
