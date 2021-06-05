from datetime import datetime, timedelta
from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")

default_args = {
    "start_date": datetime(2021, 3, 10),
    "end_date": datetime(2021, 3, 15),
    "depends_on_past": True,
    "dataflow_default_options": {
        "project": project_id,
        "temp_location": bucket_path + "/tmp/",
        "numWorkers": 1,
    },
}
with models.DAG(
    "Tugas1-CSV-to-BQ",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:
    load_to_bigquery = DataflowTemplateOperator(
        task_id="load_to_bigquery",
        template="gs://dataflow-templates/latest/GCS_Text_to_BigQuery",
        parameters={
            "javascriptTextTransformFunctionName": "transformCSVtoJSON",
            "JSONPath": bucket_path +
            "/bucket/schema.json",
            "javascriptTextTransformGcsPath": bucket_path +
            "/bucket/transform.js",
            "inputFilePattern": bucket_path +
            "/keyword_search_search_{{ ds_nodash }}.csv",
            "outputTable": project_id +
            ":searched_keyword.tugas1a",
            "bigQueryLoadingTemporaryDirectory": bucket_path +
            "/tmp/",
        },
    )
    bq_top_search_daily = BigQueryOperator(
        task_id="bq_top_search_daily_query",
        sql="""SELECT search_keyword as keyword, count(search_keyword) as search_count, created_at as tanggal
               FROM `week-2-de-blank-space-johan.searched_keyword.tugas1a`
               WHERE created_at = '{{ ds }} 00:00:00 UTC'
               GROUP BY search_keyword, created_at
               ORDER BY search_count desc
               LIMIT 1;""",
        use_legacy_sql=False,
        destination_dataset_table="week-2-de-blank-space-johan:searched_keyword.tugas1b",
        write_disposition='WRITE_APPEND')
    load_to_bigquery >> bq_top_search_daily
