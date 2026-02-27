from airflow.decorators import dag, task
from datetime import datetime

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig



@dag(
    dag_id="meta",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["meta"],
)

def meta():
    
    upload_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_gcs",
        src="/usr/local/airflow/include/dataset/meta_ads_raw.csv",   # caminho no container
        dst="bronze/meta_ads_raw.csv",
        bucket="campanhas",
        gcp_conn_id="gcp",
        mime_type="text/csv",
    )

    create_meta_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_meta_dataset",
        dataset_id="meta",
        gcp_conn_id="gcp",
        exists_ok=True
    )

    gcs_to_bronze = aql.load_file(
        task_id="gcs_to_bronze",
        input_file=File(
            path="gs://campanhas/bronze/meta_ads_raw.csv",
            conn_id="gcp",
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name="meta_ads_raw",
            conn_id="gcp",
            metadata=Metadata(schema="meta"),
        ),
        use_native_support=False,
    )

    transform = DbtTaskGroup(
        group_id="silver",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:models/transform"],
        ),
    )

    report = DbtTaskGroup(
        group_id="report",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:models/analytics"],
        ),
    )
    upload_gcs >> create_meta_dataset >> gcs_to_bronze >> transform >> report

meta()
