from datetime import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


DEFAULT_DBT_ROOT_PATH = "/usr/local/airflow/dags/dbt/netflix"


profile_config = ProfileConfig(
    profile_name="netflix",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default", 
        profile_args={"database": "MOVIELENS", "schema": "DEV"},
    )
)

with DAG(
    dag_id="netflix_dbt_pipeline",
    start_date=datetime(2026, 7, 27),
    schedule="@daily",
    catchup=False,
) as dag:

    
    transform_data = DbtTaskGroup(
        group_id="run_dbt_models",
        project_config=ProjectConfig(DEFAULT_DBT_ROOT_PATH),
        profile_config=profile_config,
    )

    transform_data