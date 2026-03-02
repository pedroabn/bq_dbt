# include/dbt/cosmos_config.py

from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.constants import ExecutionMode
from pathlib import Path

DBT_CONFIG = ProfileConfig(
    profile_name='meta',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/include/dbt/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/include/dbt/',
)

DBT_EXECUTION_CONFIG = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path='/usr/local/airflow/dbt_venv/bin/dbt',
)