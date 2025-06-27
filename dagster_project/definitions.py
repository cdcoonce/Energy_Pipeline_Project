from pathlib import Path
from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_project import jobs, sensors
from dagster_project.assets import dbt_assets

defs = Definitions(
    assets=[*dbt_assets],
    jobs=[jobs.notify_on_trends_job],
    sensors=[sensors.trends_sensor],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(Path(__file__).resolve().parent.parent / "dbt_project"),
            profiles_dir=str(Path.home() / ".dbt"),
        )
    }
)
