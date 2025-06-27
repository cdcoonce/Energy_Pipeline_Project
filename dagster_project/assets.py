
from dagster import asset
from dagster_dbt import load_assets_from_dbt_project
from pathlib import Path

import polars as pl
import duckdb
import pyarrow

@asset
def power_output_cleaned():
    df = pl.read_csv("data/raw/power_output.csv")

    df = df.with_columns([
        pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M").alias("timestamp")
    ])

    df = df.with_columns([
        (pl.col("power_kw") / pl.col("capacity_kw")).alias("capacity_factor"),
        pl.col("timestamp").dt.date().alias("date"),
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.col("timestamp").dt.weekday().alias("weekday")
    ])

    df.write_csv("data/processed/power_output_cleaned.csv")
    return df

@asset
def daily_summary(power_output_cleaned: pl.DataFrame) -> pl.DataFrame:

    df = power_output_cleaned.group_by("date").agg([
        pl.col("power_kw").sum().alias("daily_total_kw"),
        pl.col("capacity_factor").mean().alias("avg_capacity_factor")
    ])

    # Save the summary to a CSV
    df.write_csv("data/processed/daily_summary.csv")
    print("Saved daily_summary.csv")
    return df

@asset() # deps=["power_output_cleaned"]
def power_output_table(power_output_cleaned: pl.DataFrame) -> str:
    print("Loading into DuckDB...")

    # Connect to Duckdb
    con = duckdb.connect("data/energy.duckdb")

    # Register the Polars DataFrame first
    con.register("power_output_cleaned_df", power_output_cleaned.to_pandas())

    # Load into DuckDB as a table
    con.execute("CREATE OR REPLACE TABLE power_output_cleaned AS SELECT * FROM power_output_cleaned_df")

    con.close()

    print("Loaded into DuckDB.")
    return "power_output_cleaned table created"


@asset()
def plant_locations_cleaned():
    df = pl.read_csv("data/raw/plant_locations.csv")

    # Clean the DataFrame — format plant_id as 'asset-XXX'
    df = df.with_columns(
        pl.col("plant_id").str.replace("_", "-").alias("plant_id")
    ).with_columns(
        pl.col("plant_id").str.replace("asset", "ASSET").alias("plant_id")
    )   

    df.write_csv("data/processed/plant_locations_cleaned.csv")
    return df

@asset(deps=["power_output_table"]) # deps=["power_output_cleaned"]
def plant_locations_table(plant_locations_cleaned: pl.DataFrame) -> pl.DataFrame:
    # Load CSV using Polars
    df = pl.read_csv("data/processed/plant_locations_cleaned.csv")

    # Connect to DuckDB and write the table
    con = duckdb.connect("data/energy.duckdb")
    con.register("plant_locations_df", df)
    
    # Write to DuckDB table
    con.execute("CREATE OR REPLACE TABLE plant_locations AS SELECT * FROM plant_locations_df")

    result = con.sql("SELECT COUNT(*) FROM plant_locations").fetchall()
    print(f"Loaded {result[0][0]} rows into plant_locations table.")

    con.close()

    return df

## Run the dbt pipeline

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent / "dbt_project"
DBT_PROFILES_DIR = Path.home() / ".dbt"

dbt_assets = load_assets_from_dbt_project(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
)