
from dagster import asset
import polars as pl
import duckdb
import pyarrow

@asset
def power_output_cleaned():
    df = pl.read_csv("../data/raw/power_output.csv")

    df = df.with_columns([
        pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M").alias("timestamp")
    ])

    df = df.with_columns([
        (pl.col("power_kw") / pl.col("capacity_kw")).alias("capacity_factor"),
        pl.col("timestamp").dt.date().alias("date"),
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.col("timestamp").dt.weekday().alias("weekday")
    ])

    df.write_csv("../data/processed/power_output_cleaned.csv")
    return df

@asset
def daily_summary(power_output_cleaned: pl.DataFrame) -> pl.DataFrame:

    df = power_output_cleaned.group_by("date").agg([
        pl.col("power_kw").sum().alias("daily_total_kw"),
        pl.col("capacity_factor").mean().alias("avg_capacity_factor")
    ])

    # Save the summary to a CSV
    df.write_csv("../data/processed/daily_summary.csv")
    print("Saved daily_summary.csv")
    return df

@asset(deps=["power_output_cleaned"])
def power_output_table(power_output_cleaned: pl.DataFrame) -> str:
    print("Loading into DuckDB...")

    # Connect to Duckdb
    con = duckdb.connect("../data/energy.duckdb")

    # Register the Polars DataFrame first
    con.register("power_output_cleaned_df", power_output_cleaned.to_pandas())

    # Load into DuckDB as a table
    con.execute("CREATE OR REPLACE TABLE power_output_cleaned AS SELECT * FROM power_output_cleaned_df")

    print("Loaded into DuckDB.")
    return "power_output_cleaned table created"