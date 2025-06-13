import polars as pl

# Load the raw data
df = pl.read_csv("Energy_Pipeline_Project/data/raw/power_output.csv")

# Convert timestamp to datetime
df = df.with_columns([
    pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M").alias("timestamp")
])

# Calculate capacity factor
df = df.with_columns([
    (pl.col("power_kw") / pl.col("capacity_kw")).alias("capacity_factor")
])

# Extract Time Based Features
df = df.with_columns([
    pl.col("timestamp").dt.date().alias("date"),
    pl.col("timestamp").dt.hour().alias("hour"),
    pl.col("timestamp").dt.weekday().alias("weekday")
])

# Print the columns and data types
print("Schema")
print(df.schema)

# Print First 5 Rows
print("\nSample Data:")
print(df.head(5))

# Save cleaned data
df.write_csv("Energy_Pipeline_Project/data/processed/power_output_cleaned.csv")
print("\nCleaned data saved to Energy_Pipeline_Project/data/processed/power_output_cleaned.csv\n")