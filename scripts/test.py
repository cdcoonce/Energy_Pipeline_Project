import duckdb

con = duckdb.connect("data/energy.duckdb")

# Run a query
df = con.execute("SELECT * FROM power_output_cleaned LIMIT 5 ").fetchdf()
print(df)