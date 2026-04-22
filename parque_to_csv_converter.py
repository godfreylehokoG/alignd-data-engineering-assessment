import pandas as pd

df = pd.read_parquet('health_lapses.parquet')
print(df.columns.tolist())
print(df.head())
print(df.dtypes)

# Output to CSV
output_file = 'health_lapses.csv'
df.to_csv(output_file, index=False)
print(f"\nSuccessfully exported to {output_file}")