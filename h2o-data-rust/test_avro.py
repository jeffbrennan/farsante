import polars as pl

df = pl.read_avro("G1_2e1_2e1_2_0.avro")
print(df.head())
