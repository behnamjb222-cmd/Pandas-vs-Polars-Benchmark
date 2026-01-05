import pandas as pd
import polars as pl
import numpy as np
import time
import os

# 1. Generate Fake Big Data (10 Million Rows)
ROW_COUNT = 10_000_000
print(f"🚀 Generating {ROW_COUNT:,} rows of data...")

data = {
    'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], ROW_COUNT),
    'value_1': np.random.rand(ROW_COUNT),
    'value_2': np.random.randint(1, 100, ROW_COUNT)
}

# Save to CSV to be fair (reading from disk is part of the job)
df_temp = pd.DataFrame(data)
df_temp.to_csv('big_data.csv', index=False)
del df_temp # Clear memory
print("✅ Data generated and saved to 'big_data.csv'.")
print("-" * 30)

# --- ROUND 1: PANDAS 🐼 ---
print("🐼 Starting PANDAS Benchmark...")
start_time = time.time()

# Read & GroupBy
df_pd = pd.read_csv('big_data.csv')
res_pd = df_pd.groupby('category').agg({'value_1': 'mean', 'value_2': 'sum'})

pandas_time = time.time() - start_time
print(f"🏁 Pandas Time: {pandas_time:.4f} seconds")

# --- ROUND 2: POLARS 🐻‍❄️ ---
print("\n🐻‍❄️ Starting POLARS Benchmark...")
start_time = time.time()

# Read & GroupBy (Polars uses multi-threading automatically)
df_pl = pl.read_csv('big_data.csv')
res_pl = df_pl.group_by('category').agg([
    pl.col('value_1').mean(),
    pl.col('value_2').sum()
])

polars_time = time.time() - start_time
print(f"🏁 Polars Time: {polars_time:.4f} seconds")

# --- RESULT ---
print("-" * 30)
speedup = pandas_time / polars_time
print(f"🔥 WOW! Polars was {speedup:.2f}x faster than Pandas!")