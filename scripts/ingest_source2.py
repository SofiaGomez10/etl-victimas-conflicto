import pandas as pd
import os

def ingest_source2(csv_path, output_path):

    df = pd.read_csv(csv_path, encoding="latin-1", low_memory=False)
    df["source"] = "source2_santander"

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Source 2 ingested: {len(df)} records")