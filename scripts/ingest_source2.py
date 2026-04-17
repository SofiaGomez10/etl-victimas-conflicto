import pandas as pd
import os

def ingest_source2(csv_path, output_path):
    """
    Ingests victim data from CSV file and saves it as a Parquet file.

    Reads the CSV at "csv_path". Tags each row with "source2_santander" for traceability
    and writes the result to "output_path" in Parquet format. Parent directories
    are created automatically if missing.

    Args:
        csv_path (str): Path to the source CSV file.
        output_path (str): Destination path for the output Parquet file.
    """

    df = pd.read_csv(csv_path, encoding="latin-1", low_memory=False)
    df["source"] = "source2_santander"

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Source 2 ingested: {len(df)} records")