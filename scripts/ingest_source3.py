import pandas as pd
import requests
import os

def ingest_source3(url, start_year, output_path):

    all_records = []
    page_size = 50000
    offset = 0

    while True:
        params = {
            "$limit": page_size,
            "$offset": offset,
            "$where": f"anio >= '{start_year}'",
            "$order": "anio DESC"
        }

        try:
            response = requests.get(url, params=params, timeout=120)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception(f"API request failed: {e}")

        records = response.json()

        if not records:
            break

        all_records.extend(records)
        print(f"Downloaded {len(all_records)} records so far...")

        if len(records) < page_size:
            break

        offset += page_size

    df = pd.DataFrame(all_records)
    df["source"] = "source3_api_displacement"

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)

    print(f"Source 3 ingested: {len(df)} records")