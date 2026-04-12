import pandas as pd
import sqlite3
import os

def ingest_source1(sqlite_path, csv_path, output_path):

    if os.path.exists(sqlite_path):
        conn = sqlite3.connect(sqlite_path)
        df = pd.read_sql("""
            SELECT
                v.vulnerability_index,
                v.total_victim,
                p.sex,
                p.ethnic_group,
                p.classification,
                a.victimization_fact,
                l.commune,
                r.date_processing,
                r.year
            FROM victims v
            JOIN person p ON v.id_person = p.id_person
            JOIN victimizing_act a ON v.id_act = a.id_act
            JOIN location l ON v.id_location = l.id_location
            JOIN registration_date r ON v.id_date = r.id_date
        """, conn)
        conn.close()
    else:
        raise FileNotFoundError(f"SQLite DB not found at {sqlite_path}")

    df["source"] = "source1_cali"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Source 1 ingested: {len(df)} records")

