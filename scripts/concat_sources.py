import pandas as pd
import os


COLUMNS = [
    "date_processing",
    "year",
    "month",
    "state_dept",
    "victimization_fact",
    "sex",
    "ethnic_group",
    "age_range",
    "total_victim",
]


def load_sources():
    df1 = pd.read_parquet("data/processed/source1_transformed.parquet")
    df2 = pd.read_parquet("data/processed/source2_transformed.parquet")
    df3 = pd.read_parquet("data/processed/source3_transformed.parquet")

    return df1, df2, df3


def select_common_columns(df):
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    return df[COLUMNS]


def concat_sources(df1, df2, df3):
    df1 = select_common_columns(df1)
    df2 = select_common_columns(df2)
    df3 = select_common_columns(df3)

    df_final = pd.concat([df1, df2, df3], ignore_index=True)

    return df_final


def save_output(df):
    output_path = "data/processed/dataset_final.parquet"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_parquet(output_path, index=False)

    print(f"Filas totales: {len(df)}")
    print(f"Columns: {list(df.columns)}")


def run_concat(source1_path, source2_path, source3_path, output_path):
    """Entry point for Airflow task."""
    df1 = pd.read_parquet(source1_path)
    df2 = pd.read_parquet(source2_path)
    df3 = pd.read_parquet(source3_path)

    df_final = concat_sources(df1, df2, df3)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_final.to_parquet(output_path, index=False)

    print(f"Concat complete: {len(df_final)} rows")
    print(f"Columns: {list(df_final.columns)}")


if __name__ == "__main__":
    df1, df2, df3 = load_sources()
    df_final = concat_sources(df1, df2, df3)
    save_output(df_final)