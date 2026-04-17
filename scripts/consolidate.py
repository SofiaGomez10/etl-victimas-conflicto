import pandas as pd
import os


GROUP_COLS = [
    "date_processing",
    "year",
    "month",
    "state_dept",
    "victimization_fact",
    "sex",
    "ethnic_group",
    "age_range",
]


def consolidate(input_path: str, output_path: str):
    print("Loading dataset_final...")
    df = pd.read_parquet(input_path)
    print(f"Rows before consolidation: {len(df)}")
    print(f"Duplicates before: {df.duplicated().sum()}")

    # Convert category columns back to string for groupby
    for col in df.select_dtypes(include="category").columns:
        df[col] = df[col].astype(str)

    # Keep only group cols that exist
    group_cols = [c for c in GROUP_COLS if c in df.columns]

    df = (
        df.groupby(group_cols, dropna=False)
        .agg(total_victim=("total_victim", "sum"))
        .reset_index()
    )

    print(f"Rows after consolidation: {len(df)}")
    print(f"Duplicates after: {df.duplicated().sum()}")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Saved to {output_path}")


if __name__ == "__main__":
    consolidate(
        input_path="data/processed/dataset_final.parquet",
        output_path="data/processed/dataset_consolidated.parquet",
    )