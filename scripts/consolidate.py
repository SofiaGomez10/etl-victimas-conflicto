# Import required libraries
import pandas as pd
import os

# Columns used as grouping keys during consolidation
GROUP_COLS = [
    "date_processing",
    "year",
    "month",
    "state_dept",
    "victimization_fact",
    "sex",
    "ethnic_group",
    "age_range",
    "source",
]


def consolidate(input_path: str, output_path: str):
    """
    Consolidates the final dataset by grouping and aggregating victim counts.

    Loads a Parquet file, converts categorical columns to strings to avoid
    groupby compatibility issues, groups by the defined dimension columns,
    sums total_victim, and saves the result as a new Parquet file.

    Args:
        input_path (str): Path to the input Parquet file.
        output_path (str): Path where the consolidated Parquet file will be saved.
    """
    print("Loading dataset_final...")
    df = pd.read_parquet(input_path)
    print(f"Rows before consolidation: {len(df)}")
    print(f"Duplicates before: {df.duplicated().sum()}")

    # Convert category columns to string to ensure groupby compatibility
    for col in df.select_dtypes(include="category").columns:
        df[col] = df[col].astype(str)

    # Use only grouping columns that are present in the DataFrame
    group_cols = [c for c in GROUP_COLS if c in df.columns]

    df = (
        df.groupby(group_cols, dropna=False)
        .agg(total_victim=("total_victim", "sum"))
        .reset_index()
    )

    print(f"Rows after consolidation: {len(df)}")
    print(f"Duplicates after: {df.duplicated().sum()}")

    # Create output directory if it does not exist and save result
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Saved to {output_path}")


if __name__ == "__main__":
    consolidate(
        input_path="data/processed/dataset_final.parquet",
        output_path="data/processed/dataset_consolidated.parquet",
    )