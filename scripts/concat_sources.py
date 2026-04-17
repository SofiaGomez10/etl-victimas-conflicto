#import libraries 
import pandas as pd
import os

#Define name of columns that will be present in the final dataset
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
    "source",
]


def load_sources():
    """
    Uploads data sources transformed from parquet format files.

    This function reads three files located in the path 'data/processed/',
    corresponding to different previously processed data sources,
    and returns them as panda DataFrames.

    Returns:
        tuple: A tuple with three DataFrames (df1, df2, df3), each
        representing a different data source.
    """
    df1 = pd.read_parquet("data/processed/source1_transformed.parquet")
    df2 = pd.read_parquet("data/processed/source2_transformed.parquet")
    df3 = pd.read_parquet("data/processed/source3_transformed.parquet")
    return df1, df2, df3


def select_common_columns(df):
    """
    Standardizes the structure of a DataFrame to match the predefined COLUMNS list.

    This function ensures that all required columns are present in the input DataFrame.
    For each column in COLUMNS:
        - If the column does not exist in the DataFrame, it is created with empty values (pd.NA).
        - If the column exists, it is left unchanged.
    Finally, the DataFrame is returned with exactly the columns listed in COLUMNS,
    preserving the specified order.

    Returns:
        pd.DataFrame: A DataFrame containing all columns from COLUMNS, in the correct order,
        with missing columns filled with empty values.
    """

    for col in COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[COLUMNS]


def concat_sources(df1, df2, df3):
    """
    Selects common columns from three DataFrames and concatenates them into one.

    Args:
        df1, df2, df3 (pd.DataFrame): Input data sources.

    Returns:
        pd.DataFrame: Concatenated DataFrame.
    """
    df1 = select_common_columns(df1)
    df2 = select_common_columns(df2)
    df3 = select_common_columns(df3)
    return pd.concat([df1, df2, df3], ignore_index=True)


def save_output(df):
    """
    Save the DataFrame in a parquet file and show statistics.

    The function creates the output folder if it does not exist, save the DataFrame
    in 'data/processed/dataset_final.parquet' and then print:
        - The total number of rows.
        - The number of records per source.

    Args:
        df (pd.DataFrame): The DataFrame that contains the combined data.

    Returns:
        None    
    """
    output_path = "data/processed/dataset_final.parquet"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Final total dataset rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")


def run_concat(source1_path, source2_path, source3_path, output_path):
    """
     Entry point for Airflow task.
        Execute the merge process of multiple data sources.

        This function acts as an entry point for an Airflow task. It is responsible for:
        - Upload three datasets from parquet files.
        - Unify them using the `concat_sources`function.
        - Save the result in the specified path.
        - Show basic result information (number of rows and distribution by source).

        Args:
            source1_path (str): Path of the parquet file from source 1.
            source2_path (str): Path of the parquet file from source 2.
            source3_path (str): Path to the parquet file of source 3.
            output_path (str): The route where the final unified dataset will be saved.

        Returns:
            None
    """
    df1 = pd.read_parquet(source1_path)
    df2 = pd.read_parquet(source2_path)
    df3 = pd.read_parquet(source3_path)

    df_final = concat_sources(df1, df2, df3)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_final.to_parquet(output_path, index=False)

    print(f"Concat complete: {len(df_final)} rows")
    print(f"Columns: {list(df_final.columns)}")

# Runs the full flow: upload, concat and save data
if __name__ == "__main__":
    df1, df2, df3 = load_sources()
    df_final = concat_sources(df1, df2, df3)
    save_output(df_final)