import pandas as pd
import unicodedata
import os


def remove_special_chars(text: str) -> str:
    if not isinstance(text, str):
        return text
    nfkd = unicodedata.normalize("NFKD", text)
    cleaned = "".join(c for c in nfkd if not unicodedata.combining(c))
    return cleaned.lower().strip()


def normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include="object").columns:
        if df[col].astype(str).str.contains(r"[A-ZÁÉÍÓÚÑ]", regex=True).any():
            df[col] = df[col].apply(remove_special_chars)
    return df


def normalize_unknown_values(df: pd.DataFrame) -> pd.DataFrame:
    unknown_variants = [
        "no registra",
        "no informa",
        "sin informacion",
        "no informado",
        "no registrado",
        "nan",
        "none",
        "nd",
        "",
    ]

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].fillna("sin informacion")
        df[col] = df[col].replace(unknown_variants, "sin informacion")

    return df


def normalize_ethnicity(df: pd.DataFrame) -> pd.DataFrame:
    if "ethnic_group" in df.columns:
        afro_variants = [
            "afrodescendiente",
            "negro",
            "moreno",
        ]
        df["ethnic_group"] = df["ethnic_group"].replace(afro_variants, "afrocolombiano")
    return df


def normalize_age_range(df: pd.DataFrame) -> pd.DataFrame:
    if "age_range" in df.columns:
        classification_map = {
            "adulthood": "27-59",
            "youth": "14-26",
            "childhood": "0-13",
            "older adults": "60+",
        }
        df["age_range"] = df["age_range"].replace(classification_map)
    return df


def prepare_for_groupby(df: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric and date columns before groupby. No category conversion yet."""
    if "vulnerability_index" in df.columns:
        df["vulnerability_index"] = pd.to_numeric(df["vulnerability_index"], errors="coerce")

    if "date_processing" in df.columns:
        df["date_processing"] = pd.to_datetime(df["date_processing"], errors="coerce")
        df["year"] = df["date_processing"].dt.year.astype("Int64")
        df["month"] = df["date_processing"].dt.month.astype("Int64")

    return df


def group_and_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Each row in source 1 represents one victim.
    Use size() to count victims per group instead of summing.
    """
    group_cols = [
        "vulnerability_index",
        "sex",
        "ethnic_group",
        "age_range",
        "victimization_fact",
        "commune",
        "date_processing",
        "year",
        "month",
        "source",
        "state_dept",
    ]

    group_cols = [c for c in group_cols if c in df.columns]

    df = (
        df.groupby(group_cols, dropna=False)
        .size()
        .reset_index(name="total_victim")
    )

    return df


def cast_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Convert to category AFTER groupby when dataframe is smaller."""
    categorical_cols = [
        "sex",
        "ethnic_group",
        "age_range",
        "victimization_fact",
        "commune",
        "state_dept",
    ]

    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype("category")

    return df


def transform_source1(input_path: str, output_path: str):
    print("Loading source 1...")
    df = pd.read_parquet(input_path)
    print(f"Rows before transform: {len(df)}")

    df["source"] = "cali"
    df["state_dept"] = "valle del cauca"

    if "classification" in df.columns:
        df = df.rename(columns={"classification": "age_range"})

    # Drop total_victim from SQLite if present (it has wrong values)
    if "total_victim" in df.columns:
        df = df.drop(columns=["total_victim"])

    df = normalize_text_columns(df)
    df = normalize_unknown_values(df)
    df = normalize_ethnicity(df)
    df = normalize_age_range(df)

    # Prepare numeric/date types before groupby
    df = prepare_for_groupby(df)

    # Count victims per group using size()
    df = group_and_aggregate(df)

    # Convert to category AFTER groupby
    df = cast_categories(df)

    print(f"Rows after transform: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Saved to {output_path}")


if __name__ == "__main__":
    transform_source1(
        input_path="data/processed/source1.parquet",
        output_path="data/processed/source1_transformed.parquet",
    )