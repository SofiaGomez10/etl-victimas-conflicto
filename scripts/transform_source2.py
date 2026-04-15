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


def map_hecho_codes(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        1: "acto terrorista/atentados/combates/enfrentamientos/hostigamientos",
        2: "amenaza",
        3: "delitos contra la libertad e integridad sexual",
        4: "desaparicion forzada",
        5: "desplazamiento",
        6: "homicidio",
        7: "minas antipersonal/municion sin explotar/artefacto explosivo",
        8: "secuestro",
        9: "tortura",
        10: "vinculacion de ninos a grupos armados",
        11: "abandono o despojo de tierras",
        12: "perdida de bienes",
        13: "lesiones fisicas",
        14: "lesiones psicologicas",
        15: "confinamiento",
        20: "sin informacion",
    }

    if "param_victimization_fact" in df.columns:
        df["param_victimization_fact"] = pd.to_numeric(df["param_victimization_fact"], errors="coerce")
        df["param_victimization_fact"] = df["param_victimization_fact"].map(mapping)

    return df


def normalize_ethnicity(df: pd.DataFrame) -> pd.DataFrame:
    if "ethnic_group" in df.columns:
        df["ethnic_group"] = df["ethnic_group"].replace({
            "indigena (acreditado ra)": "indigena",
            "indigena": "indigena",
            "negro(a) o afrocolombiano(a)": "afrocolombiano",
            "afrocolombiano (acreditado ra)": "afrocolombiano",
            "negro (acreditado ra)": "afrocolombiano",
            "raizal del archipielago de san andres y providencia": "raizal",
            "gitano(a) rom": "rom",
            "gitano (rrom) (acreditado ra)": "rom",
        })
    return df


def normalize_age_range(df: pd.DataFrame) -> pd.DataFrame:
    if "age_range" in df.columns:
        df["age_range"] = df["age_range"].str.replace("entre", "", regex=False)
        df["age_range"] = df["age_range"].str.replace("y", "-", regex=False)
        df["age_range"] = df["age_range"].str.strip()
    return df


def prepare_for_groupby(df: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric and date columns before groupby. No category conversion yet."""
    if "date_processing" in df.columns:
        df["date_processing"] = pd.to_datetime(df["date_processing"], errors="coerce")
        df["year"] = df["date_processing"].dt.year.astype("Int64")
        df["month"] = df["date_processing"].dt.month.astype("Int64")

    if "total_victim" in df.columns:
        df["total_victim"] = pd.to_numeric(df["total_victim"], errors="coerce")
        df["total_victim_flag"] = df["total_victim"].apply(
            lambda x: "sin_informacion" if pd.isna(x) else "reportado"
        )

    return df


def group_and_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = [
        "date_processing",
        "year",
        "month",
        "victimization_fact",
        "sex",
        "ethnic_group",
        "age_range",
        "state_dept",
        "source",
        "total_victim_flag",
    ]

    group_cols = [c for c in group_cols if c in df.columns]

    df = (
        df.groupby(group_cols, dropna=False)
        .agg(total_victim=("total_victim", "sum"))
        .reset_index()
    )

    return df


def cast_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Convert to category AFTER groupby when dataframe is smaller."""
    categorical_cols = [
        "sex",
        "ethnic_group",
        "victimization_fact",
        "age_range",
        "state_dept",
        "total_victim_flag",
    ]

    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype("category")

    return df


def transform_source2(input_path: str, output_path: str):
    print("Loading source 2...")
    df = pd.read_parquet(input_path)

    df.columns = [col.lower() for col in df.columns]

    df = df.rename(columns={
        "fecha_corte": "date_processing",
        "hecho": "victimization_fact",
        "param_hecho": "param_victimization_fact",
        "sexo": "sex",
        "etnia": "ethnic_group",
        "ciclo_vital": "age_range",
        "per_ocu": "total_victim",
    })

    df["source"] = "santander"
    df["state_dept"] = "santander"

    df = normalize_text_columns(df)
    df = normalize_unknown_values(df)

    df = map_hecho_codes(df)

    df["victimization_fact"] = df["victimization_fact"].fillna(df["param_victimization_fact"])

    df = normalize_ethnicity(df)
    df = normalize_age_range(df)

    # Prepare numeric/date types before groupby (no category yet)
    df = prepare_for_groupby(df)

    # Groupby on string columns (no category explosion)
    df = group_and_aggregate(df)

    # Convert to category AFTER groupby (fewer rows now)
    df = cast_categories(df)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)

    print(f"Source 2 transformed: {len(df)} rows")


if __name__ == "__main__":
    transform_source2(
        input_path="data/processed/source2.parquet",
        output_path="data/processed/source2_transformed.parquet",
    )