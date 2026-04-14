import pandas as pd
import unicodedata
import os


def remove_special_chars(text: str) -> str:
    if not isinstance(text, str):
        return text
    nfkd = unicodedata.normalize("NFKD", text)
    cleaned = "".join(c for c in nfkd if not unicodedata.combining(c))
    return cleaned.lower().strip()


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [remove_special_chars(col).replace(" ", "_") for col in df.columns]
    return df


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
        "sin definir",
        "nan",
        "none",
        "nd",
        "",
    ]

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].fillna("sin informacion")
        df[col] = df[col].replace(unknown_variants, "sin informacion")

    return df


def normalize_departments(df: pd.DataFrame) -> pd.DataFrame:
    departamentos = {
        "91": "amazonas",
        "05": "antioquia",
        "81": "arauca",
        "08": "atlantico",
        "11": "bogota, d.c.",
        "13": "bolivar",
        "15": "boyaca",
        "17": "caldas",
        "18": "caqueta",
        "85": "casanare",
        "19": "cauca",
        "20": "cesar",
        "27": "choco",
        "23": "cordoba",
        "25": "cundinamarca",
        "94": "guainia",
        "95": "guaviare",
        "41": "huila",
        "44": "la guajira",
        "47": "magdalena",
        "50": "meta",
        "52": "narino",
        "54": "norte de santander",
        "86": "putumayo",
        "63": "quindio",
        "66": "risaralda",
        "88": "san andres",
        "68": "santander",
        "70": "sucre",
        "73": "tolima",
        "76": "valle del cauca",
        "97": "vaupes",
        "99": "vichada",
    }

    dept_to_code = {v: k for k, v in departamentos.items()}

    if "cod_estado_depto" in df.columns:
        df["cod_estado_depto"] = df["cod_estado_depto"].astype(str).str.zfill(2)
        df.loc[
            df["cod_estado_depto"].isin(["00", "0", "sin informacion", "nan", "none"]),
            "cod_estado_depto"
        ] = "sin informacion"

    if "state_dept" in df.columns:
        df.loc[
            df["state_dept"].isin(["sin definir", "sin informacion", "nan", "none"]),
            "state_dept"
        ] = "sin informacion"

    if "cod_estado_depto" in df.columns and "state_dept" in df.columns:
        mask_code_missing = (
            df["cod_estado_depto"].eq("sin informacion")
            & df["state_dept"].ne("sin informacion")
        )
        df.loc[mask_code_missing, "cod_estado_depto"] = (
            df.loc[mask_code_missing, "state_dept"].map(dept_to_code)
        )

        mask_name_missing = (
            df["state_dept"].eq("sin informacion")
            & df["cod_estado_depto"].ne("sin informacion")
        )
        df.loc[mask_name_missing, "state_dept"] = (
            df.loc[mask_name_missing, "cod_estado_depto"].map(departamentos)
        )

    if "cod_estado_depto" in df.columns:
        df.loc[
            ~df["cod_estado_depto"].isin(list(departamentos.keys()) + ["sin informacion"]),
            "cod_estado_depto"
        ] = "sin informacion"

    if "state_dept" in df.columns:
        df.loc[
            ~df["state_dept"].isin(list(departamentos.values()) + ["sin informacion"]),
            "state_dept"
        ] = "sin informacion"

    return df


def normalize_ethnicity(df: pd.DataFrame) -> pd.DataFrame:
    if "ethnic_group" in df.columns:
        df["ethnic_group"] = df["ethnic_group"].replace({
            "afrocolombiano (acreditado ra)": "afrocolombiano",
            "negro(a) o afrocolombiano(a)": "afrocolombiano",
            "negro (acreditado ra)": "afrocolombiano",
            "indigena": "indigena",
            "indigena (acreditado ra)": "indigena",
            "gitano(a) rom": "rom",
            "gitano (rrom) (acreditado ra)": "rom",
            "raizal del archipielago de san andres y providencia": "raizal",
            "palenquero (acreditado ra)": "palenquero",
            "palenquero": "palenquero",
        })
    return df


def normalize_age_range(df: pd.DataFrame) -> pd.DataFrame:
    if "age_range" in df.columns:
        df["age_range"] = df["age_range"].astype(str)
        df["age_range"] = df["age_range"].str.replace("entre", "", regex=False)
        df["age_range"] = df["age_range"].str.replace("y", "-", regex=False)
        df["age_range"] = df["age_range"].str.strip()
    return df


def cast_data_types(df: pd.DataFrame) -> pd.DataFrame:
    if "date_processing" in df.columns:
        df["date_processing"] = pd.to_datetime(
            df["date_processing"],
            format="mixed",
            dayfirst=True,
            errors="coerce"
        )

        # Extraer year y month desde date_processing
        df["year"] = df["date_processing"].dt.year.astype("Int64")
        df["month"] = df["date_processing"].dt.month.astype("Int64")

    if "total_victim" in df.columns:
        df["total_victim"] = pd.to_numeric(df["total_victim"], errors="coerce")
        df["total_victim_flag"] = df["total_victim"].apply(
            lambda x: "sin_informacion" if pd.isna(x) else "reportado"
        )

    categorical_cols = [
        "nom_rpt",
        "cod_pais",
        "pais",
        "cod_estado_depto",
        "state_dept",
        "sex",
        "ethnic_group",
        "age_range",
        "per_llegada",
        "eventos",
        "source",
        "total_victim_flag",
    ]

    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype("category")

    return df


def group_and_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = [col for col in df.columns if col != "total_victim"]

    df = (
        df.groupby(group_cols, dropna=False)
        .agg(total_victim=("total_victim", "sum"))
        .reset_index()
    )

    return df


def transform_source3(input_path: str, output_path: str):
    print("Loading source 3...")
    df = pd.read_parquet(input_path)
    print(f"Rows before transform: {len(df)}")

    df = normalize_column_names(df)

    df = df.rename(columns={
        "fecha_corte": "date_processing",
        "estado_depto": "state_dept",
        "sexo": "sex",
        "hecho": "victimization_fact",
        "etnia": "ethnic_group",
        "ciclo_vital": "age_range",
        "per_ocu": "total_victim",
    })

    df["source"] = "nacional (colombia)"

    df = normalize_text_columns(df)
    df = normalize_unknown_values(df)

    df = normalize_departments(df)
    df = normalize_ethnicity(df)
    df = normalize_age_range(df)

    if "per_llegada" in df.columns:
        df["per_llegada"] = df["per_llegada"].fillna("sin informacion")

    if "eventos" in df.columns:
        df["eventos"] = df["eventos"].fillna("sin informacion")

    df = cast_data_types(df)

    df = group_and_aggregate(df)

    print(f"Rows after transform: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Saved to {output_path}")


if __name__ == "__main__":
    transform_source3(
        input_path="data/processed/source3.parquet",
        output_path="data/processed/source3_transformed.parquet",
    )