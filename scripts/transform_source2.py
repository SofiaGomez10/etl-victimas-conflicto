#Import necessary libraries
import pandas as pd
import unicodedata
import os


def remove_special_chars(text: str) -> str:
    """
    Removes accents and special characters from a string,
    returning the result in lowercase without leading or trailing spaces.

    Args:
        text (str): Input string to clean.

    Returns:
        str: Cleaned string in lowercase. If input is not a string, returns it unchanged.
    """
    if not isinstance(text, str):
        return text
    #Breaks down each character into base letter + accent (NFKD) and removes the accents
    nfkd = unicodedata.normalize("NFKD", text)
    cleaned = "".join(c for c in nfkd if not unicodedata.combining(c))
    return cleaned.lower().strip()


def normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies remove_special_chars() to all text columns in the DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame to normalize.

    Returns:
        pd.DataFrame: DataFrame with normalized text columns.
    """
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(remove_special_chars)
    return df


def normalize_unknown_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unifies all variants of missing or unknown values into a single
    standard label 'sin informacion' across all text columns.

    Args:
        df (pd.DataFrame): Input DataFrame to normalize.

    Returns:
        pd.DataFrame: DataFrame with standardized unknown values.
    """
    unknown_variants = [
        "no registra", "no informa", "sin informacion", "no informado",
        "no registrado", "nan", "none", "nd", "",
    ]
    for col in df.select_dtypes(include="object").columns:
        #fillna covers real NaN, truly empty cells
        df[col] = df[col].fillna("sin informacion")
        #replace covers strings that mean missing data
        df[col] = df[col].replace(unknown_variants, "sin informacion")
    return df


def map_hecho_codes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Maps numeric victimization fact codes to their corresponding
    descriptive labels based on the official classification.

    Args:
        df (pd.DataFrame): Input DataFrame with numeric victimization codes.

    Returns:
        pd.DataFrame: DataFrame with victimization codes replaced by descriptive labels.
    """
    mapping = {
        1: "acto terrorista/atentados/combates/enfrentamientos/hostigamientos",
        2: "amenaza", 3: "delitos contra la libertad e integridad sexual",
        4: "desaparicion forzada", 5: "desplazamiento", 6: "homicidio",
        7: "minas antipersonal/municion sin explotar/artefacto explosivo",
        8: "secuestro", 9: "tortura", 10: "vinculacion de ninos a grupos armados",
        11: "abandono o despojo de tierras", 12: "perdida de bienes",
        13: "lesiones fisicas", 14: "lesiones psicologicas",
        15: "confinamiento", 20: "sin informacion",
    }
    if "param_victimization_fact" in df.columns:
        #Converts to numeric first to ensure the mapping works correctly
        df["param_victimization_fact"] = pd.to_numeric(df["param_victimization_fact"], errors="coerce")
        df["param_victimization_fact"] = df["param_victimization_fact"].map(mapping)
    return df


def normalize_ethnicity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Consolidates textual variants of ethnic group labels into
    standardized categories for consistency across sources.

    Args:
        df (pd.DataFrame): Input DataFrame to normalize.

    Returns:
        pd.DataFrame: DataFrame with standardized ethnicity values.
    """
    if "ethnic_group" in df.columns:
        #Maps all variants of each ethnic group to a single standard label
        df["ethnic_group"] = df["ethnic_group"].replace({
            "indigena (acreditado ra)": "indigena", "indigena": "indigena",
            "negro(a) o afrocolombiano(a)": "afrocolombiano",
            "afrocolombiano (acreditado ra)": "afrocolombiano",
            "negro (acreditado ra)": "afrocolombiano",
            "raizal del archipielago de san andres y providencia": "raizal",
            "gitano(a) rom": "rom", "gitano (rrom) (acreditado ra)": "rom",
        })
    return df


def normalize_age_range(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes age range labels by removing filler words and
    replacing connectors with a uniform format.

    Args:
        df (pd.DataFrame): Input DataFrame to normalize.

    Returns:
        pd.DataFrame: DataFrame with standardized age range values.
    """
    if "age_range" in df.columns:
        #Removes the word "entre" from the age range label
        df["age_range"] = df["age_range"].str.replace("entre", "", regex=False)
        #Replaces "y" connector with "-" for a consistent interval format
        df["age_range"] = df["age_range"].str.replace("y", "-", regex=False)
        df["age_range"] = df["age_range"].str.replace(" - ", "-", regex=False)
        df["age_range"] = df["age_range"].str.strip()
    return df


def normalize_victimization_fact(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fixes encoding errors and inconsistent formatting in victimization
    fact labels, standardizing them to a clean uniform value.

    Args:
        df (pd.DataFrame): Input DataFrame to normalize.

    Returns:
        pd.DataFrame: DataFrame with standardized victimization fact labels.
    """
    if "victimization_fact" in df.columns:
        #Replaces corrupted or inconsistently formatted labels with their correct standard value
        df["victimization_fact"] = df["victimization_fact"].replace({
            "acto terrorista / atentados / combates / enfrentamientos / hostigamientos": "acto terrorista/atentados/combates/enfrentamientos/hostigamientos",
            "desaparicia3n forzada": "desaparicion forzada",
            "desaparicia\x83a3n forzada": "desaparicion forzada",
            "desaparicii¿1⁄2n forzada": "desaparicion forzada",
            "vinculacia3n de nia±os nia±as y adolescentes a actividades relacionadas con grupos armados": "vinculacion de ninos ninas y adolescentes a actividades relacionadas con grupos armados",
            "minas antipersonal, municii¿1⁄2n sin explotar y artefacto explosivo improvisado": "minas antipersonal, municion sin explotar y artefacto explosivo improvisado",
            "minas antipersonal, municia3n sin explotar y artefacto explosivo improvisado": "minas antipersonal, municion sin explotar y artefacto explosivo improvisado",
        })
    return df


def prepare_for_groupby(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts key columns to the correct data types and extracts
    year and month as separate columns to enable grouping operations.

    Args:
        df (pd.DataFrame): Input DataFrame to prepare.

    Returns:
        pd.DataFrame: DataFrame ready for groupby operations.
    """
    if "date_processing" in df.columns:
        # format="mixed" handles dates with inconsistent formats across rows
        df["date_processing"] = pd.to_datetime(
            df["date_processing"], format="mixed", dayfirst=True, errors="coerce"
        )
        #Extracts year and month for temporal grouping
        df["year"] = df["date_processing"].dt.year.astype("Int64")
        df["month"] = df["date_processing"].dt.month.astype("Int64")
    if "total_victim" in df.columns:
        #Converts to numeric; invalid values become NaN instead of raising an error
        df["total_victim"] = pd.to_numeric(df["total_victim"], errors="coerce")
    return df


def group_and_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Groups the DataFrame by key demographic and geographic columns,
    aggregating the total number of victims per group.

    Args:
        df (pd.DataFrame): Input DataFrame to group and aggregate.

    Returns:
        pd.DataFrame: Aggregated DataFrame with total victims and data quality flag per group.
    """
    group_cols = [
        "date_processing", "year", "month", "victimization_fact",
        "sex", "ethnic_group", "age_range", "state_dept", "source",
    ]
    #Filters out columns that are not present in the DataFrame
    group_cols = [c for c in group_cols if c in df.columns]
    #Groups by all available columns and sums total_victim per group
    df = (
        df.groupby(group_cols, dropna=False)
        .agg(total_victim=("total_victim", "sum"))
        .reset_index()
    )
    #Creates a flag column to identify groups where the victim count could not be determined
    df["total_victim_flag"] = df["total_victim"].apply(
        lambda x: "sin informacion" if pd.isna(x) else "reportado"
    )
    return df


def cast_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts key categorical columns to the 'category' dtype
    to reduce memory usage compared to plain strings.

    Args:
        df (pd.DataFrame): Input DataFrame to cast.

    Returns:
        pd.DataFrame: DataFrame with categorical columns properly typed.
    """
    for col in ["sex", "ethnic_group", "victimization_fact", "age_range", "state_dept", "total_victim_flag"]:
        if col in df.columns:
            df[col] = df[col].astype("category")
    return df


def transform_source2(input_path: str, output_path: str):
    """
    Transforms raw data from source 2 (Santander) into a standardized format.
    Applies column renaming, normalization, type casting, and aggregation
    to prepare the dataset for further analysis.

    Args:
        input_path (str): Path to the input parquet file containing raw data.
        output_path (str): Path where the transformed parquet file will be saved.

    Returns:
        None: Writes the transformed DataFrame to the specified output path.
    """
    print("Loading source 2...")
    #Loads the parquet file into a DataFrame
    df = pd.read_parquet(input_path)
    #Converts all column names to lowercase
    df.columns = [col.lower() for col in df.columns]
    #Renames columns to standardized names
    df = df.rename(columns={
        "fecha_corte": "date_processing", "hecho": "victimization_fact",
        "param_hecho": "param_victimization_fact", "sexo": "sex",
        "etnia": "ethnic_group", "ciclo_vital": "age_range", "per_ocu": "total_victim",
    })
    #Adds source and state_dept columns
    df["source"] = "santander"
    df["state_dept"] = "santander"
    #Normalizes text columns and standardizes unknown values
    df = normalize_text_columns(df)
    df = normalize_unknown_values(df)
    #Maps victimization codes to descriptive labels
    df = map_hecho_codes(df)
    #Fills missing 'victimization_fact' values using 'param_victimization_fact'
    df["victimization_fact"] = df["victimization_fact"].fillna(df["param_victimization_fact"])
    df = normalize_victimization_fact(df)
    #Normalizes ethnicity labels and standardizes age range labels
    df = normalize_ethnicity(df)
    df = normalize_age_range(df)
    #Applies the full transformation pipeline to prepare for grouping and aggregation
    df = prepare_for_groupby(df)
    df = group_and_aggregate(df)
    df = cast_categories(df)
    #Ensures output directory exists before saving
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Source 2 transformed: {len(df)} rows")

#Executes the transformation when the script is run directly
if __name__ == "__main__":
    transform_source2(
        input_path="data/processed/source2.parquet",
        output_path="data/processed/source2_transformed.parquet",
    )