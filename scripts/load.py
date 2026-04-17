#import the necessary libraries
import pandas as pd
import pymysql

# Configuration for the DW 
DB_CONFIG = {
    "host": "mysql_dw",
    "port": 3306,
    "user": "etl_user",
    "password": "etl_password",
    "database": "dw_victims",
    "charset": "utf8mb4",
}

# Data Warehouse script
DDL = [
    """CREATE TABLE IF NOT EXISTS person (
        id_person INT PRIMARY KEY NOT NULL,
        sex VARCHAR(100),
        ethnic_group VARCHAR(100),
        age_range VARCHAR(100)
    )""",
    """CREATE TABLE IF NOT EXISTS victimizing_act (
        id_act INT PRIMARY KEY NOT NULL,
        victimization_fact VARCHAR(100)
    )""",
    """CREATE TABLE IF NOT EXISTS location (
        id_location INT PRIMARY KEY NOT NULL,
        state_dept VARCHAR(100)
    )""",
    """CREATE TABLE IF NOT EXISTS registration_date (
        date_processing DATE PRIMARY KEY NOT NULL,
        year INT,
        month INT
    )""",
    """CREATE TABLE IF NOT EXISTS victims (
        id_person INT,
        id_act INT,
        id_location INT,
        date_processing DATE,
        total_victim INT,
        source VARCHAR(100),
        CONSTRAINT fk_person FOREIGN KEY (id_person) REFERENCES person(id_person),
        CONSTRAINT fk_act FOREIGN KEY (id_act) REFERENCES victimizing_act(id_act),
        CONSTRAINT fk_location FOREIGN KEY (id_location) REFERENCES location(id_location),
        CONSTRAINT fk_date FOREIGN KEY (date_processing) REFERENCES registration_date(date_processing)
    )""",
]


def make_dim(df, cols):
    """
    Builds a table from some columns in the given DataFrame.

    Extracts the specified columns, removes duplicate rows, resets the index,
    and inserts a surrogate integer key column 'id' starting at 1. The result
    is a deduplicated dimension table ready to be loaded into the Data Warehouse.

    Args:
        df (pd.DataFrame): Source DataFrame containing the columns to extract.
        cols (list[str]): List of column names that define the dimension.
    """
    dim = df[cols].drop_duplicates().reset_index(drop=True)
    dim.insert(0, "id", range(1, len(dim) + 1))
    return dim


def load_to_mysql(dataset_path: str):
    """
    Loads the validated consolidated dataset into MySQL following a star schema.

    Reads the Parquet file at 'dataset_path', creates the schema tables if they
    do not exist, builds four dimension tables (person, victimizing_act, location,
    registration_date) using make_dim, inserts each dimension into its corresponding
    MySQL table using INSERT IGNORE to avoid duplicates, and then resolves the
    surrogate keys via merge operations to populate the fact table (victims) with
    one row per record. 

    Args:
        dataset_path (str): Path to the consolidated Parquet file to be loaded.
    """
    print("Loading data...")
    df = pd.read_parquet(dataset_path)
    print(f"Rows: {len(df)}")

    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Create tables
    for ddl in DDL:
        cur.execute(ddl)
    conn.commit()
    print("Schema ready")

    # Dimensions
    dim_person = make_dim(df, ["sex", "ethnic_group", "age_range"])
    dim_act = make_dim(df, ["victimization_fact"])
    dim_location = make_dim(df, ["state_dept"])
    dim_date = df[["date_processing", "year", "month"]].drop_duplicates(subset=["date_processing"])

    for _, r in dim_person.iterrows():
        cur.execute("INSERT IGNORE INTO person VALUES (%s,%s,%s,%s)",
                    (r.id, r.sex, r.ethnic_group, r.age_range))

    for _, r in dim_act.iterrows():
        cur.execute("INSERT IGNORE INTO victimizing_act VALUES (%s,%s)",
                    (r.id, r.victimization_fact))

    for _, r in dim_location.iterrows():
        cur.execute("INSERT IGNORE INTO location VALUES (%s,%s)",
                    (r.id, r.state_dept))

    for _, r in dim_date.iterrows():
        date_val = pd.Timestamp(r.date_processing).date() if pd.notna(r.date_processing) else None
        if date_val:
            cur.execute("INSERT IGNORE INTO registration_date VALUES (%s,%s,%s)",
                        (date_val,
                         int(r.year) if pd.notna(r.year) else None,
                         int(r.month) if pd.notna(r.month) else None))

    conn.commit()
    print("Dimensions loaded")

    # Fact table
    df = df.merge(dim_person, on=["sex", "ethnic_group", "age_range"], how="left")
    df = df.merge(dim_act, on=["victimization_fact"], how="left", suffixes=("", "_act"))
    df = df.merge(dim_location, on=["state_dept"], how="left", suffixes=("", "_loc"))

    for _, r in df.iterrows():
        date_val = pd.Timestamp(r.date_processing).date() if pd.notna(r.date_processing) else None
        if not date_val:
            continue
        cur.execute(
            "INSERT INTO victims VALUES (%s,%s,%s,%s,%s,%s)",
            (
                int(r.id) if pd.notna(r.id) else None,
                int(r["id_act"]) if pd.notna(r["id_act"]) else None,
                int(r["id_loc"]) if pd.notna(r["id_loc"]) else None,
                date_val,
                int(r.total_victim) if pd.notna(r.total_victim) else None,
                r.source if pd.notna(r.source) else None,
            ),
        )

    conn.commit()
    conn.close()
    print(f"Fact table loaded: {len(df)} rows")
    print("Load complete")


if __name__ == "__main__":
    load_to_mysql(dataset_path="data/processed/dataset_consolidated.parquet")