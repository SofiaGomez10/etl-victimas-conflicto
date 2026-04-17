import pandas as pd
import pymysql


DB_CONFIG = {
    "host": "mysql_dw",
    "port": 3306,
    "user": "etl_user",
    "password": "etl_password",
    "database": "dw_victims",
    "charset": "utf8mb4",
}

DDL = [
    """CREATE TABLE IF NOT EXISTS person (
        id_person INT PRIMARY KEY,
        sex VARCHAR(100),
        ethnic_group VARCHAR(100),
        age_range VARCHAR(100)
    )""",
    """CREATE TABLE IF NOT EXISTS victimizing_act (
        id_act INT PRIMARY KEY,
        victimization_fact VARCHAR(100)
    )""",
    """CREATE TABLE IF NOT EXISTS location (
        id_location INT PRIMARY KEY,
        state_dept VARCHAR(100)
    )""",
    """CREATE TABLE IF NOT EXISTS registration_date (
        date_processing DATE PRIMARY KEY,
        year INT,
        month INT
    )""",
    """CREATE TABLE IF NOT EXISTS victims (
        id_person INT,
        id_act INT,
        id_location INT,
        date_processing DATE,
        total_victim INT,
        FOREIGN KEY (id_person) REFERENCES person(id_person),
        FOREIGN KEY (id_act) REFERENCES victimizing_act(id_act),
        FOREIGN KEY (id_location) REFERENCES location(id_location),
        FOREIGN KEY (date_processing) REFERENCES registration_date(date_processing)
    )""",
]


def make_dim(df, cols):
    dim = df[cols].drop_duplicates().reset_index(drop=True)
    dim.insert(0, "id", range(1, len(dim) + 1))
    return dim


def load_to_mysql(dataset_path: str):
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
        cur.execute("INSERT IGNORE INTO person VALUES (%s,%s,%s,%s)", (r.id, r.sex, r.ethnic_group, r.age_range))

    for _, r in dim_act.iterrows():
        cur.execute("INSERT IGNORE INTO victimizing_act VALUES (%s,%s)", (r.id, r.victimization_fact))

    for _, r in dim_location.iterrows():
        cur.execute("INSERT IGNORE INTO location VALUES (%s,%s)", (r.id, r.state_dept))

    for _, r in dim_date.iterrows():
        date_val = pd.Timestamp(r.date_processing).date() if pd.notna(r.date_processing) else None
        if date_val:
            cur.execute("INSERT IGNORE INTO registration_date VALUES (%s,%s,%s)",
                        (date_val, int(r.year) if pd.notna(r.year) else None,
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
            "INSERT INTO victims VALUES (%s,%s,%s,%s,%s)",
            (
                int(r.id) if pd.notna(r.id) else None,
                int(r["id_act"]) if "id_act" in r and pd.notna(r["id_act"]) else None,
                int(r["id_loc"]) if "id_loc" in r and pd.notna(r["id_loc"]) else None,
                date_val,
                int(r.total_victim) if pd.notna(r.total_victim) else None,
            ),
        )

    conn.commit()
    conn.close()
    print(f"Fact table loaded: {len(df)} rows")
    print("Load complete")


if __name__ == "__main__":
    load_to_mysql(dataset_path="data/processed/dataset_consolidated.parquet")