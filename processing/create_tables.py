import pandas as pd
import sqlite3
from sqlalchemy import (create_engine, BigInteger, Float, Date, String, Boolean)
from uuid import uuid4

def get_engine(table_name="aerodb"):
    # use an engine to write out a DF to SQL
    db = f"sqlite:////home/aerotract/.aerodb/{table_name}.db"
    return create_engine(db)

def get_connection(table_name="aerodb"):
    # use a connection to query SQL into a DF
    db = f"/home/aerotract/.aerodb/{table_name}.db"
    return sqlite3.connect(db)

def cleanstr(df, col):
        repl = [".", "&"]
        repl = "[" + "".join(repl) + "]"
        df[col] = df[col].str.strip()
        df[col] = df[col].str.replace(repl, "_", regex=True)
        df[col] = df[col].str.replace(" ", "")
        df[col] = df[col].str.upper()
        return df

def create_clients_db(raw_data_path="data/clients-raw.csv"):
    # Step 1 of migrating TaskMaster - clients
    df = pd.read_csv(raw_data_path)
    # rename the cols to more appropriate SQL column names and define types
    df_cols = ["Client ID", "Client Name", "Category", "Client Creation Data", "Notes"]
    table_cols = ["CLIENT_ID", "CLIENT_NAME", "CATEGORY", "CLIENT_CREATION_DATA", "CLIENT_NOTES"]
    dtypes = [BigInteger, String(50), String(50), Date, String(255)]
    dtypes_map = {c: d for c,d in zip(table_cols, dtypes)}
    df = df[df_cols]
    df.columns = table_cols
    # set an index for our DF and SQL table
    df.set_index("CLIENT_ID", inplace=True)
    # clean some values
    df["CLIENT_CREATION_DATA"] = pd.to_datetime(df["CLIENT_CREATION_DATA"])
    df = cleanstr(df, "CLIENT_NAME")
    engine = get_engine()
    df.to_sql('clients', con=engine, if_exists='replace', dtype=dtypes_map, index_label="CLIENT_ID")

def match_client_names(df):
    # given a df with "CLIENT_ID" being the client name, return a list of matching IDs
    client_conn = get_connection()
    query_ids = []
    for _, c in df.iterrows():
        query = "SELECT client_id FROM clients where client_name = :name"
        id_query = pd.read_sql(query, client_conn, params={"name": c["CLIENT_ID"]})
        query_ids.append(id_query["CLIENT_ID"][0])
    return query_ids

def create_projects_db(raw_data_path="data/projects-raw.csv"):
    df = pd.read_csv(raw_data_path)
    df_cols = ["Project ID", "Client Name", "Project Name", "Project Creation Date", "Questions", "Notes"]
    table_cols = ["PROJECT_ID", "CLIENT_ID", "PROJECT_NAME", "PROJECT_CREATION_DATA", "PROJECT_QUESTIONS", "PROJECT_NOTES"]
    df = df[df_cols]
    df.columns = table_cols
    df.set_index("PROJECT_ID", inplace=True)
    df["PROJECT_CREATION_DATA"] = pd.to_datetime(df["PROJECT_CREATION_DATA"])
    df = cleanstr(df, "PROJECT_NAME")
    df = cleanstr(df, "CLIENT_ID")
    # perform the client ID match and replace names with IDs
    df["CLIENT_ID"] = match_client_names(df)
    dtypes = [BigInteger, BigInteger, String(50), Date, String(255), String(255)]
    dtypes_map = {c: d for c,d in zip(table_cols, dtypes)}
    engine = get_engine()
    df.to_sql('projects', con=engine, if_exists='replace', dtype=dtypes_map, index_label="PROJECT_ID")

def match_project_names(df):
    # given a df with "PROJECT_ID" being the name and "CLIENT_ID" being the ID, return a list
    # of the corresponding project IDs for each entry, or -1 if they are missing
    project_conn = get_connection()
    query = "select * from projects"
    pquery = pd.read_sql(query, project_conn)
    pquery_ids = []
    for _, c in df.iterrows():
        projmask = pquery["PROJECT_NAME"] == c["PROJECT_ID"]
        climask = pquery["CLIENT_ID"] == c["CLIENT_ID"]
        msg = ""
        if projmask.sum() == 0:
            msg += "no project found: " + str(c["PROJECT_ID"])
        if climask.sum() == 0:
            if len(msg) != 0:
                msg += "\n"
            msg += "no client found: " + str(c["CLIENT_ID"])
        if len(msg) != 0:
            print("---------------")
            print(msg)
            print("---------------")
        pq = pquery[(projmask) & (climask)]
        if len(pq) == 0:
            pquery_ids.append(-1)
        else:
            pquery_ids.append(pq.iloc[0]["PROJECT_ID"])
    return pquery_ids

def load_and_process_activeprojects(raw_data_path="data/activeprojects-raw.csv"):
    df = pd.read_csv(raw_data_path)
    df_cols = ["Client", "Project", "ID", "Site", "Acres"]
    df = df[df_cols]
    table_cols = ["CLIENT_ID", "PROJECT_ID", "STAND_ID", "STAND_NAME", "ACRES"]
    df.columns = table_cols
    df = cleanstr(df, "CLIENT_ID")
    df = cleanstr(df, "PROJECT_ID")
    df = cleanstr(df, "STAND_NAME")
    df["CLIENT_ID"] = match_client_names(df)
    df["PROJECT_ID"] = match_project_names(df)
    df["STAND_PERSISTENT_ID"] = list(range(1000000, df.shape[0]+1000000))
    table_cols = df.columns
    return df

def create_stands_from_activeprojects_db(raw_data_path="data/activeprojects-raw.csv"):
    engine = get_engine()
    df = load_and_process_activeprojects(raw_data_path)
    stand_proj_df = df[["STAND_PERSISTENT_ID", "PROJECT_ID"]]
    stand_proj_df.set_index("STAND_PERSISTENT_ID", inplace=True)
    sp_dtypes = [BigInteger, BigInteger]
    sp_dtypes_map = {c: d for c,d in zip(stand_proj_df.columns, sp_dtypes)}
    stand_proj_df.to_sql('stand_project_ids', con=engine, if_exists='replace', dtype=sp_dtypes_map, index_label="STAND_PERSISTENT_ID")
    del df["PROJECT_ID"]
    dtypes = [BigInteger, BigInteger, String(50), Float]
    dtypes_map = {c: d for c,d in zip(df.columns, dtypes)}
    df.set_index("STAND_PERSISTENT_ID", inplace=True)
    df.to_sql('stands', con=engine, if_exists='replace', dtype=dtypes_map, index_label="STAND_PERSISTENT_ID")

def add_stand_ids_to_projects_db():
    project_conn = get_connection()
    query = "select * from projects"
    projects = pd.read_sql(query, project_conn)
    stands = load_and_process_activeprojects()
    stand_ids = []
    for project in projects["PROJECT_ID"].unique():
        proj_stands = stands[stands["PROJECT_ID"] == project]
        uids = proj_stands["STAND_PERSISTENT_ID"].unique()
        proj_stand_ids = ",".join([str(x) for x in uids])
        stand_ids.append(proj_stand_ids)
    projects["STAND_PERSISTENT_IDS"] = stand_ids
    engine = get_engine()
    projects.set_index("PROJECT_ID", inplace=True)
    projects.to_sql('projects', con=engine, if_exists='replace', index_label="PROJECT_ID")

def load_and_process_metadata(raw_data_path="data/projectmeta-raw.csv"):
    df = pd.read_csv(raw_data_path)
    df = cleanstr(df, "CLIENT_ID")
    df = cleanstr(df, "PROJECT_ID")
    df = cleanstr(df, "STAND_NAME")
    df["CLIENT_ID"] = match_client_names(df)
    df["PROJECT_ID"] = match_project_names(df)
    df["FLIGHT_ID"] = list(range(10000000, df.shape[0]+10000000))
    df.set_index("FLIGHT_ID", inplace=True)
    conn = get_connection()
    stands = pd.read_sql("select * from stands", conn)
    stand_pids = []
    for _, row in df.iterrows():
        stand = stands[(stands["STAND_ID"] == row["STAND_ID"]) & (stands["STAND_NAME"] == row["STAND_NAME"])]
        stand_pids.append(stand["STAND_PERSISTENT_ID"].values[0])
    df["STAND_PERSISTENT_ID"] = stand_pids
    return df

def create_flights_db(raw_data_path="data/projectmeta-raw.csv"):
    df = load_and_process_metadata(raw_data_path)
    table_cols = ["CLIENT_ID", "PROJECT_ID", "STAND_PERSISTENT_ID", "FLIGHT_COMPLETE"] # FLIGHT_ID is index
    df = df[table_cols]
    dtypes = [BigInteger, BigInteger, BigInteger, Boolean]
    dtypes_map = {c: d for c,d in zip(df.columns, dtypes)}
    df.to_sql('flights', con=get_engine(), if_exists='replace', dtype=dtypes_map, index_label="FLIGHT_ID")

def create_flight_ai_db(raw_data_path="data/projectmeta-raw.csv"):
    df = load_and_process_metadata(raw_data_path)
    table_cols = ['TRAINING_READY', 'TRAINING_DONE',
       'AI_READY', 'AI_OUTPUT', 'QA_DONE', 'AI_RESULT_MODELED', 'QC_READY',
       'AI_TPA', 'QC_PLOT_TPA', 'AI_TREE_COUNT_RED', 'AI_TREE_COUNT_BROWN',
       'QC_APPROVED', 'CLEANED_AI_TO_PRODUCTS']
    dtypes = [Boolean, Boolean,
              Boolean, String(255), Boolean, Boolean, Boolean,
              Float, Float, Float, Float,
              Boolean, Boolean,
              BigInteger]
    df = df[table_cols]
    df[df.index.name] = df.index
    df["AI_FLIGHT_ID"] = list(range(df.shape[0]))
    df.set_index("AI_FLIGHT_ID", inplace=True)
    dtypes_map = {c: d for c,d in zip(df.columns, dtypes)}
    df.to_sql('flight_ai', con=get_engine(), if_exists='replace', dtype=dtypes_map, index_label="AI_FLIGHT_ID")

def create_flight_files_db(raw_data_path="data/projectmeta-raw.csv"):
    df = load_and_process_metadata(raw_data_path)
    table_cols = [
        "FLIGHT_IMAGES_DELIVERED", "FLIGHT_PLANS_NAS", "FLIGHT_IMAGES_DD", "SHP_NAS", "KML_NAS",
        "INDIVIDUAL_SHP_NAS", "GRID_QA_NAS", "RAW_IMAGES_NAS", "POLYGON_DD",
        "CROPPED", "SAMPLE_AVAILABLE", "SAMPLE_DD", "ORTHO_4IN_NAS",
        "ORTHO_PIX4D_NAS", "ORTHO_DD_NAS", "AI_OUTPUT", "NAS_FOLDERS"]
    dtypes = [
        Boolean, Boolean, Boolean, Boolean, Boolean,
        Boolean, Boolean, Boolean, Boolean,
        Boolean, Boolean, Boolean, Boolean,
        Boolean, Boolean, String(255), Boolean,
        BigInteger
    ]
    df = df[table_cols]
    df["FILES_FLIGHT_ID"] = list(range(df.shape[0]))
    df[df.index.name] = df.index
    df.set_index("FILES_FLIGHT_ID", inplace=True)
    dtypes_map = {c: d for c,d in zip(df.columns, dtypes)}
    df.to_sql('flight_files', con=get_engine(), if_exists='replace', dtype=dtypes_map, index_label="FILES_FLIGHT_ID")

def check_columns():
    meta = load_and_process_metadata()
    flights = pd.read_sql("select * from flights", get_connection())
    ai = pd.read_sql("select * from flight_ai", get_connection())
    files = pd.read_sql("select * from flight_files", get_connection())
    print(len(meta.columns))
    cols = []
    for x in [flights, ai, files]:
        cols.extend(x.columns.tolist())
    print(len(cols))
    print(set(meta.columns) - set(cols))

if __name__ == "__main__":
    order = [ 
        create_clients_db,
        create_projects_db,
        create_stands_from_activeprojects_db,
        add_stand_ids_to_projects_db,
        create_flights_db,
        create_flight_ai_db,
        create_flight_files_db,
        check_columns,
    ]

    for fn in order:
        fn()