import pandas as pd
import sqlite3
from sqlalchemy import (create_engine, BigInteger, Float, Date, String)

def get_engine(table_name):
    # use an engine to write out a DF to SQL
    db = f"sqlite:////home/aerotract/.aerodb/{table_name}.db"
    return create_engine(db)

def get_connection(table_name):
    # use a connection to query SQL into a DF
    db = f"/home/aerotract/.aerodb/{table_name}.db"
    return sqlite3.connect(db)

def create_clients_db(raw_data_path="data/clients-raw.csv"):
    # Step 1 of migrating TaskMaster - clients
    df = pd.read_csv(raw_data_path)
    # rename the cols to more appropriate SQL column names and define types
    df_cols = ["Client ID", "Client Name", "Category", "Client Creation Data", "Notes"]
    table_cols = ["ID", "NAME", "CATEGORY", "CREATION_DATA", "NOTES"]
    dtypes = [BigInteger, String(50), String(50), Date, String(255)]
    dtypes_map = {c: d for c,d in zip(table_cols, dtypes)}
    df = df[df_cols]
    df.columns = table_cols
    # set an index for our DF and SQL table
    df.set_index("ID", inplace=True)
    # clean some values
    df["CREATION_DATA"] = pd.to_datetime(df["CREATION_DATA"])
    df["NAME"] = df["NAME"].str.strip()
    engine = get_engine("clients")
    df.to_sql('clients', con=engine, if_exists='replace', dtype=dtypes_map, index_label="ID")

def match_client_names(df):
    # given a df with "CLIENT_ID" being the client name, return a list of matching IDs
    client_conn = sqlite3.connect("/home/aerotract/.aerodb/clients.db")
    query_ids = []
    for _, c in df.iterrows():
        query = "SELECT id FROM clients where name = :name"
        id_query = pd.read_sql(query, client_conn, params={"name": c["CLIENT_ID"]})
        query_ids.append(id_query["ID"][0])
    return query_ids

def create_projects_db(raw_data_path="data/projects-raw.csv"):
    df = pd.read_csv(raw_data_path)
    df_cols = ["Project ID", "Client Name", "Project Name", "Project Creation Date", "Questions", "Notes"]
    table_cols = ["ID", "CLIENT_ID", "NAME", "CREATION_DATA", "QUESTIONS", "NOTES"]
    df = df[df_cols]
    df.columns = table_cols
    df.set_index("ID", inplace=True)
    df["CREATION_DATA"] = pd.to_datetime(df["CREATION_DATA"])
    for c in ["CLIENT_ID", "NAME"]:
        df[c] = df[c].str.strip()
    # perform the client ID match and replace names with IDs
    df["CLIENT_ID"] = match_client_names(df)
    dtypes = [BigInteger, BigInteger, String(50), Date, String(255), String(255)]
    dtypes_map = {c: d for c,d in zip(table_cols, dtypes)}
    engine = get_engine("projects")
    df.to_sql('projects', con=engine, if_exists='replace', dtype=dtypes_map, index_label="ID")

def match_project_names(df):
    # given a df with "PROJECT_ID" being the name and "CLIENT_ID" being the ID, return a list
    # of the corresponding project IDs for each entry, or -1 if they are missing
    project_conn = get_connection("projects")
    query = "select * from projects"
    pquery = pd.read_sql(query, project_conn)
    pquery_ids = []
    for _, c in df.iterrows():
        pq = pquery[(pquery["NAME"] == c["PROJECT_ID"]) & (pquery["CLIENT_ID"] == c["CLIENT_ID"])]
        if len(pq) == 0:
            pquery_ids.append(-1)
        else:
            pquery_ids.append(pq.iloc[0]["ID"])
    return pquery_ids

def create_stands_db(raw_data_path="data/stands-raw.csv"):
    df = pd.read_csv(raw_data_path)
    df["ID"] = df["ID"].astype(int)
    table_cols = ["CLIENT_ID", "PROJECT_ID", "ID", "PERSISTENT_ID", "ACRES", "LOCATION"]
    df.columns = table_cols
    df.set_index("PERSISTENT_ID", inplace=True)
    dtypes = [BigInteger, BigInteger, BigInteger, BigInteger, Float, String(255)]
    dtypes_map = {c: d for c,d in zip(table_cols, dtypes)}
    # match client IDs
    df["CLIENT_ID"] = match_client_names(df)
    # match project IDs
    df["PROJECT_ID"] = match_project_names(df)
    df["PROJECT_ID"] = df["PROJECT_ID"].astype(int)
    engine = get_engine("stands")
    df.to_sql('stands', con=engine, if_exists='replace', dtype=dtypes_map, index_label="PERSISTENT_ID")

if __name__ == "__main__":
    order = [ 
        create_clients_db,
        create_projects_db,
        create_stands_db,
    ]

    for fn in order:
        fn()