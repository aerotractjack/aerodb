import pandas as pd
import sqlite3
from sqlalchemy import (create_engine, BigInteger, Float, Date, String)

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
    table_cols = ["ID", "NAME", "CATEGORY", "CREATION_DATA", "NOTES"]
    dtypes = [BigInteger, String(50), String(50), Date, String(255)]
    dtypes_map = {c: d for c,d in zip(table_cols, dtypes)}
    df = df[df_cols]
    df.columns = table_cols
    # set an index for our DF and SQL table
    df.set_index("ID", inplace=True)
    # clean some values
    df["CREATION_DATA"] = pd.to_datetime(df["CREATION_DATA"])
    df = cleanstr(df, "NAME")
    engine = get_engine()
    df.to_sql('clients', con=engine, if_exists='replace', dtype=dtypes_map, index_label="ID")

def match_client_names(df):
    # given a df with "CLIENT_ID" being the client name, return a list of matching IDs
    client_conn = get_connection()
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
    df = cleanstr(df, "NAME")
    df = cleanstr(df, "CLIENT_ID")
    # perform the client ID match and replace names with IDs
    df["CLIENT_ID"] = match_client_names(df)
    dtypes = [BigInteger, BigInteger, String(50), Date, String(255), String(255)]
    dtypes_map = {c: d for c,d in zip(table_cols, dtypes)}
    engine = get_engine()
    df.to_sql('projects', con=engine, if_exists='replace', dtype=dtypes_map, index_label="ID")

def match_project_names(df):
    # given a df with "PROJECT_ID" being the name and "CLIENT_ID" being the ID, return a list
    # of the corresponding project IDs for each entry, or -1 if they are missing
    project_conn = get_connection()
    query = "select * from projects"
    pquery = pd.read_sql(query, project_conn)
    pquery_ids = []
    for _, c in df.iterrows():
        projmask = pquery["NAME"] == c["PROJECT_ID"]
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
            pquery_ids.append(pq.iloc[0]["ID"])
    return pquery_ids

def load_and_process_activeprojects(raw_data_path="data/activeprojects-raw.csv"):
    df = pd.read_csv(raw_data_path)
    df_cols = ["Client", "Project", "ID", "Site", "Acres"]
    df = df[df_cols]
    table_cols = ["CLIENT_ID", "PROJECT_ID", "ID", "NAME", "ACRES"]
    df.columns = table_cols
    df = cleanstr(df, "CLIENT_ID")
    df = cleanstr(df, "PROJECT_ID")
    df = cleanstr(df, "NAME")
    df["CLIENT_ID"] = match_client_names(df)
    df["PROJECT_ID"] = match_project_names(df)
    df["PERSISTENT_ID"] = list(range(1000000, df.shape[0]+1000000))
    table_cols = df.columns
    return df

def create_stands_from_activeprojects_db(raw_data_path="data/activeprojects-raw.csv"):
    df = load_and_process_activeprojects(raw_data_path)
    del df["PROJECT_ID"]
    dtypes = [BigInteger, BigInteger, String(50), Float]
    dtypes_map = {c: d for c,d in zip(df.columns, dtypes)}
    df.set_index("PERSISTENT_ID", inplace=True)
    engine = get_engine()
    df.to_sql('stands', con=engine, if_exists='replace', dtype=dtypes_map, index_label="PERSISTENT_ID")

def add_stand_ids_to_projects_db():
    project_conn = get_connection()
    query = "select * from projects"
    projects = pd.read_sql(query, project_conn)
    stands = load_and_process_activeprojects()
    stand_ids = []
    for project in projects["ID"].unique():
        proj_stands = stands[stands["PROJECT_ID"] == project]
        uids = proj_stands["PERSISTENT_ID"].unique()
        proj_stand_ids = ",".join([str(x) for x in uids])
        stand_ids.append(proj_stand_ids)
    projects["STAND_IDS"] = stand_ids
    engine = get_engine()
    projects.set_index("ID", inplace=True)
    projects.to_sql('projects', con=engine, if_exists='replace', index_label="ID")
    print(projects.head())

if __name__ == "__main__":
    order = [ 
        create_clients_db,
        create_projects_db,
        create_stands_from_activeprojects_db,
        add_stand_ids_to_projects_db
    ]

    for fn in order:
        fn()