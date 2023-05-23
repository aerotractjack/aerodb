import sqlite3
import pandas as pd
import os
from pathlib import Path
from sqlalchemy import create_engine

class AeroDB:
    # Manage SQL databases storing our company client/project data

    def __init__(self, dev=True):
        base = os.getenv("AERODB_DIR") if not dev else "./.sandbox"
        self.base = Path(base)

    def con(self, db):
        # create a connection to a database for querying, all dbs 
        # contain one table that shares the same name as the file
        db = db + ".db"
        path = (self.base / db).as_posix()
        return sqlite3.connect(path)

    def engine(self, db):
        # create an engine for a specific database for writing/reading
        db = db + ".db"
        path = (self.base / db).as_posix()
        path = "sqlite:///" + path
        return create_engine(path)

    def get_table(self, name):
        # return a full table as a dataframe
        conn = self.con(name)
        query = f"SELECT * FROM {name}"
        return pd.read_sql(query, conn)

    def query_table(self, name, query=None, params=None):
        # query a table and return results as dataframe
        conn = self.con(name)
        if query is None:
            query = f"SELECT * FROM {name};"
        return pd.read_sql(query, conn, params=params)

    def list_clients(self):
        # list all clients in dictionary format with ID and NAME
        clients = self.query_table(
            "clients",
            "SELECT ID, NAME FROM clients;"
        )
        return clients.to_dict("records")

    def list_projects_for_client(self, client_id):
        # given a CLIENT_ID, return all data about projects in dict form
        projects = self.query_table(
            "projects",
            "SELECT * FROM projects WHERE CLIENT_ID = :cid",
            {"cid": client_id}
        )
        return projects.to_dict("records")

    def list_stands_for_project(self, project_id):
        # given a PROJECT_ID, return all data about stands in dict form
        stands = self.query_table(
            "stands",
            "SELECT * FROM stands WHERE PROJECT_ID = :pid",
            {"pid": project_id}
        )
        return stands.to_dict("records")

    def get_all_data(self):
        # build a data tree mapping all clients to their projects, and 
        # all projects to their stands
        clients = self.list_clients()
        for client in clients:
            client["PROJECTS"] = self.list_projects_for_client(client["ID"])
            for proj in client["PROJECTS"]:
                proj["STANDS"] = self.list_stands_for_project(proj["ID"])
        return clients

    def full_stand_info(self, stand_id):
        # query our stand, project, and client tables to see all details of stand
        stand = self.query_table(
            "stands",
            "SELECT * FROM stands WHERE PERSISTENT_ID = :sid",
            {"sid": stand_id}
        ).to_dict("records")[0]
        if stand["PROJECT_ID"] == -1:
            project = {"PROJECT_NAME": None, "PROJECT_CREATION_DATA": None}
        else:
            project = self.query_table(
                "projects",
                "SELECT NAME as PROJECT_NAME, \
                CREATION_DATA as PROJECT_CREATION_DATA FROM projects WHERE ID = :pid",
                {"pid": stand["PROJECT_ID"]}
            ).to_dict("records")[0]
        client = self.query_table(
            "clients",
            "SELECT NAME as CLIENT_NAME, \
            CREATION_DATA as CLIENT_CREATION_DATA FROM clients WHERE ID = :cid",
            {"cid": stand["CLIENT_ID"]}
        ).to_dict("records")[0]
        return {**stand, **project, **client}

    def list_stands(self):
        # return a list of dicts with the PERSISTENT_ID value for all stands
        stands = self.query_table(
            "stands",
            "SELECT PERSISTENT_ID FROM stands"
        )
        return stands.to_dict("records")

    def full_stand_info_all_stands(self, split_by_project=True):
        data = []
        for stand in self.list_stands():
            stand_data = self.full_stand_info(stand["PERSISTENT_ID"])
            data.append(stand_data)
        data = pd.DataFrame(data)
        data = data.sort_values(["CLIENT_ID", "PERSISTENT_ID"], ascending=True)
        if not split_by_project:
            return {"all": data.to_dict("records")}
        out = {}
        for project in data["PROJECT_ID"].unique():
            subs = data[data["PROJECT_ID"] == project]
            out[subs.iloc[0]["CLIENT_NAME"]] = subs.to_dict("records")
        return out

    def add_client(self, name, category, creation_data, notes):
        conn = self.con("clients")
        cursor = conn.cursor()
        cmd = "INSERT INTO clients \
        (ID, NAME, CATEGORY, CREATION_DATA, NOTES) \
        VALUES (?, ?, ?, ?, ?)"
        client_id = self.query_table(
            "clients",
            "SELECT MAX(ID) FROM clients"
        ).iloc[0].values[0] + 1
        client_id = int(client_id)
        cursor.execute(cmd, (client_id, name, category, creation_data, notes))
        conn.commit()
        cursor.close()
        conn.close()
        return self.query_table(
            "clients",
            "SELECT * FROM clients WHERE ID = :id",
            {"id": client_id}
        ).to_dict("records")[0]

if __name__ == "__main__":
    import json
    db = AeroDB()
    print(db.add_client("abc", "cat1", "1996-05-22", "who are they"))