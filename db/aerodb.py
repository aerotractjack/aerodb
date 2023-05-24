import sqlite3
import pandas as pd
import os
from pathlib import Path
from sqlalchemy import create_engine

class AeroDB:

    def __init__(self, dev=True):
        """
        Initializes an AeroDB object. Sets the base path for SQLite databases.

        Parameters:
        dev (bool): If True, uses a sandbox path for development purposes.
        """
        base = os.getenv("AERODB_DIR") if not dev else "/home/aerotract/.sandbox"
        self.base = Path(base)

    def con(self, db="aerodb"):
        """
        Establishes a connection to a SQLite database.

        Parameters:
        db (str): The name of the database to connect to.

        Returns:
        sqlite3.Connection: An SQLite connection object.
        """
        db = db + ".db"
        path = (self.base / db).as_posix()
        return sqlite3.connect(path)

    def engine(self, db="aerodb"):
        """
        Creates an SQLAlchemy engine for the SQLite database.

        Parameters:
        db (str): The name of the database to connect to.

        Returns:
        sqlalchemy.engine.Engine: An SQLAlchemy engine object.
        """
        db = db + ".db"
        path = (self.base / db).as_posix()
        path = "sqlite:///" + path
        return create_engine(path)

    def get_table(self, name):
        """
        Retrieves a table from the SQLite database.

        Parameters:
        name (str): The name of the table.

        Returns:
        pandas.DataFrame: The requested table in DataFrame format.
        """
        conn = self.con()
        query = f"SELECT * FROM {name}"
        return pd.read_sql(query, conn)

    def query_table(self, query=None, params=None, json=False):
        """
        Executes a query on the SQLite database.

        Parameters:
        query (str): The SQL query to execute.
        params (dict): The parameters for the SQL query.
        json (bool): If True, returns data as a list of dictionaries.

        Returns:
        pandas.DataFrame or list of dict: The result of the SQL query.
        """
        conn = self.con()
        if query is None:
            query = f"SELECT * FROM clients;"
        data = pd.read_sql(query, conn, params=params)
        if json:
            data = data.to_dict("records")
        return data

    def list_clients(self):
        """
        Retrieves a list of all clients in the database.

        Returns:
        list of dict: Each dictionary represents a client, with keys 
                      'CLIENT_ID' and 'CLIENT_NAME'.
        """
        clients = self.query_table(
            "SELECT ID as CLIENT_ID, NAME as CLIENT_NAME FROM clients;"
        )
        return clients.to_dict("records")
    
    def id_to_name(self, table, uid):
        res = self.query_table(
            f"SELECT NAME FROM {table} WHERE id = :id",
            {"id": uid},
            json=True
        )
        return res[0]["NAME"]

    def add_client(self, name, category, creation_data, notes):
        """
        Inserts a new client into the database.

        Parameters:
        name (str): The name of the client.
        category (str): The category of the client.
        creation_data (str): The creation data of the client.
        notes (str): Any additional notes.

        Returns:
        dict: The newly added client represented as a dictionary.
        """
        conn = self.con()
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
        new_client = self.query_table(
            "clients",
            "SELECT * FROM clients WHERE ID = :id",
            {"id": client_id}
        ).to_dict("records")[0]
        return new_client
    
    def list_projects(self):
        """
        Retrieves a list of all projects in the database.

        Returns:
        list of dict: Each dictionary represents a project, with keys 
                      'PROJECT_ID' and 'PROJECT_NAME'.
        """
        projects = self.query_table(
            "SELECT ID as PROJECT_ID, NAME as PROJECT_NAME FROM projects"
        )
        return projects.to_dict("records")
    
    def projects_by_client(self, client_ids=None, key="CLIENT_ID"):
        """
        Retrieves all projects associated with specified client IDs.

        Parameters:
        client_ids (list of dict): The client IDs for which to fetch projects.
        key (str): key by which to group entries. if None or "", data is not grouped

        Returns:
        list of dict: Each dictionary contains details about a project.
        """
        client_query = "select ID as CLIENT_ID, NAME as CLIENT_NAME from clients"
        project_query = "select ID as PROJECT_ID, \
                        NAME as PROJECT_NAME, \
                        STAND_IDS, CLIENT_ID, \
                        CREATION_DATA as PROJECT_CREATION_DATA from projects"
        client_query_params = None
        if client_ids is not None:
            placeholders = ",".join(["?"] * len(client_ids))
            client_query += f" where ID in ({placeholders})"
            project_query += f" where CLIENT_ID in ({placeholders})"
            client_query_params = client_ids
        cli = self.query_table(client_query, client_query_params)
        pro = self.query_table(project_query, client_query_params)
        df = pro.merge(cli, left_on="CLIENT_ID", right_on="CLIENT_ID", how="left")
        if key is None or len(key) == 0:
            return df.to_dict("records")
        out = {}
        for v in df[key].unique():
            out[str(v)] = df[df[key] == v].to_dict("records")
        return out
    
    def stands_by_project(self, project_ids=None, key=None):
        project_query = "select ID as PROJECT_ID, \
                        NAME as PROJECT_NAME, \
                        STAND_IDS, CLIENT_ID, \
                        CREATION_DATA as PROJECT_CREATION_DATA from projects"
        project_query_params = None
        if project_ids is not None:
            placeholders = ",".join(["?"] * len(project_ids))
            project_query += f" where ID in ({placeholders})"
            project_query_params = project_ids
        pro = self.query_table(project_query, project_query_params)
        if key is None or len(key) == 0:
            return pro.to_dict("records")
        out = {}
        for v in pro[key].unique():
            out[str(v)] = pro[pro[key] == v].to_dict("records")
        return out
        
    
    def lookup_stand(self, stand_id):
        """
        Retrieves information about a stand from the database.

        Parameters:
        stand_id (str): The ID of the stand.

        Returns:
        list of dict: Each dictionary represents a stand with all its details.
        """
        if stand_id is None or stand_id == "":
            return []
        stand = self.query_table(
            "SELECT * FROM stands WHERE PERSISTENT_ID = :id", 
            {"id": stand_id}
        )
        stand = stand.to_dict("records")
        return stand
    
    def stands_for_project(self, project_ids=None, flatten=False, NAS=True):
        """
        Retrieves all stands associated with the given project IDs.

        Parameters:
        project_ids (list of dict): The project IDs for which to fetch stands.
        flatten (bool): If True, flattens the result.

        Returns:
        list of dict: Each dictionary contains details about a stand.
        """
        if project_ids is None:
            project_ids = self.list_projects()
        if not isinstance(project_ids, list):
            project_ids = [project_ids]
        resp = []
        for pid in project_ids:
            project = self.query_table(
                "SELECT * FROM projects WHERE ID = :id",
                {"id": pid["PROJECT_ID"]}
            ).to_dict("records")[0]
            stand_ids = project["STAND_IDS"].split(",")
            if project["STAND_IDS"] == "":
                continue
            stands = []
            stands = [self.lookup_stand(sid)[0] for sid in stand_ids]
            if flatten:
                for stand in stands:
                    result = {**pid, **stand}
                    resp.append(result)
            else:
                result = pid.copy()
                result["stands"] = stands
                resp.append(result)
        return resp
    
    def NAS_info_for_stand(self, client_id, project_id, stand_temp_id):
        """
        Retrieves Network Attached Storage (NAS) information for a stand 
        related to a specific client and project.

        Parameters:
        client_id (int): The ID of the client.
        project_id (int): The ID of the project.
        stand_temp_id (int): The temporary ID of the stand.

        Returns:
        list of dict: Each dictionary represents a match, with keys indicating 
                      the relevant directories and whether they exist.
        """
        base = Path("/home/aerotract/NAS/main/Clients")
        globstr = f"{client_id}_*/{project_id}_*/{stand_temp_id}_*"
        matches = list(base.glob(globstr))
        if len(matches) == 0:
            return None
        result = []
        for match in matches:
            res = {}
            res["ortho_dir"] = (match / "Data" / "ortho").as_posix()
            res["src_img_dir"] = (match / "Data" / "src_imgs").as_posix()
            res["ml_result_dir"] = (match / "ML_results").as_posix()
            res["ortho_exists"] = len(os.listdir(res["ortho_dir"])) > 0
            res["src_img_exists"] = len(os.listdir(res["src_img_dir"])) > 0
            result.append(res)
        return result
    
if __name__ == "__main__":
    import json
    db = AeroDB()
    data = db.stands_by_project(project_ids=[101017, 101018])
    print(json.dumps(data, indent=4))