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
        base = os.getenv(
            "AERODB_DIR") if not dev else "/home/aerotract/.sandbox"
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

    def execute_query(self, query=None, params=None, json=False):
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

    def get_id_col(self, table):
        """
        Returns the name of the ID column of the given table.

        Parameters:
        table (str): The name of the table.

        Returns:
        str: The name of the ID column.
        """
        if table == "clients":
            col = "CLIENT_ID"
        elif table == "projects":
            col = "PROJECT_ID"
        elif table == "stands":
            col = "STAND_PERSISTENT_ID"
        return col
    
    def get_name_col(self, table):
        """
        Returns the name of the name column of the given table.

        Parameters:
        table (str): The name of the table.

        Returns:
        str: The name of the name column.
        """
        return table.strip("s").upper() + "_NAME"
    
    def id_to_name(self, table, uid):
        """
        Returns the name of the given ID in the given table.

        Parameters:
        table (str): The name of the table.
        uid: The ID to convert to a name.

        Returns:
        str: The name corresponding to the given ID.
        """
        idcol = self.get_id_col(table)
        namecol = self.get_name_col(table)
        res = self.execute_query(
            f"SELECT {namecol} FROM {table} WHERE {idcol} = :id",
            {"id": uid},
            json=True
        )
        return res[0]["NAME"]
    
    def list_table(self, table, cols="*"):
        """
        Retrieves specified columns from the given table.

        Parameters:
        table (str): The name of the table.
        cols (str or list): The columns to retrieve. If '*', retrieves all columns.

        Returns:
        pandas.DataFrame: The requested columns from the table.
        """
        if isinstance(cols, list):
            cols = ", ".join(cols)
        query = f"SELECT {cols} FROM {table}"
        result = self.execute_query(query)
        return result
    
    def where_table_equal(self, table, search, match, cols="*"):
        """
        Retrieves rows where the specified column equals a given value.

        Parameters:
        table (str): The name of the table.
        search (str): The column to search.
        match: The value to match.
        cols (str or list): The columns to retrieve. If '*', retrieves all columns.

        Returns:
        pandas.DataFrame: The rows where the specified column equals the given value.
        """
        if isinstance(cols, list):
            cols = ", ".join(cols)
        query = f"SELECT {cols} FROM {table} WHERE {search} = ?"
        result = self.execute_query(query, params=(match,))
        return result
    
    def where_table_in(self, table, search, match, cols="*"):
        """
        Retrieves rows where the specified column is in a list of values.

        Parameters:
        table (str): The name of the table.
        search (str): The column to search.
        match (list): The values to match.
        cols (str or list): The columns to retrieve. If '*', retrieves all columns.

        Returns:
        pandas.DataFrame: The rows where the specified column is in the list of values.
        """
        if not isinstance(match, list):
            match = [match]
        plc = ", ".join(["?"] * len(match))
        if isinstance(cols, list):
            cols = ", ".join(cols)
        query = f"SELECT {cols} FROM {table} WHERE {search} in ({plc})"
        result = self.execute_query(query, params=match)
        return result
    
    def where_table_like(self, table, search, match, cols="*"):
        """
        Retrieves rows where the specified column contains a given string.

        Parameters:
        table (str): The name of the table.
        search (str): The column to search.
        match (str): The string to match.
        cols (str or list): The columns to retrieve. If '*', retrieves all columns.

        Returns:
        pandas.DataFrame: The rows where the specified column contains the given string.
        """
        match = f"%{match}%"
        if isinstance(cols, list):
            cols = ", ".join(cols)
        query = f"SELECT {cols} FROM {table} WHERE {search} LIKE ?"
        result = self.execute_query(query, params=(match,))
        return result
    
    def where_table_between(self, table, search, match, cols="*"):
        """
        Retrieves rows where the specified column falls within a range of values.

        Parameters:
        table (str): The name of the table.
        search (str): The column to search.
        match (tuple): The range of values to match.
        cols (str or list): The columns to retrieve. If '*', retrieves all columns.

        Returns:
        pandas.DataFrame: The rows where the specified column is within the range of values.
        """
        if isinstance(cols, list):
            cols = ", ".join(cols)
        query = f"SELECT {cols} FROM {table} WHERE {search} BETWEEN ? AND ?"
        result = self.execute_query(query, params=match)
        return result
    
    def get_ids(self, table, ids):
        """
        Retrieves a list of IDs from a table. If IDs are provided, it validates them against the table.

        Parameters:
        table (str): The name of the table.
        ids (list, optional): The IDs to retrieve or validate. If None, retrieves all IDs from the table.

        Returns:
        list: The list of IDs.
        """
        if isinstance(ids, int):
            ids = str(ids)
        if ids is None or len(ids) == 0:
            id_col = self.get_id_col(table)
            ids = self.get_table(table)[id_col].unique().tolist()
        if not isinstance(ids, list):
            ids = [ids]
        return ids

    def client_projects(self, client_ids=None):
        """
        Retrieves all projects for specified clients.

        Parameters:
        client_ids (list, optional): The IDs of the clients. If None, retrieves projects for all clients.

        Returns:
        dict: A dictionary mapping client IDs to a list of their projects.
        """
        client_ids = self.get_ids("clients", client_ids)
        projects = {}
        for client_id in client_ids:
            client_projects = self.where_table_equal(
                "projects", "CLIENT_ID", client_id
            )
            projects[str(client_id)] = client_projects.to_dict("records")
        return projects
    
    def project_stands(self, project_ids=None):
        """
        Retrieves all stands for specified projects.

        Parameters:
        project_ids (list, optional): The IDs of the projects. If None, retrieves stands for all projects.

        Returns:
        dict: A dictionary mapping project IDs to a list of their stands.
        """
        project_ids = self.get_ids("projects", project_ids)
        projects = self.where_table_in(
            "projects", "PROJECT_ID", project_ids
        )
        projects = projects.to_dict("records")
        stands = {}
        for project in projects:
            project_stand_list = project["STAND_PERSISTENT_IDS"]
            if len(project_stand_list) == 0 or project_stand_list is None:
                continue
            project_stand_list = project_stand_list.split(",")
            project_stands = self.where_table_in(
                "stands", "STAND_PERSISTENT_ID", project_stand_list
            )
            for ps in project_stands.to_dict("record"):
                stands[ps["STAND_PERSISTENT_ID"]] = ps
        return stands
    
    def full_stand_data(self, stand_ids=None):
        """
        Retrieves all data for specified stands, including associated clients and projects.

        Parameters:
        stand_ids (list, optional): The IDs of the stands. If None, retrieves data for all stands.

        Returns:
        list: A list of dictionaries containing stand data.
        """
        stand_ids = self.get_ids("stands", stand_ids)
        stands = self.where_table_in(
            "stands", "STAND_PERSISTENT_ID", stand_ids
        )
        stand_client_ids = stands["CLIENT_ID"].unique().tolist()
        clients = self.where_table_in("clients", "CLIENT_ID", stand_client_ids)
        stands = stands.merge(clients, on="CLIENT_ID", how="left")
        stands = stands.to_dict("records")
        for i in range(len(stands)):
            project = self.where_table_like(
                "projects", "STAND_PERSISTENT_IDS", stands[i]["STAND_PERSISTENT_ID"]
            ).to_dict("records")
            if len(project) == 0:
                continue
            stands[i] = {**stands[i], **project[0]}
        return stands
    
    def full_stand_data_view(self, data=None, key=None, cols=None):
        """
        Creates a view of the stand data, grouped by a specified key.

        Parameters:
        data (list, optional): The stand data. If None, retrieves all stand data.
        key (str, optional): The column to group by. If None, returns the ungrouped data.
        cols (list, optional): The columns to include in the view.

        Returns:
        dict: A dictionary mapping keys to a list of stand data.
        """
        if data is None:
            data = self.full_stand_data()
        if key is None:
            return data
        data = pd.DataFrame(data)
        if cols is not None and isinstance(cols, list):
            if key not in cols:
                cols.append(key)
            data = data[cols]
        uniq = data[key].unique().tolist()
        view = {}
        for val in uniq:
            sel = data[data[key] == val]
            sel = sel.to_dict("records")
            view[val] = sel
        return view

    def client_full_stand_data(self, client_ids=None):
        """
        Retrieves full stand data for the specified clients, including associated projects.
        If no clients are specified, retrieves data for all clients.

        Parameters:
        client_ids (list, optional): The IDs of the clients. If None, retrieves data for all clients.

        Returns:
        dict: A dictionary mapping client IDs to a list of full stand data.
        """
        client_ids = self.get_ids("clients", client_ids)
        projects = self.where_table_in(
            "projects", "CLIENT_ID", client_ids, "STAND_PERSISTENT_IDS"
        )
        stand_ids = []
        for sid in projects["STAND_PERSISTENT_IDS"].tolist():
            if sid is None or sid == "":
                continue
            stand_ids.extend(sid.split(","))
        stand_data = self.full_stand_data(stand_ids)
        return self.full_stand_data_view(stand_data, key="CLIENT_ID")
    
    def project_full_stand_data(self, project_ids=None):
        """
        Retrieves full stand data for the specified projects, including associated clients.
        If no projects are specified, retrieves data for all projects.

        Parameters:
        project_ids (list, optional): The IDs of the projects. If None, retrieves data for all projects.

        Returns:
        dict: A dictionary mapping client IDs to a list of full stand data.
        """
        project_ids = self.get_ids("projects", project_ids)
        projects = self.where_table_in(
            "projects", "PROJECT_ID", project_ids, "STAND_PERSISTENT_IDS"
        )
        stand_ids = []
        for sid in projects["STAND_PERSISTENT_IDS"].tolist():
            if sid is None or sid == "":
                continue
            stand_ids.extend(sid.split(","))
        stand_data = self.full_stand_data(stand_ids)
        return self.full_stand_data_view(stand_data, key="CLIENT_ID")

if __name__ == "__main__":
    import json
    db = AeroDB()
    data = db.client_full_stand_data([10049, 10050])
    print(json.dumps(data, indent=4))
