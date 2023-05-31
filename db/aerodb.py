import sqlite3
import pandas as pd
import os
from pathlib import Path
from sqlalchemy import create_engine
import json
import sys


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

    # general helper functions

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

    def handle_output(self, data, json_out=False):
        if json_out and isinstance(data, pd.DataFrame):
            data = data.to_dict("records")
        elif not json_out and isinstance(data, dict):
            data = {k: pd.DataFrame(v) for k,v in data.items()}
        elif not json_out and isinstance(data, list):
            data = pd.DataFrame(data)
        return data

    # query helper functions

    def get_table(self, name, json_out=False):
        """
        Retrieves a table from the SQLite database.

        Parameters:
        name (str): The name of the table.

        Returns:
        pandas.DataFrame: The requested table in DataFrame format.
        """
        conn = self.con()
        query = f"SELECT * FROM {name}"
        data = pd.read_sql(query, conn)
        return self.handle_output(data, json_out)

    def execute_query(self, query=None, params=None, json_out=True):
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
        return self.handle_output(data, json_out)

    def get_id_col(self, table):
        """
        Returns the name of the ID column of the given table.

        Parameters:
        table (str): The name of the table.

        Returns:
        str: The name of the ID column.
        """
        if table == "clients":
            return "CLIENT_ID"
        elif table == "projects":
            return "PROJECT_ID"
        elif table == "stands":
            return "STAND_PERSISTENT_ID"
        elif table in ["flights", "flight_ai", "flight_files"]:
            return "FLIGHT_ID"
        raise ValueError(f"No table: {table}")

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

    def list_table(self, table, cols="*", json_out=False):
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
        result = self.execute_query(query, json_out=json_out)
        return result

    def where_table_equal(self, table, search, match, cols="*", dry=False, json_out=False):
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
        params = (match,)
        if dry:
            return query, query[query.index("WHERE")+5:], params
        result = self.execute_query(query, params=params, json_out=json_out)
        return result

    def where_table_in(self, table, search, match, cols="*", dry=False, json_out=False):
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
        params = match
        if dry:
            return query, query[query.index("WHERE")+6:], params
        result = self.execute_query(query, params=params, json_out=json_out)
        return result

    def where_table_like(self, table, search, match, cols="*", dry=False, json_out=False):
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
        params = (match,)
        if dry:
            return query, query[query.index("WHERE")+6:], params
        result = self.execute_query(query, params=params, json_out=json_out)
        return result

    def where_table_between(self, table, search, match, cols="*", dry=False, json_out=False):
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
        params = match
        if dry:
            return query, query[query.index("WHERE")+5:], params
        result = self.execute_query(query, params=params, json_out=json_out)
        return result

    def query_from_json(self, jq, json_out=False):
        """
        Executes a SQL query constructed from a JSON object and retrieves the results.

        Parameters:
        jq (dict): A JSON object containing the query specification. The JSON 
            object should have the following structure:
                - "cols" (optional, default "*"): The columns to include in the result.
                - "table" (optional, default "clients"): The table to query.
                - "queries" (list): A list of query clauses, each having the 
                    following fields:
                    - "qtype": The type of the query clause ("EQUAL", "IN", "LIKE", 
                                or "BETWEEN").
                    - "search": The column to search in.
                    - "match": The value to match against.
                    - "logic" (optional): The logical operator to use when 
                                        combining this clause with the previous 
                                        one ("AND" or "OR").
        json_out (bool, optional): If True, returns the result as a list of 
                                dictionaries (default: True).

        Returns:
        pandas.DataFrame or list of dict: The result of the SQL query. If 
        'json_out' is True, returns a list of dictionaries, otherwise returns a 
        DataFrame.
        """
        cols = jq.get("cols", "*")
        table = jq.get("table", "clients")
        query = f"SELECT {cols} FROM {table} WHERE "
        params = []
        for i, q in enumerate(jq["queries"]):
            qtype = q["qtype"]
            search = q["search"]
            match = q["match"]
            qs = ""
            if i > 0:
                query += f" {q['logic']} "
            if qtype.upper() == "EQUAL":
                _, qs, ps = self.where_table_equal(
                    table, search, match, dry=True)
            elif qtype.upper() == "IN":
                _, qs, ps = self.where_table_in(table, search, match, dry=True)
            elif qtype.upper() == "LIKE":
                _, qs, ps = self.where_table_like(
                    table, search, match, dry=True)
            elif qtype.upper() == "BETWEEN":
                _, qs, ps = self.where_table_between(
                    table, search, match, dry=True)
            query += qs
            params.extend(ps)
        result = self.execute_query(query, params, json_out=json_out)
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
        if ids is None:
            id_col = self.get_id_col(table)
            ids = self.get_table(table)[id_col].unique().tolist()
        if not isinstance(ids, list):
            ids = [ids]
        return ids

    def get_table_by_ids(self, table, ids=None, json_out=False):
        idcol = self.get_id_col(table)
        if ids is None:
            table = self.get_table(table, json_out=True)
        else:
            table = self.where_table_in(table, idcol,
                                        ids, json_out=True)
        return self.handle_output(table, json_out=json_out)

    # CLIENT queries

    def clients(self, client_ids=None, json_out=True):
        return self.get_table_by_ids("clients", client_ids, json_out)

    def client_projects(self, client_ids=None, json_out=True):
        """
        Retrieves all projects for specified clients.

        Parameters:
        client_ids (list, optional): The IDs of the clients. If None, retrieves projects for all clients.

        Returns:
        dict: A dictionary mapping client IDs to a list of their projects.
        """
        client_ids = self.get_ids("clients", client_ids)
        clients = self.where_table_in("clients", "CLIENT_ID", client_ids, json_out=True)
        projects = []
        for i in range(len(clients)):
            client_projects = self.where_table_equal(
                "projects", "CLIENT_ID", clients[i]["CLIENT_ID"]
            )
            for cp in client_projects.to_dict("records"):
                proj = {**clients[i], **cp}
                projects.append(proj)
        return self.data_view(projects, key="CLIENT_ID", json_out=json_out)

    def client_stands_full_data(self, client_ids=None, json_out=True):
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
        stand_data = self.stand_full_data(stand_ids, json_out=json_out)
        return self.data_view(stand_data, "CLIENT_ID", json_out=json_out)

    def client_flights_full_data(self, client_ids=None, json_out=True):
        client_ids = self.get_ids("clients", client_ids)
        flight_ids = self.where_table_in(
            "flights", "CLIENT_ID", client_ids, "FLIGHT_ID"
        )
        flight_ids = flight_ids["FLIGHT_ID"].tolist()
        flight_data = self.flight_full_data(flight_ids)
        return self.data_view(flight_data, "CLIENT_ID", json_out=json_out)

    # PROJECT queries

    def projects(self, project_ids=None, json_out=True):
        return self.get_table_by_ids("projects", project_ids, json_out)

    def project_stands(self, project_ids=None, json_out=True):
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
        stands = []
        for i, project in enumerate(projects):
            project_stand_list = project["STAND_PERSISTENT_IDS"]
            if len(project_stand_list) == 0 or project_stand_list is None:
                continue
            project_stand_list = project_stand_list.split(",")
            project_stands = self.where_table_in(
                "stands", "STAND_PERSISTENT_ID", project_stand_list
            )
            for ps in project_stands.to_dict("records"):
                ps = {**ps, **project}
                stands.append(ps)
        return self.data_view(stands, key="PROJECT_ID", json_out=json_out)

    def project_stands_full_data(self, project_ids=None, json_out=True):
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
        stand_data = self.stand_full_data(stand_ids)
        return self.data_view(stand_data, key="PROJECT_ID", json_out=json_out)

    def project_flights_full_data(self, project_ids=None, json_out=True):
        project_ids = self.get_ids("projects", project_ids)
        flight_ids = self.where_table_in(
            "flights", "PROJECT_ID", project_ids, "FLIGHT_ID",
        )
        flight_ids = flight_ids["FLIGHT_ID"].tolist()
        flight_data = self.flight_full_data(flight_ids)
        return self.data_view(flight_data, "PROJECT_ID", json_out=json_out)

    # STAND queries

    def stands(self, stand_ids=None, json_out=True):
        return self.get_table_by_ids("stands", stand_ids, json_out)

    def stand_flights_full_data(self, stand_ids=None, json_out=True):
        stand_ids = self.get_ids("stands", stand_ids)
        stands = self.where_table_in(
            "stands", "STAND_PERSISTENT_ID", stand_ids)
        stands = stands.to_dict("records")
        stand_flights = []
        for i in range(len(stands)):
            query = {
                "table": "flights",
                "queries": [
                    {
                        "qtype": "equal",
                        "search": "STAND_PERSISTENT_ID",
                        "match": stands[i]["STAND_PERSISTENT_ID"]
                    },
                    {
                        "logic": "and",
                        "qtype": "equal",
                        "search": "CLIENT_ID",
                        "match": stands[i]["CLIENT_ID"]
                    }
                ]
            }
            flights = self.query_from_json(query, json_out=False)
            flight_ids = flights["FLIGHT_ID"].tolist()
            for flight in self.flight_full_data(flight_ids=flight_ids, json_out=True):
                stand_flight = {**stands[i], **flight}
                stand_flights.append(stand_flight)
        return self.data_view(stand_flights, key="STAND_PERSISTENT_ID", json_out=json_out)

    def stand_full_data(self, stand_ids=None, json_out=True):
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
            client = self.where_table_equal(
                "clients", "CLIENT_ID", stands[i]["CLIENT_ID"]
            ).to_dict("records")
            stands[i] = {**stands[i], **project[0], **client[0]}
        return self.handle_output(stands, json_out=json_out)

    # FLIGHT queries

    def flights(self, flight_ids=None, json_out=True):
        return self.get_table_by_ids("flights", flight_ids, json_out)

    def flight_full_data(self, flight_ids=None, json_out=True):
        flight_ids = self.get_ids("flights", flight_ids)
        flights = self.where_table_in("flights", "FLIGHT_ID", flight_ids)
        flights = flights.to_dict("records")
        for i in range(len(flights)):
            flight_ai = self.where_table_equal(
                "flight_ai", "FLIGHT_ID", flights[i]["FLIGHT_ID"]
            ).to_dict("records")[0]
            flight_files = self.where_table_equal(
                "flight_files", "FLIGHT_ID", flights[i]["FLIGHT_ID"]
            ).to_dict("records")[0]
            stand = self.where_table_equal(
                "stands", "STAND_PERSISTENT_ID", flights[i]["STAND_PERSISTENT_ID"]
            ).to_dict("records")[0]
            client = self.where_table_equal(
                "clients", "CLIENT_ID", flights[i]["CLIENT_ID"]
            ).to_dict("records")[0]
            project = self.where_table_equal(
                "projects", "PROJECT_ID", flights[i]["PROJECT_ID"]
            ).to_dict("records")[0]
            flights[i] = {
                **flights[i], **flight_ai, **flight_files,
                **stand, **client, **project
            }
        return self.handle_output(flights, json_out=json_out)

    # data filtering/sorting/management

    def mask_expr_from_json(self, json_filter):
        expr = ''
        for obj in json_filter:
            if "op" in obj:
                expr += f'{obj["op"]} '
            expr += f'({obj["col"]} {obj["clause"]} {obj["val"]}) '
        expr = expr.rstrip('and ').rstrip('or ')
        return expr

    def data_view(self, data=None, key=None, cols=None, json_out=True):
        """
        Creates a view of the stand data, grouped by a specified key.

        Parameters:
        data (list, optional): The stand data. If None, retrieves all stand data.
        key (str, optional): The column to group by. If None, returns the ungrouped data.
        cols (list, optional): The columns to include in the view.

        Returns:
        dict: A dictionary mapping keys to a list of stand data.
        """
        if data is None or len(data) == 0:
            data = self.flight_full_data()
        return self.handle_output(data, json_out=json_out)
        # if key is None or len(key) == 0:
        #     return data
        # data = pd.DataFrame(data)
        # if cols is not None and isinstance(cols, list) and len(cols) > 0:
        #     if key not in cols:
        #         cols.append(key)
        #     data = data[cols]
        # uniq = data[key].unique().tolist()
        # view = {}
        # for val in uniq:
        #     sel = data[data[key] == val]
        #     sel = sel.to_dict("records")
        #     view[val] = sel
        # return self.handle_output(view, json_out=json_out)

    def data_filter(self, json_filter, data=None, json_out=True):
        if data is None or len(data) == 0:
            data = self.flight_full_data()
        if len(json_filter) > 0:
            mask_expr = self.mask_expr_from_json(json_filter)
            mask = data.eval(mask_expr)
            data = data[mask]
        return self.handle_output(data, json_out=json_out)


def list_aerodb_fns():
    import inspect
    public_prefs = [
        "client",
        "project",
        "stand",
        "flight",
        "data"
    ]
    fn_names = []
    # Retrieve all methods of the AeroDB class
    functions = inspect.getmembers(AeroDB, predicate=inspect.isfunction)
    # For each method in AeroDB, create a Flask endpoint unless it's a
    # private method
    for fn_name, _ in functions:
        public = False
        for pref in public_prefs:
            if fn_name.startswith(pref):
                public = True
                break
        if not public:
            continue
        fn_names.append(fn_name)
    return fn_names


if __name__ == "__main__":
    import json
    db = AeroDB()
    data = db.stand_flights_full_data(json_out=False)
    print(data)
