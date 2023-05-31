from flask import Flask, render_template, jsonify, redirect, url_for, request
import requests
from datetime import datetime
import json
import pandas as pd
import sys
sys.path.append("/home/aerotract/software/aerotract_db/db")
from aerodb import list_aerodb_fns

def api_url(endpoint):
    endpoint = endpoint.lstrip("/")
    return f"http://127.0.0.1:5056/{endpoint}"

def get_api(endpoint):
    url = api_url(endpoint)
    return lambda: requests.get(url)

def get_fns_for(prefix):
    fn_names = []
    for fn_name in list_aerodb_fns():
        if not fn_name.startswith(prefix):
            continue
        fn_names.append(fn_name)
    return fn_names

def load_schema():
    with open("/home/aerotract/software/aerotract_db/dashboard/files/schema.json", "r") as fp:
        return json.loads(fp.read())
    
def to_dataframe(data):
    _df = lambda x: pd.DataFrame(x).to_html()
    if isinstance(data, dict):
        return {k: _df(v) for k,v in data.items()}
    elif isinstance(data, list):
        return {"data": _df(data)}
    raise ValueError(f"I dont know how to convert type {type(data)} to dataframe")

def to_tables(search, data):
    tables = {}
    column_names = []
    if isinstance(data, list):
        data = {search: data}
    for k, v in data.items():
        table = {"column_names": [], "data": []}
        if len(v) > 0:
            table["column_names"] = list(v[0].keys())
            table["data"] = v
        tables[k] = table
        column_names = table["column_names"]
    return tables, column_names

app = Flask(__name__, template_folder="./templates")

app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-store'
    return response

@app.route("/health")
def health():
    return f"{datetime.now()}"

@app.route("/")
@app.route("/home")
def home():
    home_views = {
        "View Clients": "client",
        "View Projects": "project",
        "View Stands": "stand",
        "View Flights": "flight",
    }
    return render_template("home.html", views=home_views)

@app.route("/browse")
def browse():
    search = request.args.get('values')
    schema = load_schema()
    functions = schema[search]
    return render_template("browse.html", data=functions, search=search)

@app.route('/view/<api_endpoint>', methods=['POST'])
def view(api_endpoint):
    api_call = get_api(api_endpoint)
    search = request.form.get("search")
    data = api_call().json()
    schema = load_schema()
    desc = schema[search]["functions"][api_endpoint]["description"]
    presets = schema[search]["functions"][api_endpoint].get("selection_groups", {})
    data, column_names = to_tables(desc, data)
    return render_template("datatables.html", tables=data, 
                           column_names=column_names, presets=presets)


if __name__ == "__main__":
    app.debug = True
    app.run(port=5055, host="0.0.0.0")
