from flask import Flask, render_template, jsonify, redirect, url_for, request
import requests
from datetime import datetime
import json
import sys

app = Flask(__name__, template_folder="./templates")

@app.route("/health")
def health():
    return "healthy"

@app.route("/")
@app.route("/home")
def home():
    return render_template("home.html")

@app.route("/browse/clients")
def browse_clients():
    resp = requests.get("http://127.0.0.1:5056/api/clients").json()
    return render_template("browse_clients.html", clients=resp)

@app.route("/browse/projects", methods=["POST", "GET"])
def browse_projects():
    selected_client_ids = request.form.getlist("clientCheck")
    url = "http://127.0.0.1:5056/api/projects"
    params = {}
    if len(selected_client_ids) == 0:
        all_clients_ids = requests.get("http://127.0.0.1:5056/api/clients").json()
        selected_client_ids = [str(c["CLIENT_ID"]) for c in all_clients_ids]
    params = {"client_id": ",".join(selected_client_ids)}
    cidmap = {}
    for cid in selected_client_ids:
        nameq = requests.get("http://127.0.0.1:5056/api/name/clients/" + str(cid)).json()
        cidmap[cid] = nameq["name"]
    resp = requests.get(url, params=params).json()
    return render_template("browse_projects.html", projects=resp, client_id_map=cidmap)

@app.route("/browse/stands", methods=["POST", "GET"])
def browse_stands():
    selected_project_ids = request.form.getlist("projectCheck")
    url = "http://127.0.0.1:5056/api/stands"
    params = {"project_id": ",".join(selected_project_ids)}
    resp = requests.get(url, params=params).json()
    pidmap = {}
    for pid in selected_project_ids:
        nameq = requests.get("http://127.0.0.1:5056/api/name/projects/" + str(pid)).json()
        pidmap[pid] = nameq["name"]
    return render_template("browse_stands.html", data=resp, project_id_map=pidmap)

@app.route("/new/client")
def new_client():
    return render_template("new_client.html")

@app.route("/new/client/submit", methods=["POST", "GET"])
def new_client_submit():
    body = dict(request.form)
    resp = requests.post(
        "http://127.0.0.1:5056/api/clients/new", data=body).json()
    return render_template("client_added.html", data=resp)


if __name__ == "__main__":
    app.debug = True
    app.run(port=5055, host="0.0.0.0")
