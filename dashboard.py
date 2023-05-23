from flask import Flask, render_template, jsonify, redirect, url_for, request
import requests
from datetime import datetime

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
    req = requests.get("http://127.0.0.1:5056/api/list/clients")
    return render_template("browse_clients.html", clients=req.json())

@app.route("/browse/projects", methods=["POST", "GET"])
def browse_projects():
    selection = request.form.getlist("clientCheck")
    selection = [s.split("*") for s in selection]
    selected_ids = [s[0] for s in selection]
    selected_names = [s[1] for s in selection]
    query = ",".join(selected_ids)
    req = requests.get("http://127.0.0.1:5056/api/list/projects?client_id="+query).json()
    resp = [{
        "client_name": selected_names[i],
        "projects": req[i]
    } for i in range(len(selection))]
    return render_template("browse_projects.html", companies=resp)

@app.route("/browse/stands", methods=["POST", "GET"])
def browse_stands():
    selection = request.form.getlist("projectCheck")
    selection = [s.split("*") for s in selection]
    selected_ids = [s[0] for s in selection]
    selected_names = [s[1] for s in selection]
    company_names = [s[2] for s in selection]
    query = ",".join(selected_ids)
    req = requests.get("http://127.0.0.1:5056/api/list/stands?project_id="+query).json()
    resp = [{
        "project_name": selected_names[i],
        "company_name": company_names[i],
        "stands": req[i]
    } for i in range(len(selection))]
    return render_template("browse_stands.html", data=resp)

@app.route("/browse/stands/full")
def browse_stands_full():
    data = requests.get("http://127.0.0.1:5056/api/list/stands/full").json()
    return render_template("full_stand_info.html", data=data)

@app.route("/new/client")
def new_client():
    return render_template("new_client.html")

@app.route("/new/client/submit", methods=["POST", "GET"])
def new_client_submit():
    body = dict(request.form)
    resp = requests.post("http://127.0.0.1:5056/api/add/client", data=body).json()
    return render_template("client_added.html", data=resp)

if __name__ == "__main__":
    app.debug = True
    app.run(port=5055)