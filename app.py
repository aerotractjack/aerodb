from flask import Flask, jsonify, request, Response
from aerodb import AeroDB

app = Flask(__name__)
db = AeroDB()

@app.route("/api/list/clients")
def clients():
    clients = db.list_clients()
    return jsonify(clients)

@app.route("/api/list/projects")
def projects_for_client():
    client_ids = request.args.get("client_id", "")
    if client_ids == "":
        return Response("Please specify `client_id` as a query parameter", 400)
    client_ids = client_ids.split(",")
    projects = []
    for client_id in client_ids:
        client_id = int(client_id)
        project = db.list_projects_for_client(client_id)
        projects.append(project)
    return jsonify(projects)

@app.route("/api/list/stands")
def stands_for_project():
    project_ids = request.args.get("project_id", "")
    if project_ids == "":
        return Response("Please specify `project_id` as a query parameter", 400)
    project_ids = project_ids.split(",")
    stands = []
    for project_id in project_ids:
        project_id = int(project_id)
        stand = db.list_stands_for_project(project_id)
        stands.append(stand)
    return jsonify(stands)

@app.route("/api/all")
def get_all_data():
    return jsonify(db.get_all_data())

@app.route("/api/list/stands/full")
def list_stands_full_info():
    return jsonify(db.full_stand_info_all_stands())

@app.route("/api/add/client", methods=["POST"])
def new_client():
    name = request.form["name"] 
    category = request.form["category"] 
    creation_data = request.form["creation_data"]
    notes = request.form["notes"]
    resp = db.add_client(name, category, creation_data, notes)
    return jsonify(resp)

if __name__ == "__main__":
    app.debug = True
    app.run(port=5056, host="0.0.0.0")