from flask import Flask, jsonify, request, Response
import sys
sys.path.append("/home/aerotract/software/aerotract_db/db")
from aerodb import AeroDB

app = Flask(__name__)
db = AeroDB()

@app.route("/api/clients")
def clients():
    clients = db.list_clients()
    return jsonify(clients)

@app.route("/api/name/<category>/<uid>")
def name(category, uid):
    res = db.id_to_name(category, int(uid))
    res = {"name": res}
    return jsonify(res)

@app.route("/api/projects")
def projects_for_client():
    client_ids = request.args.get("client_id", None)
    flatten = bool(request.args.get("key", "CLIENT_ID"))
    if client_ids is not None and len(client_ids) > 0:
        client_ids = [{"CLIENT_ID": c} for c in client_ids.split(",")]
    projects = db.projects_by_client(client_ids, flatten)
    return jsonify(projects)

@app.route("/api/stands")
def stands_for_project():
    project_ids = request.args.get("project_id", None)
    flatten = bool(request.args.get("flatten", False))
    if project_ids is not None:
        project_ids = [{"PROJECT_ID": p} for p in project_ids.split(",")]
    stands = db.stands_for_project(project_ids, flatten)
    return jsonify(stands)

@app.route("/api/nas")
def nas_info_for_stand():
    client_id = request.args.get("client_id", None)
    project_id = request.args.get("project_id", None)
    stand_temp_id = request.args.get("stand_temp_id", None)
    if None in [client_id, project_id, stand_temp_id]:
        return "Must specify all `client_id`, `project_id`, and `stand_temp_id`", 400
    nasinfo = db.NAS_info_for_stand(client_id, project_id, stand_temp_id)
    return jsonify(nasinfo)

@app.route("/api/clients/new", methods=["POST"])
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