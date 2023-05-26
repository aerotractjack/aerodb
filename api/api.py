from flask import Flask, jsonify, request, Response
import inspect
import sys
sys.path.append("/home/aerotract/software/aerotract_db/db")
from aerodb import AeroDB

app = Flask(__name__)
db = AeroDB()

def make_route_fn(fn):
    def call_fn():
        kw = request.get_json()
        if kw is None:
            kw = {}
        return getattr(db, fn)(**kw)
    return call_fn

def build_routes(app):
    private = ["__init__", "con", "engine"]
    functions = inspect.getmembers(AeroDB, predicate=inspect.isfunction)
    for fn_name, fn in functions:
        if fn_name in private:
            continue
        app.add_url_rule(
            f'/{fn_name}', 
            endpoint=fn_name, 
            view_func=make_route_fn(fn_name),
            methods=["POST", "GET"]
        )

if __name__ == "__main__":
    app.debug = True
    build_routes(app)
    app.run(port=5056, host="0.0.0.0")