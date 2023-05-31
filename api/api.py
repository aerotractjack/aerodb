from flask import Flask, jsonify, request, Response
import inspect
import sys
sys.path.append("/home/aerotract/software/aerotract_db/db")
from aerodb import AeroDB, list_aerodb_fns

app = Flask(__name__)
db = AeroDB()

app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-store'
    return response

# This function dynamically generates a Flask endpoint function for a given 
# method name of the AeroDB class
def make_route_fn(fn_name):
    def call_fn():
        # Extract the JSON data from the incoming request
        kw = request.get_json()
        if kw is None:
            kw = {}
        kw.update({"json_out": True})
        # Call the specified AeroDB method with the JSON data as arguments
        fn = getattr(db, fn_name)(**kw)
        # Return the result of the AeroDB method as a JSON response
        result = jsonify(fn)
        return result
    return call_fn

# This function builds Flask endpoints for all non-private methods of the 
# AeroDB class
def build_routes(app):
    # Retrieve all methods of the AeroDB class
    functions = inspect.getmembers(AeroDB, predicate=inspect.isfunction)
    # For each method in AeroDB, create a Flask endpoint unless it's a 
    # private method
    for fn_name in list_aerodb_fns():
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