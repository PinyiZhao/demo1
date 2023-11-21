from flask import Flask, request, jsonify
from cli import DatabaseCLI
from flask_cors import CORS
from rdb import API


app = Flask(__name__, static_url_path='', static_folder='static')

CORS(app)
# app = Flask(__name__)

cli = DatabaseCLI()
api = API()

@app.route('/')
def home():
    return app.send_static_file("index.html")

@app.route('/ysh')
def home_ysh():
    return app.send_static_file("index_ysh.html")

@app.route('/run-command')
def run_command():
    try:
        command = request.args['command']
        print(command)
        output = cli.process_command(command)
        print(output)
        return jsonify({"output": output})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/run-add')
def add_command():
    try:
        command = request.args['command']
        print(command)
        output = cli.process_command(command)
        print(output)
        return output
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/run-del')
def del_command():
    try:
        command = request.args['command']
        print(command)
        output = cli.process_command(command)
        print(output)
        return output
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/run-upd')
def upd_command():
    try:
        command = request.args['command']
        print(command)
        output = cli.process_command(command)
        print(output)
        return output
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/run-ysh')
def ysh_command():
    try:
        command = request.args['command']
        print(command)
        print(isinstance(command, str))
        output = api.handle_input(command)
        print(output)
        return output
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(port=8080, debug=True)

# @app.route('/eventSearch')
# def eventSearch():
#     payload = {'keyword': request.args['keyword'], 'segmentId': request.args['segmentId'],
#                'radius': request.args['radius'], 'unit': request.args['unit'],
#                'geoPoint': request.args['geoPoint']}
#     r = requests.get(urlPlusKey,
#                      params=payload)
#
#     return r.json()