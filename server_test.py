from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route('/api/v1/status', methods=['GET'])
def status():
    return jsonify({'status': 'ok'})


@app.route('/api/v1/messages', methods=['POST'])
def messages():
    data = request.get_json()
    data['delivered'] = True
    return jsonify(data)


