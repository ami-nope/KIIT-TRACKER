from flask import Flask, render_template, request, jsonify
import json
import os

app = Flask(__name__)

LOCATION_FILE = 'bus_location.json'

if not os.path.exists(LOCATION_FILE):
    with open(LOCATION_FILE, 'w') as f:
        json.dump({'lat': 20.3549, 'lng': 85.8161}, f)

@app.route('/')
def student_view():
    return render_template('student.html')

@app.route('/driver')
def driver_view():
    return render_template('driver.html')

@app.route('/api/location', methods=['GET'])
def get_location():
    with open(LOCATION_FILE, 'r') as f:
        location = json.load(f)
    return jsonify(location)

@app.route('/api/location', methods=['POST'])
def update_location():
    data = request.json
    with open(LOCATION_FILE, 'w') as f:
        json.dump(data, f)
    return jsonify({'status': 'success'})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
