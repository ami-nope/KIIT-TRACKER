from flask import Flask, render_template, request, jsonify, session, redirect, url_for, Response, stream_with_context
from flask_cors import CORS
import json
import os
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.middleware.proxy_fix import ProxyFix
import threading
import queue
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)
app.config['WTF_CSRF_ENABLED'] = False
app.secret_key = os.environ.get('FLASK_SECRET', 'dev-secret-change-this')

CORS(app, resources={r"/api/*": {"origins": "*"}})
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

RENDER_URL = os.environ.get('RENDER_EXTERNAL_URL', '')
ON_RENDER = bool(os.environ.get('RENDER') or RENDER_URL)
IS_HTTPS = RENDER_URL.startswith('https://')
app.config.update(
    SESSION_COOKIE_SAMESITE='Lax',
    SESSION_COOKIE_SECURE=IS_HTTPS if ON_RENDER else False,
    PREFERRED_URL_SCHEME='https' if IS_HTTPS else 'http'
)

# ---------- file paths ----------
BUSES_FILE = os.path.join(BASE_DIR, 'buses_location.json')
LOCATIONS_FILE = os.path.join(BASE_DIR, 'locations.json')
CREDENTIALS_FILE = os.path.join(BASE_DIR, 'credentials.json')

# ---------- simple JSON helpers ----------
def load_json(path, default):
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path, data):
    try:
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass

def ensure_files():
    if not os.path.exists(BUSES_FILE):
        save_json(BUSES_FILE, {})
    if not os.path.exists(LOCATIONS_FILE):
        save_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    if not os.path.exists(CREDENTIALS_FILE):
        save_json(CREDENTIALS_FILE, {"admins": [], "institute_name": "INSTITUTE"})

# ---------- in-memory caches ----------
_buses = {}
_buses_lock = threading.Lock()
_worker_started = False

APP_START_TS = time.time()
REQUESTS_TOTAL = 0

def _init_app():
    global _buses, _worker_started
    ensure_files()
    _buses = load_json(BUSES_FILE, {})
    if not _worker_started:
        _worker_started = True
        threading.Thread(target=_sync_worker, daemon=True).start()

@app.before_request
def _before():
    global REQUESTS_TOTAL
    if not hasattr(app, '_ready'):
        _init_app()
        app._ready = True
    REQUESTS_TOTAL += 1

def _sync_worker():
    last = None
    while True:
        time.sleep(2)
        try:
            with _buses_lock:
                snap = json.dumps(_buses, sort_keys=True)
            if snap != last:
                save_json(BUSES_FILE, json.loads(snap))
                last = snap
        except Exception:
            pass

# ---------- SSE ----------
_subscribers_lock = threading.Lock()
_subscribers = []

def broadcast(payload):
    try:
        data = json.dumps(payload)
    except Exception:
        data = json.dumps({"error": "bad-payload"})
    with _subscribers_lock:
        for q in list(_subscribers):
            try:
                q.put_nowait(data)
            except Exception:
                pass

@app.route('/events')
def sse_events():
    if os.environ.get('DISABLE_SSE', '').lower() in ('1', 'true', 'yes'):
        return Response('SSE disabled', status=503, mimetype='text/plain')
    def stream():
        q = queue.Queue(maxsize=100)
        with _subscribers_lock:
            _subscribers.append(q)
        yield 'event: ping\ndata: "connected"\n\n'
        try:
            while True:
                try:
                    hb = max(5, int(os.environ.get('SSE_HEARTBEAT_SEC', '20')))
                    msg = q.get(timeout=hb)
                    yield f'data: {msg}\n\n'
                except queue.Empty:
                    yield 'event: ping\ndata: {}\n\n'
        finally:
            with _subscribers_lock:
                try:
                    _subscribers.remove(q)
                except ValueError:
                    pass
    return Response(stream_with_context(stream()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no', 'Connection': 'keep-alive'})

# ---------- credentials helpers ----------
def load_credentials():
    return load_json(CREDENTIALS_FILE, {"admins": [], "institute_name": "INSTITUTE"})

def save_credentials(data):
    save_json(CREDENTIALS_FILE, data)

# ---------- auth ----------
def login_required(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if 'admin' not in session:
            return redirect(url_for('admin_login'))
        return fn(*args, **kwargs)
    return wrapper

# ---------- page routes ----------
@app.route('/health')
def health_check():
    return 'OK', 200

@app.route('/')
def student_view():
    creds = load_credentials()
    return render_template('student.html', institute_name=creds.get('institute_name', 'INSTITUTE'))

@app.route('/driver')
def driver_view():
    creds = load_credentials()
    return render_template('driver.html', institute_name=creds.get('institute_name', 'INSTITUTE'))

@app.route('/simulator')
def simulator_view():
    creds = load_credentials()
    return render_template('simulator.html', institute_name=creds.get('institute_name', 'INSTITUTE'))

@app.route('/admin')
@login_required
def admin_view():
    creds = load_credentials()
    return render_template('admin.html', institute_name=creds.get('institute_name', 'INSTITUTE'), admin_user=session.get('admin'))

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    try:
        creds = load_credentials()
        if request.method == 'GET':
            return render_template('admin_login.html', credentials_exist=bool(creds.get('admins')),
                                   institute_name=creds.get('institute_name', 'INSTITUTE'))

        data = request.form
        action = data.get('action')
        institute = data.get('institute_name', '').strip()
        username = data.get('username', '').strip()
        password = data.get('password', '').strip()
        error_text = None

        if 'admins' not in creds:
            creds['admins'] = []

        if action == 'signup':
            pin = (data.get('signup_pin', '') or '').strip()
            if pin != '456123':
                error_text = "Invalid signup pin."
            elif not username or not password:
                error_text = "Provide username and password."
            elif any(a.get('username') == username for a in creds['admins']):
                error_text = "Admin username already exists."
            else:
                creds['institute_name'] = institute or creds.get('institute_name', 'INSTITUTE')
                creds['admins'].append({'username': username, 'password_hash': generate_password_hash(password)})
                save_credentials(creds)
                session['admin'] = username
                return redirect(url_for('admin_view'))

        elif action == 'login':
            if not creds.get('admins'):
                error_text = "No admin accounts exist. Please signup first."
            else:
                admin = next((a for a in creds['admins'] if a.get('username') == username), None)
                if not admin:
                    error_text = "Invalid username."
                elif admin.get('password_hash') and check_password_hash(admin['password_hash'], password):
                    session['admin'] = username
                    return redirect(url_for('admin_view'))
                else:
                    error_text = "Invalid password."
        else:
            error_text = "Invalid action."

        return render_template('admin_login.html', credentials_exist=bool(creds.get('admins')),
                               institute_name=institute or creds.get('institute_name', 'INSTITUTE'), error_text=error_text)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return f"Server Error: {str(e)}", 500

@app.route('/admin/logout')
def admin_logout():
    session.pop('admin', None)
    return redirect(url_for('admin_login'))

# ---------- admin user management ----------
@app.route('/admin/users')
@login_required
def admin_users():
    creds = load_credentials()
    users = []
    for adm in creds.get('admins', []):
        users.append({'type': 'Admin', 'username': adm.get('username', ''), 'password': '************'})
    for s in creds.get('students', []):
        users.append({'type': 'Student', 'username': s.get('username', ''), 'password': '************'})
    return jsonify({'users': users})

@app.route('/admin/admins', methods=['GET'])
@login_required
def list_admins():
    creds = load_credentials()
    return jsonify({'admins': [{'username': a.get('username', '')} for a in creds.get('admins', [])]})

@app.route('/admin/admins', methods=['POST'])
@login_required
def add_admin():
    data = request.json or {}
    username = (data.get('username', '') or '').strip()
    password = (data.get('password', '') or '').strip()
    pin = (data.get('pin', '') or '').strip()
    if pin != '456123':
        return jsonify({'error': 'Invalid pin'}), 400
    if not username or not password:
        return jsonify({'error': 'Provide username and password'}), 400
    creds = load_credentials()
    if any(a.get('username') == username for a in creds.get('admins', [])):
        return jsonify({'error': 'Admin username already exists'}), 400
    creds.setdefault('admins', []).append({'username': username, 'password_hash': generate_password_hash(password)})
    save_credentials(creds)
    return jsonify({'status': 'success', 'username': username})

@app.route('/admin/admins/<username>', methods=['DELETE'])
@login_required
def delete_admin(username):
    creds = load_credentials()
    admins = creds.get('admins', [])
    new = [a for a in admins if a.get('username') != username]
    if len(new) == len(admins):
        return jsonify({'error': 'Admin not found'}), 404
    creds['admins'] = new
    save_credentials(creds)
    if session.get('admin') == username:
        session.pop('admin', None)
    return jsonify({'status': 'success'})

@app.route('/admin/admins/<username>/password', methods=['POST'])
@login_required
def change_admin_password(username):
    data = request.json or {}
    new_pw = (data.get('password', '') or '').strip()
    pin = (data.get('pin', '') or '').strip()
    if pin != '456123':
        return jsonify({'error': 'Invalid pin'}), 400
    if not new_pw:
        return jsonify({'error': 'Provide new password'}), 400
    creds = load_credentials()
    admin = next((a for a in creds.get('admins', []) if a.get('username') == username), None)
    if not admin:
        return jsonify({'error': 'Admin not found'}), 404
    admin['password_hash'] = generate_password_hash(new_pw)
    save_credentials(creds)
    return jsonify({'status': 'success'})

# ---------- metrics ----------
@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    creds = load_credentials()
    return jsonify({'total_transports': int(creds.get('total_transports', 100))})

@app.route('/api/metrics', methods=['POST'])
@login_required
def update_metrics():
    data = request.json or {}
    try:
        total = int(data.get('total_transports'))
    except (TypeError, ValueError):
        return jsonify({'error': 'Invalid total_transports'}), 400
    if total < 0:
        return jsonify({'error': 'Provide non-negative total_transports'}), 400
    creds = load_credentials()
    creds['total_transports'] = total
    save_credentials(creds)
    return jsonify({'status': 'success', 'total_transports': total})

# ---------- bus APIs ----------
@app.route('/api/buses', methods=['GET'])
def get_all_buses():
    return jsonify(_buses)

@app.route('/api/buses/clear', methods=['POST'])
def clear_all_buses():
    global _buses
    with _buses_lock:
        _buses = {}
    save_json(BUSES_FILE, {})
    try:
        broadcast({'type': 'buses_clear'})
    except Exception:
        pass
    return jsonify({'status': 'success'})

@app.route('/api/bus/<int:bus_number>', methods=['POST'])
def update_bus_location(bus_number):
    raw = request.get_json(silent=True) or request.form.to_dict() or {}
    try:
        lat = float(raw.get('lat'))
        lng = float(raw.get('lng'))
    except (TypeError, ValueError):
        return jsonify({'error': 'Provide numeric lat and lng'}), 400
    last_update = raw.get('lastUpdate') or time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    bus_id = str(bus_number)
    with _buses_lock:
        existing = _buses.get(bus_id, {})
        route_id = raw.get('routeId', existing.get('routeId'))
        _buses[bus_id] = {'lat': lat, 'lng': lng, 'lastUpdate': last_update, 'routeId': route_id}
        current_data = _buses[bus_id]
    try:
        broadcast({'type': 'bus_update', 'bus': bus_id, 'data': current_data})
    except Exception:
        pass
    return jsonify({'status': 'success', 'bus': bus_number})

@app.route('/api/bus/<int:bus_number>', methods=['DELETE'])
def stop_bus(bus_number):
    bus_id = str(bus_number)
    with _buses_lock:
        _buses.pop(bus_id, None)
    try:
        broadcast({'type': 'bus_stop', 'bus': bus_id})
    except Exception:
        pass
    return jsonify({'status': 'success'})

@app.route('/api/bus/<int:bus_number>/route', methods=['POST'])
def set_bus_route(bus_number):
    data = request.get_json(silent=True) or {}
    route_id = data.get('routeId')
    bus_id = str(bus_number)
    with _buses_lock:
        if bus_id in _buses:
            _buses[bus_id]['routeId'] = route_id
        else:
            _buses[bus_id] = {'lat': 0, 'lng': 0, 'lastUpdate': '', 'routeId': route_id}
    try:
        broadcast({'type': 'route_set', 'bus': bus_id, 'routeId': route_id})
    except Exception:
        pass
    return jsonify({'status': 'success'})

@app.route('/api/bus-routes', methods=['GET'])
def get_bus_routes():
    with _buses_lock:
        result = {k: v.get('routeId') for k, v in _buses.items()}
    return jsonify(result)

# ---------- location APIs ----------
@app.route('/api/locations', methods=['GET'])
def get_locations():
    return jsonify(load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []}))

@app.route('/api/hostels', methods=['GET'])
def get_hostels():
    locs = load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    return jsonify(locs.get('hostels', []))

@app.route('/api/classes', methods=['GET'])
def get_classes():
    locs = load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    return jsonify(locs.get('classes', []))

@app.route('/api/routes', methods=['GET'])
def get_routes():
    locs = load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    return jsonify(locs.get('routes', []))

@app.route('/api/route', methods=['POST'])
def create_route():
    data = request.json
    locs = load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    route = {
        'id': data.get('id', f"route_{len(locs.get('routes', [])) + 1}"),
        'name': data['name'],
        'waypoints': data['waypoints'],
        'stops': data.get('stops', []),
        'color': data.get('color', '#FF5722')
    }
    routes = locs.get('routes', [])
    idx = next((i for i, r in enumerate(routes) if r['id'] == route['id']), -1)
    if idx >= 0:
        routes[idx] = route
    else:
        routes.append(route)
    locs['routes'] = routes
    save_json(LOCATIONS_FILE, locs)
    return jsonify({'status': 'success', 'route': route})

@app.route('/api/route/<route_id>', methods=['DELETE'])
def delete_route(route_id):
    locs = load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    locs['routes'] = [r for r in locs.get('routes', []) if r['id'] != route_id]
    save_json(LOCATIONS_FILE, locs)
    return jsonify({'status': 'success'})

@app.route('/api/hostel', methods=['POST'])
def create_hostel():
    data = request.json
    locs = load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    hostel = {
        'id': f"hostel_{len(locs.get('hostels', [])) + 1}",
        'name': data['name'],
        'lat': data['lat'],
        'lng': data['lng'],
        'capacity': data.get('capacity', 100)
    }
    locs['hostels'].append(hostel)
    save_json(LOCATIONS_FILE, locs)
    return jsonify({'status': 'success', 'hostel': hostel})

@app.route('/api/hostel/<hostel_id>', methods=['DELETE'])
def delete_hostel(hostel_id):
    locs = load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    locs['hostels'] = [h for h in locs.get('hostels', []) if h['id'] != hostel_id]
    save_json(LOCATIONS_FILE, locs)
    return jsonify({'status': 'success'})

@app.route('/api/class', methods=['POST'])
def create_class():
    data = request.json
    locs = load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    cls = {
        'id': f"class_{len(locs.get('classes', [])) + 1}",
        'name': data['name'],
        'lat': data['lat'],
        'lng': data['lng'],
        'department': data.get('department', 'Unknown')
    }
    locs['classes'].append(cls)
    save_json(LOCATIONS_FILE, locs)
    return jsonify({'status': 'success', 'class': cls})

@app.route('/api/class/<class_id>', methods=['DELETE'])
def delete_class(class_id):
    locs = load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    locs['classes'] = [c for c in locs.get('classes', []) if c['id'] != class_id]
    save_json(LOCATIONS_FILE, locs)
    return jsonify({'status': 'success'})

# ---------- health / status ----------
@app.route('/healthz')
def healthz():
    return jsonify({'status': 'ok', 'uptime_sec': int(time.time() - APP_START_TS)}), 200

@app.route('/status')
def status():
    locs = load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    return jsonify({
        'uptime_sec': int(time.time() - APP_START_TS),
        'requests_total': REQUESTS_TOTAL,
        'sse_clients': len(_subscribers),
        'buses_count': len(_buses),
        'routes_count': len(locs.get('routes', [])),
        'on_render': ON_RENDER,
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
