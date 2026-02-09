import gevent.monkey
gevent.monkey.patch_all()

from flask import Flask, render_template, request, jsonify, session, redirect, url_for, Response, stream_with_context
from flask_cors import CORS
import json
import os
from werkzeug.security import generate_password_hash, check_password_hash
import base64
from werkzeug.middleware.proxy_fix import ProxyFix
import threading
import queue
import time
import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)
app.config['WTF_CSRF_ENABLED'] = False
app.secret_key = os.environ.get('FLASK_SECRET', 'dev-secret-change-this')

# Health check for load balancers (fast & no locks)
@app.route('/health')
def health_check():
    return 'OK', 200

# Enable CORS for API routes (frontends on different origins)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Make app proxy-aware (important on Render) so url_for + request.scheme honor X-Forwarded-* headers
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

# Hosted environment session hardening with HTTPS-aware configuration
RENDER_URL = os.environ.get('RENDER_EXTERNAL_URL', '')
ON_RENDER = bool(os.environ.get('RENDER') or RENDER_URL)
IS_HTTPS = RENDER_URL.startswith('https://')
app.config.update(
    SESSION_COOKIE_SAMESITE='Lax',
    SESSION_COOKIE_SECURE=IS_HTTPS if ON_RENDER else False,
    PREFERRED_URL_SCHEME='https' if IS_HTTPS else 'http'
)

# Lightweight runtime metrics
APP_START_TS = time.time()
REQUESTS_TOTAL = 0

@app.before_request
def _count_request():
    # Skip noise from health checks optionally
    global REQUESTS_TOTAL
    REQUESTS_TOTAL += 1

BUSES_FILE = os.path.join(BASE_DIR, 'buses_location.json')
LOCATIONS_FILE = os.path.join(BASE_DIR, 'locations.json')
CREDENTIALS_FILE = os.path.join(BASE_DIR, 'credentials.json')

# Ensure required files exist
if not os.path.exists(BUSES_FILE):
    with open(BUSES_FILE, 'w') as f:
        json.dump({}, f)
if not os.path.exists(LOCATIONS_FILE):
    with open(LOCATIONS_FILE, 'w') as f:
        json.dump({"hostels": [], "classes": [], "routes": []}, f)
if not os.path.exists(CREDENTIALS_FILE):
    with open(CREDENTIALS_FILE, 'w') as f:
        json.dump({"admins": [], "institute_name": "INSTITUTE"}, f)

_file_locks = {
    'buses': threading.Lock(),
    'locations': threading.Lock(),
    'credentials': threading.Lock(),
}

def _lock_for(path: str):
    if path == BUSES_FILE:
        return _file_locks['buses']
    if path == LOCATIONS_FILE:
        return _file_locks['locations']
    if path == CREDENTIALS_FILE:
        return _file_locks['credentials']
    # Fallback single lock for unknown
    return threading.Lock()

# Safe JSON helpers that handle concurrency via atomic replacement (on Linux/POSIX)
# This allows us to REMOVE read locks entirely, which is the main bottleneck.
# safe_save_json writes to a temp file and atomically renames it.
# safe_load_json reads the file without a lock (it will get either old or new version, but not partial).

def safe_load_json(path: str, default):
    # No lock needed for atomic reads on updated files
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception:
        # If file is truly corrupted (rare with atomic writes), try to reset
        try:
            with open(path, 'w') as f:
                json.dump(default, f)
        except Exception:
            pass
        return default

def safe_save_json(path: str, data):
    # Lock only ensures one writer at a time, doesn't block readers
    lock = _lock_for(path)
    with lock:
        tmp_path = path + ".tmp"
        try:
            with open(tmp_path, 'w') as f:
                json.dump(data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            # Atomic replace (on POSIX/Linux which Railway uses)
            os.replace(tmp_path, path)
        except Exception:
            # Fallback if something fails
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except:
                    pass

# === SSE subscribers ===
_subscribers_lock = threading.Lock()
_subscribers = []  # list[queue.Queue]

def broadcast(payload: dict):
    try:
        data = json.dumps(payload)
    except Exception:
        data = json.dumps({"error": "bad-payload"})
    with _subscribers_lock:
        for q in list(_subscribers):
            try:
                q.put_nowait(data)
            except Exception:
                # Drop if queue is full or closed
                pass

@app.route('/events')
def sse_events():
    # Allow Render free tier fallback via env to disable SSE
    if os.environ.get('DISABLE_SSE', '').lower() in ('1', 'true', 'yes'):
        return Response('SSE disabled', status=503, mimetype='text/plain')
    # Server-Sent Events endpoint
    def event_stream():
        q = queue.Queue(maxsize=100)
        with _subscribers_lock:
            _subscribers.append(q)
        # Initial hello to avoid proxy buffering
        yield 'event: ping\ndata: "connected"\n\n'
        try:
            while True:
                try:
                    # Heartbeat interval configurable via env
                    heartbeat = max(5, int(os.environ.get('SSE_HEARTBEAT_SEC', '20')))
                    msg = q.get(timeout=heartbeat)
                    yield f'data: {msg}\n\n'
                except queue.Empty:
                    # heartbeat to keep connection alive through proxies
                    yield 'event: ping\ndata: {}\n\n'
        finally:
            with _subscribers_lock:
                try:
                    _subscribers.remove(q)
                except ValueError:
                    pass
    headers = {
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no',  # disable buffering on some proxies
        'Connection': 'keep-alive'
    }
    return Response(stream_with_context(event_stream()), mimetype='text/event-stream', headers=headers)

def load_credentials():
    return safe_load_json(CREDENTIALS_FILE, {})

@app.route('/driver')
def driver_view():
    creds = load_credentials()
    institute = creds.get('institute_name', 'INSTITUTE')
    return render_template('driver.html', institute_name=institute)

def login_required(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if 'admin' not in session:
            return redirect(url_for('admin_login'))
        return fn(*args, **kwargs)
    return wrapper

    
def save_credentials(data: dict):
    with open(CREDENTIALS_FILE, 'w') as f:
        json.dump(data, f, indent=2)





@app.route('/')
def student_view():
    creds = load_credentials()
    institute = creds.get('institute_name', 'INSTITUTE')
    return render_template('student.html', institute_name=institute)

@app.route('/simulator')
def simulator_view():
    creds = load_credentials()
    institute = creds.get('institute_name', 'INSTITUTE')
    return render_template('simulator.html', institute_name=institute)

 
@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    creds = load_credentials()
    # Default to 100 if not set
    total_transports = int(creds.get('total_transports', 100))
    return jsonify({ 'total_transports': total_transports })

 
@app.route('/api/metrics', methods=['POST'])
@login_required
def update_metrics():
    data = request.json or {}
    try:
        total_transports = int(data.get('total_transports', None))
    except (TypeError, ValueError):
        return jsonify({ 'error': 'Invalid total_transports' }), 400
    if total_transports is None or total_transports < 0:
        return jsonify({ 'error': 'Provide non-negative total_transports' }), 400
    creds = load_credentials()
    creds['total_transports'] = total_transports
    save_credentials(creds)
    return jsonify({ 'status': 'success', 'total_transports': total_transports })


@app.route('/admin')
@login_required
def admin_view():
    creds = load_credentials()
    institute = creds.get('institute_name', 'INSTITUTE')
    user = session.get('admin')
    return render_template('admin.html', institute_name=institute, admin_user=user)

@app.route('/admin/users')
@login_required
def admin_users():
    creds = load_credentials()
    users = []
    # Admin users (support multiple accounts)
    for adm in creds.get('admins', []):
        users.append({
            'type': 'Admin',
            'username': adm.get('username', ''),
            'password': '************' if adm.get('password_hash') else ''
        })
    # Student users (example: if you have a students list in credentials.json)
    for student in creds.get('students', []):
        users.append({
            'type': 'Student',
            'username': student.get('username', ''),
            'password': '************' if student.get('password_hash') else ''
        })
    return jsonify({'users': users})


@app.route('/admin/admins', methods=['GET'])
@login_required
def list_admins():
    creds = load_credentials()
    admins = [{'username': a.get('username', '')} for a in creds.get('admins', [])]
    return jsonify({'admins': admins})


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
    admins = creds.get('admins', [])
    if any(a.get('username') == username for a in admins):
        return jsonify({'error': 'Admin username already exists'}), 400

    admins.append({'username': username, 'password_hash': generate_password_hash(password)})
    creds['admins'] = admins
    save_credentials(creds)
    return jsonify({'status': 'success', 'username': username})


@app.route('/admin/admins/<username>', methods=['DELETE'])
@login_required
def delete_admin(username):
    creds = load_credentials()
    admins = creds.get('admins', [])
    before = len(admins)
    admins = [a for a in admins if a.get('username') != username]
    if len(admins) == before:
        return jsonify({'error': 'Admin not found'}), 404
    creds['admins'] = admins
    save_credentials(creds)
    if session.get('admin') == username:
        session.pop('admin', None)
    return jsonify({'status': 'success'})


@app.route('/admin/admins/<username>/password', methods=['POST'])
@login_required
def change_admin_password(username):
    data = request.json or {}
    new_password = (data.get('password', '') or '').strip()
    pin = (data.get('pin', '') or '').strip()

    if pin != '456123':
        return jsonify({'error': 'Invalid pin'}), 400
    if not new_password:
        return jsonify({'error': 'Provide new password'}), 400

    creds = load_credentials()
    admin = next((a for a in creds.get('admins', []) if a.get('username') == username), None)
    if not admin:
        return jsonify({'error': 'Admin not found'}), 404

    admin['password_hash'] = generate_password_hash(new_password)
    save_credentials(creds)
    return jsonify({'status': 'success'})


@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    try:
        creds = load_credentials()
        
        if request.method == 'GET':
            has_admins = bool(creds.get('admins'))
            return render_template('admin_login.html', credentials_exist=has_admins, institute_name=creds.get('institute_name', 'INSTITUTE'))

        data = request.form
        action = data.get('action')
        institute = data.get('institute_name', '').strip()
        username = data.get('username', '').strip()
        password = data.get('password', '').strip()
        error_text = None

        # Ensure admins list exists
        if 'admins' not in creds:
            creds['admins'] = []

        if action == 'signup':
            signup_pin = (data.get('signup_pin', '') or '').strip()
            # Require correct pin 456123 for admin account creation
            valid_pin = signup_pin == '456123'
            if not valid_pin:
                error_text = "Invalid signup pin."
            elif not username or not password:
                error_text = "Provide username and password."
            elif any(a.get('username') == username for a in creds['admins']):
                error_text = "Admin username already exists."
            else:
                creds['institute_name'] = institute or creds.get('institute_name', 'INSTITUTE')
                creds['pin_hash'] = generate_password_hash('456123')
                creds['admins'].append({
                    'username': username,
                    'password_hash': generate_password_hash(password)
                })
                save_credentials(creds)
                session['admin'] = username
                return redirect(url_for('admin_view'))
            has_admins = bool(creds.get('admins'))
            return render_template('admin_login.html', credentials_exist=has_admins, institute_name=institute or creds.get('institute_name', 'INSTITUTE'), error_text=error_text)

        elif action == 'login':
            if not creds.get('admins'):
                error_text = "No admin accounts exist. Please signup first."
            else:
                # Find matching admin
                admin = next((a for a in creds['admins'] if a.get('username') == username), None)
                if not admin:
                    error_text = "Invalid username."
                elif admin.get('password_hash') and check_password_hash(admin['password_hash'], password):
                    session['admin'] = username
                    return redirect(url_for('admin_view'))
                else:
                    error_text = "Invalid password."
            has_admins = bool(creds.get('admins'))
            return render_template('admin_login.html', credentials_exist=has_admins, institute_name=institute or creds.get('institute_name', 'INSTITUTE'), error_text=error_text)

        has_admins = bool(creds.get('admins'))
        return render_template('admin_login.html', credentials_exist=has_admins, institute_name=institute or creds.get('institute_name', 'INSTITUTE'), error_text="Invalid action.")
    except Exception as e:
        # Failsafe for debugging
        import traceback
        traceback.print_exc()
        return f"Server Error (Admin Login): {str(e)}", 500


@app.route('/admin/logout')
def admin_logout():
    session.pop('admin', None)
    return redirect(url_for('admin_login'))












    

@app.route('/api/buses', methods=['GET'])
def get_all_buses():
    buses = safe_load_json(BUSES_FILE, {})
    return jsonify(buses)

# Removed duplicate /simulator route to avoid handler override


@app.route('/api/buses/clear', methods=['POST'])
def clear_all_buses():
    # Clear all bus entries by resetting the JSON file
    safe_save_json(BUSES_FILE, {})
    # Broadcast clear event
    try:
        broadcast({ 'type': 'buses_clear' })
    except Exception:
        pass
    return jsonify({'status': 'success'})

@app.route('/api/locations', methods=['GET'])
def get_locations():
    locations = safe_load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    return jsonify(locations)

@app.route('/api/hostels', methods=['GET'])
def get_hostels():
    locations = safe_load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    return jsonify(locations.get('hostels', []))

@app.route('/api/classes', methods=['GET'])
def get_classes():
    locations = safe_load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    return jsonify(locations.get('classes', []))

@app.route('/api/routes', methods=['GET'])
def get_routes():
    locations = safe_load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    return jsonify(locations.get('routes', []))

@app.route('/api/route', methods=['POST'])
def create_route():
    data = request.json
    locations = safe_load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    
    route = {
        'id': data.get('id', f"route_{int(len(locations.get('routes', [])) + 1)}"),
        'name': data['name'],
        'waypoints': data['waypoints'],
        'stops': data.get('stops', []),  # Add stop names
        'color': data.get('color', '#FF5722')
    }
    
    # Check if updating existing route
    routes = locations.get('routes', [])
    existing_idx = next((i for i, r in enumerate(routes) if r['id'] == route['id']), -1)
    
    if existing_idx >= 0:
        routes[existing_idx] = route
    else:
        routes.append(route)
    
    locations['routes'] = routes
    
    safe_save_json(LOCATIONS_FILE, locations)
    
    return jsonify({'status': 'success', 'route': route})

@app.route('/api/route/<route_id>', methods=['DELETE'])
def delete_route(route_id):
    locations = safe_load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    
    routes = locations.get('routes', [])
    locations['routes'] = [r for r in routes if r['id'] != route_id]
    
    safe_save_json(LOCATIONS_FILE, locations)
    
    return jsonify({'status': 'success'})

@app.route('/api/bus/<int:bus_number>', methods=['POST'])
def update_bus_location(bus_number):
    # Accept JSON or form; validate required fields
    raw = request.get_json(silent=True) or request.form.to_dict() or {}
    try:
        lat = float(raw.get('lat'))
        lng = float(raw.get('lng'))
    except (TypeError, ValueError):
        return jsonify({'error': 'Provide numeric lat and lng'}), 400
    last_update = raw.get('lastUpdate') or time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

    buses = safe_load_json(BUSES_FILE, {})
    # Preserve existing route assignment when updating location
    existing = buses.get(str(bus_number), {})
    route_id = raw.get('routeId', existing.get('routeId'))

    buses[str(bus_number)] = {
        'lat': lat,
        'lng': lng,
        'lastUpdate': last_update,
        'routeId': route_id
    }
    
    safe_save_json(BUSES_FILE, buses)
    # Broadcast bus update
    try:
        broadcast({ 'type': 'bus_update', 'bus': str(bus_number), 'data': buses.get(str(bus_number), {}) })
    except Exception:
        pass
    return jsonify({'status': 'success', 'bus': bus_number})

@app.route('/api/bus/<int:bus_number>', methods=['DELETE'])
def stop_bus(bus_number):
    buses = safe_load_json(BUSES_FILE, {})
    
    if str(bus_number) in buses:
        del buses[str(bus_number)]
    
    safe_save_json(BUSES_FILE, buses)
    # Broadcast bus stop
    try:
        broadcast({ 'type': 'bus_stop', 'bus': str(bus_number) })
    except Exception:
        pass
    return jsonify({'status': 'success'})

@app.route('/api/hostel', methods=['POST'])
def create_hostel():
    data = request.json
    locations = safe_load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    
    hostel = {
        'id': f"hostel_{len(locations.get('hostels', [])) + 1}",
        'name': data['name'],
        'lat': data['lat'],
        'lng': data['lng'],
        'capacity': data.get('capacity', 100)
    }
    
    locations['hostels'].append(hostel)
    
    safe_save_json(LOCATIONS_FILE, locations)
    
    return jsonify({'status': 'success', 'hostel': hostel})

@app.route('/api/hostel/<hostel_id>', methods=['DELETE'])
def delete_hostel(hostel_id):
    locations = safe_load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    
    locations['hostels'] = [h for h in locations.get('hostels', []) if h['id'] != hostel_id]
    
    safe_save_json(LOCATIONS_FILE, locations)
    
    return jsonify({'status': 'success'})

@app.route('/api/class', methods=['POST'])
def create_class():
    data = request.json
    locations = safe_load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    
    cls = {
        'id': f"class_{len(locations.get('classes', [])) + 1}",
        'name': data['name'],
        'lat': data['lat'],
        'lng': data['lng'],
        'department': data.get('department', 'Unknown')
    }
    
    locations['classes'].append(cls)
    
    safe_save_json(LOCATIONS_FILE, locations)
    
    return jsonify({'status': 'success', 'class': cls})

@app.route('/api/class/<class_id>', methods=['DELETE'])
def delete_class(class_id):
    locations = safe_load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    
    locations['classes'] = [c for c in locations.get('classes', []) if c['id'] != class_id]
    
    safe_save_json(LOCATIONS_FILE, locations)
    
    return jsonify({'status': 'success'})

@app.route('/api/bus/<int:bus_number>/route', methods=['POST'])
def set_bus_route(bus_number):
    data = request.get_json(silent=True) or {}
    route_id = data.get('routeId')
    
    buses = safe_load_json(BUSES_FILE, {})
    
    if str(bus_number) in buses:
        buses[str(bus_number)]['routeId'] = route_id
    else:
        buses[str(bus_number)] = {
            'lat': 0,
            'lng': 0,
            'lastUpdate': '',
            'routeId': route_id
        }
    
    safe_save_json(BUSES_FILE, buses)
    # Broadcast route assignment
    try:
        broadcast({ 'type': 'route_set', 'bus': str(bus_number), 'routeId': route_id })
    except Exception:
        pass
    return jsonify({'status': 'success'})

@app.route('/api/bus-routes', methods=['GET'])
def get_bus_routes():
    buses = safe_load_json(BUSES_FILE, {})
    
    result = {}
    for bus_num, bus_info in buses.items():
        result[bus_num] = bus_info.get('routeId', None)
    
    return jsonify(result)

# Health and status endpoints for debugging in production
@app.route('/healthz')
def healthz():
    try:
        buses = safe_load_json(BUSES_FILE, {})
        locs = safe_load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
        ok = isinstance(buses, dict) and isinstance(locs, dict)
        return jsonify({
            'status': 'ok' if ok else 'degraded',
            'uptime_sec': int(time.time() - APP_START_TS),
            'sse_clients': len(_subscribers),
            'buses_count': len(buses),
            'routes_count': len(locs.get('routes', [])),
        }), 200 if ok else 503
    except Exception:
        return jsonify({ 'status': 'error' }), 500

@app.route('/status')
def status():
    buses = safe_load_json(BUSES_FILE, {})
    locs = safe_load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    return jsonify({
        'uptime_sec': int(time.time() - APP_START_TS),
        'requests_total': REQUESTS_TOTAL,
        'sse_clients': len(_subscribers),
        'buses_count': len(buses),
        'routes_count': len(locs.get('routes', [])),
        'on_render': ON_RENDER,
        'heartbeat_sec': max(5, int(os.environ.get('SSE_HEARTBEAT_SEC', '20'))),
        'disable_sse': os.environ.get('DISABLE_SSE', '').lower() in ('1','true','yes')
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))