# gevent monkey-patching must be first — makes threading/queue/time cooperative
try:
    from gevent import monkey
    monkey.patch_all()
except ImportError:
    pass  # Allow running without gevent (localhost dev)

from flask import Flask, render_template, request, jsonify, session, redirect, url_for, Response, stream_with_context, has_request_context
from flask_cors import CORS
import json
import os
import sys
import math
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.middleware.proxy_fix import ProxyFix
from datetime import datetime, timezone
import threading
import queue
import time

# ---------- 1-D Kalman filter for GPS smoothing ----------
class GPSKalman:
    """Lightweight 1-D Kalman per axis. ~2 multiplications per update."""
    __slots__ = ('x', 'p', 'q', 'r')
    def __init__(self, process_noise=0.00001, measurement_noise=0.00005):
        self.x = None   # estimate
        self.p = 1.0     # error covariance
        self.q = process_noise
        self.r = measurement_noise
    def update(self, measurement):
        if self.x is None:
            self.x = measurement
            return measurement
        self.p += self.q
        k = self.p / (self.p + self.r)
        self.x += k * (measurement - self.x)
        self.p *= (1 - k)
        return self.x

_kalman_filters = {}  # bus_id -> {'lat': GPSKalman, 'lng': GPSKalman}

def kalman_smooth(bus_id, lat, lng):
    if bus_id not in _kalman_filters:
        _kalman_filters[bus_id] = {'lat': GPSKalman(), 'lng': GPSKalman()}
    kf = _kalman_filters[bus_id]
    return kf['lat'].update(lat), kf['lng'].update(lng)

# ---------- server-side stop detection ----------
def _haversine_m(lat1, lng1, lat2, lng2):
    """Fast equirectangular distance in meters — accurate at campus scale."""
    d2r = math.pi / 180
    dlat = (lat2 - lat1) * d2r
    dlng = (lng2 - lng1) * d2r
    x = dlng * math.cos((lat1 + lat2) * 0.5 * d2r)
    return 6371000 * math.sqrt(dlat * dlat + x * x)

_AT_STOP_M = 80  # meters — "at stop" threshold
_bus_stop_state = {}  # bus_id -> { 'atStop': str|None, 'nearestStopIdx': int, 'direction': 'up'|'down'|None }

def detect_stop_info(bus_id, lat, lng, route_id):
    """Detect nearest stop, at-stop, direction. Returns dict to merge into broadcast."""
    result = {
        'atStop': None,
        'nearestStopIdx': None,
        'nearestStopName': None,
        'nextStopName': None,
        'direction': None,
        'terminalState': None,
        'routeStopCount': None
    }
    if not route_id:
        return result
    locs = load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    route = None
    for r in locs.get('routes', []):
        if str(r.get('id')) == str(route_id):
            route = r
            break
    if not route or not route.get('waypoints') or len(route['waypoints']) < 2:
        return result
    wps = route['waypoints']
    result['routeStopCount'] = len(wps)
    stops = route.get('stops', [])
    best_idx, best_d = 0, float('inf')
    for i, wp in enumerate(wps):
        d = _haversine_m(lat, lng, wp[0], wp[1])
        if d < best_d:
            best_d = d
            best_idx = i
    result['nearestStopIdx'] = best_idx
    stop_name = stops[best_idx] if best_idx < len(stops) and stops[best_idx] else f'Stop {best_idx + 1}'
    result['nearestStopName'] = stop_name
    # Determine next stop index and name
    next_idx = best_idx + 1 if best_idx + 1 < len(stops) else best_idx
    result['nextStopName'] = stops[next_idx] if next_idx < len(stops) and stops[next_idx] else f'Stop {next_idx + 1}' if next_idx != best_idx else None
    if best_d <= _AT_STOP_M:
        result['atStop'] = stop_name
    # Direction
    prev = _bus_stop_state.get(bus_id, {})
    prev_idx = prev.get('nearestStopIdx')
    direction = prev.get('direction')
    if prev_idx is not None and prev_idx != best_idx:
        direction = 'down' if best_idx > prev_idx else 'up'
    result['direction'] = direction
    # Terminal state mirrors student-side logic so server can enforce cleanup.
    if result['atStop']:
        last_idx = len(wps) - 1
        if (direction == 'down' and best_idx == last_idx) or (direction == 'up' and best_idx == 0):
            result['terminalState'] = 'at_destination'
        elif (direction == 'down' and best_idx == 0) or (direction == 'up' and best_idx == last_idx):
            result['terminalState'] = 'at_start'
    _bus_stop_state[bus_id] = {
        'nearestStopIdx': best_idx,
        'direction': direction,
        'atStop': result['atStop'],
        'nearestStopName': result['nearestStopName'],
        'nextStopName': result['nextStopName'],
        'terminalState': result['terminalState']
    }
    return result

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
AUDIT_FILE = os.path.join(BASE_DIR, 'admin_audit.json')

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
    if not os.path.exists(AUDIT_FILE):
        save_json(AUDIT_FILE, [])

def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

def _client_ip():
    if not has_request_context():
        return 'system'
    try:
        xff = request.headers.get('X-Forwarded-For', '')
        if xff:
            return xff.split(',')[0].strip()
    except Exception:
        pass
    try:
        return request.remote_addr or 'unknown'
    except Exception:
        return 'unknown'

_audit_lock = threading.Lock()
_audit_logs = []
AUDIT_RETENTION_SEC = 4 * 24 * 60 * 60
AUDIT_MAX_ITEMS = 2000

def prune_audit_logs(logs, now_epoch=None):
    if not isinstance(logs, list):
        return []
    now_ts = now_epoch if now_epoch is not None else time.time()
    cutoff = now_ts - AUDIT_RETENTION_SEC
    kept = []
    for entry in logs:
        if not isinstance(entry, dict):
            continue
        ts = parse_iso_timestamp(entry.get('ts'))
        if ts is None:
            continue
        if ts >= cutoff:
            kept.append(entry)
    if len(kept) > AUDIT_MAX_ITEMS:
        kept = kept[-AUDIT_MAX_ITEMS:]
    return kept

def record_audit(event, status='success', username=None, details=''):
    """Persist lightweight admin audit entries for observability in admin panel."""
    try:
        actor = username
        if not actor and has_request_context():
            actor = session.get('admin')
        ua = ''
        if has_request_context() and request and request.user_agent:
            ua = request.user_agent.string or ''
        entry = {
            'ts': _utc_now_iso(),
            'event': str(event or '').strip() or 'unknown',
            'status': str(status or 'success'),
            'username': actor or 'anonymous',
            'ip': _client_ip(),
            'details': str(details or '')[:240],
            'ua': ua[:180]
        }
        with _audit_lock:
            _audit_logs[:] = prune_audit_logs(_audit_logs)
            _audit_logs.append(entry)
            _audit_logs[:] = prune_audit_logs(_audit_logs)
            save_json(AUDIT_FILE, _audit_logs)
    except Exception:
        pass

def get_process_rss_mb():
    rss_bytes = None
    try:
        import resource
        raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if raw:
            # Linux reports KB, macOS reports bytes.
            rss_bytes = int(raw) if sys.platform == 'darwin' else int(raw) * 1024
    except Exception:
        pass
    if rss_bytes is None:
        try:
            with open('/proc/self/status', 'r') as f:
                for line in f:
                    if line.startswith('VmRSS:'):
                        parts = line.split()
                        if len(parts) >= 2:
                            rss_bytes = int(parts[1]) * 1024  # kB -> bytes
                        break
        except Exception:
            pass
    if not rss_bytes:
        try:
            if sys.platform.startswith('win'):
                import ctypes
                class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                    _fields_ = [
                        ('cb', ctypes.c_uint32),
                        ('PageFaultCount', ctypes.c_uint32),
                        ('PeakWorkingSetSize', ctypes.c_size_t),
                        ('WorkingSetSize', ctypes.c_size_t),
                        ('QuotaPeakPagedPoolUsage', ctypes.c_size_t),
                        ('QuotaPagedPoolUsage', ctypes.c_size_t),
                        ('QuotaPeakNonPagedPoolUsage', ctypes.c_size_t),
                        ('QuotaNonPagedPoolUsage', ctypes.c_size_t),
                        ('PagefileUsage', ctypes.c_size_t),
                        ('PeakPagefileUsage', ctypes.c_size_t),
                    ]
                counters = PROCESS_MEMORY_COUNTERS()
                counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
                proc = ctypes.windll.kernel32.GetCurrentProcess()
                if ctypes.windll.psapi.GetProcessMemoryInfo(proc, ctypes.byref(counters), counters.cb):
                    rss_bytes = int(counters.WorkingSetSize)
        except Exception:
            pass
    if not rss_bytes:
        return None
    return round(rss_bytes / (1024 * 1024), 2)

def get_system_memory_stats():
    total_mb = None
    available_mb = None
    if sys.platform.startswith('win'):
        try:
            import ctypes
            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ('dwLength', ctypes.c_uint32),
                    ('dwMemoryLoad', ctypes.c_uint32),
                    ('ullTotalPhys', ctypes.c_uint64),
                    ('ullAvailPhys', ctypes.c_uint64),
                    ('ullTotalPageFile', ctypes.c_uint64),
                    ('ullAvailPageFile', ctypes.c_uint64),
                    ('ullTotalVirtual', ctypes.c_uint64),
                    ('ullAvailVirtual', ctypes.c_uint64),
                    ('ullAvailExtendedVirtual', ctypes.c_uint64),
                ]
            stat = MEMORYSTATUSEX()
            stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
                total_mb = round(int(stat.ullTotalPhys) / (1024 * 1024), 2)
                available_mb = round(int(stat.ullAvailPhys) / (1024 * 1024), 2)
        except Exception:
            pass
    else:
        try:
            mem_total_kb = None
            mem_available_kb = None
            with open('/proc/meminfo', 'r') as f:
                for line in f:
                    if line.startswith('MemTotal:'):
                        parts = line.split()
                        if len(parts) >= 2:
                            mem_total_kb = int(parts[1])
                    elif line.startswith('MemAvailable:'):
                        parts = line.split()
                        if len(parts) >= 2:
                            mem_available_kb = int(parts[1])
                    if mem_total_kb is not None and mem_available_kb is not None:
                        break
            if mem_total_kb is not None:
                total_mb = round(mem_total_kb / 1024, 2)
            if mem_available_kb is not None:
                available_mb = round(mem_available_kb / 1024, 2)
        except Exception:
            pass
    if total_mb is None:
        try:
            page_size = os.sysconf('SC_PAGE_SIZE')
            page_count = os.sysconf('SC_PHYS_PAGES')
            if page_size and page_count:
                total_mb = round((page_size * page_count) / (1024 * 1024), 2)
        except Exception:
            pass
    used_mb = None
    if total_mb is not None and available_mb is not None:
        used_mb = round(max(0.0, total_mb - available_mb), 2)
    return {
        'total_mb': total_mb,
        'available_mb': available_mb,
        'used_mb': used_mb
    }

_cpu_sample_lock = threading.Lock()
_cpu_last_wall = time.monotonic()
_cpu_last_proc = time.process_time()
_cpu_last_percent = 0.0

def get_process_cpu_percent():
    global _cpu_last_wall, _cpu_last_proc, _cpu_last_percent
    now_wall = time.monotonic()
    now_proc = time.process_time()
    with _cpu_sample_lock:
        dt_wall = now_wall - _cpu_last_wall
        dt_proc = now_proc - _cpu_last_proc
        _cpu_last_wall = now_wall
        _cpu_last_proc = now_proc
        if dt_wall <= 0:
            return round(_cpu_last_percent, 2)
        raw_percent = max(0.0, (dt_proc / dt_wall) * 100.0)
        cpu_cap = float(max(1, os.cpu_count() or 1) * 100)
        raw_percent = min(cpu_cap, raw_percent)
        _cpu_last_percent = (_cpu_last_percent * 0.6) + (raw_percent * 0.4)
        return round(_cpu_last_percent, 2)

def get_process_cpu_stats():
    return {
        'process_percent': get_process_cpu_percent(),
        'cores': max(1, os.cpu_count() or 1)
    }

def parse_iso_timestamp(value):
    """Parse ISO timestamp into epoch seconds; returns None on invalid."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp()
    except Exception:
        return None

# ---------- in-memory caches ----------
_buses = {}
_buses_lock = threading.Lock()
_worker_started = False

APP_START_TS = time.time()
REQUESTS_TOTAL = 0
BANDWIDTH_IN_BYTES = 0
BANDWIDTH_OUT_BYTES = 0
INACTIVE_REMOVE_SEC = 30
DESTINATION_REMOVE_SEC = 5
_bus_destination_ts = {}  # bus_id -> monotonic timestamp when destination reached

def _init_app():
    global _buses, _worker_started, _audit_logs
    ensure_files()
    raw = load_json(BUSES_FILE, {})
    logs = load_json(AUDIT_FILE, [])
    _audit_logs = prune_audit_logs(logs if isinstance(logs, list) else [])
    if _audit_logs != (logs if isinstance(logs, list) else []):
        save_json(AUDIT_FILE, _audit_logs)
    # Filter out stale buses on startup (older than INACTIVE_REMOVE_SEC)
    now = time.time()
    cleaned = {}
    for k, v in raw.items():
        try:
            ts = parse_iso_timestamp(v.get('lastUpdate'))
            if ts is not None and (now - ts) <= INACTIVE_REMOVE_SEC:
                cleaned[k] = v
        except Exception:
            pass  # drop invalid entries
    _buses = cleaned
    if cleaned != raw:
        save_json(BUSES_FILE, cleaned)
    if not _worker_started:
        _worker_started = True
        threading.Thread(target=_sync_worker, daemon=True).start()

@app.before_request
def _before():
    global REQUESTS_TOTAL, BANDWIDTH_IN_BYTES
    if not hasattr(app, '_ready'):
        _init_app()
        app._ready = True
    REQUESTS_TOTAL += 1
    try:
        in_len = request.content_length
        if in_len is None:
            in_len = int(request.headers.get('Content-Length', '0') or 0)
        if in_len and in_len > 0:
            BANDWIDTH_IN_BYTES += int(in_len)
    except Exception:
        pass

@app.after_request
def _after(resp):
    resp.headers.setdefault('Permissions-Policy', 'gamepad=(self)')
    global BANDWIDTH_OUT_BYTES
    try:
        out_len = resp.calculate_content_length()
        if out_len is None and not resp.is_streamed:
            body = resp.get_data(as_text=False)
            out_len = len(body) if body is not None else 0
        if out_len and out_len > 0:
            BANDWIDTH_OUT_BYTES += int(out_len)
    except Exception:
        pass
    return resp

def _auto_cleanup_buses():
    now_epoch = time.time()
    now_mono = time.monotonic()
    removed = []
    with _buses_lock:
        for bus_id, data in list(_buses.items()):
            reason = None
            reached_ts = _bus_destination_ts.get(bus_id)
            if reached_ts is not None and (now_mono - reached_ts) >= DESTINATION_REMOVE_SEC:
                reason = 'destination_timeout'
            else:
                last_ts = parse_iso_timestamp((data or {}).get('lastUpdate'))
                if last_ts is None or (now_epoch - last_ts) >= INACTIVE_REMOVE_SEC:
                    reason = 'inactivity_timeout'
            if not reason:
                continue
            removed_data = _buses.pop(bus_id, None)
            if not removed_data:
                continue
            removed.append((str(bus_id), removed_data.get('routeId'), reason))
            _kalman_filters.pop(str(bus_id), None)
            _bus_stop_state.pop(str(bus_id), None)
            _bus_last_broadcast.pop(str(bus_id), None)
            _bus_destination_ts.pop(str(bus_id), None)
        # Drop dangling destination timers.
        for bus_id in list(_bus_destination_ts.keys()):
            if bus_id not in _buses:
                _bus_destination_ts.pop(bus_id, None)
    for bus_id, route_id, reason in removed:
        record_audit('bus_auto_remove', status='success', username='system', details=f'bus={bus_id} reason={reason}')
        try:
            broadcast({'type': 'bus_stop', 'bus': bus_id, 'routeId': route_id, 'reason': reason})
        except Exception:
            pass

def _sync_worker():
    last = None
    while True:
        time.sleep(1)
        try:
            _auto_cleanup_buses()
            with _buses_lock:
                snap = {k: dict(v) for k, v in _buses.items()}
            snap_str = json.dumps(snap, sort_keys=True)
            if snap_str != last:
                save_json(BUSES_FILE, snap)
                last = snap_str
        except Exception:
            pass

# ---------- SSE ----------
_subscribers_lock = threading.Lock()
_subscribers = {}   # routeId|"all" -> [queue, ...]

def broadcast(payload):
    try:
        data = json.dumps(payload)
    except Exception:
        data = json.dumps({"error": "bad-payload"})
    # Extract routeId for targeted delivery
    route_id = None
    if isinstance(payload, dict):
        route_id = payload.get('routeId')
        if not route_id:
            bus_data = payload.get('data')
            if isinstance(bus_data, dict):
                route_id = bus_data.get('routeId')
    with _subscribers_lock:
        if route_id:
            # Targeted: send to "all" subscribers + matching route subscribers
            sent = set()
            for q in list(_subscribers.get('all', [])):
                sent.add(id(q))
                try:
                    q.put_nowait(data)
                except Exception:
                    pass
            for q in list(_subscribers.get(route_id, [])):
                if id(q) not in sent:
                    try:
                        q.put_nowait(data)
                    except Exception:
                        pass
        else:
            # No route context (buses_clear, etc.) — send to all subscribers
            sent = set()
            for group in _subscribers.values():
                for q in list(group):
                    if id(q) not in sent:
                        sent.add(id(q))
                        try:
                            q.put_nowait(data)
                        except Exception:
                            pass

@app.route('/events')
def sse_events():
    if os.environ.get('DISABLE_SSE', '').lower() in ('1', 'true', 'yes'):
        return Response('SSE disabled', status=503, mimetype='text/plain')
    route_id = request.args.get('routeId') or 'all'
    def stream():
        q = queue.Queue(maxsize=100)
        hb = max(5, int(os.environ.get('SSE_HEARTBEAT_SEC', '20')))
        with _subscribers_lock:
            _subscribers.setdefault(route_id, []).append(q)
        yield 'event: ping\ndata: "connected"\n\n'
        try:
            while True:
                try:
                    msg = q.get(timeout=hb)
                    yield f'data: {msg}\n\n'
                except queue.Empty:
                    yield 'event: ping\ndata: {}\n\n'
        finally:
            with _subscribers_lock:
                try:
                    subs = _subscribers.get(route_id)
                    if subs:
                        subs.remove(q)
                        if not subs:
                            del _subscribers[route_id]
                except (ValueError, KeyError):
                    pass
    return Response(stream_with_context(stream()), mimetype='text/event-stream',
                    headers={
                        'Cache-Control': 'no-cache, no-transform',
                        'X-Accel-Buffering': 'no',
                        'Connection': 'keep-alive',
                        'Content-Type': 'text/event-stream; charset=utf-8',
                        'Transfer-Encoding': 'chunked',
                    })

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
                record_audit('admin_signup', status='failed', username=username or 'anonymous', details='invalid_pin')
            elif not username or not password:
                error_text = "Provide username and password."
                record_audit('admin_signup', status='failed', username=username or 'anonymous', details='missing_credentials')
            elif any(a.get('username') == username for a in creds['admins']):
                error_text = "Admin username already exists."
                record_audit('admin_signup', status='failed', username=username, details='username_exists')
            else:
                creds['institute_name'] = institute or creds.get('institute_name', 'INSTITUTE')
                creds['admins'].append({'username': username, 'password_hash': generate_password_hash(password)})
                save_credentials(creds)
                session['admin'] = username
                record_audit('admin_signup', status='success', username=username, details='signup_created')
                return redirect(url_for('admin_view'))

        elif action == 'login':
            if not creds.get('admins'):
                error_text = "No admin accounts exist. Please signup first."
                record_audit('admin_login', status='failed', username=username or 'anonymous', details='no_admin_accounts')
            else:
                admin = next((a for a in creds['admins'] if a.get('username') == username), None)
                if not admin:
                    error_text = "Invalid username."
                    record_audit('admin_login', status='failed', username=username or 'anonymous', details='invalid_username')
                elif admin.get('password_hash') and check_password_hash(admin['password_hash'], password):
                    session['admin'] = username
                    record_audit('admin_login', status='success', username=username, details='login_ok')
                    return redirect(url_for('admin_view'))
                else:
                    error_text = "Invalid password."
                    record_audit('admin_login', status='failed', username=username or 'anonymous', details='invalid_password')
        else:
            error_text = "Invalid action."
            record_audit('admin_login', status='failed', username=username or 'anonymous', details='invalid_action')

        return render_template('admin_login.html', credentials_exist=bool(creds.get('admins')),
                               institute_name=institute or creds.get('institute_name', 'INSTITUTE'), error_text=error_text)
    except Exception as e:
        import traceback
        traceback.print_exc()
        record_audit('admin_login', status='error', username=(request.form.get('username', '').strip() if request and request.form else 'anonymous'), details='server_error')
        return f"Server Error: {str(e)}", 500

@app.route('/admin/logout')
def admin_logout():
    username = session.get('admin') or 'anonymous'
    session.pop('admin', None)
    record_audit('admin_logout', status='success', username=username, details='logout')
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
        record_audit('admin_add', status='failed', username=session.get('admin') or 'anonymous', details=f'invalid_pin target={username or "-"}')
        return jsonify({'error': 'Invalid pin'}), 400
    if not username or not password:
        record_audit('admin_add', status='failed', username=session.get('admin') or 'anonymous', details='missing_credentials')
        return jsonify({'error': 'Provide username and password'}), 400
    creds = load_credentials()
    if any(a.get('username') == username for a in creds.get('admins', [])):
        record_audit('admin_add', status='failed', username=session.get('admin') or 'anonymous', details=f'username_exists target={username}')
        return jsonify({'error': 'Admin username already exists'}), 400
    creds.setdefault('admins', []).append({'username': username, 'password_hash': generate_password_hash(password)})
    save_credentials(creds)
    record_audit('admin_add', status='success', username=session.get('admin') or 'anonymous', details=f'target={username}')
    return jsonify({'status': 'success', 'username': username})

@app.route('/admin/admins/<username>', methods=['DELETE'])
@login_required
def delete_admin(username):
    actor = session.get('admin') or 'anonymous'
    creds = load_credentials()
    admins = creds.get('admins', [])
    new = [a for a in admins if a.get('username') != username]
    if len(new) == len(admins):
        record_audit('admin_delete', status='failed', username=actor, details=f'not_found target={username}')
        return jsonify({'error': 'Admin not found'}), 404
    creds['admins'] = new
    save_credentials(creds)
    if session.get('admin') == username:
        session.pop('admin', None)
    record_audit('admin_delete', status='success', username=actor, details=f'target={username}')
    return jsonify({'status': 'success'})

@app.route('/admin/admins/<username>/password', methods=['POST'])
@login_required
def change_admin_password(username):
    data = request.json or {}
    new_pw = (data.get('password', '') or '').strip()
    pin = (data.get('pin', '') or '').strip()
    if pin != '456123':
        record_audit('admin_password_change', status='failed', username=session.get('admin') or 'anonymous', details=f'invalid_pin target={username}')
        return jsonify({'error': 'Invalid pin'}), 400
    if not new_pw:
        record_audit('admin_password_change', status='failed', username=session.get('admin') or 'anonymous', details=f'missing_password target={username}')
        return jsonify({'error': 'Provide new password'}), 400
    creds = load_credentials()
    admin = next((a for a in creds.get('admins', []) if a.get('username') == username), None)
    if not admin:
        record_audit('admin_password_change', status='failed', username=session.get('admin') or 'anonymous', details=f'not_found target={username}')
        return jsonify({'error': 'Admin not found'}), 404
    admin['password_hash'] = generate_password_hash(new_pw)
    save_credentials(creds)
    record_audit('admin_password_change', status='success', username=session.get('admin') or 'anonymous', details=f'target={username}')
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

@app.route('/admin/performance', methods=['GET'])
@login_required
def admin_performance():
    locs = load_json(LOCATIONS_FILE, {"hostels": [], "classes": [], "routes": []})
    creds = load_credentials()
    uptime_sec = int(time.time() - APP_START_TS)
    with _buses_lock:
        buses_count = len(_buses)
    with _subscribers_lock:
        sse_clients = sum(len(v) for v in _subscribers.values())
    with _audit_lock:
        pruned_logs = prune_audit_logs(_audit_logs)
        if len(pruned_logs) != len(_audit_logs):
            _audit_logs[:] = pruned_logs
            save_json(AUDIT_FILE, _audit_logs)
        all_logs = list(_audit_logs)
    memory_stats = get_system_memory_stats()
    cpu_stats = get_process_cpu_stats()
    success_events = [e for e in all_logs if e.get('event') in ('admin_login', 'admin_signup') and e.get('status') == 'success']
    failed_events = [e for e in all_logs if e.get('event') in ('admin_login', 'admin_signup') and e.get('status') != 'success']
    recent_audit = list(reversed(all_logs[-120:]))
    uptime_for_rate = max(1, uptime_sec)
    return jsonify({
        'server_time': _utc_now_iso(),
        'uptime_sec': uptime_sec,
        'memory': {
            'process_rss_mb': get_process_rss_mb(),
            'system_total_mb': memory_stats.get('total_mb'),
            'system_used_mb': memory_stats.get('used_mb'),
            'system_available_mb': memory_stats.get('available_mb')
        },
        'cpu': cpu_stats,
        'bandwidth': {
            'in_bytes': BANDWIDTH_IN_BYTES,
            'out_bytes': BANDWIDTH_OUT_BYTES,
            'in_mb': round(BANDWIDTH_IN_BYTES / (1024 * 1024), 2),
            'out_mb': round(BANDWIDTH_OUT_BYTES / (1024 * 1024), 2),
            'avg_in_kbps': round(((BANDWIDTH_IN_BYTES * 8) / 1000) / uptime_for_rate, 2),
            'avg_out_kbps': round(((BANDWIDTH_OUT_BYTES * 8) / 1000) / uptime_for_rate, 2)
        },
        'requests_total': REQUESTS_TOTAL,
        'sse_clients': sse_clients,
        'buses_count': buses_count,
        'routes_count': len(locs.get('routes', [])),
        'hostels_count': len(locs.get('hostels', [])),
        'classes_count': len(locs.get('classes', [])),
        'admin': {
            'current_admin': session.get('admin'),
            'admins_count': len(creds.get('admins', [])),
            'successful_logins': len(success_events),
            'failed_logins': len(failed_events),
            'last_success_login': success_events[-1]['ts'] if success_events else None,
            'last_failed_login': failed_events[-1]['ts'] if failed_events else None
        },
        'audit_logs': recent_audit
    })

@app.route('/admin/performance/export', methods=['GET'])
@login_required
def admin_performance_export():
    export_format = str(request.args.get('format', 'md') or 'md').strip().lower()
    if export_format not in ('md', 'txt'):
        return jsonify({'error': 'format must be one of: md, txt'}), 400
    with _audit_lock:
        _audit_logs[:] = prune_audit_logs(_audit_logs)
        logs = list(_audit_logs)
        save_json(AUDIT_FILE, _audit_logs)
    generated_ts = _utc_now_iso()
    stamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    if export_format == 'md':
        lines = [
            '# Admin Audit Log Export',
            '',
            f'Generated (UTC): {generated_ts}',
            f'Entries: {len(logs)}',
            '',
            '| Time (UTC) | User | Event | Status | IP | Details |',
            '|---|---|---|---|---|---|'
        ]
        for entry in logs:
            ts = str(entry.get('ts') or '--').replace('\n', ' ').replace('\r', ' ')
            user = str(entry.get('username') or '--').replace('\n', ' ').replace('\r', ' ')
            event = str(entry.get('event') or '--').replace('\n', ' ').replace('\r', ' ')
            status = str(entry.get('status') or '--').replace('\n', ' ').replace('\r', ' ')
            ip = str(entry.get('ip') or '--').replace('\n', ' ').replace('\r', ' ')
            details = str(entry.get('details') or '--').replace('\n', ' ').replace('\r', ' ')
            lines.append(
                f'| {ts.replace("|", "\\|")} | {user.replace("|", "\\|")} | '
                f'{event.replace("|", "\\|")} | {status.replace("|", "\\|")} | '
                f'{ip.replace("|", "\\|")} | {details.replace("|", "\\|")} |'
            )
        body = '\n'.join(lines) + '\n'
        mimetype = 'text/markdown'
        ext = 'md'
    else:
        lines = [
            'Admin Audit Log Export',
            f'Generated (UTC): {generated_ts}',
            f'Entries: {len(logs)}',
            ''
        ]
        for entry in logs:
            ts = str(entry.get('ts') or '--').replace('\n', ' ').replace('\r', ' ')
            user = str(entry.get('username') or '--').replace('\n', ' ').replace('\r', ' ')
            event = str(entry.get('event') or '--').replace('\n', ' ').replace('\r', ' ')
            status = str(entry.get('status') or '--').replace('\n', ' ').replace('\r', ' ')
            ip = str(entry.get('ip') or '--').replace('\n', ' ').replace('\r', ' ')
            details = str(entry.get('details') or '--').replace('\n', ' ').replace('\r', ' ')
            lines.append(f'{ts} | {user} | {event} | {status} | {ip} | {details}')
        body = '\n'.join(lines) + '\n'
        mimetype = 'text/plain'
        ext = 'txt'
    resp = Response(body, mimetype=mimetype)
    resp.headers['Content-Disposition'] = f'attachment; filename=\"admin-audit-{stamp}.{ext}\"'
    return resp

# ---------- bus APIs ----------
@app.route('/api/buses', methods=['GET'])
def get_all_buses():
    # Enrich with latest stop state for polling fallback
    with _buses_lock:
        result = {}
        for bus_id, data in _buses.items():
            entry = dict(data)
            ss = _bus_stop_state.get(bus_id, {})
            entry['atStop'] = ss.get('atStop')
            entry['nearestStopName'] = ss.get('nearestStopName') if 'nearestStopName' not in entry else entry['nearestStopName']
            entry['direction'] = ss.get('direction')
            entry['terminalState'] = ss.get('terminalState')
            result[bus_id] = entry
    return jsonify(result)

@app.route('/api/buses/clear', methods=['POST'])
def clear_all_buses():
    global _buses
    removed_ids = []
    with _buses_lock:
        removed_ids = list(_buses.keys())
        _buses = {}
        _bus_destination_ts.clear()
        _bus_last_broadcast.clear()
        _bus_stop_state.clear()
        _kalman_filters.clear()
    save_json(BUSES_FILE, {})
    try:
        broadcast({'type': 'buses_clear'})
    except Exception:
        pass
    actor = session.get('admin') if has_request_context() else 'system'
    preview = ','.join(removed_ids[:10])
    if len(removed_ids) > 10:
        preview += ',...'
    record_audit(
        'Deleted all transports',
        status='success',
        username=actor or 'anonymous',
        details=f'count={len(removed_ids)} ids={preview or "-"}'
    )
    return jsonify({'status': 'success'})

_bus_last_broadcast = {}  # bus_id -> monotonic timestamp of last broadcast

@app.route('/api/bus/<int:bus_number>', methods=['POST'])
def update_bus_location(bus_number):
    raw = request.get_json(silent=True) or request.form.to_dict() or {}
    try:
        raw_lat = float(raw.get('lat'))
        raw_lng = float(raw.get('lng'))
    except (TypeError, ValueError):
        return jsonify({'error': 'Provide numeric lat and lng'}), 400
    # Kalman smoothing
    bus_id = str(bus_number)
    lat, lng = kalman_smooth(bus_id, raw_lat, raw_lng)
    parsed_last_update = parse_iso_timestamp(raw.get('lastUpdate'))
    if parsed_last_update is None:
        last_update = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    else:
        last_update = datetime.fromtimestamp(parsed_last_update, timezone.utc).isoformat().replace('+00:00', 'Z')
    heading = raw.get('heading')  # device heading from driver/simulator
    should_broadcast = True
    now_mono = time.monotonic()
    with _buses_lock:
        existing = _buses.get(bus_id, {})
        route_id = raw.get('routeId', existing.get('routeId'))
        if existing.get('routeId') != route_id:
            _bus_destination_ts.pop(bus_id, None)
        # Skip broadcast if position hasn't meaningfully changed (~0.5m)
        # Always broadcast at least every 3s even when stationary (heartbeat)
        if existing and 'lat' in existing and 'lng' in existing:
            dlat = abs(lat - existing['lat'])
            dlng = abs(lng - existing['lng'])
            if dlat < 0.000005 and dlng < 0.000005:
                last_bc = _bus_last_broadcast.get(bus_id, 0)
                if (now_mono - last_bc) < 3:
                    should_broadcast = False
        entry = {'lat': lat, 'lng': lng, 'lastUpdate': last_update, 'routeId': route_id}
        if heading is not None:
            try:
                entry['heading'] = float(heading)
            except (TypeError, ValueError):
                pass
        _buses[bus_id] = entry
        current_data = dict(entry)
    # Server-side stop detection — enrich broadcast payload
    stop_info = detect_stop_info(bus_id, lat, lng, route_id)
    current_data.update(stop_info)
    with _buses_lock:
        if stop_info.get('terminalState') == 'at_destination':
            _bus_destination_ts.setdefault(bus_id, now_mono)
            current_data['removeInSec'] = max(0, round(DESTINATION_REMOVE_SEC - (now_mono - _bus_destination_ts[bus_id]), 2))
        else:
            _bus_destination_ts.pop(bus_id, None)
    try:
        if should_broadcast:
            _bus_last_broadcast[bus_id] = now_mono
            broadcast({'type': 'bus_update', 'bus': bus_id, 'data': current_data})
    except Exception:
        pass
    return jsonify({'status': 'success', 'bus': bus_number})

@app.route('/api/bus/<int:bus_number>', methods=['DELETE'])
def stop_bus(bus_number):
    bus_id = str(bus_number)
    with _buses_lock:
        removed = _buses.pop(bus_id, None)
    route_id = removed.get('routeId') if removed else None
    # Clean up Kalman + stop state
    _kalman_filters.pop(bus_id, None)
    _bus_stop_state.pop(bus_id, None)
    _bus_last_broadcast.pop(bus_id, None)
    _bus_destination_ts.pop(bus_id, None)
    try:
        broadcast({'type': 'bus_stop', 'bus': bus_id, 'routeId': route_id})
    except Exception:
        pass
    actor = session.get('admin') if has_request_context() else 'system'
    if removed:
        record_audit(
            'Deleted transport',
            status='success',
            username=actor or 'anonymous',
            details=f'bus={bus_id} route={route_id or "-"} lat={removed.get("lat")} lng={removed.get("lng")}'
        )
    else:
        record_audit(
            'Deleted transport',
            status='failed',
            username=actor or 'anonymous',
            details=f'bus={bus_id} reason=not_found'
        )
    return jsonify({'status': 'success'})

@app.route('/api/bus/<int:bus_number>/route', methods=['POST'])
def set_bus_route(bus_number):
    data = request.get_json(silent=True) or {}
    route_id = data.get('routeId')
    bus_id = str(bus_number)
    with _buses_lock:
        if bus_id in _buses:
            _buses[bus_id]['routeId'] = route_id
            _bus_destination_ts.pop(bus_id, None)
        # Don't create a bus entry just for route assignment
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
        'sse_clients': sum(len(v) for v in _subscribers.values()),
        'buses_count': len(_buses),
        'routes_count': len(locs.get('routes', [])),
        'on_render': ON_RENDER,
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
