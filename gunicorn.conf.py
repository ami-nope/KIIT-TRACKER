import os

workers = 1
worker_class = 'gevent'
timeout = 120
graceful_timeout = 30
try:
    port = int(os.environ.get('PORT', '10000'))
except (TypeError, ValueError):
    port = 10000
bind = f'0.0.0.0:{port}'
