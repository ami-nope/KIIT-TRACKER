import os

workers = 1
worker_class = 'gthread'
threads = 4
timeout = 120
graceful_timeout = 30
bind = '0.0.0.0:' + os.environ.get('PORT', '10000')
