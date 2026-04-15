import sys
sys.path.insert(0, '/opt/autopipe')
from src.utils.database import Database

db = Database({
    'host': 'pipeline-db',
    'port': 5432,
    'name': 'job_search_autopipe',
    'user': 'autopipe',
    'password': 'autopipe_password',
})
db.initialize_schema()
print('Schema initialized successfully!')
