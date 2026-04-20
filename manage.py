"""Run the Fitbit Flask app. Set PYTHONDONTWRITEBYTECODE=1 to avoid writing .pyc files."""
import os

# Avoid __pycache__/*.pyc churn in the repo (bytecode is still compiled in memory).
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

from app import create_app, db
import sys

app = create_app()

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'createdb':
        with app.app_context():
            db.create_all()
        print("Database created!")
    else:
        if os.getenv('FLASK_ENV', '').lower() == 'production':
            print(
                'WARNING: FLASK_ENV=production but using the Flask development server. '
                'Run: gunicorn -c gunicorn.conf.py wsgi:application'
            )
        host = os.getenv('HOST', '127.0.0.1')
        port = int(os.getenv('PORT', '3000'))
        app.run(
            host=host,
            port=port,
            debug=app.config.get('DEBUG', False),
            use_reloader=app.config.get('DEBUG', False),
        )
