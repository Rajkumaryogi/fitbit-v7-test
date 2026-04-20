"""WSGI entry for Gunicorn: gunicorn -c gunicorn.conf.py wsgi:application"""
from app import create_app

application = create_app()
