import logging
import os

from flask import Flask, jsonify, request, has_request_context
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from werkzeug.exceptions import HTTPException
from werkzeug.middleware.proxy_fix import ProxyFix

from app.config import DevelopmentConfig, ProductionConfig

logger = logging.getLogger(__name__)
db = SQLAlchemy()


def create_app():
    app = Flask(__name__, static_folder=None)
    env = os.getenv("FLASK_ENV", "development")
    if env == "production":
        app.config.from_object(ProductionConfig)
    else:
        app.config.from_object(DevelopmentConfig)

    secret = app.config.get("SECRET_KEY")
    if not secret:
        raise RuntimeError(
            "SECRET_KEY is missing in environment. Set SECRET_KEY in .env (do NOT rely on a random value)."
        )

    if env == "production":
        for key in ("FITBIT_CLIENT_ID", "FITBIT_CLIENT_SECRET", "REDIRECT_URI"):
            if not app.config.get(key):
                raise RuntimeError(f"{key} must be set when FLASK_ENV=production")

    if app.config.get("TRUST_PROXY_HEADERS"):
        app.wsgi_app = ProxyFix(
            app.wsgi_app,
            x_for=1,
            x_proto=1,
            x_host=1,
            x_port=1,
            x_prefix=1,
        )

    origins = list(app.config.get("ALLOWED_ORIGINS") or [])
    if env == "production" and not origins:
        raise RuntimeError(
            "ALLOWED_ORIGINS must be set in production (comma-separated), e.g. https://app.example.com"
        )
    if not origins:
        origins = ["http://localhost:5173", "http://127.0.0.1:5173"]

    cors_resources = {
        r"/api/*": {"origins": origins},
        r"/login": {"origins": origins},
        r"/callback": {"origins": origins},
    }
    if app.config.get("ENABLE_DOCS"):
        cors_resources[r"/docs"] = {"origins": origins}
    if app.config.get("ENABLE_DEBUG_DB_VIEWS"):
        cors_resources[r"/debug/*"] = {"origins": origins}

    CORS(app, resources=cors_resources, supports_credentials=True)

    db.init_app(app)

    from .routes import bp as main_bp

    app.register_blueprint(main_bp)

    with app.app_context():
        db.create_all()

    @app.errorhandler(Exception)
    def handle_unhandled_exception(exc):
        if isinstance(exc, HTTPException):
            return exc.get_response()
        logger.exception("Unhandled exception: %s", exc)
        if app.debug:
            raise
        if has_request_context() and request.path.startswith("/api/"):
            return jsonify({"error": "Internal server error"}), 500
        from werkzeug.exceptions import InternalServerError

        return InternalServerError().get_response()

    logger.info(
        "Fitbit polling auto-sync removed — use Subscription webhooks (/api/fitbit/webhook), "
        "POST /api/fitbit/sync, or a scheduler."
    )

    @app.after_request
    def _security_headers(response):
        if app.config.get("ENV") == "production":
            response.headers.setdefault("X-Content-Type-Options", "nosniff")
            response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
            response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        return response

    return app
