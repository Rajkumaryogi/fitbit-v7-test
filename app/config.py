import os
from dotenv import load_dotenv

load_dotenv()


def _truthy(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).lower() in ("1", "true", "yes", "on")


def _origins_list() -> list:
    raw = os.getenv("ALLOWED_ORIGINS", "")
    return [o.strip() for o in raw.split(",") if o.strip()]


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY")
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///fitbit_data.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    FITBIT_CLIENT_ID = os.getenv("FITBIT_CLIENT_ID")
    FITBIT_CLIENT_SECRET = os.getenv("FITBIT_CLIENT_SECRET")
    REDIRECT_URI = os.getenv("REDIRECT_URI")
    OAUTH2_AUTHORIZE_URL = os.getenv("OAUTH2_AUTHORIZE_URL", "https://www.fitbit.com/oauth2/authorize")
    OAUTH2_TOKEN_URL = os.getenv("OAUTH2_TOKEN_URL", "https://api.fitbit.com/oauth2/token")
    API_BASE_URL = os.getenv("API_BASE_URL", "https://api.fitbit.com/1/user/-/")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    ALLOWED_ORIGINS = _origins_list()
    TRUST_PROXY_HEADERS = _truthy("TRUST_PROXY_HEADERS", "false")
    # Fitbit Subscription API (webhook) — subscriber verification + optional non-default subscriber
    FITBIT_SUBSCRIBER_VERIFICATION_CODE = os.getenv("FITBIT_SUBSCRIBER_VERIFICATION_CODE", "")
    FITBIT_SUBSCRIBER_ID = os.getenv("FITBIT_SUBSCRIBER_ID", "")


class DevelopmentConfig(Config):
    DEBUG = _truthy("FLASK_DEBUG", "0")
    ENV = "development"
    TESTING = False
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-key-change-in-production")
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL", "sqlite:///fitbit_data.db")
    SESSION_TYPE = "filesystem"
    SESSION_PERMANENT = False
    SESSION_USE_SIGNER = True
    SESSION_COOKIE_SAMESITE = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")
    SESSION_COOKIE_SECURE = _truthy("SESSION_COOKIE_SECURE", "false")
    SESSION_COOKIE_HTTPONLY = True
    ENABLE_DOCS = _truthy("ENABLE_DOCS", "true")
    ENABLE_DEBUG_DB_VIEWS = _truthy("ENABLE_DEBUG_DB_VIEWS", "true")


class ProductionConfig(Config):
    DEBUG = False
    ENV = "production"
    TESTING = False
    PREFERRED_URL_SCHEME = "https"
    SESSION_TYPE = "filesystem"
    SESSION_PERMANENT = False
    SESSION_USE_SIGNER = True
    SESSION_COOKIE_SAMESITE = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")
    SESSION_COOKIE_SECURE = _truthy("SESSION_COOKIE_SECURE", "true")
    SESSION_COOKIE_HTTPONLY = True
    ENABLE_DOCS = _truthy("ENABLE_DOCS", "false")
    ENABLE_DEBUG_DB_VIEWS = _truthy("ENABLE_DEBUG_DB_VIEWS", "false")

    _db_uri = os.getenv("DATABASE_URL", "")
    if _db_uri and not _db_uri.startswith("sqlite"):
        SQLALCHEMY_ENGINE_OPTIONS = {
            "pool_pre_ping": True,
            "pool_recycle": int(os.getenv("SQLALCHEMY_POOL_RECYCLE", "280")),
        }
