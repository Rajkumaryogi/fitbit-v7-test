import requests
import base64
import logging
import threading
from flask import current_app
from .auth import generate_pkce
from . import db
import time
import json

logger = logging.getLogger(__name__)

# Serialize Fitbit OAuth refresh + Dynamo persistence when many parallel GETs hit 401.
_fitbit_oauth_lock = threading.Lock()

def _basic_auth_header(client_id, client_secret):
    creds = f"{client_id}:{client_secret}"
    return base64.b64encode(creds.encode()).decode()

def exchange_code_for_tokens(client_id, code, code_verifier, redirect_uri):
    token_url = current_app.config.get('OAUTH2_TOKEN_URL')
    client_secret = current_app.config.get('FITBIT_CLIENT_SECRET')
    auth_header = _basic_auth_header(client_id, client_secret)
    headers = {
        'Authorization': f'Basic {auth_header}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'client_id': client_id,
        'grant_type': 'authorization_code',
        'code': code,
        'code_verifier': code_verifier,
        'redirect_uri': redirect_uri
    }
    resp = requests.post(token_url, data=data, headers=headers)
    logger.debug("TOKEN EXCHANGE: %s %s", resp.status_code, resp.text[:500])
    try:
        return resp.json()
    except Exception:
        return {'error': 'invalid_json', 'raw': resp.text}

def refresh_access_token(refresh_token):
    token_url = current_app.config.get('OAUTH2_TOKEN_URL')
    client_id = current_app.config.get('FITBIT_CLIENT_ID')
    client_secret = current_app.config.get('FITBIT_CLIENT_SECRET')
    auth_header = _basic_auth_header(client_id, client_secret)
    headers = {
        'Authorization': f'Basic {auth_header}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id
    }
    resp = requests.post(token_url, data=data, headers=headers)
    logger.debug("REFRESH TOKEN RESP: %s %s", resp.status_code, resp.text[:500])
    try:
        return resp.json()
    except Exception:
        return {'error': 'invalid_json', 'raw': resp.text}

# low-level GET with logging + single retry after refresh
def _get_with_retry(url, access_token, user_obj=None):
    headers = {'Authorization': f'Bearer {access_token}'}
    resp = requests.get(url, headers=headers, timeout=45)
    logger.debug("GET %s -> %s", url, resp.status_code)
    if resp.status_code == 401 and user_obj and getattr(user_obj, "refresh_token", None):
        with _fitbit_oauth_lock:
            # Another thread may have refreshed already — retry with latest bearer first.
            latest = getattr(user_obj, "access_token", None) or access_token
            headers = {"Authorization": f"Bearer {latest}"}
            resp = requests.get(url, headers=headers, timeout=45)
            logger.debug("GET after lock (maybe refreshed peer) %s -> %s", url, resp.status_code)
            if resp.status_code != 401:
                return resp
            refreshed = refresh_access_token(user_obj.refresh_token)
            if "access_token" not in refreshed:
                logger.warning("Fitbit token refresh failed for GET %s", url[:120])
                return resp
            new_access = refreshed["access_token"]
            new_refresh = refreshed.get("refresh_token", user_obj.refresh_token)
            user_obj.access_token = new_access
            user_obj.refresh_token = new_refresh
            cognito_id = getattr(user_obj, "cognito_user_id", None)
            if cognito_id:
                try:
                    from . import dynamodb_client

                    dynamodb_client.save_tokens(
                        str(cognito_id),
                        new_access,
                        new_refresh,
                        int(refreshed.get("expires_in", 28800)),
                        fitbit_user_id=getattr(user_obj, "fitbit_user_id", None),
                    )
                    logger.info("Persisted Fitbit refresh to DynamoDB for user %s", str(cognito_id)[:8])
                except Exception as e:
                    logger.warning("DynamoDB save_tokens after Fitbit refresh failed: %s", e)
            try:
                from .models import User

                if isinstance(user_obj, User):
                    db.session.add(user_obj)
                    db.session.commit()
                    logger.info("Updated user tokens in SQLite after refresh")
            except Exception as e:
                db.session.rollback()
                logger.warning("DB commit error updating tokens: %s", e)
            headers = {"Authorization": f"Bearer {new_access}"}
            resp = requests.get(url, headers=headers, timeout=45)
            logger.debug("RETRY GET %s -> %s", url, resp.status_code)
    return resp


def _date_str(date='today'):
    """Resolve Fitbit date path segment: 'today', 'yesterday', or YYYY-MM-DD."""
    from datetime import date as dt_date, timedelta

    if date == 'today':
        return dt_date.today().isoformat()
    if date == 'yesterday':
        return (dt_date.today() - timedelta(days=1)).isoformat()
    return date


# public fetch functions; return requests.Response
def fetch_heart_rate(access_token, user_obj=None, date='today'):
    ds = _date_str(date)
    url = current_app.config.get('API_BASE_URL') + f'activities/heart/date/{ds}/1d.json'
    return _get_with_retry(url, access_token, user_obj)

def fetch_sleep(access_token, user_obj=None, date='today'):
    # date: 'today', 'yesterday', or YYYY-MM-DD — so we can include last night's sleep
    if date == 'today':
        from datetime import date as dt_date
        date_str = dt_date.today().isoformat()
    elif date == 'yesterday':
        from datetime import date as dt_date, timedelta
        date_str = (dt_date.today() - timedelta(days=1)).isoformat()
    else:
        date_str = date
    url = current_app.config.get('API_BASE_URL') + f'sleep/date/{date_str}.json'
    return _get_with_retry(url, access_token, user_obj)

def fetch_steps(access_token, user_obj=None, date='today'):
    ds = _date_str(date)
    url = current_app.config.get('API_BASE_URL') + f'activities/steps/date/{ds}/1d.json'
    return _get_with_retry(url, access_token, user_obj)

def fetch_weight(access_token, user_obj=None, date='today'):
    # prefer YYYY-MM-DD; allow 'today', 'yesterday', or explicit date string
    if date == 'today':
        from datetime import date as dt_date
        date_str = dt_date.today().isoformat()
    elif date == 'yesterday':
        from datetime import date as dt_date, timedelta
        date_str = (dt_date.today() - timedelta(days=1)).isoformat()
    else:
        date_str = date
    url = current_app.config.get('API_BASE_URL') + f'body/log/weight/date/{date_str}.json'
    return _get_with_retry(url, access_token, user_obj)

def fetch_activities(access_token, user_obj=None, after_date=None, limit=50):
    # after_date should be in 'YYYY-MM-DD' or None (default: 7 days back for richer history)
    from datetime import date as dt_date, timedelta

    if after_date:
        after = after_date
    else:
        after = (dt_date.today() - timedelta(days=7)).isoformat()
    url = current_app.config.get('API_BASE_URL') + f'activities/list.json?afterDate={after}&sort=desc&limit={limit}&offset=0'
    return _get_with_retry(url, access_token, user_obj)

def token_has_scope(tokens_or_scope_str, needed_scope):
    # tokens_or_scope_str can be a dict (token response) or a space-separated scope string
    if isinstance(tokens_or_scope_str, dict):
        scope_str = tokens_or_scope_str.get('scope', '')
    else:
        scope_str = tokens_or_scope_str or ''
    return needed_scope in scope_str.split()

def fetch_nutrition(access_token, user_obj=None, date='today'):
    ds = _date_str(date)
    url = current_app.config.get('API_BASE_URL') + f'foods/log/date/{ds}.json'
    return _get_with_retry(url, access_token, user_obj)

def fetch_hydration(access_token, user_obj=None, date='today'):
    ds = _date_str(date)
    url = current_app.config.get('API_BASE_URL') + f'foods/log/water/date/{ds}.json'
    return _get_with_retry(url, access_token, user_obj)

def fetch_blood_pressure(access_token, user_obj=None, date='today'):
    ds = _date_str(date)
    url = current_app.config.get('API_BASE_URL') + f'bp/date/{ds}.json'
    return _get_with_retry(url, access_token, user_obj)

def fetch_body_fat(access_token, user_obj=None, date='today'):
    ds = _date_str(date)
    url = current_app.config.get('API_BASE_URL') + f'body/fat/date/{ds}.json'
    return _get_with_retry(url, access_token, user_obj)

def fetch_oxygen_saturation(access_token, user_obj=None, date='today'):
    ds = _date_str(date)
    url = current_app.config.get('API_BASE_URL') + f'oxygen-saturation/date/{ds}.json'
    return _get_with_retry(url, access_token, user_obj)

def fetch_respiratory_rate(access_token, user_obj=None, date='today'):
    ds = _date_str(date)
    url = current_app.config.get('API_BASE_URL') + f'respiratory-rate/date/{ds}.json'
    return _get_with_retry(url, access_token, user_obj)

class _JsonBody:
    """Minimal response shape for parse_response(resp) -> resp.json()."""

    __slots__ = ("_data",)

    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


def fetch_temperature(access_token, user_obj=None, date='today'):
    """
    Temperature: current Web API uses temp/core (manual) and temp/skin (sleep delta).
    Legacy body/temperature is merged when Fitbit still returns it.
    """
    ds = _date_str(date)
    base = current_app.config.get('API_BASE_URL')
    rows = []

    def _safe_json(resp):
        if resp is None or getattr(resp, "status_code", 0) != 200:
            return {}
        try:
            return resp.json()
        except Exception:
            return {}

    core = _get_with_retry(f"{base}temp/core/date/{ds}.json", access_token, user_obj)
    for x in _safe_json(core).get("tempCore") or []:
        dt = x.get("dateTime")
        val = x.get("value")
        if val is None or not dt:
            continue
        rows.append({"dateTime": dt, "value": val, "logId": dt, "_fitbitTempSource": "core"})

    skin = _get_with_retry(f"{base}temp/skin/date/{ds}.json", access_token, user_obj)
    for x in _safe_json(skin).get("tempSkin") or []:
        dt = x.get("dateTime")
        nr = (x.get("value") or {}).get("nightlyRelative")
        if nr is None or not dt:
            continue
        rows.append(
            {
                "date": dt,
                "value": nr,
                "logId": f"skin_{dt}",
                "_fitbitTempSource": "skin_delta",
                "logType": x.get("logType"),
            }
        )

    legacy = _get_with_retry(f"{base}body/temperature/date/{ds}.json", access_token, user_obj)
    lj = _safe_json(legacy)
    for key in ("temp", "temperature"):
        chunk = lj.get(key)
        if not chunk:
            continue
        seq = chunk if isinstance(chunk, list) else [chunk]
        for item in seq:
            if isinstance(item, dict):
                rows.append({**item, "_fitbitTempSource": "legacy"})

    return _JsonBody({"temp": rows})

def fetch_vo2_max(access_token, user_obj=None, date='today'):
    ds = _date_str(date)
    url = current_app.config.get('API_BASE_URL') + f'cardioscore/date/{ds}.json'
    return _get_with_retry(url, access_token, user_obj)

def fetch_hrv(access_token, user_obj=None, date='today'):
    ds = _date_str(date)
    url = current_app.config.get('API_BASE_URL') + f'hrv/date/{ds}.json'
    return _get_with_retry(url, access_token, user_obj)

def fetch_ecg(access_token, user_obj=None):
    from datetime import date as dt_date, timedelta
    after = (dt_date.today() - timedelta(days=30)).isoformat()
    url = current_app.config.get('API_BASE_URL') + f'ecg/list.json?afterDate={after}&sort=desc&limit=10&offset=0'
    return _get_with_retry(url, access_token, user_obj)

def fetch_active_zone_minutes(access_token, user_obj=None, date='today'):
    ds = _date_str(date)
    url = current_app.config.get('API_BASE_URL') + f'activities/active-zone-minutes/date/{ds}/1d.json'
    return _get_with_retry(url, access_token, user_obj)

def fetch_blood_glucose(access_token, user_obj=None, date='today'):
    ds = _date_str(date)
    url = current_app.config.get('API_BASE_URL') + f'blood-glucose/date/{ds}.json'
    return _get_with_retry(url, access_token, user_obj)

def fetch_irn_alerts(access_token, user_obj=None):
    from datetime import date as dt_date, timedelta
    after = (dt_date.today() - timedelta(days=30)).isoformat()
    url = current_app.config.get('API_BASE_URL') + f'irn/alerts/list.json?afterDate={after}&sort=desc&limit=10&offset=0'
    return _get_with_retry(url, access_token, user_obj)

def fetch_devices(access_token, user_obj=None):
    url = current_app.config.get('API_BASE_URL') + 'devices.json'
    return _get_with_retry(url, access_token, user_obj)
