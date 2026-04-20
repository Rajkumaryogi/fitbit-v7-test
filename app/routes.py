from flask import Blueprint, session, redirect, request, current_app as app, jsonify, Response, abort
from flask_cors import cross_origin  # Add this import
from .auth import generate_pkce, build_auth_url
import os
import json
from datetime import datetime, timedelta, timezone
import traceback
from typing import Optional, Set

from .fitbit_client import (
    _date_str,
    exchange_code_for_tokens,
    fetch_heart_rate,
    fetch_sleep,
    fetch_steps,
    fetch_weight,
    fetch_activities,
    fetch_nutrition,
    fetch_hydration,
    fetch_blood_pressure,
    fetch_body_fat,
    fetch_oxygen_saturation,
    fetch_respiratory_rate,
    fetch_temperature,
    fetch_vo2_max,
    fetch_hrv,
    fetch_ecg,
    fetch_active_zone_minutes,
    fetch_blood_glucose,
    fetch_irn_alerts,
    fetch_devices,
)
from .models import (
    db, User, HeartRate, Sleep, Steps, Weight, Activity, Nutrition, Hydration,
    BloodPressure, BodyFat, OxygenSaturation, RespiratoryRate, Temperature
)
from . import dynamodb_client
from . import fitbit_to_vitals7
from . import fitbit_subscriptions
import secrets
import time
import threading
import requests
from flask import render_template_string
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from types import SimpleNamespace

bp = Blueprint('main', __name__)

# Root logger: DEBUG forces very slow I/O; default INFO (override with FITBIT_LOG_LEVEL=DEBUG).
if not logging.root.handlers:
    logging.basicConfig(
        level=os.getenv("FITBIT_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
logger = logging.getLogger(__name__)


def _client_safe_error_detail(exc: BaseException, status_code: int) -> str:
    if status_code < 500:
        return str(exc)
    try:
        if app.config.get("ENV") == "production":
            return "Internal server error"
    except RuntimeError:
        pass
    if os.getenv("FLASK_ENV", "").lower() == "production":
        return "Internal server error"
    return str(exc)


# In-memory store for PKCE verifiers keyed by state.
PKCE_STORE = {}

# -----------------------
# Helpers
# -----------------------
def _publish_realtime_gateway(cognito_user_id: str, source: str = 'fitbit') -> None:
    """Notify central SSE gateway so the Vitals7 SPA refreshes without polling."""
    if not cognito_user_id:
        return
    is_prod = os.getenv("FLASK_ENV", "").lower() == "production"
    base = (os.getenv("VITALS_REALTIME_GATEWAY_URL") or "").strip().rstrip("/")
    secret = os.getenv("VITALS_REALTIME_GATEWAY_SECRET")
    if not base:
        if is_prod:
            return
        base = "http://localhost:8095"
    if is_prod and not (secret or "").strip():
        logger.warning("Realtime gateway: VITALS_REALTIME_GATEWAY_SECRET missing; skipping publish")
        return
    if not (secret or "").strip():
        secret = "vitals7-local-dev-realtime"
    try:
        requests.post(
            f'{base}/internal/publish',
            json={
                'userId': str(cognito_user_id),
                'type': 'vitals7_refresh',
                'source': source,
                'data': {'at': datetime.now(timezone.utc).isoformat()},
            },
            headers={'X-Gateway-Secret': secret},
            timeout=5,
        )
    except Exception as e:
        logger.warning('Realtime gateway publish failed: %s', e)


def _check_user_authenticated():
    """Check if a user is authenticated via session"""
    user_id = session.get('user_id')
    if not user_id:
        return None
    
    user = User.query.get(user_id)
    if not user or not user.access_token:
        return None
    
    return user


@bp.route('/health', methods=['GET'])
def health():
    """Load balancer / platform liveness (no auth)."""
    return jsonify({'status': 'ok'}), 200


# -----------------------
# Vitals7 configure (called by Connect button before Fitbit OAuth)
# -----------------------
@bp.route('/api/vitals7/configure', methods=['POST', 'OPTIONS'])
@cross_origin()
def vitals7_configure():
    """Acknowledge Connect flow. Fitbit sync writes directly to DynamoDB user_vitals (no HTTP Vitals API)."""
    if request.method == 'OPTIONS':
        return '', 200
    try:
        data = request.get_json() or {}
        cognito_user_id = data.get('cognitoUserId')
        if not cognito_user_id:
            return jsonify({'error': 'cognitoUserId is required'}), 400
        logger.info('Fitbit connector configure acknowledged for user %s', str(cognito_user_id)[:8] + '...')
        return jsonify({'success': True, 'message': 'Fitbit connector ready (user_vitals)'})
    except Exception as e:
        logger.error('vitals7_configure: %s', e)
        return jsonify({'error': _client_safe_error_detail(e, 500)}), 500


# -----------------------
# NEW API ENDPOINTS FOR REACT APP
# -----------------------

@bp.route('/api/fitbit/status', methods=['GET', 'OPTIONS'])
@cross_origin()
def fitbit_status():
    """Check if Fitbit is authenticated and get basic status.
    Checks: 1) DynamoDB Fitbit tokens (Vitals7 Connect) when userId is passed, 2) SQLite User with tokens, 3) any Fitbit row in TOKENS_TABLE.
    """
    if request.method == 'OPTIONS':
        return '', 200

    try:
        cognito_user_id = request.args.get('userId') or request.args.get('cognitoUserId')
        if cognito_user_id:
            try:
                item = dynamodb_client.get_tokens(cognito_user_id)
                api_name = (item.get('api_name') or '') if item else ''
                if item and str(api_name).lower() == 'fitbit' and item.get('access_token'):
                    return jsonify({
                        'connected': True,
                        'user_id': item.get('fitbit_user_id'),
                        'status': 'authenticated_via_vitals7',
                        'message': 'Fitbit linked for this user (DynamoDB)',
                    })
            except Exception as dyn_e:
                logger.debug("DynamoDB user-scoped status check: %s", dyn_e)
            return jsonify({
                'connected': False,
                'status': 'not_connected',
                'message': 'No Fitbit link for this user',
            })

        # 1) Check for any user in SQLite with valid tokens (non-empty)
        user = User.query.filter(User.access_token.isnot(None)).filter(User.access_token != '').first()
        if user:
            return jsonify({
                'connected': True,
                'user_id': user.fitbit_user_id,
                'last_sync': user.last_sync.isoformat() if user.last_sync else None,
                'status': 'authenticated'
            })

        # 2) Check DynamoDB for Fitbit tokens (saved when user connected via Vitals7 modal with state=cognitoUserId)
        try:
            if dynamodb_client.has_any_fitbit_tokens():
                return jsonify({
                    'connected': True,
                    'status': 'authenticated_via_vitals7',
                    'message': 'Fitbit linked via Vitals7'
                })
        except Exception as dyn_e:
            logger.debug("DynamoDB status check skipped: %s", dyn_e)

        return jsonify({
            'connected': False,
            'status': 'not_connected',
            'message': 'Please connect your Fitbit account'
        })

    except Exception as e:
        logger.error("Error checking Fitbit status: %s", e)
        return jsonify({
            'connected': False,
            'error': _client_safe_error_detail(e, 500),
            'status': 'error'
        }), 500

@bp.route('/api/fitbit/data', methods=['GET', 'OPTIONS'])
@cross_origin()
def get_fitbit_data():
    """Fitbit metrics are stored in DynamoDB ``user_vitals`` only; this endpoint returns link status, not raw Fitbit JSON."""
    if request.method == 'OPTIONS':
        return '', 200

    try:
        cognito_user_id = request.args.get('userId') or request.args.get('cognitoUserId')
        if cognito_user_id:
            item = dynamodb_client.get_tokens(cognito_user_id)
            api_name = (item.get('api_name') or '') if item else ''
            if item and str(api_name).lower() == 'fitbit' and item.get('access_token'):
                return jsonify(
                    {
                        'connected': True,
                        'storage': 'DynamoDB',
                        'message': 'Fitbit readings are written to the user_vitals table. Use the Vitals7 API to query them.',
                        'fitbitUserId': item.get('fitbit_user_id'),
                        'lastVitalsPushAt': item.get('last_vitals7_push_at'),
                    }
                )
            return (
                jsonify(
                    {
                        'connected': False,
                        'message': 'No Fitbit link for this user',
                    }
                ),
                404,
            )

        user = User.query.filter(User.access_token.isnot(None)).filter(User.access_token != '').first()
        if user:
            return jsonify(
                {
                    'connected': True,
                    'storage': 'SQLite',
                    'message': 'OAuth tokens are in the local SQLite User row; use POST /api/fitbit/sync to refresh. Vitals7 user_vitals applies when linking via Cognito + DynamoDB.',
                    'fitbitUserId': user.fitbit_user_id,
                    'lastSynced': user.last_sync.isoformat() if user.last_sync else None,
                }
            )

        try:
            if dynamodb_client.has_any_fitbit_tokens():
                return jsonify(
                    {
                        'connected': True,
                        'storage': 'DynamoDB',
                        'message': 'At least one Fitbit-linked user exists in vitals-di-tokens.',
                    }
                )
        except Exception as dyn_e:
            logger.debug('get_fitbit_data DynamoDB check: %s', dyn_e)

        return (
            jsonify(
                {
                    'connected': False,
                    'message': 'Not connected',
                }
            ),
            404,
        )

    except Exception as e:
        logger.error('Error getting Fitbit data: %s\n%s', e, traceback.format_exc())
        return jsonify({'error': _client_safe_error_detail(e, 500), 'connected': False}), 500

@bp.route('/api/fitbit/sync', methods=['POST', 'OPTIONS'])
@cross_origin()
def sync_fitbit_data():
    """Trigger a manual sync of Fitbit data"""
    if request.method == 'OPTIONS':
        # Handle preflight requests
        return '', 200
    
    try:
        body = request.get_json(silent=True) or {}
        cognito_user_id = body.get('cognitoUserId') or body.get('userId')
        if cognito_user_id:
            item = dynamodb_client.get_tokens(cognito_user_id)
            api_name = (item.get('api_name') or '') if item else ''
            if not item or str(api_name).lower() != 'fitbit' or not item.get('access_token'):
                return jsonify({
                    'error': 'Not authenticated. Please connect Fitbit first.',
                    'connected': False,
                }), 401
            fitbit_uid = item.get('fitbit_user_id')
            if not fitbit_uid:
                return jsonify({
                    'error': 'Fitbit user id missing in stored tokens; reconnect Fitbit once.',
                    'connected': False,
                }), 401
            from types import SimpleNamespace
            dyn_user = SimpleNamespace(
                fitbit_user_id=fitbit_uid,
                access_token=item['access_token'],
                refresh_token=item.get('refresh_token') or '',
                last_sync=None,
                cognito_user_id=str(cognito_user_id),
                token_expires_at_ms=_fitbit_token_expires_at_ms_from_item(item),
            )
            success, parsed_data = _fetch_and_store_fitbit_data(dyn_user)
            rows_written = 0
            if success and parsed_data:
                try:
                    import time as _time

                    rows_written = _save_fitbit_to_user_vitals(cognito_user_id, parsed_data)
                    if rows_written > 0:
                        dynamodb_client.update_last_vitals7_push_at(cognito_user_id, int(_time.time() * 1000))
                except Exception as e:
                    logger.warning("DynamoDB user_vitals (scoped sync): %s", e)
            if success:
                _publish_realtime_gateway(cognito_user_id, 'fitbit')
            last_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z") if success else None
            return jsonify(
                {
                    'success': bool(success),
                    'message': 'Fitbit data synced to user_vitals (DynamoDB)' if success else 'Sync incomplete',
                    'lastSynced': last_iso,
                    'rowsWritten': rows_written,
                }
            )

        # Get the first user with valid (non-empty) tokens
        user = User.query.filter(User.access_token.isnot(None)).filter(User.access_token != '').first()

        if not user:
            return (
                jsonify(
                    {
                        'error': 'Not authenticated. Please connect Fitbit first.',
                        'connected': False,
                    }
                ),
                401,
            )

        logger.info("Manual sync requested for user %s", user.fitbit_user_id)
        success, _ = _fetch_and_store_fitbit_data(user)
        if success:
            user.last_sync = datetime.now(timezone.utc)
            db.session.commit()
            return jsonify(
                {
                    'success': True,
                    'message': 'Fitbit data fetched from Fitbit API (SQLite session). Link via Vitals7 + Cognito to write user_vitals.',
                    'lastSynced': user.last_sync.isoformat(),
                    'rowsWritten': 0,
                }
            )

        return (
            jsonify(
                {
                    'success': False,
                    'error': 'Failed to fetch data from Fitbit',
                }
            ),
            500,
        )

    except Exception as e:
        logger.error("Error syncing Fitbit data: %s\n%s", e, traceback.format_exc())
        return jsonify({'success': False, 'error': _client_safe_error_detail(e, 500)}), 500


def _fitbit_token_expires_at_ms_from_item(item: Optional[dict]) -> Optional[int]:
    """Dynamo ``expires_at`` may be int or Decimal; used for proactive OAuth refresh."""
    if not item:
        return None
    v = item.get("expires_at")
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _sync_fitbit_dynamo_user_worker(
    cognito_user_id: str, extra_notification_dates: Optional[Set[str]] = None
) -> None:
    """Background sync for one Cognito user (Fitbit Subscriber / webhook)."""
    try:
        item = dynamodb_client.get_tokens(cognito_user_id)
        api_name = (item.get('api_name') or '') if item else ''
        if not item or str(api_name).lower() != 'fitbit' or not item.get('access_token'):
            logger.warning('Fitbit webhook: no tokens for user %s', cognito_user_id[:8])
            return
        fitbit_uid = item.get('fitbit_user_id')
        if not fitbit_uid:
            logger.warning(
                'Fitbit webhook: DynamoDB token row missing fitbit_user_id for user %s; reconnect OAuth',
                cognito_user_id[:8],
            )
            return
        from types import SimpleNamespace
        dyn_user = SimpleNamespace(
            fitbit_user_id=fitbit_uid,
            access_token=item['access_token'],
            refresh_token=item.get('refresh_token') or '',
            last_sync=None,
            cognito_user_id=str(cognito_user_id),
            token_expires_at_ms=_fitbit_token_expires_at_ms_from_item(item),
        )
        success, parsed_data = _fetch_and_store_fitbit_data(
            dyn_user, extra_fetch_dates=extra_notification_dates
        )
        if success and parsed_data:
            try:
                import time as _time

                n = _save_fitbit_to_user_vitals(cognito_user_id, parsed_data)
                if n > 0:
                    dynamodb_client.update_last_vitals7_push_at(cognito_user_id, int(_time.time() * 1000))
                else:
                    logger.info(
                        'Fitbit webhook: user_vitals write skipped (0 changed rows) for user %s',
                        cognito_user_id[:8],
                    )
            except Exception as e:
                logger.warning('DynamoDB user_vitals (webhook sync): %s', e)
        if success:
            _publish_realtime_gateway(cognito_user_id, 'fitbit')
        logger.info('Fitbit webhook sync finished user=%s success=%s', cognito_user_id[:8], success)
    except Exception:
        logger.exception('Fitbit webhook sync worker failed')


def _fitbit_notifications_from_request():
    """Parse JSON array, single JSON object, or multipart ``updates`` field (Fitbit subscriber docs)."""
    data = request.get_json(silent=True, force=True)
    if data is None and request.data:
        try:
            raw = request.get_data(as_text=True)
            if raw and raw.strip():
                data = json.loads(raw)
        except (json.JSONDecodeError, TypeError, ValueError):
            data = None
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    if request.form and 'updates' in request.form:
        try:
            parsed = json.loads(request.form['updates'])
            if isinstance(parsed, list):
                return parsed
            if isinstance(parsed, dict):
                return [parsed]
        except (json.JSONDecodeError, TypeError):
            pass
    return []


@bp.route('/api/fitbit/webhook', methods=['GET', 'POST', 'HEAD', 'OPTIONS'])
@cross_origin()
def fitbit_subscriber_webhook():
    """
    Fitbit Subscription API subscriber endpoint.
    - Configure this **exact** URL in https://dev.fitbit.com/apps (Subscriber URL, JSON).
    - Subscriber verification: GET ``?verify=`` — respond **204** for the code shown in the portal,
      **404** for any other value (see Fitbit "Verify a Subscriber").
    - Notifications: POST JSON array — respond **204** within 5s; sync runs in a background thread.
    """
    if request.method == 'OPTIONS':
        return '', 200
    if request.method == 'HEAD':
        return '', 204
    if request.method == 'GET':
        verify = request.args.get('verify')
        if verify is None:
            return jsonify({'status': 'ok'}), 200
        expected = (app.config.get('FITBIT_SUBSCRIBER_VERIFICATION_CODE') or '').strip()
        if not expected:
            logger.warning(
                'Fitbit subscriber GET verify=... but FITBIT_SUBSCRIBER_VERIFICATION_CODE is not set '
                '(copy the code from Fitbit app subscriber "Verify" flow into .env).'
            )
            return '', 404
        if verify == expected:
            return '', 204
        return '', 404
    try:
        notifications = _fitbit_notifications_from_request()
        if not notifications and request.method == 'POST' and request.data:
            logger.warning(
                'Fitbit webhook: POST had body (%d bytes) but parsed 0 notifications; check Content-Type/JSON',
                len(request.data or b''),
            )
        owner_ids: Set[str] = set()
        owner_notification_dates: dict[str, Set[str]] = {}
        for n in notifications:
            if not isinstance(n, dict):
                continue
            col = (n.get('collectionType') or '').strip()
            oid = n.get('ownerId') or n.get('ownerid')
            if oid is None or (isinstance(oid, str) and not oid.strip()):
                continue
            oid_s = str(oid).strip()
            nd = n.get("date")
            if nd and isinstance(nd, str) and len(nd.strip()) >= 10:
                owner_notification_dates.setdefault(oid_s, set()).add(nd.strip()[:10])
            if col == 'userRevokedAccess':
                cid = dynamodb_client.get_cognito_id_by_fitbit_owner_id(oid_s)
                if cid:
                    try:
                        dynamodb_client.remove_tokens(cid)
                        logger.info('Fitbit webhook: removed tokens (userRevokedAccess) for %s', cid[:8])
                    except Exception:
                        logger.exception('Fitbit webhook: remove_tokens on revoke')
                continue
            if col == 'deleteUser':
                cid = dynamodb_client.get_cognito_id_by_fitbit_owner_id(oid_s)
                if cid:
                    try:
                        dynamodb_client.remove_tokens(cid)
                        logger.info('Fitbit webhook: removed tokens (deleteUser) for %s', cid[:8])
                    except Exception:
                        logger.exception('Fitbit webhook: remove_tokens on deleteUser')
                continue
            owner_ids.add(oid_s)

        flask_app = app._get_current_object()

        def _run_all():
            with flask_app.app_context():
                for oid in owner_ids:
                    cid = dynamodb_client.get_cognito_id_by_fitbit_owner_id(oid)
                    if cid:
                        extra_dates = owner_notification_dates.get(oid) or None
                        _sync_fitbit_dynamo_user_worker(cid, extra_notification_dates=extra_dates)
                    else:
                        logger.warning('Fitbit webhook: no DynamoDB Fitbit row for ownerId=%s', oid[:12])

        if owner_ids:
            threading.Thread(target=_run_all, daemon=True, name='fitbit-webhook-sync').start()
        return '', 204
    except Exception as e:
        logger.error('fitbit_subscriber_webhook: %s', e)
        return '', 204


@bp.route('/api/fitbit/register-subscriptions', methods=['POST', 'OPTIONS'])
@cross_origin()
def fitbit_register_subscriptions():
    """(Re)create Fitbit push subscriptions for a Cognito user using stored DynamoDB tokens."""
    if request.method == 'OPTIONS':
        return '', 200
    try:
        body = request.get_json(silent=True) or {}
        cognito_user_id = body.get('cognitoUserId') or body.get('userId') or request.args.get('userId')
        if not cognito_user_id:
            return jsonify({'success': False, 'error': 'cognitoUserId or userId required'}), 400
        item = dynamodb_client.get_tokens(str(cognito_user_id))
        if not item or str(item.get('api_name') or '').lower() != 'fitbit' or not item.get('access_token'):
            return jsonify({'success': False, 'error': 'No Fitbit tokens for this user'}), 401
        from .fitbit_client import refresh_access_token

        access_token = item['access_token']
        rt = item.get('refresh_token') or ''
        sub_id_hdr = (app.config.get('FITBIT_SUBSCRIBER_ID') or '').strip() or None
        r = fitbit_subscriptions.ensure_fitbit_subscriptions(
            access_token, str(cognito_user_id), subscriber_id=sub_id_hdr
        )
        if not r.get('ok') and r.get('status_code') == 401 and rt:
            ref = refresh_access_token(rt)
            if ref.get('access_token'):
                access_token = ref['access_token']
                new_rt = ref.get('refresh_token', rt)
                dynamodb_client.save_tokens(
                    str(cognito_user_id),
                    access_token,
                    new_rt,
                    int(ref.get('expires_in', 28800)),
                    fitbit_user_id=item.get('fitbit_user_id'),
                )
                r = fitbit_subscriptions.ensure_fitbit_subscriptions(
                    access_token, str(cognito_user_id), subscriber_id=sub_id_hdr
                )
        return jsonify({'success': bool(r.get('ok')), **r}), (200 if r.get('ok') else 502)
    except Exception as e:
        logger.error('fitbit_register_subscriptions: %s', e)
        return jsonify({'success': False, 'error': _client_safe_error_detail(e, 500)}), 500


@bp.route('/api/fitbit/disconnect', methods=['POST', 'OPTIONS'])
@cross_origin()
def disconnect_fitbit():
    """Disconnect Fitbit: clear local tokens and remove DynamoDB Fitbit tokens so status returns not connected."""
    if request.method == 'OPTIONS':
        return '', 200
    try:
        body = request.get_json(silent=True) or {}
        cognito_user_id = body.get('cognitoUserId') or body.get('userId')

        # Per-user disconnect (Vitals7 modal): only remove this user's DynamoDB row — do not wipe
        # SQLite tokens (shared / legacy global disconnect handles those separately).
        if cognito_user_id:
            try:
                dynamodb_client.remove_tokens(str(cognito_user_id))
            except Exception as e:
                logger.warning("DynamoDB remove_tokens failed: %s", e)
            return jsonify(
                {'success': True, 'message': 'Fitbit disconnected for this user'}
            )

        # Legacy global disconnect (no user id): clear DynamoDB Fitbit rows and SQLite tokens
        try:
            dynamodb_client.remove_all_fitbit_tokens()
        except Exception as e:
            logger.warning("DynamoDB remove_all_fitbit_tokens failed: %s", e)
        users = User.query.filter(User.access_token.isnot(None)).filter(User.access_token != '').all()
        for u in users:
            u.access_token = ''
            u.refresh_token = ''
            db.session.add(u)
        db.session.commit()
        return jsonify({'success': True, 'message': 'Fitbit disconnected'})
    except Exception as e:
        logger.error("Error disconnecting Fitbit: %s", e)
        return jsonify({'success': False, 'error': _client_safe_error_detail(e, 500)}), 500


def _dedupe_dict_list(items, key_attrs=('logId', 'date', 'dateTime', 'dateOfSleep')):
    """
    Fitbit 'today' + 'yesterday' fetches often return the same calendar row twice.
    Keep the first occurrence (typically from the 'today' response, merged as lb+la).
    """
    if not items:
        return items
    out = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            out.append(item)
            continue
        key = None
        for attr in key_attrs:
            v = item.get(attr)
            if v is not None and str(v).strip() != '':
                key = (attr, str(v))
                break
        if key is None:
            out.append(item)
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _merge_list_field(left, right, field, dedupe_key_attrs=('logId', 'date', 'dateTime', 'dateOfSleep')):
    """Merge list `field` from two Fitbit JSON dicts (e.g. bp, fat, activities-heart)."""
    a = left if isinstance(left, dict) else {}
    b = right if isinstance(right, dict) else {}
    la = a.get(field) or []
    lb = b.get(field) or []
    if not isinstance(la, list):
        la = [la] if la else []
    if not isinstance(lb, list):
        lb = [lb] if lb else []
    out = {**a, **b}
    merged = lb + la
    if dedupe_key_attrs and merged:
        merged = _dedupe_dict_list(merged, dedupe_key_attrs)
    out[field] = merged
    return out


def _merge_nutrition(yesterday_d, today_d):
    y = yesterday_d if isinstance(yesterday_d, dict) else {}
    t = today_d if isinstance(today_d, dict) else {}
    foods = (y.get('foods') or []) + (t.get('foods') or [])
    if foods:
        foods = _dedupe_dict_list(foods, ('logId', 'logDate', 'foodId', 'date'))
    out = {**t}
    out['foods'] = foods
    if t.get('summary') or y.get('summary'):
        out['summary'] = t.get('summary') or y.get('summary')
    return out


def _merge_hydration(yesterday_d, today_d):
    """Keep both daily summaries as a list for DynamoDB + transforms."""
    rows = []
    for label, d in (('yesterday', yesterday_d), ('today', today_d)):
        if isinstance(d, dict) and d:
            row = {**d, '_fitbitDayLabel': label}
            rows.append(row)
    return rows


def _fitbit_notification_dates_expand(dates: Set[str]) -> Set[str]:
    """Include each date plus the prior calendar day (Fitbit often ties updates across midnight)."""
    out: set[str] = set()
    for raw in dates or ():
        ds = (str(raw).strip()[:10] if raw else "") or ""
        if len(ds) != 10 or ds[4] != "-" or ds[7] != "-":
            continue
        out.add(ds)
        try:
            prev = (datetime.fromisoformat(ds).date() - timedelta(days=1)).isoformat()
            out.add(prev)
        except ValueError:
            pass
    return out


def _fitbit_merge_notification_dates(parsed: dict, user, parse_response, dates: Optional[Set[str]]) -> dict:
    """Merge extra calendar days from Fitbit subscription ``date`` fields (beyond server UTC today/yesterday)."""
    if not dates:
        return parsed
    today_s = _date_str("today")
    yest_s = _date_str("yesterday")
    expanded = _fitbit_notification_dates_expand(dates)
    out = dict(parsed)
    for ds in sorted(expanded):
        if ds in (today_s, yest_s):
            continue
        tok = user.access_token
        sd = parse_response(fetch_steps(tok, user, date=ds))
        out["steps_data"] = _merge_list_field(
            out.get("steps_data"), sd if isinstance(sd, dict) else {}, "activities-steps"
        )
        hd = parse_response(fetch_heart_rate(tok, user, date=ds))
        out["hr_data"] = _merge_list_field(
            out.get("hr_data"), hd if isinstance(hd, dict) else {}, "activities-heart"
        )
        sl = parse_response(fetch_sleep(tok, user, date=ds))
        if isinstance(sl, dict) and sl.get("sleep"):
            prev = out.get("sleep_data") or {}
            if not isinstance(prev, dict):
                prev = {}
            merged_sleep = (prev.get("sleep") or []) + (sl.get("sleep") or [])
            out["sleep_data"] = {**prev, **sl, "sleep": merged_sleep}
        wt = parse_response(fetch_weight(tok, user, date=ds))
        out["weight_data"] = _merge_list_field(
            out.get("weight_data"), wt if isinstance(wt, dict) else {}, "weight"
        )
    return out


def _save_fitbit_to_user_vitals(cognito_user_id: str, parsed_data: dict) -> int:
    """Transform Fitbit API payloads and upsert rows into DynamoDB user_vitals (vitals7api-vitals schema)."""
    if not cognito_user_id or not parsed_data:
        return 0
    err_keys = []
    if logger.isEnabledFor(logging.DEBUG):
        for k, v in (parsed_data or {}).items():
            if isinstance(v, dict) and v.get("errors"):
                err_keys.append(k)
        if err_keys:
            logger.debug("Fitbit merged payload contains errors key(s): %s", ", ".join(err_keys[:12]))
    payloads = fitbit_to_vitals7.all_payloads(user_id=cognito_user_id, **parsed_data)
    if not payloads:
        for k, v in (parsed_data or {}).items():
            if isinstance(v, dict) and v.get("errors"):
                logger.warning(
                    "Fitbit → user_vitals: 0 payloads; merged field %s includes Fitbit errors: %s",
                    k,
                    str(v.get("errors"))[:400],
                )
                break
    logger.info(
        "Fitbit → user_vitals: normalized %d payload(s) for user %s",
        len(payloads),
        str(cognito_user_id)[:8],
    )
    n = dynamodb_client.save_payloads_to_user_vitals(cognito_user_id, payloads)
    logger.info("Fitbit → user_vitals: %d row(s) written for user %s", n, str(cognito_user_id)[:8])
    return n


def _fitbit_merge_raw_responses(raw: dict) -> dict:
    """Turn parallel/sequential raw Fitbit JSON blobs into the parsed bundle used by transforms and user_vitals."""
    r = raw or {}
    hr_data = _merge_list_field(r.get("hr_y"), r.get("hr_t"), "activities-heart")

    sleep_today = r.get("sleep_today") or {}
    sleep_yesterday = r.get("sleep_yesterday") or {}
    sleep_list_today = (sleep_today.get("sleep") or []) if isinstance(sleep_today, dict) else []
    sleep_list_yesterday = (sleep_yesterday.get("sleep") or []) if isinstance(sleep_yesterday, dict) else []
    sleep_data = (
        {**sleep_today, "sleep": sleep_list_yesterday + sleep_list_today}
        if isinstance(sleep_today, dict)
        else sleep_today
    )

    steps_data = _merge_list_field(r.get("steps_y"), r.get("steps_t"), "activities-steps")

    weight_today = r.get("weight_today") or {}
    weight_yesterday = r.get("weight_yesterday") or {}
    wt_list = (weight_today.get("weight") or []) if isinstance(weight_today, dict) else []
    wy_list = (weight_yesterday.get("weight") or []) if isinstance(weight_yesterday, dict) else []
    weight_merged = wy_list + wt_list
    if weight_merged:
        weight_merged = _dedupe_dict_list(weight_merged, ("logId", "date", "dateTime"))
    weight_data = (
        {**weight_today, "weight": weight_merged} if isinstance(weight_today, dict) else weight_today
    )

    activities_data = r.get("activities") or {}

    nutrition_data = _merge_nutrition(r.get("nutrition_y"), r.get("nutrition_t"))
    hydration_data = _merge_hydration(r.get("hydration_y"), r.get("hydration_t"))

    bp_data = _merge_list_field(r.get("bp_y"), r.get("bp_t"), "bp")
    bodyfat_data = _merge_list_field(r.get("bodyfat_y"), r.get("bodyfat_t"), "fat")
    oxygen_data = _merge_list_field(r.get("oxygen_y"), r.get("oxygen_t"), "oxygenData")
    resp_rate_data = _merge_list_field(r.get("resp_y"), r.get("resp_t"), "respiratoryRateValues")

    temp_y = r.get("temp_y") or {}
    temp_t = r.get("temp_t") or {}
    ty = temp_y.get("temp") or temp_y.get("temperature") or []
    tt = temp_t.get("temp") or temp_t.get("temperature") or []
    if not isinstance(ty, list):
        ty = [ty] if ty else []
    if not isinstance(tt, list):
        tt = [tt] if tt else []
    temp_merged = ty + tt
    if temp_merged:
        temp_merged = _dedupe_dict_list(temp_merged, ("logId", "date", "dateTime"))
    temp_data = {**temp_t, "temp": temp_merged} if isinstance(temp_t, dict) else temp_t

    vo2_max_data = _merge_list_field(r.get("vo2_y"), r.get("vo2_t"), "cardioScore")
    hrv_data = _merge_list_field(r.get("hrv_y"), r.get("hrv_t"), "hrv")
    ecg_data = r.get("ecg")
    azm_data = _merge_list_field(r.get("azm_y"), r.get("azm_t"), "activities-active-zone-minutes")

    bg_y = r.get("bg_y") or {}
    bg_t = r.get("bg_t") or {}
    bg_reads = (bg_y.get("bgReadings") or []) + (bg_t.get("bgReadings") or [])
    bg_data = {**bg_t}
    if bg_reads:
        bg_data["bgReadings"] = _dedupe_dict_list(bg_reads, ("logId", "dateTime", "date"))

    irn_data = r.get("irn")
    devices_data = r.get("devices")

    return {
        "hr_data": hr_data,
        "sleep_data": sleep_data,
        "steps_data": steps_data,
        "weight_data": weight_data,
        "activities_data": activities_data,
        "nutrition_data": nutrition_data,
        "hydration_data": hydration_data,
        "bp_data": bp_data,
        "bodyfat_data": bodyfat_data,
        "oxygen_data": oxygen_data,
        "respiratory_data": resp_rate_data,
        "temp_data": temp_data,
        "vo2_max_data": vo2_max_data,
        "hrv_data": hrv_data,
        "ecg_data": ecg_data,
        "active_zone_minutes_data": azm_data,
        "blood_glucose_data": bg_data,
        "irn_data": irn_data,
        "devices_data": devices_data,
    }


def _fitbit_download_parallel(flask_app, user, parse_response) -> dict:
    """Issue many Fitbit GETs concurrently; always read ``user.access_token`` so OAuth refresh updates apply."""
    max_w = max(4, min(20, int(os.getenv("FITBIT_FETCH_PARALLELISM", "12"))))

    def run(thunk):
        with flask_app.app_context():
            return thunk()

    def _tok():
        return user.access_token

    tasks = [
        ("hr_y", lambda: parse_response(fetch_heart_rate(_tok(), user, date="yesterday"))),
        ("hr_t", lambda: parse_response(fetch_heart_rate(_tok(), user, date="today"))),
        ("sleep_today", lambda: parse_response(fetch_sleep(_tok(), user, date="today"))),
        ("sleep_yesterday", lambda: parse_response(fetch_sleep(_tok(), user, date="yesterday"))),
        ("steps_y", lambda: parse_response(fetch_steps(_tok(), user, date="yesterday"))),
        ("steps_t", lambda: parse_response(fetch_steps(_tok(), user, date="today"))),
        ("weight_today", lambda: parse_response(fetch_weight(_tok(), user, date="today"))),
        ("weight_yesterday", lambda: parse_response(fetch_weight(_tok(), user, date="yesterday"))),
        ("activities", lambda: parse_response(fetch_activities(_tok(), user)) or {}),
        ("nutrition_y", lambda: parse_response(fetch_nutrition(_tok(), user, date="yesterday"))),
        ("nutrition_t", lambda: parse_response(fetch_nutrition(_tok(), user, date="today"))),
        ("hydration_y", lambda: parse_response(fetch_hydration(_tok(), user, date="yesterday"))),
        ("hydration_t", lambda: parse_response(fetch_hydration(_tok(), user, date="today"))),
        ("bp_y", lambda: parse_response(fetch_blood_pressure(_tok(), user, date="yesterday"))),
        ("bp_t", lambda: parse_response(fetch_blood_pressure(_tok(), user, date="today"))),
        ("bodyfat_y", lambda: parse_response(fetch_body_fat(_tok(), user, date="yesterday"))),
        ("bodyfat_t", lambda: parse_response(fetch_body_fat(_tok(), user, date="today"))),
        ("oxygen_y", lambda: parse_response(fetch_oxygen_saturation(_tok(), user, date="yesterday"))),
        ("oxygen_t", lambda: parse_response(fetch_oxygen_saturation(_tok(), user, date="today"))),
        ("resp_y", lambda: parse_response(fetch_respiratory_rate(_tok(), user, date="yesterday"))),
        ("resp_t", lambda: parse_response(fetch_respiratory_rate(_tok(), user, date="today"))),
        ("temp_y", lambda: parse_response(fetch_temperature(_tok(), user, date="yesterday")) or {}),
        ("temp_t", lambda: parse_response(fetch_temperature(_tok(), user, date="today")) or {}),
        ("vo2_y", lambda: parse_response(fetch_vo2_max(_tok(), user, date="yesterday"))),
        ("vo2_t", lambda: parse_response(fetch_vo2_max(_tok(), user, date="today"))),
        ("hrv_y", lambda: parse_response(fetch_hrv(_tok(), user, date="yesterday"))),
        ("hrv_t", lambda: parse_response(fetch_hrv(_tok(), user, date="today"))),
        ("ecg", lambda: parse_response(fetch_ecg(_tok(), user))),
        ("azm_y", lambda: parse_response(fetch_active_zone_minutes(_tok(), user, date="yesterday"))),
        ("azm_t", lambda: parse_response(fetch_active_zone_minutes(_tok(), user, date="today"))),
        ("bg_y", lambda: parse_response(fetch_blood_glucose(_tok(), user, date="yesterday")) or {}),
        ("bg_t", lambda: parse_response(fetch_blood_glucose(_tok(), user, date="today")) or {}),
        ("irn", lambda: parse_response(fetch_irn_alerts(_tok(), user))),
        ("devices", lambda: parse_response(fetch_devices(_tok(), user))),
    ]
    out: dict = {}
    with ThreadPoolExecutor(max_workers=max_w) as ex:
        future_to_name = {ex.submit(run, thunk): name for name, thunk in tasks}
        for fut in as_completed(future_to_name):
            name = future_to_name[fut]
            try:
                out[name] = fut.result()
            except Exception as e:
                logger.warning("Fitbit parallel fetch %s failed: %s", name, e)
                out[name] = None
    return out


def _fetch_and_store_fitbit_data(user, extra_fetch_dates: Optional[Set[str]] = None):
    """Fetch all Fitbit data types; merge today + yesterday where the API is date-scoped (same pattern as sleep/weight)."""
    try:
        def parse_response(resp):
            if resp is None:
                return None
            try:
                sc = getattr(resp, "status_code", None)
                if sc is not None and sc >= 400:
                    url = getattr(resp, "url", "") or ""
                    logger.warning(
                        "Fitbit API HTTP %s for %s", sc, (url[:160] + "...") if len(url) > 160 else url
                    )
                    return None
                if hasattr(resp, "json"):
                    return resp.json()
                return resp
            except Exception as e:
                logger.error("Parse JSON error: %s", e)
                return None

        def _tok():
            return user.access_token

        if isinstance(user, SimpleNamespace):
            from .fitbit_client import maybe_refresh_expiring_fitbit_token

            setattr(user, "_fitbit_oauth_error", False)
            maybe_refresh_expiring_fitbit_token(user)
            if getattr(user, "_fitbit_oauth_error", False):
                logger.warning(
                    "Skipping Fitbit resource fetch after failed proactive token refresh (reconnect Fitbit)."
                )
                return False, None

        # Parallel Fitbit HTTP for Dynamo-only user objects (no concurrent SQLAlchemy token refresh).
        if isinstance(user, SimpleNamespace):
            raw = _fitbit_download_parallel(app._get_current_object(), user, parse_response)
        else:
            raw = {
                "hr_y": parse_response(fetch_heart_rate(_tok(), user, date="yesterday")),
                "hr_t": parse_response(fetch_heart_rate(_tok(), user, date="today")),
                "sleep_today": parse_response(fetch_sleep(_tok(), user, date="today")),
                "sleep_yesterday": parse_response(fetch_sleep(_tok(), user, date="yesterday")),
                "steps_y": parse_response(fetch_steps(_tok(), user, date="yesterday")),
                "steps_t": parse_response(fetch_steps(_tok(), user, date="today")),
                "weight_today": parse_response(fetch_weight(_tok(), user, date="today")),
                "weight_yesterday": parse_response(fetch_weight(_tok(), user, date="yesterday")),
                "activities": parse_response(fetch_activities(_tok(), user)) or {},
                "nutrition_y": parse_response(fetch_nutrition(_tok(), user, date="yesterday")),
                "nutrition_t": parse_response(fetch_nutrition(_tok(), user, date="today")),
                "hydration_y": parse_response(fetch_hydration(_tok(), user, date="yesterday")),
                "hydration_t": parse_response(fetch_hydration(_tok(), user, date="today")),
                "bp_y": parse_response(fetch_blood_pressure(_tok(), user, date="yesterday")),
                "bp_t": parse_response(fetch_blood_pressure(_tok(), user, date="today")),
                "bodyfat_y": parse_response(fetch_body_fat(_tok(), user, date="yesterday")),
                "bodyfat_t": parse_response(fetch_body_fat(_tok(), user, date="today")),
                "oxygen_y": parse_response(fetch_oxygen_saturation(_tok(), user, date="yesterday")),
                "oxygen_t": parse_response(fetch_oxygen_saturation(_tok(), user, date="today")),
                "resp_y": parse_response(fetch_respiratory_rate(_tok(), user, date="yesterday")),
                "resp_t": parse_response(fetch_respiratory_rate(_tok(), user, date="today")),
                "temp_y": parse_response(fetch_temperature(_tok(), user, date="yesterday")) or {},
                "temp_t": parse_response(fetch_temperature(_tok(), user, date="today")) or {},
                "vo2_y": parse_response(fetch_vo2_max(_tok(), user, date="yesterday")),
                "vo2_t": parse_response(fetch_vo2_max(_tok(), user, date="today")),
                "hrv_y": parse_response(fetch_hrv(_tok(), user, date="yesterday")),
                "hrv_t": parse_response(fetch_hrv(_tok(), user, date="today")),
                "ecg": parse_response(fetch_ecg(_tok(), user)),
                "azm_y": parse_response(fetch_active_zone_minutes(_tok(), user, date="yesterday")),
                "azm_t": parse_response(fetch_active_zone_minutes(_tok(), user, date="today")),
                "bg_y": parse_response(fetch_blood_glucose(_tok(), user, date="yesterday")) or {},
                "bg_t": parse_response(fetch_blood_glucose(_tok(), user, date="today")) or {},
                "irn": parse_response(fetch_irn_alerts(_tok(), user)),
                "devices": parse_response(fetch_devices(_tok(), user)),
            }

        parsed = _fitbit_merge_raw_responses(raw)
        if extra_fetch_dates:
            parsed = _fitbit_merge_notification_dates(parsed, user, parse_response, extra_fetch_dates)
        if isinstance(user, SimpleNamespace) and getattr(user, "_fitbit_oauth_error", False):
            logger.warning(
                "Fitbit sync incomplete: OAuth refresh failed or requests remained unauthorized. "
                "Reconnect Fitbit in the app; confirm server FITBIT_CLIENT_ID / FITBIT_CLIENT_SECRET match dev.fitbit.com."
            )
            return False, None
        return True, parsed
    except Exception as e:
        logger.error("Error fetching Fitbit data: %s", e)
        return False, None


# -----------------------
# Existing OAuth + data fetch routes
# -----------------------

@bp.route('/')
def index():
    return "<a href='/login'>Login with Fitbit</a> | <a href='/docs'>Docs</a>"

@bp.route('/login')
@cross_origin()
def login():
    # Generate new PKCE pair
    code_verifier, code_challenge = generate_pkce()

    # State from query (cognito_user_id from Vitals7 frontend) or random for standalone use
    state = request.args.get('state') or secrets.token_urlsafe(32)
    PKCE_STORE[state] = {
        'code_verifier': code_verifier,
        'created_at': time.time()
    }

    session['oauth_state'] = state

    auth_url = build_auth_url(code_challenge, state)
    return redirect(auth_url)

@bp.route('/callback')
@cross_origin()
def callback():
    code = request.args.get('code')
    state = request.args.get('state')

    if not code:
        return "No code returned from Fitbit", 400
    if not state:
        return "No state returned from Fitbit", 400

    # Retrieve code_verifier from server-side PKCE_STORE
    pkce_entry = PKCE_STORE.get(state)
    if not pkce_entry:
        logger.warning("Missing PKCE entry for state (expired or invalid)")
        return "PKCE verifier missing or expired. Please start the login process again.", 400

    code_verifier = pkce_entry.get('code_verifier')

    # Optionally remove the used PKCE entry
    try:
        del PKCE_STORE[state]
    except KeyError:
        pass

    client_id = app.config['FITBIT_CLIENT_ID']
    redirect_uri = app.config['REDIRECT_URI']

    # Exchange code for tokens (requires code_verifier)
    tokens = exchange_code_for_tokens(client_id, code, code_verifier, redirect_uri)
    if not isinstance(tokens, dict):
        logger.error("Invalid token response type: %s", type(tokens).__name__)
        return "Invalid token response", 400
    if 'errors' in tokens or tokens.get('error'):
        logger.error("Fitbit token exchange error: %s", tokens.get('error') or tokens.get('errors'))
        return "Token exchange error", 400

    access_token = tokens.get('access_token')
    refresh_token = tokens.get('refresh_token')
    # user id could be present under different keys
    user_id = tokens.get('user_id') or (tokens.get('user', {}) or {}).get('encodedId')

    if not access_token or not user_id:
        logger.error("Missing access_token or user id in Fitbit token response")
        return "Invalid token response from Fitbit", 400

    # Save or update user in DB
    user = User.query.filter_by(fitbit_user_id=user_id).first()
    if not user:
        user = User(
            fitbit_user_id=user_id, 
            access_token=access_token, 
            refresh_token=refresh_token,
            last_sync=datetime.now(timezone.utc)
        )
        db.session.add(user)
    else:
        user.access_token = access_token
        user.refresh_token = refresh_token
        user.last_sync = datetime.now(timezone.utc)
    db.session.commit()

    session['user_id'] = user.id

    # Fetch and store data immediately after login
    parsed_data = None
    try:
        _, parsed_data = _fetch_and_store_fitbit_data(user)
    except Exception as e:
        logger.error("Error fetching initial data after login: %s", e)

    # Store in DynamoDB and post to Vitals7 when state is cognito_user_id (from Vitals7 Connect)
    cognito_user_id = state
    try:
        expires_in = int(tokens.get('expires_in', 28800))
        dynamodb_client.save_tokens(
            user_id=cognito_user_id,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=expires_in,
            fitbit_user_id=user_id,
        )
    except Exception as e:
        logger.warning("DynamoDB save_tokens failed: %s", e)

    # Register Fitbit push subscriptions (async) so new cloud data triggers /api/fitbit/webhook → user_vitals.
    try:
        flask_app = app._get_current_object()
        sub_cognito = str(cognito_user_id)
        sub_token = access_token

        def _bg_subscribe():
            with flask_app.app_context():
                try:
                    sub_hdr = (flask_app.config.get("FITBIT_SUBSCRIBER_ID") or "").strip() or None
                    r = fitbit_subscriptions.ensure_fitbit_subscriptions(
                        sub_token,
                        sub_cognito,
                        subscriber_id=sub_hdr,
                    )
                    if not r.get("ok"):
                        logger.warning("Fitbit subscription setup incomplete: %s", r)
                except Exception as sub_e:
                    logger.warning("Fitbit subscription background task failed: %s", sub_e)

        threading.Thread(target=_bg_subscribe, daemon=True, name="fitbit-subscribe").start()
    except Exception as e:
        logger.warning("Could not start Fitbit subscription thread: %s", e)

    if parsed_data:
        try:
            import time as _time

            n = _save_fitbit_to_user_vitals(cognito_user_id, parsed_data)
            if n > 0:
                dynamodb_client.update_last_vitals7_push_at(cognito_user_id, int(_time.time() * 1000))
        except Exception as e:
            logger.warning("DynamoDB user_vitals after OAuth: %s", e)

    # Return a success page with instructions for React app
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Fitbit Connected Successfully</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100vh;
                margin: 0;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            }
            .container {
                background: white;
                padding: 40px;
                border-radius: 10px;
                box-shadow: 0 10px 40px rgba(0,0,0,0.1);
                text-align: center;
                max-width: 500px;
            }
            h1 {
                color: #333;
                margin-bottom: 20px;
            }
            p {
                color: #666;
                line-height: 1.6;
                margin-bottom: 30px;
            }
            .success-icon {
                font-size: 60px;
                color: #4CAF50;
                margin-bottom: 20px;
            }
            button {
                background: #667eea;
                color: white;
                border: none;
                padding: 12px 30px;
                border-radius: 5px;
                font-size: 16px;
                cursor: pointer;
                transition: background 0.3s;
            }
            button:hover {
                background: #5a67d8;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="success-icon">✓</div>
            <h1>Fitbit Connected Successfully!</h1>
            <p>Your Fitbit account has been connected. You can now close this window and return to the Vitals7 app to view your data.</p>
            <button onclick="window.close()">Close Window</button>
        </div>
        <script>
            // Notify parent window that authentication is complete
            window.opener.postMessage({
                type: 'FITBIT_AUTH_COMPLETE',
                success: true,
                timestamp: new Date().toISOString()
            }, '*');
            
            // Auto-close after 3 seconds
            setTimeout(() => {
                window.close();
            }, 3000);
        </script>
    </body>
    </html>
    """

# -----------------------
# Existing Docs and file endpoints
# -----------------------

DOCS_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Fitbit App - Docs</title>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:24px}
    h1{font-size:24px}
    pre{background:#f6f8fa;padding:12px;border-radius:6px;overflow:auto}
    a.button{display:inline-block;padding:8px 12px;border-radius:6px;background:#0d6efd;color:#fff;text-decoration:none;margin-right:8px}
    .small{color:#666;font-size:13px}
    .api-endpoint {background: #f8f9fa; padding: 10px; border-left: 4px solid #0d6efd; margin: 10px 0;}
  </style>
</head>
<body>
  <h1>Fitbit App — Local Docs</h1>
  <p class="small">Development-only docs for your local Fitbit integration.</p>

  <h2>API Endpoints for React App</h2>
  
  <div class="api-endpoint">
    <strong>GET /api/fitbit/status</strong>
    <p>Check if Fitbit is authenticated</p>
    <pre>Response: {"connected": true/false, "user_id": "...", "last_sync": "..."}</pre>
  </div>
  
  <div class="api-endpoint">
    <strong>GET /api/fitbit/data</strong>
    <p>Get Fitbit data formatted for React app</p>
  </div>
  
  <div class="api-endpoint">
    <strong>POST /api/fitbit/sync</strong>
    <p>Trigger manual sync of Fitbit data</p>
  </div>

  <h2>Push notifications (Fitbit Subscription API)</h2>
  <p>Add subscriber URL in <a href="https://dev.fitbit.com/apps" target="_blank" rel="noopener">Fitbit Developer</a> pointing to your public <strong>HTTPS</strong> <code>.../api/fitbit/webhook</code> (JSON). Run subscriber verification: set <code>FITBIT_SUBSCRIBER_VERIFICATION_CODE</code> in <code>.env</code> to the code shown when you click Verify (GET <code>?verify=</code> must return <strong>204</strong> for the correct code and <strong>404</strong> for the wrong one).</p>
  <p>After OAuth, the app registers an <strong>all-collections</strong> subscription so new Fitbit cloud data sends POST notifications; the server responds <strong>204</strong> immediately and syncs to <code>user_vitals</code> in a background thread (do not block the webhook). Optional <code>FITBIT_SUBSCRIBER_ID</code> matches a non-default subscriber in the Fitbit app settings.</p>
  <pre>POST /api/fitbit/register-subscriptions
Body: {"cognitoUserId": "&lt;uuid&gt;"}</pre>

  <h2>Authentication</h2>
  <p><a href="/login" class="button">/login</a> — Start Fitbit OAuth2 flow (PKCE).</p>

  <h2>Data fetch callback</h2>
  <p><code>/callback</code> — Fitbit redirects here after consent. Tokens go to DynamoDB <code>vitals-di-tokens</code> (and SQLite for standalone); readings go to <code>user_vitals</code> when Cognito user id is present.</p>

  <h2>Debug DB views</h2>
  <pre>
GET /debug/saved/steps
GET /debug/saved/heart
GET /debug/saved/sleep
GET /debug/saved/weight
  </pre>

  <h2>Notes</h2>
  <ul>
    <li>No local Fitbit JSON snapshot file — data is persisted to DynamoDB <code>user_vitals</code> and tokens to <code>vitals-di-tokens</code> only.</li>
    <li>No periodic in-process sync — use webhooks, <code>POST /api/fitbit/sync</code>, or a scheduler.</li>
    <li>CORS is enabled for localhost:5173 (React dev server).</li>
  </ul>
</body>
</html>
"""

@bp.route('/docs')
def docs():
    if not app.config.get('ENABLE_DOCS'):
        abort(404)
    return render_template_string(DOCS_HTML)

# -----------------------
# Simple DB debug views
# -----------------------
@bp.route('/debug/saved/<metric>')
def debug_saved(metric):
    if not app.config.get('ENABLE_DEBUG_DB_VIEWS'):
        abort(404)
    limit = int(request.args.get('limit', 20))
    if metric == 'steps':
        rows = Steps.query.order_by(Steps.id.desc()).limit(limit).all()
    elif metric == 'heart':
        rows = HeartRate.query.order_by(HeartRate.id.desc()).limit(limit).all()
    elif metric == 'sleep':
        rows = Sleep.query.order_by(Sleep.id.desc()).limit(limit).all()
    elif metric == 'weight':
        rows = Weight.query.order_by(Weight.id.desc()).limit(limit).all()
    else:
        return jsonify({'error': 'unknown metric'}), 400

    def row_to_dict(r):
        return {c.name: getattr(r, c.name) for c in r.__table__.columns}
    return jsonify([row_to_dict(r) for r in rows])