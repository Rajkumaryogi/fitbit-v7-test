"""
Fitbit Subscription API — register push notifications so new cloud data triggers your subscriber URL.

See https://dev.fitbit.com/build/reference/web-api/subscription/create-subscription/
"""
from __future__ import annotations

import hashlib
import logging
import os
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

API_ROOT = os.getenv("FITBIT_API_ROOT", "https://api.fitbit.com")


def subscription_id_for_cognito(cognito_user_id: str) -> str:
    """Stable id <= 50 chars, unique per Cognito user for the 'all collections' subscription."""
    h = hashlib.sha256(str(cognito_user_id).encode("utf-8")).hexdigest()[:24]
    sid = f"v7-{h}"
    return sid[:50]


def _subscriber_headers(access_token: str, subscriber_id: Optional[str]) -> dict[str, str]:
    h: dict[str, str] = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Length": "0",
    }
    sid = (subscriber_id or os.getenv("FITBIT_SUBSCRIBER_ID") or "").strip()
    if sid:
        h["X-Fitbit-Subscriber-Id"] = sid
    return h


def create_all_collections_subscription(
    access_token: str,
    cognito_user_id: str,
    *,
    subscriber_id: Optional[str] = None,
) -> dict[str, Any]:
    """
    POST /1/user/-/apiSubscriptions/{subscription-id}.json — one subscription for all
    collection types (activities, body, foods, sleep, userRevokedAccess, …) per Fitbit docs.
    Requires matching OAuth scopes (see app/auth.py).
    """
    sub_id = subscription_id_for_cognito(cognito_user_id)
    url = f"{API_ROOT}/1/user/-/apiSubscriptions/{sub_id}.json"
    headers = _subscriber_headers(access_token, subscriber_id)
    try:
        r = requests.post(url, headers=headers, data="", timeout=45)
        ok = r.status_code in (200, 201)
        if r.status_code == 409:
            # Already subscribed for this stream / id — treat as success
            ok = True
            logger.info("Fitbit subscription already exists (409) for sub_id=%s", sub_id)
        elif not ok:
            logger.warning(
                "Fitbit subscription POST %s -> %s %s",
                sub_id,
                r.status_code,
                (r.text or "")[:500],
            )
        else:
            logger.info("Fitbit subscription created/ok sub_id=%s http=%s", sub_id, r.status_code)
        try:
            body = r.json() if r.text else {}
        except Exception:
            body = {"raw": (r.text or "")[:300]}
        return {"ok": ok, "status_code": r.status_code, "subscription_id": sub_id, "body": body}
    except Exception as e:
        logger.warning("Fitbit subscription request failed: %s", e)
        return {"ok": False, "error": str(e), "subscription_id": sub_id}


def ensure_fitbit_subscriptions(
    access_token: str,
    cognito_user_id: str,
    *,
    subscriber_id: Optional[str] = None,
) -> dict[str, Any]:
    """Idempotent: (re)create the all-collections subscription for this user."""
    if not access_token or not cognito_user_id:
        return {"ok": False, "error": "missing access_token or cognito_user_id"}
    return create_all_collections_subscription(
        access_token, cognito_user_id, subscriber_id=subscriber_id
    )
