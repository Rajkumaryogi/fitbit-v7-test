"""
DynamoDB client for Fitbit connector.

Tables (override with env):
  TOKENS_TABLE — default ``vitals-di-tokens`` (partition key ``userId`` String, same item shape as legacy vitals7-tokens).
  USER_VITALS_TABLE — default ``user_vitals`` (vitals7api-vitals schema).

Create ``vitals-di-tokens`` once in AWS, e.g.::

  aws dynamodb create-table --table-name vitals-di-tokens \\
    --attribute-definitions AttributeName=userId,AttributeType=S \\
    --key-schema AttributeName=userId,KeyType=HASH \\
    --billing-mode PAY_PER_REQUEST --region us-east-1

Credentials: ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, optional ``AWS_SESSION_TOKEN``,
``AWS_REGION``. Optional ``AWS_DYNAMODB_ENDPOINT`` for a custom DynamoDB endpoint.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

TOKENS_TABLE = os.getenv("TOKENS_TABLE", "vitals-di-tokens")
USER_VITALS_TABLE = os.getenv("USER_VITALS_TABLE", "user_vitals")

_ddb_resource = None


def _get_resource():
    """Singleton boto3 DynamoDB resource (creating a new resource per call is very slow)."""
    global _ddb_resource
    if _ddb_resource is not None:
        return _ddb_resource
    region = os.getenv("AWS_REGION", "us-east-1")
    kwargs: dict[str, Any] = {"region_name": region}
    endpoint = (os.getenv("AWS_DYNAMODB_ENDPOINT") or "").strip()
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    ak = (os.getenv("AWS_ACCESS_KEY_ID") or "").strip()
    sk = (os.getenv("AWS_SECRET_ACCESS_KEY") or "").strip()
    st = (os.getenv("AWS_SESSION_TOKEN") or "").strip()
    if ak and sk:
        kwargs["aws_access_key_id"] = ak
        kwargs["aws_secret_access_key"] = sk
        if st:
            kwargs["aws_session_token"] = st
    _ddb_resource = boto3.resource("dynamodb", **kwargs)
    return _ddb_resource


FITBIT_KEY_SUFFIX = "#fitbit"


def _fitbit_key(cognito_user_id: str) -> str:
    """DynamoDB key for a Fitbit token row: {cognitoUserId}#fitbit."""
    return f"{cognito_user_id}{FITBIT_KEY_SUFFIX}"


def save_tokens(
    user_id: str,
    access_token: str,
    refresh_token: str,
    expires_in: int,
    api_name: str = "Fitbit",
    fitbit_user_id: Optional[str] = None,
) -> None:
    """Save Fitbit tokens to TOKENS_TABLE (default vitals-di-tokens). Key = {user_id}#fitbit per device."""
    table = _get_resource().Table(TOKENS_TABLE)
    now_ms = int(time.time() * 1000)
    expires_at = now_ms + (expires_in * 1000)
    item = {
        "userId": _fitbit_key(user_id),
        "cognito_user_id": user_id,
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_in": expires_in,
        "expires_at": expires_at,
        "created_at": now_ms,
        "updated_at": now_ms,
        "api_name": api_name,
        "token_type": "bearer",
    }
    if fitbit_user_id:
        item["fitbit_user_id"] = fitbit_user_id
    table.put_item(Item=item)
    logger.info("Saved Fitbit tokens to DynamoDB for user %s (key %s)", user_id, _fitbit_key(user_id))


def get_tokens(user_id: str) -> Optional[dict]:
    """Get Fitbit tokens for a Cognito user. Tries {user_id}#fitbit first, falls back to bare user_id (old format)."""
    global _ddb_resource
    table = _get_resource().Table(TOKENS_TABLE)
    try:
        r = table.get_item(Key={"userId": _fitbit_key(user_id)})
        item = r.get("Item")
        if item:
            return item
        r2 = table.get_item(Key={"userId": user_id})
        item2 = r2.get("Item")
        if item2 and str(item2.get("api_name", "")).lower() == "fitbit":
            return item2
        return None
    except ClientError as e:
        code = (e.response or {}).get("Error", {}).get("Code", "") or ""
        if code in ("ExpiredTokenException", "UnrecognizedClientException", "InvalidClientTokenId"):
            logger.error(
                "get_tokens failed (%s): refresh AWS credentials (e.g. rotate "
                "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN on Render, or use an IAM role). %s",
                code,
                e,
            )
            _ddb_resource = None
        else:
            logger.warning("get_tokens failed: %s", e)
        return None


def remove_tokens(user_id: str) -> None:
    """Remove Fitbit tokens for user. Deletes both new (#fitbit) and old (bare) key formats."""
    try:
        table = _get_resource().Table(TOKENS_TABLE)
        table.delete_item(Key={"userId": _fitbit_key(user_id)})
        r = table.get_item(Key={"userId": user_id})
        item = r.get("Item")
        if item and str(item.get("api_name", "")).lower() == "fitbit":
            table.delete_item(Key={"userId": user_id})
        logger.info("Removed Fitbit tokens for user %s from DynamoDB", user_id)
    except ClientError as e:
        logger.warning("remove_tokens failed: %s", e)


def remove_all_fitbit_tokens() -> int:
    """Remove all Fitbit token rows from DynamoDB. Returns count removed."""
    try:
        table = _get_resource().Table(TOKENS_TABLE)
        r = table.scan(
            ProjectionExpression="userId",
            FilterExpression="api_name = :fn",
            ExpressionAttributeValues={":fn": "Fitbit"},
        )
        items = r.get("Items", [])
        for it in items:
            table.delete_item(Key={"userId": it["userId"]})
        return len(items)
    except Exception as e:
        logger.warning("remove_all_fitbit_tokens failed: %s", e)
        return 0


def get_cognito_id_by_fitbit_owner_id(fitbit_owner_id: str) -> Optional[str]:
    """
    Find Cognito user id for a Fitbit ownerId (Subscriber API notifications).
    Scans TOKENS_TABLE for api_name Fitbit and matching fitbit_user_id.

    Paginates the full table: a single scan page can miss the row (DynamoDB 1MB
    limit per scan) which would break webhooks while still returning 204.
    """
    if not fitbit_owner_id:
        return None
    fid = str(fitbit_owner_id).strip()
    if not fid:
        return None
    try:
        table = _get_resource().Table(TOKENS_TABLE)
        # Case variants on api_name; ownerId must match stored fitbit_user_id string.
        kwargs: dict[str, Any] = {
            "FilterExpression": "(api_name = :a OR api_name = :b) AND fitbit_user_id = :fid",
            "ExpressionAttributeValues": {":a": "Fitbit", ":b": "fitbit", ":fid": fid},
            "ProjectionExpression": "userId, cognito_user_id, fitbit_user_id",
        }
        start_key = None
        while True:
            if start_key:
                kwargs["ExclusiveStartKey"] = start_key
            r = table.scan(**kwargs)
            for it in r.get("Items", []):
                cid = it.get("cognito_user_id")
                if cid:
                    return str(cid)
                uid = it.get("userId") or ""
                if isinstance(uid, str) and uid.endswith(FITBIT_KEY_SUFFIX):
                    return uid[: -len(FITBIT_KEY_SUFFIX)]
            start_key = r.get("LastEvaluatedKey")
            if not start_key:
                break
        return None
    except Exception as e:
        logger.warning("get_cognito_id_by_fitbit_owner_id failed: %s", e)
        return None


def has_any_fitbit_tokens() -> bool:
    """Return True if DynamoDB has any Fitbit tokens (e.g. from Vitals7 Connect flow)."""
    try:
        table = _get_resource().Table(TOKENS_TABLE)
        r = table.scan(
            ProjectionExpression="userId",
            FilterExpression="api_name = :fn",
            ExpressionAttributeValues={":fn": "Fitbit"},
            Limit=1,
        )
        return bool(r.get("Items"))
    except Exception as e:
        logger.warning("has_any_fitbit_tokens failed: %s", e)
        return False


def _recorded_at_epoch(iso_s: str) -> int:
    """Match vitals7api-vitals: recordedAt stored as epoch seconds (UTC)."""
    s = (iso_s or "").strip().replace("Z", "+00:00")
    if not s:
        return int(time.time())
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return int(time.time())
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp())


def _normalize_source_id(source_id: Optional[str]) -> str:
    if source_id is not None and str(source_id).strip():
        return str(source_id).strip().replace("#", "_")[:180]
    return ""


def _deterministic_source_id(payload: dict) -> str:
    """Same idea as VitalsRepository.create_vitals when sourceId is absent."""
    vitals = payload.get("vitals") or []
    vit_blob = sorted(
        (
            {"vitalType": str(v.get("vitalType", "")), "value": str(v.get("value", "")), "units": str(v.get("units", ""))}
            for v in vitals
            if isinstance(v, dict)
        ),
        key=lambda x: x["vitalType"],
    )
    blob = json.dumps(
        {"vitals": vit_blob, "recordedAt": payload.get("recordedAt"), "recordedBy": payload.get("recordedBy")},
        sort_keys=True,
    )
    return "auto_" + hashlib.sha256(blob.encode()).hexdigest()[:28]


def save_payloads_to_user_vitals(cognito_user_id: str, payloads: list[dict]) -> int:
    """
    Write Fitbit-derived payloads to user_vitals using the same item_key / fields as vitals7api-vitals
    VitalsRepository.create_vitals (one Dynamo row per vital reading).

    Uses batch_get_item + batch_writer instead of per-row get+put (orders of magnitude fewer round trips).
    """
    if not cognito_user_id or not payloads:
        if cognito_user_id and not payloads:
            logger.info("user_vitals save: 0 payloads (nothing to write) user=%s", str(cognito_user_id)[:8])
        return 0
    table = _get_resource().Table(USER_VITALS_TABLE)
    client = table.meta.client
    created_epoch = int(datetime.now(tz=timezone.utc).timestamp())
    # item_key -> full item (last duplicate key wins)
    by_key: dict[str, dict[str, Any]] = {}

    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        vitals = payload.get("vitals") or []
        if not vitals:
            continue
        raw_sid = _normalize_source_id(payload.get("sourceId"))
        if not raw_sid:
            raw_sid = _deterministic_source_id(payload)
        source_id = raw_sid.replace("#", "_")[:180]
        recorded_epoch = _recorded_at_epoch(str(payload.get("recordedAt") or ""))
        recorded_by = str(payload.get("recordedBy") or "Fitbit")
        device_used = payload.get("deviceUsed")

        for vital in vitals:
            if not isinstance(vital, dict):
                continue
            vtype = str(vital.get("vitalType") or "").strip()
            if not vtype:
                continue
            val = vital.get("value")
            units = str(vital.get("units") or "")
            row_id = str(uuid.uuid4())
            item_key = f"VIT#{vtype}#SRC#{source_id}#{vtype}"

            item: dict[str, Any] = {
                "user_id": cognito_user_id,
                "item_key": item_key,
                "id": row_id,
                "vitalType": vtype,
                "value": str(val),
                "units": units,
                "recordedAt": recorded_epoch,
                "recordedBy": recorded_by,
                "created_at": created_epoch,
                "created_by": cognito_user_id,
                "sourceId": source_id,
            }
            if device_used:
                item["deviceUsed"] = device_used
            by_key[item_key] = item

    if not by_key:
        logger.info(
            "user_vitals save: %d payload(s), 0 keys (payloads had no vitals) user=%s",
            len(payloads),
            str(cognito_user_id)[:8],
        )
        return 0

    keys = [{"user_id": cognito_user_id, "item_key": k} for k in by_key.keys()]
    existing_map: dict[str, dict[str, Any]] = {}
    for i in range(0, len(keys), 100):
        chunk = keys[i : i + 100]
        req: dict[str, Any] = {
            USER_VITALS_TABLE: {
                "Keys": chunk,
                "ProjectionExpression": "user_id, item_key, #v, id, created_at, created_by",
                "ExpressionAttributeNames": {"#v": "value"},
            }
        }
        try:
            while req:
                resp = client.batch_get_item(RequestItems=req)
                for it in resp.get("Responses", {}).get(USER_VITALS_TABLE, []):
                    ik = it.get("item_key")
                    if ik:
                        existing_map[str(ik)] = it
                unproc = resp.get("UnprocessedKeys") or {}
                req = unproc if unproc else {}
        except ClientError as e:
            logger.warning("batch_get_item failed: %s", e)

    to_put: list[dict[str, Any]] = []
    for item_key, item in by_key.items():
        ex = existing_map.get(item_key)
        if ex and str(ex.get("value", "")) == str(item["value"]):
            continue
        if ex:
            item["id"] = ex.get("id", item["id"])
            item["created_at"] = ex.get("created_at", item["created_at"])
            item["created_by"] = ex.get("created_by", item["created_by"])
        to_put.append(item)

    if not to_put:
        logger.info(
            "user_vitals save: payloads=%d keys=%d all values unchanged (no put) user=%s",
            len(payloads),
            len(by_key),
            str(cognito_user_id)[:8],
        )
        return 0

    try:
        with table.batch_writer() as batch:
            for item in to_put:
                batch.put_item(Item=item)
    except ClientError as e:
        logger.warning("save_payloads_to_user_vitals batch put failed: %s", e)
        return 0

    return len(to_put)


def update_last_vitals7_push_at(user_id: str, timestamp_ms: int) -> None:
    """Store epoch-ms of last successful user_vitals write on the Fitbit token row (name kept for compatibility)."""
    try:
        table = _get_resource().Table(TOKENS_TABLE)
        table.update_item(
            Key={"userId": _fitbit_key(user_id)},
            UpdateExpression="SET last_vitals7_push_at = :ts, updated_at = :now",
            ExpressionAttributeValues={":ts": timestamp_ms, ":now": int(time.time() * 1000)},
        )
    except ClientError as e:
        logger.warning("update_last_vitals7_push_at failed: %s", e)
