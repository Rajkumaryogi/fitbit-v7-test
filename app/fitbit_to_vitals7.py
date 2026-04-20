"""
Transform Fitbit API responses into normalized payloads for DynamoDB user_vitals.
Each function returns a list of dicts with vitals[], recordedAt, recordedBy, deviceUsed, sourceId
(consumed by fitbit_app.dynamodb_client.save_payloads_to_user_vitals).
"""
import logging
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _iso(ts: Any) -> str:
    if ts is None:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(ts, str):
        if ts.endswith("Z") or "+" in ts:
            return ts
        return ts + "T00:00:00Z"
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _source_id(record: dict, fallback: Any = None, *, kind: str) -> str:
    """
    Withings-style scoped sourceId: `fitbit_{kind}_{vendorStableId}` so the same calendar day
    does not collide across metrics (e.g. steps vs heart-rate) on Vitals7 upserts.
    """
    summ = record.get("summary") if isinstance(record.get("summary"), dict) else {}
    inner = (
        record.get("logId")
        or record.get("id")
        or record.get("dateTime")
        or record.get("date")
        or record.get("dateOfSleep")
        or record.get("startTime")
        or summ.get("date")
        or summ.get("dateTime")
        or fallback
    )
    if inner is not None and str(inner).strip() != "":
        part = str(inner).strip().replace("#", "_")
        return f"fitbit_{kind}_{part}"[:512]
    return f"fitbit_{kind}"[:512]


def _fitbit_weight_units(record: dict) -> str:
    """Unit label from Fitbit log when present; otherwise infer scale from magnitude (value is never converted)."""
    u = record.get("weightUnit") or record.get("unit")
    if u is not None and str(u).strip():
        s = str(u).strip().lower()
        if s in ("lbs", "lb", "pounds", "pound"):
            return "lbs"
        if s in ("kg", "kilograms", "kilogram"):
            return "kg"
        if "stone" in s or s == "st":
            return "st"
        return str(u).strip()
    try:
        v = float(record.get("weight") or 0)
    except (TypeError, ValueError):
        return "kg"
    # Typical adult: kg ~40–180, lbs ~80–400 — label only, value unchanged.
    if v >= 80:
        return "lbs"
    return "kg"


# --- Heart rate (activities-heart)
def transform_heart_rate(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or "activities-heart" not in response:
        return out
    for act in response.get("activities-heart", []):
        date = act.get("date")
        value = act.get("value") or {}
        resting = value.get("restingHeartRate")
        if resting is not None:
            out.append({
                "vitals": [{"vitalType": "heart-rate", "value": int(resting), "units": "bpm"}],
                "recordedAt": _iso(date),
                "recordedBy": "Fitbit",
                "deviceUsed": "Fitbit",
                "sourceId": _source_id(act, date, kind="heart-rate"),
            })
    return out


# --- Sleep
def transform_sleep(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or "sleep" not in response:
        return out
    for rec in response.get("sleep", []):
        date = rec.get("dateOfSleep") or rec.get("date")
        if rec.get("minutesAsleep") is not None:
            try:
                m = float(rec.get("minutesAsleep") or 0)
            except (TypeError, ValueError):
                m = 0.0
            if m > 0:
                out.append({
                    "vitals": [{"vitalType": "sleep-hours", "value": m, "units": "minutes"}],
                    "recordedAt": _iso(date),
                    "recordedBy": "Fitbit",
                    "deviceUsed": "Fitbit",
                    "sourceId": _source_id(rec, rec.get("logId") or date, kind="sleep-hours"),
                })
            continue
        dur = rec.get("duration")
        if dur is not None:
            try:
                dms = float(dur)
            except (TypeError, ValueError):
                continue
            if dms > 0:
                out.append({
                    "vitals": [{"vitalType": "sleep-hours", "value": dms, "units": "ms"}],
                    "recordedAt": _iso(date),
                    "recordedBy": "Fitbit",
                    "deviceUsed": "Fitbit",
                    "sourceId": _source_id(rec, rec.get("logId") or date, kind="sleep-duration-ms"),
                })
    return out


# --- Steps (activities-steps)
def transform_steps(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or "activities-steps" not in response:
        return out
    for act in response.get("activities-steps", []):
        date = act.get("date")
        value = int(act.get("value") or 0)
        out.append({
            "vitals": [{"vitalType": "steps", "value": value, "units": "count"}],
            "recordedAt": _iso(date),
            "recordedBy": "Fitbit",
            "deviceUsed": "Fitbit",
            "sourceId": _source_id(act, date, kind="steps"),
        })
    return out


# --- Weight (body/log/weight)
def transform_weight(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or "weight" not in response:
        return out
    for w in response.get("weight", []):
        val = w.get("weight")
        if val is not None:
            date = w.get("date") or w.get("logId")
            v = float(val)
            wu = _fitbit_weight_units(w)
            out.append({
                "vitals": [{"vitalType": "weight", "value": round(v, 4), "units": wu}],
                "recordedAt": _iso(date),
                "recordedBy": "Fitbit",
                "deviceUsed": "Fitbit",
                "sourceId": _source_id(w, w.get("logId") or date, kind="weight"),
            })
    return out


# --- Blood pressure (bp)
def transform_blood_pressure(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or "bp" not in response:
        return out
    for bp in response.get("bp", []):
        systolic = bp.get("systolic")
        diastolic = bp.get("diastolic")
        date = bp.get("date") or bp.get("logId")
        vitals = []
        if systolic is not None:
            vitals.append({"vitalType": "blood-pressure-systolic", "value": int(systolic), "units": "mmHg"})
        if diastolic is not None:
            vitals.append({"vitalType": "blood-pressure-diastolic", "value": int(diastolic), "units": "mmHg"})
        if vitals:
            out.append({
                "vitals": vitals,
                "recordedAt": _iso(date),
                "recordedBy": "Fitbit",
                "deviceUsed": "Fitbit",
                "sourceId": _source_id(bp, bp.get("logId") or date, kind="blood-pressure"),
            })
    return out


# --- Body fat (body/fat)
def transform_body_fat(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or "fat" not in response:
        return out
    for f in response.get("fat", []):
        val = f.get("fat")
        if val is not None:
            date = f.get("date") or f.get("logId")
            out.append({
                "vitals": [{"vitalType": "body-fat-percentage", "value": float(val), "units": "%"}],
                "recordedAt": _iso(date),
                "recordedBy": "Fitbit",
                "deviceUsed": "Fitbit",
                "sourceId": _source_id(f, f.get("logId") or date, kind="body-fat"),
            })
    return out


# --- Activity / exercise logs (activities/list)
def transform_activities(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or not isinstance(response, dict):
        return out
    for act in response.get("activities") or []:
        dur_ms = act.get("duration") or 0
        try:
            dur_ms = int(dur_ms)
        except (TypeError, ValueError):
            dur_ms = 0
        cals = act.get("calories")
        if cals is not None:
            try:
                cals = float(cals)
            except (TypeError, ValueError):
                cals = 0.0
        else:
            cals = 0.0
        start = act.get("startTime") or act.get("startDate")
        vitals = []
        if dur_ms > 0:
            vitals.append({"vitalType": "exercise-duration", "value": dur_ms, "units": "ms"})
        if cals and cals > 0:
            vitals.append({"vitalType": "active-calories", "value": cals, "units": "kcal"})
        if not vitals:
            continue
        out.append({
            "vitals": vitals,
            "recordedAt": _iso(start),
            "recordedBy": "Fitbit",
            "deviceUsed": "Fitbit",
            "sourceId": _source_id(act, act.get("logId") or start, kind="activity"),
        })
    return out


# --- Food log / daily nutrition summary
def transform_nutrition(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or not isinstance(response, dict):
        return out
    summary = response.get("summary") or {}
    if not summary:
        return out
    date = response.get("date") or summary.get("date")
    vitals = []
    cal = summary.get("calories") or summary.get("caloriesIn")
    if cal is not None:
        vitals.append({"vitalType": "nutrition-calories", "value": float(cal), "units": "kcal"})
    for key, vtype, unit in (
        ("protein", "dietary-protein", "g"),
        ("carbs", "dietary-carbs", "g"),
        ("fat", "dietary-fat", "g"),
    ):
        v = summary.get(key)
        if v is not None:
            try:
                vitals.append({"vitalType": vtype, "value": float(v), "units": unit})
            except (TypeError, ValueError):
                pass
    if not vitals:
        return out
    sid = _source_id(response, date, kind="nutrition")
    out.append({
        "vitals": vitals,
        "recordedAt": _iso(date),
        "recordedBy": "Fitbit",
        "deviceUsed": "Fitbit",
        "sourceId": sid,
    })
    return out


# --- Water / hydration (one or more daily blocks from merge)
def transform_hydration(response: Any, user_id: str) -> list[dict]:
    out = []
    rows = response if isinstance(response, list) else ([response] if response else [])
    for block in rows:
        if not isinstance(block, dict):
            continue
        summ = block.get("summary") if isinstance(block.get("summary"), dict) else block
        if not isinstance(summ, dict):
            continue
        water = summ.get("water")
        if water is None:
            continue
        try:
            w = float(water)
        except (TypeError, ValueError):
            continue
        raw_unit = summ.get("waterUnit")
        if raw_unit is not None and str(raw_unit).strip():
            wu = str(raw_unit).strip()
        else:
            wu = "ml"
        label = block.get("_fitbitDayLabel") or "day"
        day = summ.get("date") or label
        out.append({
            "vitals": [{"vitalType": "water-intake", "value": round(w, 4), "units": wu}],
            "recordedAt": _iso(summ.get("date") or day),
            "recordedBy": "Fitbit",
            "deviceUsed": "Fitbit",
            "sourceId": _source_id(
                {**summ, "_fitbitDayLabel": label},
                f"{label}_{day}",
                kind="water",
            ),
        })
    return out


# --- Oxygen saturation
def transform_oxygen(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or not isinstance(response, dict):
        return out
    readings = response.get("oxygenData") or response.get("values") or []
    for rec in readings:
        val = rec.get("value") or rec.get("spo2")
        if val is not None:
            date = rec.get("dateTime") or rec.get("date")
            out.append({
                "vitals": [{"vitalType": "oxygen-saturation", "value": float(val), "units": "%"}],
                "recordedAt": _iso(date),
                "recordedBy": "Fitbit",
                "deviceUsed": "Fitbit",
                "sourceId": _source_id(rec, date, kind="spo2"),
            })
    return out


# --- Respiratory rate
def transform_respiratory(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or "respiratoryRateValues" not in response:
        return out
    for rec in response.get("respiratoryRateValues", []):
        val = rec.get("value") or rec.get("breathingRate")
        if val is not None:
            date = rec.get("dateTime") or rec.get("date")
            out.append({
                "vitals": [{"vitalType": "respiratory-rate", "value": float(val), "units": "breaths/min"}],
                "recordedAt": _iso(date),
                "recordedBy": "Fitbit",
                "deviceUsed": "Fitbit",
                "sourceId": _source_id(rec, date, kind="respiratory-rate"),
            })
    return out


# --- Temperature (temp/core, temp/skin, legacy body/temperature)
def transform_temperature(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response:
        return out
    if "temp" not in response and "temperature" not in response:
        return out
    data = response.get("temp") or response.get("temperature") or []
    for rec in (data if isinstance(data, list) else [data]):
        if not isinstance(rec, dict):
            continue
        src = rec.get("_fitbitTempSource")
        val = rec.get("temp") if rec.get("temp") is not None else rec.get("value")
        if val is None:
            continue
        date = rec.get("dateTime") or rec.get("date") or rec.get("logId")

        # Skin sensor: nightlyRelative is delta vs personal baseline (Fitbit temp/skin API; unit follows user locale).
        if src == "skin_delta":
            try:
                delta = float(val)
            except (TypeError, ValueError):
                continue
            out.append(
                {
                    "vitals": [
                        {
                            "vitalType": "body-temperature",
                            "value": round(delta, 4),
                            "units": rec.get("tempUnit") or "Δ vs baseline (Fitbit)",
                        }
                    ],
                    "recordedAt": _iso(date),
                    "recordedBy": "Fitbit",
                    "deviceUsed": "Fitbit Skin Temperature",
                    "sourceId": _source_id(rec, rec.get("logId") or date, kind="body-temperature-skin"),
                }
            )
            continue

        # Core (manual) and legacy: absolute body temperature (value and scale as Fitbit sent).
        try:
            fv = float(val)
        except (TypeError, ValueError):
            continue
        tu = rec.get("tempUnit") or rec.get("unit")
        if tu is not None and str(tu).strip():
            tunit = str(tu).strip()
        elif fv <= 50:
            tunit = "°C"
        else:
            tunit = "°F"
        device = "Fitbit Core Temperature" if src == "core" else "Fitbit"
        kind = "body-temperature-core" if src == "core" else "body-temperature"
        out.append(
            {
                "vitals": [{"vitalType": "body-temperature", "value": round(fv, 4), "units": tunit}],
                "recordedAt": _iso(date),
                "recordedBy": "Fitbit",
                "deviceUsed": device,
                "sourceId": _source_id(rec, rec.get("logId") or date, kind=kind),
            }
        )
    return out


# --- VO2 Max / Cardio Fitness Score
def transform_vo2_max(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or "cardioScore" not in response:
        return out
    for rec in response.get("cardioScore", []):
        date = rec.get("dateTime")
        val = (rec.get("value") or {}).get("vo2Max")
        if val is None:
            continue
        # Fitbit may return a range like "37-41"; take the midpoint
        if isinstance(val, str) and "-" in val:
            parts = val.split("-")
            try:
                val = round((float(parts[0]) + float(parts[1])) / 2, 1)
            except (ValueError, IndexError):
                continue
        else:
            try:
                val = round(float(val), 1)
            except (ValueError, TypeError):
                continue
        out.append({
            "vitals": [{"vitalType": "vo2-max", "value": val, "units": "mL/kg/min"}],
            "recordedAt": _iso(date),
            "recordedBy": "Fitbit",
            "deviceUsed": "Fitbit",
            "sourceId": _source_id(rec, date, kind="vo2-max"),
        })
    return out


# --- Heart Rate Variability
def transform_hrv(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or "hrv" not in response:
        return out
    for rec in response.get("hrv", []):
        date = rec.get("dateTime")
        val = (rec.get("value") or {}).get("dailyRmssd")
        if val is not None:
            out.append({
                "vitals": [{"vitalType": "heart-rate-variability", "value": round(float(val), 2), "units": "ms"}],
                "recordedAt": _iso(date),
                "recordedBy": "Fitbit",
                "deviceUsed": "Fitbit",
                "sourceId": _source_id(rec, date, kind="hrv"),
            })
    return out


# --- ECG
def transform_ecg(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or "ecgReadings" not in response:
        return out
    # Encode result as numeric: 0=Normal, 1=AFib, 2=Unclassified, 3=No analysis
    result_map = {"NORMAL": 0, "SINUS_RHYTHM": 0, "AFIB_DETECTED": 1, "UNCLASSIFIED": 2, "NO_ANALYSIS": 3}
    for rec in response.get("ecgReadings", []):
        result = rec.get("resultClassification", "UNCLASSIFIED")
        val = result_map.get(result, 2)
        ts = rec.get("startTime")
        out.append({
            "vitals": [{"vitalType": "ecg-result", "value": val, "units": result}],
            "recordedAt": _iso(ts),
            "recordedBy": "Fitbit",
            "deviceUsed": "Fitbit",
            "sourceId": _source_id(rec, ts, kind="ecg"),
        })
    return out


# --- Active Zone Minutes
def transform_active_zone_minutes(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response or "activities-active-zone-minutes" not in response:
        return out
    for rec in response.get("activities-active-zone-minutes", []):
        date = rec.get("dateTime")
        val = (rec.get("value") or {}).get("activeZoneMinutes")
        if val is not None:
            out.append({
                "vitals": [{"vitalType": "active-zone-minutes", "value": int(val), "units": "minutes"}],
                "recordedAt": _iso(date),
                "recordedBy": "Fitbit",
                "deviceUsed": "Fitbit",
                "sourceId": _source_id(rec, date, kind="active-zone-minutes"),
            })
    return out


# --- Blood Glucose
def transform_blood_glucose(response: Optional[dict], user_id: str) -> list[dict]:
    out = []
    if not response:
        return out
    readings = response.get("bgReadings") or response.get("activities-glucose") or []
    for rec in readings:
        val = rec.get("glucose") or rec.get("value")
        if val is None:
            continue
        date = rec.get("dateTime") or rec.get("date")
        time_part = rec.get("time", "")
        ts = f"{date}T{time_part}" if date and time_part else date
        unit = rec.get("unit") or rec.get("glucoseUnit") or "mg/dL"
        out.append({
            "vitals": [{"vitalType": "blood-glucose", "value": round(float(val), 4), "units": str(unit)}],
            "recordedAt": _iso(ts or date),
            "recordedBy": "Fitbit",
            "deviceUsed": "Fitbit",
            "sourceId": _source_id(rec, ts or date, kind="blood-glucose"),
        })
    return out


def all_payloads(
    hr_data=None,
    sleep_data=None,
    steps_data=None,
    weight_data=None,
    activities_data=None,
    nutrition_data=None,
    hydration_data=None,
    bp_data=None,
    bodyfat_data=None,
    oxygen_data=None,
    respiratory_data=None,
    temp_data=None,
    vo2_max_data=None,
    hrv_data=None,
    ecg_data=None,
    active_zone_minutes_data=None,
    blood_glucose_data=None,
    user_id: str = "",
    **_ignored,  # irn_data, devices_data, etc.
) -> list[dict]:
    """Build all Vitals7 payloads from Fitbit API responses."""
    payloads = []
    payloads.extend(transform_heart_rate(hr_data, user_id))
    payloads.extend(transform_sleep(sleep_data, user_id))
    payloads.extend(transform_steps(steps_data, user_id))
    payloads.extend(transform_weight(weight_data, user_id))
    payloads.extend(transform_activities(activities_data, user_id))
    payloads.extend(transform_nutrition(nutrition_data, user_id))
    payloads.extend(transform_hydration(hydration_data, user_id))
    payloads.extend(transform_blood_pressure(bp_data, user_id))
    payloads.extend(transform_body_fat(bodyfat_data, user_id))
    payloads.extend(transform_oxygen(oxygen_data, user_id))
    payloads.extend(transform_respiratory(respiratory_data, user_id))
    payloads.extend(transform_temperature(temp_data, user_id))
    payloads.extend(transform_vo2_max(vo2_max_data, user_id))
    payloads.extend(transform_hrv(hrv_data, user_id))
    payloads.extend(transform_ecg(ecg_data, user_id))
    payloads.extend(transform_active_zone_minutes(active_zone_minutes_data, user_id))
    payloads.extend(transform_blood_glucose(blood_glucose_data, user_id))
    return payloads
