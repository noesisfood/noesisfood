from __future__ import annotations

import copy
import logging
from collections import defaultdict
from typing import Any, Dict


_EVENT_COUNTERS: Dict[str, int] = defaultdict(int)


def _normalize_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple, set)):
        return [_normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize_value(val) for key, val in value.items()}
    return str(value)


def log_event(logger: logging.Logger, event: str, **fields: Any) -> Dict[str, Any]:
    payload = {"event": str(event or "").strip() or "unknown_event"}
    for key, value in fields.items():
        payload[str(key)] = _normalize_value(value)
    _EVENT_COUNTERS[payload["event"]] += 1
    logger.info("%s", payload)
    return payload


def get_event_counters() -> Dict[str, int]:
    return copy.deepcopy(dict(_EVENT_COUNTERS))


def get_beta_monitoring_summary() -> Dict[str, Any]:
    counters = get_event_counters()
    return {
        "monitoring_window": "current_process_lifetime",
        "scan_started": int(counters.get("scan_started", 0)),
        "scan_completed": int(counters.get("scan_completed", 0)),
        "scan_failed": int(counters.get("scan_failed", 0)),
        "feedback_submitted": int(counters.get("feedback_submission_completed", 0)),
        "feedback_failed": int(counters.get("feedback_submission_failed", 0)),
        "correction_submitted": int(counters.get("correction_submitted", 0)),
    }


def reset_event_counters() -> None:
    _EVENT_COUNTERS.clear()
