"""Loader base utilities. The payload hash matches beacon-indexer's."""
import hashlib
import json
from typing import Any, Dict


def calculate_payload_hash(data: Dict[str, Any]) -> str:
    """Deterministic 64-bit hex hash of a payload (sorted keys)."""
    try:
        payload_json = json.dumps(data, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()[:16]
    except Exception:
        return hashlib.sha256(str(data).encode("utf-8")).hexdigest()[:16]


def canonical_json(data: Dict[str, Any]) -> str:
    """Canonical JSON string stored in the raw audit log."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
