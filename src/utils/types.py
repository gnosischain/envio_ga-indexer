"""GraphQL scalar -> ClickHouse type mapping + value coercion.

Pure functions, unit-tested. Strategy (see plan):
  ID / String              -> String
  Int (block field)        -> UInt64
  Int (other)              -> Int64        (holds timestamps, counts, indices; signed-safe)
  BigInt / numeric         -> Int256       (balances/values CAN be negative; covers all real
                                            magnitudes up to ~5.78e76. Fields that use the
                                            uint256-max sentinel (e.g. expiryTime) override to
                                            UInt256 via entities.yaml `field_types`.)
  Boolean                  -> Bool
  Float                    -> Float64
  jsonb / json             -> String       (stored as JSON text)
  any other custom scalar  -> LowCardinality(String)   (enums)

We use sentinel defaults ('' / 0 / false) rather than Nullable(T).
"""
import re
from typing import Any

# GraphQL scalar type names (lowercased) we recognise explicitly.
_STRING = {"id", "string", "text", "bytea", "citext"}
_BOOL = {"boolean", "bool"}
_INT = {"int", "integer", "int4", "smallint", "int2"}
_BIGNUM = {"bigint", "numeric", "uint256", "biginteger", "int8", "decimal"}
_FLOAT = {"float", "float8", "double", "double precision", "real"}
_JSON = {"jsonb", "json"}
_TS = {"timestamptz", "timestamp", "timestamp without time zone", "timestamp with time zone"}

# Integer column ranges, for defensive clamping (a stray out-of-range value should
# never abort an entire insert batch).
_RANGES = {
    "Int256": (-(2 ** 255), 2 ** 255 - 1),
    "UInt256": (0, 2 ** 256 - 1),
    "Int128": (-(2 ** 127), 2 ** 127 - 1),
    "UInt128": (0, 2 ** 128 - 1),
    "Int64": (-(2 ** 63), 2 ** 63 - 1),
    "UInt64": (0, 2 ** 64 - 1),
    "Int32": (-(2 ** 31), 2 ** 31 - 1),
    "UInt32": (0, 2 ** 32 - 1),
}


def to_snake(name: str) -> str:
    """camelCase / PascalCase GraphQL field name -> snake_case ClickHouse column."""
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.replace("__", "_").lower()


def is_block_field(gql_name: str) -> bool:
    fn = gql_name.lower()
    return fn in ("blocknumber", "block_number", "block")


def gql_scalar_to_ch(gql_type: str, gql_field_name: str = "") -> str:
    """Map a GraphQL scalar type name to a ClickHouse column type."""
    t = (gql_type or "").strip().lower()
    if t in _STRING:
        return "String"
    if t in _BOOL:
        return "Bool"
    if t in _INT:
        return "UInt64" if is_block_field(gql_field_name) else "Int64"
    if t in _BIGNUM:
        return "Int256"
    if t in _FLOAT:
        return "Float64"
    if t in _JSON:
        return "String"
    if t in _TS:
        return "Int64"
    # Unknown custom scalar (enum-like) -> low-cardinality string.
    return "LowCardinality(String)"


def default_for(ch_type: str) -> Any:
    """Sentinel default for a ClickHouse type."""
    if "String" in ch_type:
        return ""
    if ch_type == "Bool":
        return False
    if ch_type.startswith(("UInt", "Int")):
        return 0
    if ch_type.startswith("Float"):
        return 0.0
    return ""


def coerce(ch_type: str, value: Any) -> Any:
    """Coerce a raw GraphQL value into the Python value clickhouse-connect wants."""
    if value is None:
        return default_for(ch_type)

    if "String" in ch_type:               # String or LowCardinality(String)
        return value if isinstance(value, str) else str(value)
    if ch_type == "Bool":
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "t")
        return bool(value)
    if ch_type.startswith(("UInt", "Int")):
        if isinstance(value, bool):
            n = int(value)
        elif isinstance(value, int):
            n = value
        elif isinstance(value, float):
            n = int(value)
        else:
            s = str(value).strip()
            if s == "" or s.lower() in ("none", "null"):
                return 0
            try:
                n = int(s)
            except ValueError:
                try:
                    n = int(float(s))
                except ValueError:
                    return 0
        lo, hi = _RANGES.get(ch_type, (None, None))
        if lo is not None:
            if n < lo:
                return lo
            if n > hi:
                return hi
        return n
    if ch_type.startswith("Float"):
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
    return str(value)


def ddl_default(ch_type: str) -> str:
    """SQL DEFAULT clause fragment for a ClickHouse type."""
    if "String" in ch_type:
        return "DEFAULT ''"
    if ch_type == "Bool":
        return "DEFAULT false"
    if ch_type.startswith(("UInt", "Int")):
        return "DEFAULT 0"
    if ch_type.startswith("Float"):
        return "DEFAULT 0"
    return "DEFAULT ''"
