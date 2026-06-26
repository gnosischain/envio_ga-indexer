"""Generic GraphQL dict -> typed ClickHouse row parser, driven by an EntitySpec."""
from typing import Any, Dict

from src.registry.schema import EntitySpec
from src.utils.types import coerce
from src import observability as obs


class GenericEntityParser:
    def __init__(self, spec: EntitySpec):
        self.spec = spec
        self._known = set(spec.gql_field_names)

    def to_typed_row(self, gql_row: Dict[str, Any], synced_block: int = 0) -> Dict[str, Any]:
        """Map a GraphQL row into a typed-table row (sentinels for missing values)."""
        row: Dict[str, Any] = {}
        for f in self.spec.fields:
            row[f.ch_name] = coerce(f.ch_type, gql_row.get(f.gql_name))

        row["_deleted"] = 0
        block_val = gql_row.get(self.spec.block_field) if self.spec.block_field else None
        if block_val not in (None, ""):
            try:
                row["_synced_block"] = int(block_val)
            except (TypeError, ValueError):
                row["_synced_block"] = synced_block or 0
        else:
            row["_synced_block"] = synced_block or 0

        # Schema-drift signal: any returned key we don't know about.
        unknown = [k for k in gql_row.keys() if k not in self._known]
        if unknown:
            obs.unknown_fields_total.labels(entity=self.spec.name).inc(len(unknown))

        return row
