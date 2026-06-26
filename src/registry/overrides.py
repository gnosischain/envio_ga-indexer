"""Runtime registry accessor.

generated.py already has the final, yaml-overlaid EntitySpecs (introspect.py bakes
config/entities.yaml in at codegen time). This module is the runtime API: it loads
the generated registry and applies the ENABLED_ENTITIES env filter.
"""
from typing import List, Optional

from src.config import config
from src.registry.schema import EntitySpec

try:
    from src.registry.generated import ENTITIES as _ALL  # type: ignore
except Exception:  # not yet generated
    _ALL: List[EntitySpec] = []


def all_specs(include_disabled: bool = False) -> List[EntitySpec]:
    specs = list(_ALL)
    if not include_disabled:
        specs = [s for s in specs if s.enabled]
    if config.ENABLED_ENTITIES:
        wanted = {x.lower() for x in config.ENABLED_ENTITIES}
        specs = [s for s in specs if s.name.lower() in wanted or s.gql_type.lower() in wanted]
    return specs


def get_spec(name: str) -> Optional[EntitySpec]:
    key = name.lower()
    for s in _ALL:
        if key in (s.name.lower(), s.gql_type.lower()):
            return s
    return None
