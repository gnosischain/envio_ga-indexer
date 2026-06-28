"""Unit tests over the generated registry + the generic parser.

These assert the correctness invariants are upheld by the committed codegen
output (no network / ClickHouse needed).
"""
from src.registry.generated import ENTITIES
from src.registry.schema import SyncStrategy
from src.parsers.generic import GenericEntityParser


def test_entity_count():
    assert len(ENTITIES) == 28


def test_every_entity_has_id():
    for s in ENTITIES:
        assert any(f.gql_name == "id" for f in s.fields), s.gql_type


def test_inv2_block_cursor_is_immutable():
    # block_cursor REQUIRES mutable:false (else it would silently miss in-place updates).
    for s in ENTITIES:
        if s.strategy == SyncStrategy.BLOCK_CURSOR:
            assert s.mutable is False, s.gql_type
            assert s.block_field, s.gql_type


def test_inv2_field_cursor_has_cursor():
    for s in ENTITIES:
        if s.strategy == SyncStrategy.FIELD_CURSOR:
            assert s.cursor_field, s.gql_type
            assert s.field_by_gql(s.cursor_field), s.gql_type


def test_inv3_partition_is_immutable():
    # Partition keys must be immutable: block (block_cursor), month-of-creation-ts
    # (opt-in via ts_immutable), or none. Never a mutable column.
    for s in ENTITIES:
        p = s.partition_expr
        assert p == "" or p.startswith(("intDiv(", "toStartOfMonth(", "cityHash64(")), (s.gql_type, p)
        if s.strategy == SyncStrategy.BLOCK_CURSOR:
            assert p.startswith("intDiv("), (s.gql_type, p)


def test_transaction_action_optimized():
    ta = next(s for s in ENTITIES if s.name == "transaction_action")
    assert ta.partition_expr.startswith("toStartOfMonth("), ta.partition_expr
    assert ta.order_by == "avatar_id, timestamp, id"
    assert any(ix["expr"] == "transaction_id" for ix in ta.indexes)


def test_expiry_time_is_uint256():
    tr = next(s for s in ENTITIES if s.gql_type == "TrustRelation")
    f = tr.field_by_gql("expiryTime")
    assert f is not None and f.ch_type == "UInt256"


def test_parser_maps_and_coerces():
    spec = next(s for s in ENTITIES if s.name == "transfer")
    row = spec  # noqa
    parser = GenericEntityParser(spec)
    gql_row = {"id": "abc", "blockNumber": 123, "value": "-5", "from": "0xaa", "to": "0xbb"}
    typed = parser.to_typed_row(gql_row, synced_block=999)
    assert typed["id"] == "abc"
    assert typed["block_number"] == 123
    assert typed["value"] == -5            # Int256, negative preserved
    assert typed["from"] == "0xaa"
    assert typed["_deleted"] == 0
    assert typed["_synced_block"] == 123   # from the row's blockNumber
