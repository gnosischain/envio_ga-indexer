"""Unit tests for the GraphQL->ClickHouse type mapping and value coercion."""
from src.utils.types import gql_scalar_to_ch, to_snake, coerce, ddl_default

UINT256_MAX = 2 ** 256 - 1
INT256_MAX = 2 ** 255 - 1
INT256_MIN = -(2 ** 255)


def test_to_snake():
    assert to_snake("blockNumber") == "block_number"
    assert to_snake("lastStatusUpdate") == "last_status_update"
    assert to_snake("transactionHash") == "transaction_hash"
    assert to_snake("AvatarTotalBalanceV2") == "avatar_total_balance_v2"
    assert to_snake("Metri_Order") == "metri_order"
    assert to_snake("Metri_Pay_RolesModule") == "metri_pay_roles_module"
    assert to_snake("id") == "id"
    assert to_snake("isPartOfStreamOrHub") == "is_part_of_stream_or_hub"


def test_scalar_mapping():
    assert gql_scalar_to_ch("ID") == "String"
    assert gql_scalar_to_ch("String") == "String"
    assert gql_scalar_to_ch("Boolean") == "Bool"
    assert gql_scalar_to_ch("Int", "logIndex") == "Int64"
    assert gql_scalar_to_ch("Int", "blockNumber") == "UInt64"
    assert gql_scalar_to_ch("numeric") == "Int256"     # default signed (balances can be negative)
    assert gql_scalar_to_ch("bigint") == "Int256"
    assert gql_scalar_to_ch("avatartype") == "LowCardinality(String)"   # unknown enum scalar


def test_coerce_sentinels():
    assert coerce("String", None) == ""
    assert coerce("LowCardinality(String)", None) == ""
    assert coerce("Bool", None) is False
    assert coerce("Int256", None) == 0
    assert coerce("UInt64", None) == 0


def test_coerce_values():
    assert coerce("String", 123) == "123"
    assert coerce("Bool", "true") is True
    assert coerce("Bool", "false") is False
    assert coerce("Int256", "-12676209385253768484484570") == -12676209385253768484484570
    assert coerce("UInt256", str(UINT256_MAX)) == UINT256_MAX
    assert coerce("Int64", "1728911120") == 1728911120


def test_coerce_clamps_out_of_range():
    # uint256-max into an Int256 column clamps instead of crashing the batch.
    assert coerce("Int256", UINT256_MAX) == INT256_MAX
    # negative into UInt256 clamps to 0.
    assert coerce("UInt256", -5) == 0
    # huge negative into Int256 clamps to min.
    assert coerce("Int256", -(2 ** 300)) == INT256_MIN


def test_ddl_default():
    assert ddl_default("String") == "DEFAULT ''"
    assert ddl_default("Bool") == "DEFAULT false"
    assert ddl_default("Int256") == "DEFAULT 0"
    assert ddl_default("LowCardinality(String)") == "DEFAULT ''"
