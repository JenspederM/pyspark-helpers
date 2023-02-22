from pathlib import Path

import pytest
from pyspark_helpers.schema import (
    _recurse_schema,
    parse_array,
    parse_struct,
    parse_value,
    schema_from_json,
)

from pyspark_helpers.utils import get_logger

logger = get_logger(__name__, "INFO")


def test_handle_array():
    logger.info("Testing _handle_array")
    test = ["test"]
    schema = parse_array(test)
    assert schema is not None
    assert isinstance(schema, dict)
    assert len(schema) > 0


def test_handle_struct():
    logger.info("Testing _handle_struct")
    test = {"test": "test"}
    schema = parse_struct(test)
    assert schema is not None
    assert isinstance(schema, dict)
    assert len(schema) > 0


def test_recurse_schema():
    logger.info("Testing _recurse_schema")
    test = {"test": "test"}
    schema = _recurse_schema(test)
    assert schema is not None
    assert isinstance(schema, list)
    assert len(schema) > 0


def test_get_value_type():
    logger.info("Testing _get_value_type")
    type_map = {
        "string": "str",
        "integer": 100,
        "long": 2147483649,
        "double": 3.14,
        "boolean": True,
    }

    for key, value in type_map.items():
        _type = parse_value(value)
        assert _type == key


def json_tests():
    return [l for l in Path("json_tests").rglob("./data/schema/*.json")]


@pytest.mark.parametrize("test", json_tests())
def test_read_schema(test):
    logger.info(f"Testing {test}")
    schema = schema_from_json(test, to_pyspark=False)
    assert schema is not None
    assert isinstance(schema, dict)
    assert len(schema) > 0


def test_read_schema_transformed():
    logger.info("Testing read_schema")
    tests = Path("json_tests").rglob("*.json")

    for test in tests:
        logger.info(f"Testing {test}")
        schema = schema_from_json(test, to_pyspark=True)
        assert schema is not None
        assert isinstance(schema, dict)
        assert len(schema) > 0
