from datetime import datetime
from pathlib import Path

from pyspark.sql.types import ArrayType, StructType

from pyspark_helpers.schema import (
    _recurse_schema,
    parse_array,
    parse_struct,
    parse_value,
    schema_from_json,
)
from pyspark_helpers.utils import get_logger

logger = get_logger(__name__)

DATA = Path("./tests/data/schema/")
RESULTS = Path("./tests/results/schema/")
RESULTS.mkdir(parents=True, exist_ok=True)
TESTS = [l for l in DATA.rglob("*.json")]


def test_handle_array():
    logger.info("Testing filled array")
    test = ["test"]
    schema = parse_array(test)
    assert schema is not None
    assert isinstance(schema, dict)
    assert len(schema) > 0

    logger.info("Testing empty array")
    test = []
    schema = parse_array(test)
    assert schema is not None
    assert isinstance(schema, dict)
    assert len(schema) > 0

    logger.info("Testing empty array of arrays")
    test = [[[]]]
    schema = parse_array(test)
    assert schema is not None
    assert isinstance(schema, dict)
    assert len(schema) > 0

    logger.info("Testing filled array of arrays")
    test = [[[1, 2, 3], [4, 5, 6], [7, 8, 9]]]
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


def test_read_schema():
    for test in TESTS:
        logger.info(f"Testing {test}")
        schema = schema_from_json(test, to_pyspark=False)
        assert schema is not None
        assert isinstance(schema, dict)
        assert len(schema) > 0


def test_read_schema_transformed():
    logger.info("Testing read_schema")
    for test in TESTS:
        logger.info(f"Testing {test}")
        schema = schema_from_json(test, to_pyspark=True)
        assert schema is not None
        assert isinstance(schema, StructType) or isinstance(schema, ArrayType)


def test_output_json_schema():
    logger.info("Testing output_json_schema")
    for test in TESTS:
        logger.info(f"Testing {test}")
        schema = schema_from_json(
            test,
            to_pyspark=False,
            output=f"{RESULTS}/{test.stem}-schema.json",
            overwrite=True,
        )
        assert schema is not None
        assert isinstance(schema, dict)
        assert len(schema) > 0


def test_output_python_schema():
    logger.info("Testing output_json_schema")
    for test in TESTS:
        logger.info(f"Testing {test}")
        schema = schema_from_json(
            test,
            to_pyspark=True,
            output=f"{RESULTS}/{test.stem}-pyspark.py",
            overwrite=True,
        )
        assert schema is not None
        assert isinstance(schema, StructType) or isinstance(schema, ArrayType)
