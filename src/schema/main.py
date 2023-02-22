from utils import get_logger
from schema.core import bulk_schema_from_json

from pathlib import Path

logger = get_logger(__name__)


if __name__ == "__main__":
    # Get all JSON files in the data directory
    files = [f for f in Path("src/schema/test/data").rglob("*.json")]

    # Get dict schemas from JSON files
    json_schemas = bulk_schema_from_json(
        files=files,
        to_pyspark=False,
        output="src/schema/test/_cache",
    )

    # Get pyspark schemas from JSON files
    pyspark_schemas = bulk_schema_from_json(
        files=files,
        to_pyspark=True,
        output="src/schema/test/_cache",
    )

    for file, schema in zip(files, pyspark_schemas):
        logger.info(f"Schema for {file.name}:")
        logger.info(schema)
