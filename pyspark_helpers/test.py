from pyspark_helpers.schema import schema_from_json, bulk_schema_from_json
from pathlib import Path

from pprint import pprint
import json

data_dir = "tests/data/schema"
path = Path(f"{data_dir}/struct.json")

## One file
schema = schema_from_json(path)

with open(path, "r") as _file:
    data = json.load(_file)
    pprint(data)
    pprint(schema)

print(data["a_null"])


## A whole directory
files = [Path(f) for f in Path(data_dir).glob(f"struct.json")]
schemas = bulk_schema_from_json(files)
