# PySpark Helpers

A collection of tools to help when developing PySpark applications


## Installation

With pip
```
pip install pyspark_helpers
```

With poetry
```
poetry add pyspark_helpers
```

## Usage

### Auto Generate PySpark Schemas from JSON examples

Through cli:

```sh
psh-schema-from-json -p ./tests/data/schema/array.json -o ./results/array_schema.json
```

Or

```py
from pyspark_helpers.schema import schema_from_jsom, bulk_schema_from_jsom
from pathlib import Path

data_dir = "data/json"


## One file
schema = schma_from_json(f"{data_dir}/file.json")

print(schema)

## A whole directory
files = [Path(f) for f in Path.glob(f"{data_dir}/*.json")]
schems = bulk_schema_from_jsom(files)

for _file, schema in zip(files, schemas):
    print(_file.name, schemas)

```
