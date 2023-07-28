import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import black
import isort
import pyspark.sql.types as T

from pyspark_helpers.utils import get_logger

logger = get_logger(__name__)


def _recurse_schema(d: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Recurse schema.

    Args:
        d (Dict[str, Any]): Schema to recurse.

    Returns:
        dict: Recursed schema.
    """
    schema = []

    for key, value in d.items():
        _value: Dict[str, Any] = {}

        if key != "fields":
            _value["name"] = key
        if isinstance(value, dict):
            _value["type"] = parse_struct(value)
        elif isinstance(value, list):
            if len(value) == 0:
                continue
            _value["type"] = parse_array(value)
        else:
            _value["type"] = parse_value(value)
        _value["nullable"] = False
        _value["metadata"] = {}
        schema.append(_value)

    return schema


def parse_value(value: Any) -> str:
    """Parse value.

    Args:
        value (Any): Value to parse.

    Returns:
        str: Parsed value.
    """
    type_map = {
        "str": "string",
        "int": "integer",
        "float": "double",
        "bool": "boolean",
    }

    _type = type_map.get(type(value).__name__, "string")

    if _type == "integer":
        if value > 2147483647:
            _type = "long"

    return _type


def parse_array(value: List[Any]) -> Dict[str, Any]:
    """Parse array.

    Args:
        value (List[Any]): Array to parse.

    Returns:
        dict: Parsed array.
    """

    schemas = []
    contains_null = any([item is None for item in value])

    for item in value:
        schema: Dict[str, Any] = {
            "type": "array",
        }
        if isinstance(item, dict):
            schema["elementType"] = parse_struct(item)
        elif isinstance(item, list):
            schema["elementType"] = parse_array(item)
        else:
            if item is None:
                contains_null = True
            schema["elementType"] = parse_value(item)
        schema["containsNull"] = contains_null
        schemas.append(schema)

    if len(schemas) > 0:
        count = Counter([json.dumps(s) for s in schemas])
        schema = json.loads(count.most_common(1)[0][0])
    else:
        schema = {
            "type": "array",
            "elementType": "string",
            "containsNull": True,
        }

    return schema


def parse_struct(value: Dict[str, Any]) -> dict:
    """Parse struct.

    Args:
        value (Dict[str, Any]): Struct to parse.

    Returns:
        dict: Parsed struct.
    """

    schema = {
        "type": "struct",
        "fields": _recurse_schema(value),
    }

    return schema


def _get_pyspark_type(schema: Dict[str, Any]) -> Union[T.StructType, T.ArrayType]:
    """Get transform schema.

    Args:
        schema (Dict[str, Any]): Schema to transform.

    Returns:
        Union[T.StructType, T.ArrayType]: Transformed schema.

    Raises:
        ValueError: If schema is not a dict or list.
    """
    _type = schema.get("type", None)

    ## We ignore the type because mypy doesn't know that the type is
    ## T.StructType or T.ArrayType.
    if _type == "array":
        return T.ArrayType  # type: ignore
    elif _type == "struct":
        return T.StructType  # type: ignore
    else:
        raise ValueError("Schema must be dict or list.")


def save_schema(
    schema: Union[Dict[str, Any], T.ArrayType, T.StructType],
    output: Union[str, Path],
    overwrite: bool = False,
) -> None:
    """Save schema to file.

    Args:
        schema (Dict[str, Any]): Schema to save.
        output (Union[str, Path]): Output path.
        overwrite (bool, optional): Overwrite existing file. Defaults to False.

    Raises:
        ValueError: If output is a file and overwrite is False.
    """
    if isinstance(output, str):
        output = Path(output)

    if output.exists() and not output.is_dir() and not overwrite:
        raise ValueError("A file already exists at the output path.")
    elif output.is_dir():
        output.mkdir(parents=True, exist_ok=True)
        output_path = output / f"schema-{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    else:
        if not output.parent.exists():
            output.parent.mkdir(parents=True, exist_ok=True)
        output_path = output.parent / output.stem

    ext = "json"

    if type(schema) is T.ArrayType or type(schema) is T.StructType:
        print("Saving schema as python file.")
        ext = "py"

    write_mode = "w" if not overwrite else "w+"

    with open(f"{output_path}.{ext}", write_mode) as f:
        if ext == "json":
            json.dump(schema, f)
        else:
            string_schema = str(schema)
            import_statement = get_imports(string_schema)
            script = f"{import_statement}\n\n{string_schema}"
            res = black.format_str(script, mode=black.FileMode())
            print("Formatted:", res)
            f.write(res)

    return script


def get_imports(string_schema):
    pyspark_types = re.findall(r"([A-Z]\w+)(Type|Field)", string_schema)
    unique_pyspark_types = set(["".join(t) for t in pyspark_types])

    imports = ", ".join(unique_pyspark_types)
    import_statement = f"from pyspark.sql.types import {imports}"
    print("Imports:", imports)
    print("Sorted:", isort.code(string_schema))
    print("Import statement:", import_statement)
    print("Sorted import statement:", isort.code(import_statement))
    return import_statement


def get_pyspark_schema(schema: Dict[str, Any]) -> Union[T.StructType, T.ArrayType]:
    """Transform schema to pyspark schema.

    Args:
        schema (Dict[str, Any]): Schema to transform.

    Returns:
        Union[T.StructType, T.ArrayType]: Transformed schema.
    """
    pyspark_type = _get_pyspark_type(schema)
    pyspark_schema = None

    try:
        logger.debug("Transforming schema to pyspark schema. {}".format(schema))
        pyspark_schema = pyspark_type.fromJson(schema)
        logger.debug(f"Successfully parsed schema: {str(pyspark_schema)}")
    except Exception as e:
        logger.error("Schema parsing failed: ", e)
        raise e
        # raise e

    return pyspark_schema


def load_json(path: Union[Path, str]) -> Dict[str, Any]:
    """Load json file.

    Args:
        path (Union[Path, str, List[Path], List[str]]): Path to json file. Can be a list of paths.
    Returns:
        Union[List[dict], dict]: Json file as dictionary or list.
    """
    if isinstance(path, str):
        path = Path(path)

    with open(path, "r") as f:
        data = json.load(f)

    return data


def parse_json(data: Union[dict, list]) -> Dict[str, Any]:
    """Parse json file.

    Args:
        data (Union[dict, list]): Json file as dictionary or list.

    Returns:
        Union[dict, list]: Parsed json file.
    """
    if isinstance(data, dict):
        data = parse_struct(data)
    elif isinstance(data, list):
        data = parse_array(data)
    else:
        raise ValueError("Schema must be dict or list.")

    return data


def schema_from_json(
    path: Union[str, Path],
    to_pyspark: bool = False,
    output: Optional[Union[str, Path]] = None,
    overwrite: bool = False,
) -> Union[Dict[str, Any], T.ArrayType, T.StructType]:
    """Read schema file.

    Args:
        path (str): Path to schema file. Defaults to None.
        to_pyspark (bool, optional): Transform schema to pyspark schema. Defaults to False.
        output (Optional[Path], optional): Saves schema as json to this path. Defaults to None.
        overwrite (bool, optional): Overwrite existing file. Defaults to False.

    Returns:
        dict: Schema file as dictionary.
    """

    data = load_json(path)
    schema = parse_json(data)

    if to_pyspark is True:
        pyspark_schema = get_pyspark_schema(schema)

        if output is not None:
            output = save_schema(pyspark_schema, output, overwrite=overwrite)
            print(f"Schema saved to {output}")

        return pyspark_schema

    if output is not None:
        save_schema(schema, output, overwrite=overwrite)

    return schema


def bulk_schema_from_json(
    files: Union[List[Path], List[str]],
    to_pyspark: bool = False,
    output: Optional[Path] = None,
    overwrite: bool = False,
) -> List[Union[Dict[str, Any], T.ArrayType, T.StructType]]:
    """Read schema file in bulk.

    Args:
        files (List[Path]): List of paths to schema files.
        to_pyspark (bool, optional): Transform schema to pyspark schema. Defaults to False.
        output (Path, optional): Path to save schema. Defaults to None.
        overwrite (bool, optional): Overwrite existing file. Defaults to False.

    Returns:
        Union[List[dict], List[T.ArrayType], List[T.StructType]]: List of schemas.
    """

    schemas = []

    for _file in files:
        logger.info(f"Parsing {_file}")
        schema = schema_from_json(
            _file, to_pyspark=to_pyspark, output=output, overwrite=overwrite
        )
        schemas.append(schema)

    return schemas


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        "-p",
        type=str,
        required=True,
        help="Path to schema file.",
    )
    parser.add_argument(
        "--to_pyspark",
        "-t",
        action="store_true",
        default=False,
        help="Transform schema to pyspark schema.",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Path to save schema.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite existing file.",
    )

    args = parser.parse_args()

    schema = schema_from_json(args.path, to_pyspark=args.to_pyspark, output=args.output)

    print(schema)


if __name__ == "__main__":
    main()
