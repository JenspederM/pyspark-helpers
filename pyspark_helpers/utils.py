import logging
import tempfile

from typing import Tuple
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

ROOT_LOGGER = logging.getLogger("pyspark_helpers")

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    level="DEBUG",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_logger(name: str) -> logging.Logger:
    """Get logger.

    Args:
        name (str): Name of logger.
        log_level (str, optional): Log level. Defaults to "INFO".

    Returns:
        logging.Logger: Logger.
    """
    return ROOT_LOGGER.getChild(name)


def create_spark_session() -> Tuple[SparkSession, str]:
    logging.info("Configuring Spark session for testing environment")
    warehouse_dir = tempfile.TemporaryDirectory().name
    _builder = (
        SparkSession.builder.master("local[1]")
        .config("spark.hive.metastore.warehouse.dir", Path(warehouse_dir).as_uri())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark: SparkSession = configure_spark_with_delta_pip(_builder).getOrCreate()
    logging.info("Spark session configured")
    return spark, warehouse_dir
