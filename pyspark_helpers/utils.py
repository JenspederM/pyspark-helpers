import logging
import tempfile
from pathlib import Path
from typing import Tuple

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

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

    Returns:
        logging.Logger: Logger.
    """
    return ROOT_LOGGER.getChild(name)


def create_spark_session() -> Tuple[SparkSession, str]:
    """Create Spark session.

    Returns:
        Tuple[SparkSession, str]: Spark session and warehouse directory.
    """
    logging.info("Configuring Spark session for testing environment")
    warehouse_dir = tempfile.TemporaryDirectory().name
    warehouse_dir_uri = Path(warehouse_dir).as_uri()
    _builder = (
        SparkSession.builder.master("local[1]")
        .config("spark.hive.metastore.warehouse.dir", warehouse_dir_uri)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark: SparkSession = configure_spark_with_delta_pip(_builder).getOrCreate()
    logging.info("Spark session configured")
    return spark, warehouse_dir
