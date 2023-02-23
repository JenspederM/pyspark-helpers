import logging
import tempfile

from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_logger(name: str, log_level="INFO") -> logging.Logger:
    """Get logger.

    Args:
        name (str): Name of logger.
        log_level (str, optional): Log level. Defaults to "INFO".

    Returns:
        logging.Logger: Logger.
    """

    logging.basicConfig(
        format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        level=log_level,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger(name)


def create_spark_session():
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
