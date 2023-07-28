import logging
import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from pyspark_helpers.utils import create_spark_session


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    This fixture provides preconfigured SparkSession with Hive and Delta support.
    After the test session, temporary warehouse directory is deleted.
    :return: SparkSession
    """
    spark, warehouse_dir = create_spark_session()
    yield spark
    logging.info("Shutting down Spark session")
    spark.stop()
    if Path(warehouse_dir).exists():
        shutil.rmtree(warehouse_dir)
