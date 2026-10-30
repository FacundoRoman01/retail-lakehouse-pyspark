import os
import sys
import pytest
from pyspark.sql import SparkSession

# Forzar a PySpark a usar el ejecutable del entorno virtual actual (.venv)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    """
    Sesión ligera de PySpark compartida para todos los archivos de test.
    Se enciende una sola vez por sesión de testing.
    """
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("RetailLakehouse-UnitTests")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()