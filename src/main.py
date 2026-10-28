import logging
import sys
from pyspark.sql import SparkSession
from pyspark.errors import PySparkException

# 1. Configuración global del Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("RetailLakehouse")


def get_spark_session(app_name: str = "RetailLakehouse") -> SparkSession:
    """Inicializa y configura la sesión de Spark."""
    logger.info("Inicializando SparkSession...")
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    
    # Suprimir logs verbosos internos de Spark/JVM
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("SparkSession configurada exitosamente.")
    return spark


def main():
    spark = None
    try:
        spark = get_spark_session()

        path_bronze = "data/bronze/online_retail_raw.csv"
        logger.info(f"Leyendo dataset crudo desde la capa Bronze: {path_bronze}")

        df_bronze = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(path_bronze)

        total_filas = df_bronze.count()
        logger.info(f"Datos crudos cargados. Total de registros leídos: {total_filas}")

        logger.info("Esquema inferido por Spark:")
        df_bronze.printSchema()

        logger.info("Muestra de los primeros 5 registros:")
        df_bronze.show(5, truncate=False)

    except FileNotFoundError as fnf_err:
        logger.error(f"Error de archivo no encontrado: {fnf_err}")
        sys.exit(1)
    except PySparkException as ps_err:
        logger.error(f"Error interno durante la ejecución en PySpark: {ps_err}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error inesperado en el pipeline: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Garantiza la liberación de memoria en la JVM ante cualquier falla
        if spark is not None:
            logger.info("Liberando recursos y cerrando SparkSession...")
            spark.stop()
            logger.info("SparkSession cerrada de forma limpia.")


if __name__ == "__main__":
    main()