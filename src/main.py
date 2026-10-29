import logging
import sys
from pyspark.sql import SparkSession
from pyspark.errors import PySparkException


from bronze_to_silver import clean_and_transform_silver

# Configuración global del Logger
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

        # --- ETAPA 1: INGESTA BRONZE ---
        path_bronze = "data/bronze/online_retail_raw.csv"
        logger.info(f"Leyendo dataset crudo desde la capa Bronze: {path_bronze}")

        df_bronze = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(path_bronze)

        total_bronze = df_bronze.count()
        logger.info(f"Datos crudos cargados en Bronze: {total_bronze} registros.")

        # --- ETAPA 2: PROCESAMIENTO SILVER (DATA QUALITY) ---
        df_silver = clean_and_transform_silver(df_bronze)

        total_silver = df_silver.count()
        descartados = total_bronze - total_silver
        logger.info(f"Registros válidos en Silver: {total_silver} | Registros descartados (precio <= 0): {descartados}")

        logger.info("Muestra de 5 registros limpios en Silver:")
        df_silver.select(
            "id_factura", "id_cliente", "descripcion", 
            "cantidad", "precio_unitario", "monto_total", "tipo_transaccion", "fecha"
        ).show(5, truncate=False)

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