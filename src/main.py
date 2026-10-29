import logging
import sys
from pyspark.sql import SparkSession
from pyspark.errors import PySparkException


from bronze_to_silver import clean_and_transform_silver
from silver_to_gold import compute_gold_metrics, load_gold_to_mysql, save_gold_to_lake

# Configuración global del Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("RetailLakehouse")


def get_spark_session(app_name: str = "RetailLakehouse") -> SparkSession:
    """Inicializa y configura la sesión de Spark."""
    logger.info("Inicializando SparkSession...")
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("SparkSession configurada exitosamente.")
    return spark


def main():
    spark = None
    try:
        spark = get_spark_session()

        # --- ETAPA 1: INGESTA BRONZE ---
        path_bronze = "data/bronze/online_retail_raw.csv"
        logger.info(f"Leyendo dataset crudo desde Bronze: {path_bronze}")

        df_bronze = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(path_bronze)

        total_bronze = df_bronze.count()
        logger.info(f"Datos crudos cargados en Bronze: {total_bronze} registros.")

        # --- ETAPA 2: DATA QUALITY Y CURACIÓN SILVER ---
        df_silver = clean_and_transform_silver(df_bronze)
        total_silver = df_silver.count()
        descartados = total_bronze - total_silver
        logger.info(f"Datos curados en Silver: {total_silver} registros | Descartados: {descartados}")

        # --- ETAPA 3: ANALÍTICA Y PERSISTENCIA GOLD ---
        df_kpi_provincia, df_kpi_productos = compute_gold_metrics(df_silver)

        logger.info("Vista previa Top 5 Provincias por Facturación:")
        df_kpi_provincia.show(5, truncate=False)

        logger.info("Vista previa Top 5 Productos por Facturación:")
        df_kpi_productos.show(5, truncate=False)

        # 1. Guardar en el Data Lake (Parquet)
        save_gold_to_lake(df_kpi_provincia, df_kpi_productos)

        # 2. Cargar en el Data Warehouse / RDBMS (MySQL)
        load_gold_to_mysql(df_kpi_provincia, df_kpi_productos)

     

        logger.info("Pipeline Medallón (Bronze -> Silver -> Gold) ejecutado con éxito.")

    except FileNotFoundError as fnf_err:
        logger.error(f"Archivo no encontrado: {fnf_err}")
        sys.exit(1)
    except PySparkException as ps_err:
        logger.error(f"Falla interna en PySpark: {ps_err}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error inesperado en el pipeline: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if spark is not None:
            logger.info("Cerrando SparkSession y liberando JVM...")
            spark.stop()
            logger.info("SparkSession cerrada y recursos liberados.")


if __name__ == "__main__":
    main()