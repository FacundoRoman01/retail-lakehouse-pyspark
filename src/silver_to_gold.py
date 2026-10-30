import logging
import os
from dotenv import load_dotenv
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType
from sqlalchemy import create_engine

logger = logging.getLogger("RetailLakehouse.Gold")

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3307")
DB_NAME = os.getenv("DB_NAME")


def compute_gold_metrics(df_silver: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Calcula agregaciones analíticas de negocio a partir de la capa Silver:
    1. KPIs por Provincia (Facturación neta, volumen y ticket promedio).
    2. Rendimiento por Producto (Ventas vs Devoluciones e ingresos brutos).
    """
    logger.info("Calculando métricas analíticas de negocio en Capa Gold...")

    # --- TABLA 1: KPIs por Provincia ---
    df_kpi_provincia = df_silver.groupBy("provincia").agg(
        F.sum("monto_total").cast(DecimalType(18, 2)).alias("facturacion_neta"),
        F.sum("cantidad").alias("unidades_netas"),
        F.countDistinct("id_factura").alias("total_pedidos"),
        (F.sum("monto_total") / F.countDistinct("id_factura")).cast(DecimalType(18, 2)).alias("ticket_promedio")
    ).orderBy(F.col("facturacion_neta").desc())

    # --- TABLA 2: Rendimiento por Producto y Devoluciones ---
    df_kpi_productos = df_silver.groupBy("codigo_producto", "descripcion", "categoria").agg(
        F.sum(
            F.when(F.col("tipo_transaccion") == "VENTA", F.col("cantidad")).otherwise(0)
        ).alias("unidades_vendidas"),
        F.sum(
            F.when(F.col("tipo_transaccion") == "DEVOLUCION", F.abs(F.col("cantidad"))).otherwise(0)
        ).alias("unidades_devueltas"),
        F.sum(
            F.when(F.col("tipo_transaccion") == "VENTA", F.col("monto_total")).otherwise(0)
        ).cast(DecimalType(18, 2)).alias("ingresos_brutos")
    ).orderBy(F.col("ingresos_brutos").desc())

    logger.info("Métricas Gold calculadas exitosamente en memoria.")
    return df_kpi_provincia, df_kpi_productos


def save_gold_to_lake(df_provincia: DataFrame, df_productos: DataFrame):
    """
    Persiste los dataframes agregados en data/gold/ en formato Parquet (Snappy).
    """
    path_gold = "data/gold"
    path_provincia = os.path.join(path_gold, "kpi_ventas_provincia")
    path_productos = os.path.join(path_gold, "kpi_rendimiento_productos")

    os.makedirs(path_provincia, exist_ok=True)
    os.makedirs(path_productos, exist_ok=True)

    logger.info(f"Guardando Parquets de Capa Gold en: {path_gold}")

    pdf_provincia = df_provincia.toPandas()
    pdf_productos = df_productos.toPandas()

    pdf_provincia.to_parquet(
        os.path.join(path_provincia, "kpi_ventas_provincia.parquet"),
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    pdf_productos.to_parquet(
        os.path.join(path_productos, "kpi_rendimiento_productos.parquet"),
        engine="pyarrow",
        compression="snappy",
        index=False
    )
    logger.info("Archivos Parquet de Capa Gold persistidos con éxito.")


def load_gold_to_mysql(df_provincia: DataFrame, df_productos: DataFrame):
    """
    Carga los DataFrames a MySQL usando SQLAlchemy y Pandas en lotes optimizados (bulk insert).
    """
    logger.info("Iniciando carga optimizada de Capa Gold hacia MySQL...")
    connection_url = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_url)

    try:
        pdf_provincia = df_provincia.toPandas()
        pdf_productos = df_productos.toPandas()

        # Inserción masiva de KPIs por Provincia
        pdf_provincia.to_sql(
            name="kpi_ventas_provincia",
            con=engine,
            if_exists="replace",
            index=False,
            chunksize=1000,
            method="multi"
        )
        logger.info("Tabla 'kpi_ventas_provincia' persistida en MySQL (bulk insert).")

        # Inserción masiva de Rendimiento de Productos
        pdf_productos.to_sql(
            name="kpi_rendimiento_productos",
            con=engine,
            if_exists="replace",
            index=False,
            chunksize=1000,
            method="multi"
        )
        logger.info("Tabla 'kpi_rendimiento_productos' persistida en MySQL (bulk insert).")

    except Exception as e:
        logger.error(f"Falla al escribir en MySQL: {e}", exc_info=True)
        raise e
    finally:
        engine.dispose()
        logger.info("Conexión con MySQL cerrada correctamente.")