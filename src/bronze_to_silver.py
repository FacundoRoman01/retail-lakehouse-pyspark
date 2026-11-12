import logging
import os
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

logger = logging.getLogger("RetailLakehouse.Silver")

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ


def clean_and_transform_silver(df_bronze: DataFrame) -> DataFrame:
    logger.info("Iniciando Data Quality: transformación Bronze -> Silver...")

    # 1. Filtro de validez
    df_valid_price = df_bronze.filter(F.col("precio_unitario") > 0)

    # 2. Imputación y transformaciones
    df_silver = df_valid_price \
        .withColumn(
            "id_cliente",
            F.coalesce(F.col("id_cliente"), F.lit("CONSUMIDOR_FINAL"))
        ) \
        .withColumn(
            "tipo_transaccion",
            F.when(F.col("cantidad") < 0, "DEVOLUCION").otherwise("VENTA")
        ) \
        .withColumn(
            "monto_total",
            F.round(F.col("cantidad") * F.col("precio_unitario"), 2)
        ) \
        .withColumn(
            "fecha",
            F.to_date(F.col("fecha_factura"))
        )

    # 3. Persistir en Silver según el entorno
    if IS_DATABRICKS:
        logger.info("Guardando datos curados en Silver como Tabla Delta en Unity Catalog...")
        df_silver.write.format("delta").mode("overwrite").saveAsTable("workspace.default.silver_ventas")
        logger.info("Tabla 'workspace.default.silver_ventas' persistida en Delta Lake.")
    else:
        path_silver = "data/silver/ventas"
        os.makedirs(path_silver, exist_ok=True)
        logger.info(f"Guardando datos curados en Silver: {path_silver}")

        df_arrow = df_silver.toPandas()
        df_arrow.to_parquet(
            os.path.join(path_silver, "ventas_silver.parquet"),
            engine="pyarrow",
            compression="snappy",
            index=False
        )
        logger.info("Capa Silver generada y persistida exitosamente en Parquet.")

    return df_silver