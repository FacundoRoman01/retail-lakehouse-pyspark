import logging
import os
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

logger = logging.getLogger("RetailLakehouse.Silver")


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

    # 3. Persistir en Silver con PyArrow 
    path_silver = "data/silver/ventas"
    os.makedirs(path_silver, exist_ok=True)
    logger.info(f"Guardando datos curados en Silver: {path_silver}")

    # Convertimos a formato columnar PyArrow/Pandas y guardamos el Parquet
    df_arrow = df_silver.toPandas()
    df_arrow.to_parquet(
        os.path.join(path_silver, "ventas_silver.parquet"),
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    logger.info("Capa Silver generada y persistida exitosamente en Parquet.")
    return df_silver