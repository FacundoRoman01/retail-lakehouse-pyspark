from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.bronze_to_silver import clean_and_transform_silver


def test_clean_and_transform_silver_data_quality(spark):
    """
    Verifica las reglas de Data Quality de la capa Silver.
    """
    bronze_schema = StructType([
        StructField("id_factura", StringType(), True),
        StructField("id_cliente", StringType(), True),
        StructField("precio_unitario", DoubleType(), True),
        StructField("cantidad", IntegerType(), True),
        StructField("fecha_factura", StringType(), True),
        StructField("provincia", StringType(), True)
    ])

    bronze_data = [
        ("FAC-100", "CLI_01", 150.0, 2, "2023-10-25 14:30:00", "CABA"),      # Válido
        ("FAC-101", "CLI_02", 0.0, 5, "2023-10-25 15:00:00", "Cordoba"),    # Inválido (Precio 0)
        ("FAC-102", "CLI_03", -10.0, 1, "2023-10-25 15:30:00", "Mendoza"),  # Inválido (Precio < 0)
        ("FAC-103", None, 500.0, 1, "2023-10-26 09:00:00", "CABA"),         # Nulo a imputar
        ("FAC-104", "CLI_04", 100.0, -1, "2023-10-26 10:00:00", "Santa Fe") # Devolución
    ]

    df_test_bronze = spark.createDataFrame(bronze_data, bronze_schema)
    df_result_silver = clean_and_transform_silver(df_test_bronze)

    # 1. Validar que se filtraron los dos precios inválidos (quedan 3 de 5)
    assert df_result_silver.count() == 3

    # 2. Validar imputación de cliente nulo
    row_imputed = df_result_silver.filter(df_result_silver.id_factura == "FAC-103").collect()[0]
    assert row_imputed["id_cliente"] == "CONSUMIDOR_FINAL"

    # 3. Validar tipificación de devolución
    row_dev = df_result_silver.filter(df_result_silver.id_factura == "FAC-104").collect()[0]
    assert row_dev["tipo_transaccion"] == "DEVOLUCION"
    assert row_dev["monto_total"] == -100.00
