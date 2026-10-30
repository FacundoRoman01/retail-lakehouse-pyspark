from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.silver_to_gold import compute_gold_metrics


def test_compute_gold_metrics_calculations(spark):
    """
    Verifica que las métricas e ingresos de la capa Gold se calculen
    de forma exacta a partir de los datos curados.
    """
    schema = StructType([
        StructField("id_factura", StringType(), True),
        StructField("provincia", StringType(), True),
        StructField("codigo_producto", StringType(), True),
        StructField("descripcion", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("tipo_transaccion", StringType(), True),
        StructField("cantidad", IntegerType(), True),
        StructField("monto_total", DoubleType(), True)
    ])

    data = [
        ("FAC-001", "CABA", "PROD_A", "Teclado", "Tecno", "VENTA", 2, 2000.0),
        ("FAC-002", "CABA", "PROD_A", "Teclado", "Tecno", "VENTA", 1, 1000.0),
        ("FAC-003", "CABA", "PROD_A", "Teclado", "Tecno", "DEVOLUCION", -1, -1000.0),
        ("FAC-004", "Cordoba", "PROD_B", "Mouse", "Tecno", "VENTA", 1, 500.0)
    ]

    df_test_silver = spark.createDataFrame(data, schema)
    df_kpi_provincia, df_kpi_productos = compute_gold_metrics(df_test_silver)

    # Validación CABA
    caba_rows = df_kpi_provincia.filter(df_kpi_provincia.provincia == "CABA").collect()
    assert len(caba_rows) == 1
    caba = caba_rows[0]
    assert caba["facturacion_neta"] == Decimal("2000.00")
    assert caba["unidades_netas"] == 2
    assert caba["total_pedidos"] == 3
    assert caba["ticket_promedio"] == Decimal("666.67")

    # Validación Cordoba
    cba_rows = df_kpi_provincia.filter(df_kpi_provincia.provincia == "Cordoba").collect()
    assert len(cba_rows) == 1
    cba = cba_rows[0]
    assert cba["facturacion_neta"] == Decimal("500.00")
    assert cba["unidades_netas"] == 1
    assert cba["total_pedidos"] == 1
    assert cba["ticket_promedio"] == Decimal("500.00")

    # Validación Producto PROD_A
    prod_a_rows = df_kpi_productos.filter(df_kpi_productos.codigo_producto == "PROD_A").collect()
    assert len(prod_a_rows) == 1
    prod_a = prod_a_rows[0]
    assert prod_a["unidades_vendidas"] == 3
    assert prod_a["unidades_devueltas"] == 1
    assert prod_a["ingresos_brutos"] == Decimal("3000.00")