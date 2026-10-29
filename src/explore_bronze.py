import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("ExploracionBronze") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Leer los datos crudos tal como están en Bronze
    df_bronze = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/bronze/online_retail_raw.csv")

    print("\n" + "="*50)
    print("1. RESUMEN ESTADÍSTICO (Mínimos, Máximos, Promedios)")
    print("="*50)
    df_bronze.select("cantidad", "precio_unitario").summary().show()

    print("\n" + "="*50)
    print("2. CONTEO DE VALORES NULOS POR COLUMNA")
    print("="*50)
    null_counts = [
        F.count(F.when(F.col(c).isNull(), c)).alias(c) 
        for c in df_bronze.columns
    ]
    df_bronze.select(null_counts).show()

    print("\n" + "="*50)
    print("3. CASOS RAROS: PRECIOS MENORES O IGUALES A CERO")
    print("="*50)
    df_bronze.filter(F.col("precio_unitario") <= 0).show(5, truncate=False)

    print("\n" + "="*50)
    print("4. CASOS RAROS: CANTIDADES NEGATIVAS (DEVOLUCIONES)")
    print("="*50)
    df_bronze.filter(F.col("cantidad") < 0).show(5, truncate=False)

    print("\n" + "="*50)
    print("5. CATEGORÍAS Y PROVINCIAS PRESENTES")
    print("="*50)
    print("Categorías:")
    df_bronze.groupBy("categoria").count().show()

    spark.stop()

if __name__ == "__main__":
    main()