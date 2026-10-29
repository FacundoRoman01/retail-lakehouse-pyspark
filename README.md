# Retail Lakehouse ETL Pipeline (Medallion Architecture)

Pipeline end-to-end de Ingeniería de Datos basado en la **Arquitectura Medallón** (Bronze, Silver y Gold). El proyecto ingesta datos transaccionales crudos de retail, aplica auditoría y reglas de calidad de datos (Data Quality), calcula métricas clave de negocio (KPIs) y persiste los resultados tanto en un **Data Lake** (formato Parquet) como en un **Data Warehouse** relacional (MySQL sobre Docker).

---

## 🏗 Arquitectura del Sistema

El pipeline procesa los datos en tres capas desacopladas garantizando trazabilidad, calidad y optimización para diferentes consumidores:

```text
[ Dataset Crudo (CSV) ]
           │
           ▼
     [ BRONZE ]       --> data/bronze/online_retail_raw.csv
           │              • Ingesta del dato histórico sin modificaciones
           │              • Detección de anomalías y auditoría exploratoria
           ▼
     [ SILVER ]       --> data/silver/ventas/ (Parquet)
           │              • Filtrado de registros inválidos (precios <= 0)
           │              • Imputación de clientes nulos ('CONSUMIDOR_FINAL')
           │              • Clasificación transaccional (VENTA / DEVOLUCION)
           │              • Enriquecimiento con monto_total y fechas limpias
           ▼
     [  GOLD  ]       --> Capa Analítica y de Consumo
           │
     ┌─────┴─────────────────────────────────┐
     ▼                                       ▼
[ Data Lake ]                       [ Data Warehouse ]
data/gold/ (Parquet / Snappy)       MySQL 8.0 en Docker (Puerto 3307)
• kpi_ventas_provincia              • Tabla: kpi_ventas_provincia
• kpi_rendimiento_productos         • Tabla: kpi_rendimiento_productos
(Consumo: Data Science / ML)        (Consumo: Dashboards BI / SQL)
```

---

## 🛠 Tecnologías Utilizadas

- **Apache Spark / PySpark:** Motor de cómputo distribuido para la ingesta, limpieza y agregación analítica.
- **Python:** Lenguaje principal de orquestación y scripting.
- **PyArrow & Fastparquet:** Almacenamiento columnar y compresión Snappy nativa en disco.
- **Pandas & SQLAlchemy:** Conversión y carga relacional optimizada mediante inserción masiva por lotes (*bulk insert*).
- **Docker & Docker Compose:** Contenerización del Data Warehouse (MySQL 8.0).
- **Logging & Error Handling:** Trazabilidad estandarizada mediante `logging` nativo y captura de excepciones con liberación controlada de la JVM de Spark.

---

## 📂 Estructura del Proyecto

```text
retail-lakehouse-pyspark/
├── data/
│   ├── bronze/                   # Archivo crudo inicial (online_retail_raw.csv)
│   ├── silver/                   # Datos curados y normalizados en Parquet
│   └── gold/                     # Tablas agregadas analíticas en Parquet
│       ├── kpi_rendimiento_productos/
│       └── kpi_ventas_provincia/
├── src/
│   ├── bronze_to_silver.py       # Reglas de limpieza, imputación y Data Quality
│   ├── explore_bronze.py         # Script exploratorio y auditoría estadística inicial
│   ├── generate_raw_data.py      # Generador sintético de transacciones retail
│   ├── silver_to_gold.py         # Cálculo de KPIs analíticos y carga a MySQL / Parquet
│   └── main.py                   # Orquestador principal del pipeline
├── docker-compose.yml            # Configuración del servicio MySQL en Docker
├── requirements.txt              # Dependencias del entorno virtual de Python
└── README.md                     # Documentación técnica
```

---

## 🚀 Puesta en Marcha

### 1. Requisitos Previos
- Python 3.10+ instalado.
- Java 8, 11, 17 o 21 (JDK) configurado en las variables de entorno (`JAVA_HOME`).
- Docker Desktop en ejecución.

### 2. Configuración del Entorno
Clonar el repositorio y preparar el entorno virtual:

```bash
git clone <URL_DEL_REPOSITORIO>
cd retail-lakehouse-pyspark

# Crear y activar entorno virtual
python -m venv .venv
# En Windows (PowerShell):
.\.venv\Scripts\Activate.ps1
# En Linux/Mac:
source .venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

### 3. Levantar el Data Warehouse (Docker)
Iniciar el contenedor de MySQL:

```bash
docker compose up -d
```

Verificar que el servicio esté arriba:
```bash
docker compose ps
```

### 4. Ejecutar el Pipeline Completo
Correr el orquestador general:

```bash
python src/main.py
```

---

## 📊 Reglas de Negocio y Data Quality Aplicadas

1. **Validez Financiera:** Descarte de registros erróneos con `precio_unitario <= 0`.
2. **Normalización de Clientes:** Imputación de registros sin cliente registrado (`NULL`) con el valor predeterminado `"CONSUMIDOR_FINAL"`.
3. **Clasificación Transaccional:** Creación del campo `tipo_transaccion` evaluando el signo de la cantidad:
   - `cantidad > 0` $\rightarrow$ `VENTA`
   - `cantidad < 0` $\rightarrow$ `DEVOLUCION` (asociadas a notas de crédito `NC-`).
4. **Precisión Decimal:** Casteo explícito a `DecimalType(18, 2)` en la capa Gold para evitar notación científica y garantizar consistencia en métricas monetarias.
5. **Carga Optimizada a MySQL:** Implementación de bulk insert mediante `chunksize=1000` y `method="multi"` para minimizar la latencia de red contra el Data Warehouse.

---

## 🔍 Verificación de Datos en MySQL

Para consultar las tablas cargadas en el Data Warehouse directamente desde la terminal:

```bash
docker exec -it mysql_lakehouse mysql -u data_engineer -pdata_password123 retail_analytics -e "SHOW TABLES; SELECT * FROM kpi_ventas_provincia LIMIT 5;"
```