# Retail Lakehouse ETL Pipeline (Medallion Architecture)

[![CI Data Pipeline Tests](https://github.com/FacundoRoman01/retail-lakehouse-pyspark/actions/workflows/tests.yml/badge.svg)](https://github.com/FacundoRoman01/retail-lakehouse-pyspark/actions/workflows/tests.yml)

Pipeline end-to-end de Ingeniería de Datos basado en la **Arquitectura Medallón** (Bronze, Silver y Gold) con diseño **multi-entorno (Híbrido Local / Cloud)**. El proyecto ingesta datos transaccionales crudos de retail, aplica auditoría y reglas de calidad de datos (Data Quality), calcula métricas clave de negocio (KPIs) y persiste los resultados tanto en un **entorno local** (Parquet + MySQL en Docker) como en **Databricks Lakehouse** (Unity Catalog Volumes + tablas administradas **Delta Lake**).

Incluye una suite de **pruebas unitarias automatizadas con Pytest**, **Integración Continua (CI) mediante GitHub Actions** y **orquestación en la nube con Databricks Jobs**.

---

## 🏗 Arquitectura del Sistema

El pipeline detecta automáticamente el entorno de ejecución (`DATABRICKS_RUNTIME_VERSION`) y adapta la estrategia de almacenamiento sin modificar el código de negocio:

```text
[ Dataset Crudo (CSV) ]
           │
           ▼
     [ BRONZE ]        --> Local: data/bronze/online_retail_raw.csv
           │               Cloud: /Volumes/workspace/default/raw_data/
           │               • Ingesta del dato histórico sin modificaciones
           │               • Detección de anomalías y auditoría exploratoria
           ▼
     [ SILVER ]        --> Local: data/silver/ventas/ (Parquet)
           │               Cloud: workspace.default.silver_ventas (Delta Lake)
           │               • Filtrado de registros inválidos (precios <= 0)
           │               • Imputación de clientes nulos ('CONSUMIDOR_FINAL')
           │               • Clasificación transaccional (VENTA / DEVOLUCION)
           │               • Enriquecimiento con monto_total y fechas limpias
           ▼
     [  GOLD  ]        --> Capa Analítica y de Consumo
           │
     ┌─────┴─────────────────────────────────┐
     ▼                                       ▼
[ Entorno Local ]                   [ Databricks Cloud ]
Parquet + MySQL 8.0 (Docker)        Unity Catalog & Delta Lake
- kpi_ventas_provincia              • workspace.default.gold_kpi_provincia
- kpi_rendimiento_productos         • workspace.default.gold_kpi_productos
(Consumo: BI / SQL Local)           (Consumo: Databricks SQL Warehouse)
```

---

## 🛠 Tecnologías Utilizadas

- **Apache Spark / PySpark:** Motor de cómputo distribuido para la ingesta, limpieza y agregación analítica.
- **Databricks Serverless & Unity Catalog:** Gobierno centralizado de datos (Volumes y Tablas Administradas) y cómputo elástico en la nube.
- **Delta Lake:** Almacenamiento transaccional ACID, control de esquema (*Schema Enforcement*) y versionado (*Time Travel*) para las capas Silver y Gold.
- **Databricks Jobs:** Orquestación de flujos de trabajo conectada directamente al repositorio Git.
- **Python & SQL (Window Functions):** Lenguaje de orquestación, scripting y consultas analíticas avanzadas (`DENSE_RANK`, `SUM() OVER ()`).
- **Pytest:** Framework para desarrollo y automatización de pruebas unitarias sobre DataFrames de PySpark.
- **GitHub Actions:** Orquestación de CI/CD para testing continuo automatizado sobre entornos virtuales Linux Ubuntu.
- **PyArrow & Fastparquet:** Almacenamiento columnar y compresión Snappy nativa en disco para el entorno local.
- **Pandas & SQLAlchemy:** Conversión y carga relacional optimizada mediante inserción masiva por lotes (*bulk insert*).
- **Docker & Docker Compose:** Contenerización del Data Warehouse local (MySQL 8.0).
- **Logging & Error Handling:** Trazabilidad estandarizada mediante `logging` nativo y captura de excepciones con liberación controlada de la JVM de Spark.

---

## 📂 Estructura del Proyecto

```text
retail-lakehouse-pyspark/
├── .github/
│   └── workflows/
│       └── tests.yml             # Workflow de CI para testing en GitHub Actions
├── data/
│   ├── bronze/                   # Archivo crudo inicial (online_retail_raw.csv)
│   ├── silver/                   # Datos curados y normalizados en Parquet
│   └── gold/                     # Tablas agregadas analíticas en Parquet
│       ├── kpi_rendimiento_productos/
│       └── kpi_ventas_provincia/
├── src/
│   ├── __init__.py
│   ├── bronze_to_silver.py       # Reglas de limpieza, imputación y Data Quality
│   ├── explore_bronze.py         # Script exploratorio y auditoría estadística inicial
│   ├── generate_raw_data.py      # Generador sintético de transacciones retail
│   ├── silver_to_gold.py         # Cálculo de KPIs analíticos y carga a MySQL / Parquet
│   └── main.py                   # Orquestador principal del pipeline
├── test/
│   ├── __init__.py
│   ├── conftest.py               # Fixture para inicializar la sesión Spark aislada
│   ├── test_gold_metrics.py      # Tests unitarios para las agregaciones de capa Gold
│   └── test_silver_clean.py      # Tests unitarios para las reglas de limpieza Silver
├── .env.example                  # Plantilla de variables de entorno requeridas
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

## ☁️ Despliegue y Orquestación en Databricks

El pipeline está configurado para ejecutarse de forma nativa en **Databricks Serverless** mediante un **Databricks Job** (`retail-lakehouse-etl`) sincronizado con la rama `main` de este repositorio:

1. **Gobierno en Unity Catalog:** Creación automática del esquema `workspace.default`, almacenamiento de archivos crudos en el Volume `raw_data` y registro de tablas administradas Delta (`silver_ventas`, `gold_kpi_provincia`, `gold_kpi_productos`).
2. **Orquestación con Jobs:** Ejecución modular de `src/main.py` con trazabilidad de tiempos por etapa (*Timeline*) y monitoreo de estado.
3. **Analítica Avanzada en SQL Warehouse:** Explotación de la capa Gold mediante consultas analíticas con **Window Functions** (`DENSE_RANK()` para Top-N por categoría y `SUM() OVER ()` para participación porcentual sobre la facturación nacional).

### Evidencia de Ejecución en la Nube

| Gobierno en Unity Catalog | Orquestación (Databricks Jobs) | Analítica con SQL Window Functions |
| :---: | :---: | :---: |
| ![Unity Catalog](docs/unity_catalog.png) | ![Job Timeline](docs/job_timeline.png) | ![SQL Editor](docs/sql_analytics.png) |

---





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

---

## 🔍 Verificación de Datos

### En Databricks SQL Editor

```sql
SELECT 
    provincia, 
    facturacion_neta, 
    ROUND((facturacion_neta / SUM(facturacion_neta) OVER ()) * 100, 2) AS porcentaje_participacion
FROM workspace.default.gold_kpi_provincia
ORDER BY porcentaje_participacion DESC;
```

<img width="1617" height="800" alt="image" src="https://github.com/user-attachments/assets/03fa5564-1f1f-4524-978e-d5c6e417ded5" />

