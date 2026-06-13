"""
# 05 - Reporte de calidad y conclusiones
## Objetivo
Consultas SQL de verificación, visualización de métricas y reflexión crítica.
"""
import os, sys, json
sys.path.insert(0, os.path.abspath(".."))

from src.utils import load_config, create_spark_session, setup_logger

config = load_config("config/etl_config.yaml")
log = setup_logger("reporting")

spark = create_spark_session(config)

db_path = config["database"]["path"]
url = f"jdbc:sqlite:{db_path}"

def query(sql, label):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    try:
        df = spark.read.format("jdbc").options(url=url, driver="org.sqlite.JDBC", dbtable=f"({sql}) tmp").load()
        df.show(truncate=False, n=50)
        print(f"  ({df.count()} filas)")
    except Exception as e:
        print(f"  ERROR: {e}")

# --- Consulta 1: Revenue por servicio ---
query("""
    SELECT
        service_type,
        COUNT(*) AS total_trips,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM gold_trips_clean
    GROUP BY service_type
    ORDER BY total_revenue DESC
""", "CONSULTA 1: Ingresos por tipo de servicio")

# --- Consulta 2: Métricas de calidad ---
query("""
    SELECT
        service_type,
        year,
        month,
        total_records,
        valid_records,
        rejected_records,
        quality_percentage
    FROM quality_metrics_summary
    ORDER BY year, month, service_type
""", "CONSULTA 2: Métricas de calidad por servicio/año/mes")

# --- Consulta 3: Top rutas por revenue ---
query("""
    SELECT
        pickup_location_id,
        dropoff_location_id,
        COUNT(*) AS total_trips,
        ROUND(SUM(total_amount), 2) AS total_revenue,
        ROUND(AVG(trip_duration_minutes), 2) AS avg_duration
    FROM gold_trips_clean
    GROUP BY pickup_location_id, dropoff_location_id
    ORDER BY total_revenue DESC
    LIMIT 20
""", "CONSULTA 3: Top 20 rutas por ingreso")

# --- Consulta 4: Viajes sospechosos ---
query("""
    SELECT
        service_type,
        COUNT(*) AS suspicious_count,
        ROUND(AVG(trip_duration_minutes), 2) AS avg_duration,
        ROUND(AVG(fare_amount), 2) AS avg_fare
    FROM gold_trips_clean
    WHERE is_suspicious_trip = 1
    GROUP BY service_type
    ORDER BY suspicious_count DESC
""", "CONSULTA 4: Viajes sospechosos por servicio")

# --- Consulta 5: Revenue diario ---
query("""
    SELECT
        service_type,
        trip_date,
        total_trips,
        total_revenue,
        average_fare,
        quality_percentage
    FROM gold_daily_revenue
    ORDER BY trip_date DESC
    LIMIT 20
""", "CONSULTA 5: Revenue diario (últimos 20)")

# --- Resumen general ---
print(f"\n{'='*60}")
print("  REFLEXIÓN CRÍTICA")
print(f"{'='*60}")
print("""
1. RIESGOS DE DATOS DAÑADOS:
   - Archivos corruptos pueden detener pipelines completos si no se manejan con try/except.
   - Metadatos inconsistentes (tipos ilegales) impiden la lectura y requieren aislamiento.
   - Sin trazabilidad, los datos corruptos generan silenciosamente métricas erróneas.

2. IMPACTO EN TOMA DE DECISIONES:
   - Registros con fechas inválidas o montos negativos distorsionan promedios y totales.
   - Duplicados inflan conteos de viajes y revenue, llevando a decisiones operativas incorrectas.
   - Una calidad < 95% debe desencadenar una alerta antes de consumir los datos.

3. MEDIDAS PREVENTIVAS EN PRODUCCIÓN:
   - Monitoreo de esquemas: validar cada archivo contra un schema registry antes de procesar.
   - Circuit breaker: si la tasa de rechazo supera un umbral (ej. 10%), pausar el pipeline.
   - Data contracts: acordar esquemas y reglas con las fuentes antes de la ingesta.
   - Alertas automatizadas: notificar al equipo cuando archivos van a cuarentena.
   - Idempotencia: diseñar cargas UPSERT para evitar duplicación en re-ejecuciones.
""")

spark.stop()
print("\nReporte de calidad completado.")
