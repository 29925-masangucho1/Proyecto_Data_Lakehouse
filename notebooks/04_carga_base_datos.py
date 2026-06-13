"""
# 04 - Carga en base de datos
## Objetivo
Persistir tablas gold, calidad y auditoría en SQLite.
"""
import os, sys, json
sys.path.insert(0, os.path.abspath(".."))

from src.utils import load_config, create_spark_session, generate_process_id, ensure_dir, setup_logger

config = load_config("config/etl_config.yaml")
process_id = generate_process_id()
log = setup_logger("load")

spark = create_spark_session(config)

# Cargar desde silver
silver_dir = config["paths"]["silver"]
valid_path = os.path.join(silver_dir, "valid_trips")
rejected_path = os.path.join(silver_dir, "rejected_trips")

if not os.path.exists(valid_path):
    log.error("No se encontraron datos válidos en silver. Ejecute 03_transformacion_validacion primero.")
    spark.stop()
    exit()

valid_df = spark.read.parquet(valid_path)
rejected_df = spark.read.parquet(rejected_path) if os.path.exists(rejected_path) else None

print(f"Valid trips: {valid_df.count()}")
if rejected_df:
    print(f"Rejected trips: {rejected_df.count()}")

# Cargar métricas e inventario
audit_dir = config["paths"]["audit"]
inventory_files = [f for f in os.listdir(audit_dir) if f.startswith("audit_file_inventory_") and f.endswith(".json")]
if inventory_files:
    with open(f"{audit_dir}/{inventory_files[-1]}") as f:
        inventory = json.load(f)
else:
    inventory = []

# Métricas desde silver
metrics = []
from pyspark.sql.functions import count as spark_count, sum as spark_sum, when, col
if valid_df is not None and rejected_df is not None:
    svc_months = valid_df.select("service_type", "year", "month").distinct().collect()
    for row in svc_months:
        svc, y, m = row["service_type"], row["year"], row["month"]
        vcnt = valid_df.filter(
            (col("service_type") == svc) & (col("year") == y) & (col("month") == m)
        ).count()
        rcnt = rejected_df.filter(
            (col("service_type") == svc) & (col("year") == y) & (col("month") == m)
        ).count() if rejected_df is not None else 0
        total = vcnt + rcnt
        pct = round(vcnt / total * 100, 2) if total > 0 else 0
        from datetime import datetime, timezone
        metrics.append({
            "process_id": process_id,
            "service_type": svc,
            "year": y,
            "month": m,
            "total_records": total,
            "valid_records": vcnt,
            "rejected_records": rcnt,
            "duplicate_records": 0,
            "null_critical_records": 0,
            "suspicious_records": 0,
            "quality_percentage": pct,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        })

# --- Carga a SQLite ---
from src.load import run_load

run_load(spark, config, valid_df, rejected_df, metrics, inventory, process_id, log)

print(f"\n{'='*60}")
print("CARGA COMPLETADA")
print(f"{'='*60}")
print(f"Base de datos: {config['database']['path']}")
print(f"Tablas cargadas:")
for tname, talias in config["database"]["tables"].items():
    try:
        cnt = spark.read.format("jdbc").options(
            url=f"jdbc:sqlite:{config['database']['path']}",
            driver="org.sqlite.JDBC",
            dbtable=talias,
        ).count()
        print(f"  {talias:35s}: {cnt} registros")
    except Exception as e:
        print(f"  {talias:35s}: ERROR ({str(e)[:50]})")

spark.stop()
