"""
# 03 - Transformación y validación de calidad
## Objetivo
Aplicar transformaciones, calcular métricas derivadas, separar válidos de rechazados.
"""
import os, sys, json
sys.path.insert(0, os.path.abspath(".."))

from src.utils import load_config, create_spark_session, generate_process_id, ensure_dir, setup_logger
from pyspark.sql.functions import col

config = load_config("config/etl_config.yaml")
process_id = generate_process_id()
log = setup_logger("transformations")

spark = create_spark_session(config)

# Cargar desde bronze
bronze_dir = config["paths"]["bronze"]
if not os.path.exists(bronze_dir) or not os.listdir(bronze_dir):
    log.error("Bronce vacío. Ejecute 02_diagnostico_reconstruccion primero.")
    spark.stop()
    exit()

recovered_dfs = {}
for entry in os.listdir(bronze_dir):
    path = os.path.join(bronze_dir, entry)
    if os.path.isdir(path) and any(f.endswith(".parquet") for _, _, fs in os.walk(path) for f in fs):
        df = spark.read.parquet(path)
        recovered_dfs[entry] = df
        log.info(f"Cargado desde bronze: {entry} -> {df.count()} records")

# --- Transformaciones ---
from src.transformations import run_transformations

transformed = run_transformations(spark, config, recovered_dfs, log)

# --- Calidad ---
from src.quality_rules import run_quality_checks

valid_df, rejected_df, metrics_list = run_quality_checks(spark, config, transformed, process_id, log)

print(f"\n{'='*60}")
print("MÉTRICAS DE CALIDAD POR SERVICIO/AÑO/MES")
print(f"{'='*60}")
for m in metrics_list:
    print(f"  {m['service_type']:8s} | {m['year']}-{m['month']:02d} | "
          f"total={m['total_records']:>8d} | válidos={m['valid_records']:>8d} | "
          f"rechazados={m['rejected_records']:>6d} | calidad={m['quality_percentage']:6.2f}%")

# --- Rechazados ---
if rejected_df and rejected_df.count() > 0:
    print(f"\n{'='*60}")
    print(f"REGISTROS RECHAZADOS: {rejected_df.count()}")
    print(f"{'='*60}")
    rejected_df.select("trip_id", "service_type", "rejection_stage", "rejection_rule").show(10, truncate=False)

# Guardar silver
silver_dir = config["paths"]["silver"]
ensure_dir(silver_dir)
if valid_df:
    valid_df.write.mode("overwrite").parquet(f"{silver_dir}/valid_trips")
    log.info(f"Silver válidos escrito: {silver_dir}/valid_trips ({valid_df.count()} records)")
if rejected_df:
    rejected_df.write.mode("overwrite").parquet(f"{silver_dir}/rejected_trips")
    log.info(f"Silver rechazados escrito: {silver_dir}/rejected_trips ({rejected_df.count()} records)")

print("\nTransformación y validación completada.")
spark.stop()
