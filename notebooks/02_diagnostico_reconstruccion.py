"""
# 02 - Diagnóstico y reconstrucción de esquemas
## Objetivo
Comparar esquemas reales vs esperados, clasificar recuperabilidad y reconstruir esquema canónico.
"""
import os, sys, json
sys.path.insert(0, os.path.abspath(".."))

from src.utils import load_config, create_spark_session, generate_process_id, ensure_dir, setup_logger

config = load_config("config/etl_config.yaml")
process_id = generate_process_id()
log = setup_logger("schema_recovery")

spark = create_spark_session(config)

# Cargar inventario previo
audit_dir = config["paths"]["audit"]
inventory_files = [f for f in os.listdir(audit_dir) if f.startswith("audit_file_inventory_") and f.endswith(".json")]
if not inventory_files:
    log.error("No se encontró inventario. Ejecute 01_extraccion primero.")
    spark.stop()
    exit()

with open(f"{audit_dir}/{inventory_files[-1]}") as f:
    inventory = json.load(f)

# --- Diagnóstico ---
from src.schema_recovery import run_schema_recovery

diagnostics, recovered_dfs = run_schema_recovery(spark, config, inventory, log)

print(f"\n{'='*60}")
print("DIAGNÓSTICO DE ESQUEMAS")
print(f"{'='*60}")
for fname, diag in diagnostics.items():
    cat = diag["recovery_category"]
    det = diag["diagnostic"]
    missing = ", ".join(det["missing"]) if det["missing"] else "none"
    extra = ", ".join(det["extra"]) if det["extra"] else "none"
    tm = ", ".join([f"{t['column']}({t['expected_type']}->{t['actual_type']})" for t in det["type_mismatch"]]) or "none"
    print(f"  {fname:45s} | {cat:40s}")
    print(f"  {'':45s}   faltantes: {missing}")
    print(f"  {'':45s}   extras:    {extra}")
    print(f"  {'':45s}   type_diff: {tm}")

print(f"\n{'='*60}")
print(f"Archivos recuperados: {len(recovered_dfs)}")
for fname, df in recovered_dfs.items():
    print(f"  {fname:45s} | {df.count():>8d} records | {len(df.columns)} cols canónicas")

# Guardar en bronze
bronze_dir = config["paths"]["bronze"]
ensure_dir(bronze_dir)
for fname, df in recovered_dfs.items():
    out_path = os.path.join(bronze_dir, fname.replace(".parquet", ""))
    df.write.mode("overwrite").parquet(out_path)
    log.info(f"Bronce escrito: {out_path}")

print(f"\nDatos guardados en bronze/")
spark.stop()
