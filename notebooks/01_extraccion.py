"""
# 01 - Extracción de datos
## Objetivo
Extraer archivos Parquet desde raw, construir inventario técnico y detectar corruptos.
"""
import os, sys, json
sys.path.insert(0, os.path.abspath(".."))

from src.utils import load_config, create_spark_session, generate_process_id, ensure_dir, setup_logger

config = load_config("config/etl_config.yaml")
process_id = generate_process_id()
log = setup_logger("extraction")

print(f"Process ID: {process_id}")
spark = create_spark_session(config)

# --- Extracción ---
from src.extract import run_extraction

inventory = run_extraction(spark, config, process_id, log)

print(f"\n{'='*60}")
print(f"Total archivos procesados: {len(inventory)}")
print(f"{'='*60}")

ok = [i for i in inventory if i["read_status"] == "SUCCESS"]
failed = [i for i in inventory if i["read_status"] == "FAILED"]
print(f"  OK: {len(ok)}")
print(f"  FAILED: {len(failed)}")

print(f"\n{'='*60}")
print("INVENTARIO TÉCNICO")
print(f"{'='*60}")
for i in inventory:
    status = "OK" if i["read_status"] == "SUCCESS" else "FAIL"
    print(f"  {i['file_name']:45s} | {status:4s} | registros={i['record_count']:>10d} | cols={i['column_count']} | {i.get('recovery_category','')}")

if failed:
    print(f"\n{'='*60}")
    print("ARCHIVOS FALLIDOS")
    print(f"{'='*60}")
    for f in failed:
        print(f"  {f['file_name']:45s} | {f['error_message'][:100]}")

# Guardar inventario como JSON
audit_dir = config["paths"]["audit"]
ensure_dir(audit_dir)
with open(f"{audit_dir}/audit_file_inventory_{process_id}.json", "w") as f:
    json.dump(inventory, f, indent=2, default=str)

print(f"\nInventario guardado en {audit_dir}/audit_file_inventory_{process_id}.json")
spark.stop()
