import os
import traceback
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from src.utils import (
    compute_schema_hash,
)


class Extractor:
    def __init__(self, spark, config, process_id, logger):
        self.spark = spark
        self.config = config
        self.process_id = process_id
        self.log = logger
        self.raw_base = config["paths"]["raw"]
        self.audit_dir = config["paths"]["audit"]

    def discover_files(self, service):
        base = os.path.join(self.raw_base, service)
        files = []
        for root, dirs, fnames in os.walk(base):
            for f in fnames:
                if f.endswith(".parquet"):
                    path = os.path.join(root, f)
                    rel = os.path.relpath(path, self.raw_base)
                    parts = rel.replace("\\", "/").split("/")
                    files.append(
                        {
                            "file_name": f,
                            "file_path": path,
                            "service_type": service,
                            "partition_year": self._extract_partition(parts, "year"),
                            "partition_month": self._extract_partition(parts, "month"),
                        }
                    )
        return files

    def _extract_partition(self, parts, key):
        for p in parts:
            if p.startswith(f"{key}="):
                return p.split("=")[1]
        return None

    def read_single_file(self, file_info):
        path = file_info["file_path"]
        entry = {
            "process_id": self.process_id,
            "source_system": "NYC_TLC",
            "service_type": file_info["service_type"],
            "file_name": file_info["file_name"],
            "file_path": path,
            "file_size_mb": round(
                os.path.getsize(path) / (1024 * 1024), 4
            ),
            "partition_year": file_info["partition_year"],
            "partition_month": file_info["partition_month"],
            "read_status": "FAILED",
            "record_count": 0,
            "column_count": 0,
            "schema_hash": None,
            "error_message": None,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
        df = None
        try:
            df = self.spark.read.parquet(path)
            entry["read_status"] = "SUCCESS"
            entry["record_count"] = df.count()
            entry["column_count"] = len(df.columns)
            entry["schema_hash"] = compute_schema_hash(df.schema.fields)
        except Exception as e:
            entry["read_status"] = "FAILED"
            entry["error_message"] = f"{type(e).__name__}: {str(e)[:200]}"
            self.log.warning(f"Failed to read {path}: {e}")
            traceback.print_exc()
        return entry, df

    def read_partitioned_path(self, service, year, month):
        path = os.path.join(
            self.raw_base, service, f"year={year}", f"month={month}"
        )
        try:
            df = self.spark.read.parquet(path)
            self.log.info(
                f"Read partitioned path {path}: {df.count()} records, {len(df.columns)} cols"
            )
            return df
        except Exception as e:
            self.log.error(f"Failed to read partition {path}: {e}")
            return None

    def classify_file_status(self, entry):
        if entry["read_status"] == "SUCCESS":
            if entry["record_count"] == 0:
                return "NOT_RECOVERABLE_EMPTY_FILE"
            return "RECOVERABLE"
        msg = (entry.get("error_message") or "").lower()
        if "illegal parquet type" in msg or "corrupt" in msg:
            return "NOT_RECOVERABLE_CORRUPT_METADATA"
        if "empty" in msg and "file" in msg:
            return "NOT_RECOVERABLE_EMPTY_FILE"
        if "unsupported" in msg or "not a parquet" in msg:
            return "NOT_RECOVERABLE_UNSUPPORTED_FORMAT"
        return "NOT_RECOVERABLE_CORRUPT_METADATA"

    def build_file_inventory(self, service):
        files = self.discover_files(service)
        inventory = []
        for f in files:
            entry, df = self.read_single_file(f)
            entry["recovery_category"] = self.classify_file_status(entry)
            inventory.append(entry)
        return inventory


def run_extraction(spark, config, process_id, log):
    extractor = Extractor(spark, config, process_id, log)
    all_inventory = []
    for svc in ["yellow", "green", "fhvhv"]:
        log.info(f"Extracting inventory for service: {svc}")
        inv = extractor.build_file_inventory(svc)
        all_inventory.extend(inv)
    bad_parquet_dir = os.path.join(config["paths"]["raw"], "bad_parquet")
    if os.path.exists(bad_parquet_dir):
        log.info("Extracting bad_parquet files")
        for f in os.listdir(bad_parquet_dir):
            if not f.endswith(".parquet"):
                continue
            fp = os.path.join(bad_parquet_dir, f)
            entry = {
                "process_id": process_id,
                "source_system": "APACHE_PARQUET_TESTING",
                "service_type": "bad_parquet",
                "file_name": f,
                "file_path": fp,
                "file_size_mb": round(os.path.getsize(fp) / (1024 * 1024), 4),
                "partition_year": None,
                "partition_month": None,
                "read_status": "FAILED",
                "record_count": 0,
                "column_count": 0,
                "schema_hash": None,
                "error_message": None,
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                df = spark.read.parquet(fp)
                entry["read_status"] = "SUCCESS"
                entry["record_count"] = df.count()
                entry["column_count"] = len(df.columns)
                entry["schema_hash"] = compute_schema_hash(df.schema.fields)
            except Exception as e:
                entry["read_status"] = "FAILED"
                entry["error_message"] = f"{type(e).__name__}: {str(e)[:200]}"
                log.warning(f"bad_parquet failed: {f}: {e}")
            entry["recovery_category"] = (
                "RECOVERABLE"
                if entry["read_status"] == "SUCCESS" and entry["record_count"] > 0
                else "NOT_RECOVERABLE_CORRUPT_METADATA"
            )
            all_inventory.append(entry)
    return all_inventory
