import os
from datetime import datetime, timezone

from pyspark.sql.functions import col, lit

from src.utils import read_json_metadata


class SchemaDiagnostic:
    def __init__(self, spark, config, log):
        self.spark = spark
        self.config = config
        self.log = log
        self.metadata_dir = config["paths"]["metadata"]
        self.canonical = read_json_metadata(config["canonical_schema"])
        self.business_rules = read_json_metadata(config["business_rules"])

    def load_expected_schema(self, service):
        path = os.path.join(
            self.metadata_dir, f"expected_schema_{service}.json"
        )
        return read_json_metadata(path)

    def compare_schemas(self, actual_fields, expected_cols):
        actual_names = {f.name.lower(): f for f in actual_fields}
        expected_names = {c["name"].lower(): c for c in expected_cols}
        missing = []
        extra = []
        type_mismatch = []
        for ename, ec in expected_names.items():
            if ename not in actual_names:
                missing.append(ec["name"])
            else:
                af = actual_names[ename]
                etype = ec["type"]
                atype = self._spark_type_to_str(af.dataType)
                if not self._types_compatible(atype, etype):
                    type_mismatch.append(
                        {
                            "column": ec["name"],
                            "expected_type": etype,
                            "actual_type": atype,
                        }
                    )
        for aname in actual_names:
            if aname not in expected_names:
                extra.append(actual_names[aname].name)
        return {"missing": missing, "extra": extra, "type_mismatch": type_mismatch}

    def _spark_type_to_str(self, dt):
        s = str(dt).lower()
        if "long" in s:
            return "long"
        if "integer" in s:
            return "int"
        if "double" in s:
            return "double"
        if "string" in s:
            return "string"
        if "timestamp" in s:
            return "timestamp_ntz"
        return s

    def _types_compatible(self, actual, expected):
        compat = {
            ("long", "long"): True,
            ("long", "double"): True,
            ("double", "double"): True,
            ("double", "long"): True,
            ("int", "long"): True,
            ("int", "double"): True,
            ("string", "string"): True,
            ("timestamp_ntz", "timestamp_ntz"): True,
            ("timestamp", "timestamp_ntz"): True,
        }
        return compat.get((actual, expected), False)

    def classify_recoverability(self, read_status, diagnostic, record_count):
        if read_status != "SUCCESS":
            return "NOT_RECOVERABLE_CORRUPT_METADATA"
        if record_count == 0:
            return "NOT_RECOVERABLE_EMPTY_FILE"
        if diagnostic["type_mismatch"]:
            return "RECOVERABLE_TYPE_CASTING"
        if diagnostic["missing"]:
            return "RECOVERABLE_MISSING_COLUMNS"
        return "RECOVERABLE"

    def build_canonical_df(self, df, service, source_file):
        mapping = self.canonical["column_mapping"][service]
        exprs = []
        for cdef in self.canonical["columns"]:
            cname = cdef["name"]
            src = mapping.get(cname)
            if src is None:
                exprs.append(lit(None).cast(cdef["type"]).alias(cname))
            else:
                exprs.append(col(src).alias(cname))
        base = df.select(*exprs)
        base = base.withColumn("service_type", lit(service))
        base = base.withColumn("source_file", lit(source_file))
        base = base.withColumn(
            "ingestion_timestamp",
            lit(datetime.now(timezone.utc).isoformat()).cast("timestamp"),
        )
        base = base.withColumn("quality_status", lit("pending"))
        base = base.withColumn("is_suspicious_trip", lit(False))
        return base

    def homologate_column(self, df, service, source_file):
        return self.build_canonical_df(df, service, source_file)


def run_schema_recovery(spark, config, inventory, log):
    diagnostics = {}
    recovered_dfs = {}
    diag = SchemaDiagnostic(spark, config, log)
    for entry in inventory:
        fname = entry["file_name"]
        svc = entry["service_type"]
        if entry["read_status"] != "SUCCESS":
            diagnostics[fname] = {
                "recovery_category": "NOT_RECOVERABLE_CORRUPT_METADATA",
                "diagnostic": {"missing": [], "extra": [], "type_mismatch": []},
            }
            continue
        if svc == "bad_parquet":
            diagnostics[fname] = {
                "recovery_category": "PARTIALLY_RECOVERABLE",
                "diagnostic": {"missing": [], "extra": [], "type_mismatch": []},
            }
            continue
        try:
            expected = diag.load_expected_schema(svc)
            actual = spark.read.parquet(entry["file_path"]).schema.fields
            diag_result = diag.compare_schemas(actual, expected["columns"])
            cat = diag.classify_recoverability(
                entry["read_status"], diag_result, entry["record_count"]
            )
            diagnostics[fname] = {
                "recovery_category": cat,
                "diagnostic": diag_result,
            }
            if cat.startswith("RECOVERABLE"):
                df = spark.read.parquet(entry["file_path"])
                canonical = diag.build_canonical_df(df, svc, fname)
                recovered_dfs[fname] = canonical
                log.info(
                    f"Recovered {fname} -> {cat} ({canonical.count()} records, {len(canonical.columns)} cols)"
                )
            else:
                log.warning(f"Not recoverable: {fname} -> {cat}")
        except Exception as e:
            diagnostics[fname] = {
                "recovery_category": "NOT_RECOVERABLE_CORRUPT_METADATA",
                "diagnostic": {"missing": [], "extra": [], "type_mismatch": []},
                "error": str(e)[:200],
            }
            log.error(f"Diagnostic failed for {fname}: {e}")
    return diagnostics, recovered_dfs
