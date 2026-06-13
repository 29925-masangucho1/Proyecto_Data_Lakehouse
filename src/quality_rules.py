from datetime import datetime, timezone

from pyspark.sql.functions import col, count, lit


class QualityValidator:
    def __init__(self, spark, config, log, process_id):
        self.spark = spark
        self.config = config
        self.log = log
        self.process_id = process_id
        self.critical_cols = config["quality"]["critical_columns"]

    def validate_not_null_critical(self, df):
        condition = col(self.critical_cols[0]).isNull()
        for c in self.critical_cols[1:]:
            condition = condition | col(c).isNull()
        return df.filter(condition)

    def validate_invalid_dates(self, df):
        return df.filter(
            col("pickup_datetime").isNull()
            | col("dropoff_datetime").isNull()
            | (col("pickup_datetime") > col("dropoff_datetime"))
            | (col("pickup_datetime") > lit(datetime.now(timezone.utc)))
        )

    def validate_negative_amounts(self, df):
        return df.filter(
            (col("fare_amount") < 0) | (col("total_amount") < 0)
        )

    def validate_zero_distance(self, df):
        return df.filter(
            col("trip_distance").isNotNull() & (col("trip_distance") <= 0)
        )

    def validate_invalid_duration(self, df):
        return df.filter(
            col("trip_duration_minutes").isNotNull()
            & (
                (col("trip_duration_minutes") <= 0)
                | (col("trip_duration_minutes") > 480)
            )
        )

    def validate_unrealistic_speed(self, df):
        return df.filter(
            col("average_speed_mph").isNotNull() & (col("average_speed_mph") > 100)
        )

    def find_duplicates(self, df):
        return df.groupBy(
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id",
            "fare_amount",
        ).agg(count("*").alias("dup_count")).filter(col("dup_count") > 1)

    def separate_records(self, df):
        invalid_conditions = (
            col("pickup_datetime").isNull()
            | col("dropoff_datetime").isNull()
            | (col("pickup_datetime") > col("dropoff_datetime"))
            | (col("pickup_datetime") > lit(datetime.now(timezone.utc)))
            | (col("fare_amount") < 0)
            | (col("total_amount") < 0)
            | (
                col("trip_distance").isNotNull() & (col("trip_distance") <= 0)
            )
            | (
                col("trip_duration_minutes").isNotNull()
                & (
                    (col("trip_duration_minutes") <= 0)
                    | (col("trip_duration_minutes") > 480)
                )
            )
            | (
                col("average_speed_mph").isNotNull()
                & (col("average_speed_mph") > 100)
            )
        )
        valid = df.filter(~invalid_conditions)
        rejected = df.filter(invalid_conditions)
        rejected = rejected.withColumn("quality_status", lit("rejected_business"))
        valid = valid.withColumn("quality_status", lit("valid"))
        return valid, rejected

    def build_rejected_records(self, df, stage):
        records = []
        for row in df.collect():
            row_dict = row.asDict()
            records.append(
                {
                    "process_id": self.process_id,
                    "trip_id": row_dict.get("trip_id"),
                    "service_type": row_dict.get("service_type"),
                    "source_file": row_dict.get("source_file"),
                    "rejection_stage": stage,
                    "rejection_rule": stage,
                    "rejection_column": self._find_rejection_col(row_dict),
                    "original_value": str(row_dict),
                    "technical_reason": stage,
                    "business_reason": self._business_reason(stage),
                    "rejected_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        return records

    def _find_rejection_col(self, row):
        for c in self.critical_cols:
            if row.get(c) is None:
                return c
        if row.get("fare_amount", 0) is not None and row.get("fare_amount", 0) < 0:
            return "fare_amount"
        if row.get("total_amount", 0) is not None and row.get("total_amount", 0) < 0:
            return "total_amount"
        if row.get("trip_distance") is not None and row.get("trip_distance") <= 0:
            return "trip_distance"
        return "unknown"

    def _business_reason(self, stage):
        reasons = {
            "null_critical": "Critical field is null, cannot compute metrics",
            "invalid_dates": "Invalid or inconsistent datetime values",
            "negative_amounts": "Negative monetary values not allowed",
            "zero_distance": "Trip distance must be positive",
            "invalid_duration": "Trip duration out of valid range",
            "unrealistic_speed": "Average speed exceeds maximum threshold",
            "duplicate": "Technical duplicate detected",
        }
        return reasons.get(stage, stage)

    def compute_metrics(self, valid_count, rejected_count, total_count, svc, year, month):
        pct = round((valid_count / total_count * 100), 2) if total_count > 0 else 0
        return {
            "process_id": self.process_id,
            "service_type": svc,
            "year": year,
            "month": month,
            "total_records": total_count,
            "valid_records": valid_count,
            "rejected_records": rejected_count,
            "duplicate_records": 0,
            "null_critical_records": 0,
            "suspicious_records": 0,
            "quality_percentage": pct,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }


def run_quality_checks(spark, config, transformed_dfs, process_id, log):
    validator = QualityValidator(spark, config, log, process_id)
    all_valid = []
    all_rejected = []
    metrics = []

    for fname, df in transformed_dfs.items():
        log.info(f"Quality check for {fname}")
        svc = df.select("service_type").first()[0]
        year = df.select("year").first()[0]
        month = df.select("month").first()[0]
        total = df.count()
        valid, rejected = validator.separate_records(df)
        vcnt = valid.count()
        rcnt = rejected.count()
        all_valid.append(valid)
        if rcnt > 0:
            all_rejected.append(rejected)
        m = validator.compute_metrics(vcnt, rcnt, total, svc, year, month)
        metrics.append(m)
        log.info(f"  Valid: {vcnt}, Rejected: {rcnt}, Quality: {m['quality_percentage']}%")

    final_valid = all_valid[0] if all_valid else None
    for v in all_valid[1:]:
        final_valid = final_valid.union(v)

    final_rejected = None
    if all_rejected:
        final_rejected = all_rejected[0]
        for r in all_rejected[1:]:
            final_rejected = final_rejected.union(r)

    return final_valid, final_rejected, metrics
