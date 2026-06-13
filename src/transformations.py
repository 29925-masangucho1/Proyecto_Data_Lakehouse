from datetime import datetime, timezone

from pyspark.sql.functions import (
    col,
    coalesce,
    concat,
    lit,
    md5,
    round,
    unix_timestamp,
    when,
)


class Transformer:
    def __init__(self, spark, config, log):
        self.spark = spark
        self.config = config
        self.log = log

    def normalize_column_names(self, df):
        mapping = {}
        for f in df.schema.fields:
            mapping[f.name] = f.name.lower()
        for old, new in mapping.items():
            df = df.withColumnRenamed(old, new)
        return df

    def cast_columns_safe(self, df, col_name, target_type):
        return df.withColumn(
            col_name,
            when(col(col_name).isNull(), lit(None))
            .otherwise(col(col_name).cast(target_type)),
        )

    def generate_trip_id(self, df):
        return df.withColumn(
            "trip_id",
            md5(
                concat(
                    col("source_file"),
                    lit("_"),
                    coalesce(col("pickup_datetime").cast("string"), lit("null")),
                    lit("_"),
                    coalesce(col("dropoff_datetime").cast("string"), lit("null")),
                    lit("_"),
                    coalesce(col("pickup_location_id").cast("string"), lit("null")),
                    lit("_"),
                    coalesce(col("dropoff_location_id").cast("string"), lit("null")),
                )
            ),
        )

    def compute_trip_duration(self, df):
        return df.withColumn(
            "trip_duration_minutes",
            round(
                (
                    unix_timestamp(col("dropoff_datetime"))
                    - unix_timestamp(col("pickup_datetime"))
                )
                / 60.0,
                2,
            ),
        )

    def compute_average_speed(self, df):
        return df.withColumn(
            "average_speed_mph",
            when(
                (col("trip_duration_minutes") > 0) & (col("trip_distance").isNotNull()),
                round(
                    col("trip_distance")
                    / (col("trip_duration_minutes") / 60.0),
                    2,
                ),
            ).otherwise(lit(None)),
        )

    def compute_fare_per_mile(self, df):
        return df.withColumn(
            "fare_per_mile",
            when(
                (col("trip_distance") > 0) & (col("fare_amount").isNotNull()),
                round(col("fare_amount") / col("trip_distance"), 2),
            ).otherwise(lit(None)),
        )

    def compute_tip_percentage(self, df):
        return df.withColumn(
            "tip_percentage",
            when(
                (col("fare_amount").isNotNull())
                & (col("fare_amount") > 0)
                & (col("tip_amount").isNotNull()),
                round((col("tip_amount") / col("fare_amount")) * 100, 2),
            ).otherwise(lit(None)),
        )

    def mark_airport_trips(self, df):
        airport_zones = [1, 132, 138]
        return df.withColumn(
            "is_airport_trip",
            col("pickup_location_id").isin(airport_zones)
            | col("dropoff_location_id").isin(airport_zones),
        )

    def mark_suspicious(self, df):
        return df.withColumn(
            "is_suspicious_trip",
            (col("trip_distance") <= 0)
            | (col("total_amount") <= 0)
            | (col("fare_amount") < 0)
            | (col("trip_duration_minutes") <= 0)
            | (col("trip_duration_minutes") > 480)
            | (col("average_speed_mph") > 100)
            | (col("tip_percentage") > 100)
            | (col("pickup_datetime") > col("dropoff_datetime"))
            | (col("pickup_datetime") > lit(datetime.now(timezone.utc))),
        )

    def enrich_with_partition(self, df):
        return df.withColumn("year", col("year").cast("int")).withColumn(
            "month", col("month").cast("int")
        )

    def transform_all(self, df):
        df = self.normalize_column_names(df)
        df = self.generate_trip_id(df)
        df = self.compute_trip_duration(df)
        df = self.compute_average_speed(df)
        df = self.compute_fare_per_mile(df)
        df = self.compute_tip_percentage(df)
        df = self.mark_airport_trips(df)
        df = self.mark_suspicious(df)
        df = self.enrich_with_partition(df)
        df = df.withColumn(
            "processing_date",
            lit(datetime.now(timezone.utc).isoformat()).cast("timestamp"),
        )
        return df


def run_transformations(spark, config, recovered_dfs, log):
    transformer = Transformer(spark, config, log)
    transformed = {}
    for fname, df in recovered_dfs.items():
        log.info(f"Transforming {fname}")
        try:
            result = transformer.transform_all(df)
            cnt = result.count()
            transformed[fname] = result
            log.info(f"  -> {cnt} records after transformation")
        except Exception as e:
            log.error(f"Transformation failed for {fname}: {e}")
    return transformed
