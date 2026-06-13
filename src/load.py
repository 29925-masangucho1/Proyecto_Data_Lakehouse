import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    when,
    round,
    sum,
    to_date,
)


class Loader:
    def __init__(self, spark, config, log, process_id):
        self.spark = spark
        self.config = config
        self.log = log
        self.process_id = process_id
        self.db_type = config["database"]["type"]
        self.db_path = config["database"]["path"]
        self.tables = config["database"]["tables"]

    def _get_jdbc_url(self):
        if self.db_type == "sqlite":
            return f"jdbc:sqlite:{self.db_path}"
        return None

    def _write_sqlite(self, df, table_name, mode="append"):
        url = self._get_jdbc_url()
        df.write.mode(mode).format("jdbc").options(
            url=url,
            driver="org.sqlite.JDBC",
            dbtable=table_name,
        ).save()

    def write_gold_trips_clean(self, df):
        table = self.tables["gold_trips_clean"]
        self.log.info(f"Writing {table} ({df.count()} records)")
        self._write_sqlite(df, table)

    def write_gold_daily_revenue(self, df):
        table = self.tables["gold_daily_revenue"]
        daily = (
            df.withColumn("trip_date", to_date(col("pickup_datetime")))
            .groupBy("service_type", "trip_date")
            .agg(
                count("*").alias("total_trips"),
                round(sum("total_amount"), 2).alias("total_revenue"),
                round(avg("fare_amount"), 2).alias("average_fare"),
                round(avg("tip_amount"), 2).alias("average_tip"),
                round(avg("trip_distance"), 2).alias("average_trip_distance"),
                round(avg("trip_duration_minutes"), 2).alias("average_trip_duration"),
            )
        )
        self.log.info(f"Writing {table} ({daily.count()} records)")
        self._write_sqlite(daily, table)

    def write_gold_location_performance(self, df):
        table = self.tables["gold_location_performance"]
        perf = (
            df.groupBy(
                "service_type", "pickup_location_id", "dropoff_location_id"
            )
            .agg(
                count("*").alias("total_trips"),
                round(sum("total_amount"), 2).alias("total_revenue"),
                round(avg("fare_amount"), 2).alias("average_fare"),
                round(avg("trip_distance"), 2).alias("average_distance"),
                round(avg("trip_duration_minutes"), 2).alias("average_duration"),
                round(
                    sum(when(col("is_suspicious_trip") == True, 1).otherwise(0)), 0
                ).alias("suspicious_trip_count"),
            )
        )
        self.log.info(f"Writing {table} ({perf.count()} records)")
        self._write_sqlite(perf, table)

    def write_quality_rejected(self, df):
        if df is None:
            self.log.info("No rejected records to write")
            return
        table = self.tables["quality_rejected_records"]
        self.log.info(f"Writing {table} ({df.count()} records)")
        self._write_sqlite(df, table)

    def write_quality_metrics(self, metrics_list):
        table = self.tables["quality_metrics_summary"]
        from pyspark.sql import Row

        rows = [Row(**m) for m in metrics_list]
        df = self.spark.createDataFrame(rows)
        self.log.info(f"Writing {table} ({df.count()} records)")
        self._write_sqlite(df, table)

    def write_audit_inventory(self, inventory):
        table = self.tables["audit_file_inventory"]
        from pyspark.sql import Row

        rows = [Row(**e) for e in inventory]
        df = self.spark.createDataFrame(rows)
        self.log.info(f"Writing {table} ({df.count()} records)")
        self._write_sqlite(df, table)

    def load_all(
        self,
        valid_df,
        rejected_df,
        metrics_list,
        inventory,
    ):
        self.write_gold_trips_clean(valid_df)
        self.write_gold_daily_revenue(valid_df)
        self.write_gold_location_performance(valid_df)
        self.write_quality_rejected(rejected_df)
        self.write_quality_metrics(metrics_list)
        self.write_audit_inventory(inventory)
        self.log.info("All tables written successfully")


def run_load(spark, config, valid_df, rejected_df, metrics_list, inventory, process_id, log):
    loader = Loader(spark, config, log, process_id)
    loader.load_all(valid_df, rejected_df, metrics_list, inventory)
