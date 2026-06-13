import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import yaml
from pyspark.sql import SparkSession


def setup_logger(name, log_file=None, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


def generate_process_id():
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"ETL_{timestamp}_{uid}"


def load_config(config_path="config/etl_config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def create_spark_session(config):
    builder = SparkSession.builder.appName(config["etl"]["spark_app_name"])
    builder = builder.master(config["etl"].get("spark_master", "local[*]"))
    builder = builder.config(
        "spark.sql.shuffle.partitions",
        config["etl"].get("shuffle_partitions", 200),
    )
    if config.get("spark_optimizations", {}).get("enableVectorizedReader", True):
        builder = builder.config("spark.sql.parquet.enableVectorizedReader", "true")
    if config["spark_optimizations"].get("adaptiveQueryExecution", True):
        builder = builder.config("spark.sql.adaptive.enabled", "true")
    if config["spark_optimizations"].get("parquetEnableDictionary", True):
        builder = builder.config("spark.sql.parquet.enableDictionary", "true")
    return builder.getOrCreate()


def compute_schema_hash(schema_fields):
    field_str = "|".join([f"{f.name}:{f.dataType}" for f in schema_fields])
    return hashlib.md5(field_str.encode()).hexdigest()


def read_json_metadata(path):
    with open(path, "r") as f:
        return json.load(f)


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def write_df_with_coalesce(df, output_path, partitions=None, mode="overwrite"):
    if partitions and len(partitions) > 0:
        writer = df.write.mode(mode).partitionBy(partitions)
    else:
        writer = df.write.mode(mode)
    writer.parquet(output_path)
