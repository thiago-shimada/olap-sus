
import argparse
import re
import sys
import unicodedata
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def _normalize_col(col_name: str) -> str:
    # Remove accents, lower, non-word to underscore, collapse repeats
    nfkd = unicodedata.normalize("NFKD", col_name)
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^0-9A-Za-z]+", "_", ascii_str).strip("_").lower()
    s = re.sub(r"_+", "_", s)
    if not s:
        s = "col"
    return s


def _list_paths_with_glob(spark: SparkSession, glob_path: str) -> List[str]:
    # Use Hadoop FS glob to check existence and list files/prefixes
    try:
        jvm = spark._jvm
        sc = spark.sparkContext
        hconf = sc._jsc.hadoopConfiguration()
        Path = jvm.org.apache.hadoop.fs.Path
        path = Path(glob_path)
        fs = path.getFileSystem(hconf)
        statuses = fs.globStatus(path)
        if not statuses:
            return []
        return [status.getPath().toString() for status in statuses]
    except Exception:
        # If something goes wrong, fallback to trying to read and let Spark fail
        return [glob_path]


def cleanse_dataframe(df):
    # Standardize column names
    for old in df.columns:
        new = _normalize_col(old)
        if new != old:
            df = df.withColumnRenamed(old, new)

    # Trim strings and convert empty strings to nulls
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.when(F.length(F.trim(F.col(field.name))) == 0, F.lit(None))
                .otherwise(F.trim(F.col(field.name))),
            )

    # Drop exact duplicates
    df = df.dropDuplicates()
    return df


def parse_args():
    p = argparse.ArgumentParser(description="Ingest SUS CSV from MinIO into Postgres-OLAP (staging)")
    p.add_argument("--dataset", required=True, choices=["sih", "sinasc", "sim"], help="Source dataset")
    p.add_argument("--date", required=True, help="Partition date (YYYY-MM-DD)")
    p.add_argument("--bucket", default="landing", help="MinIO bucket name")
    p.add_argument("--prefix", default="source_sus", help="Top-level key prefix inside the bucket")
    p.add_argument("--pg-url", default="jdbc:postgresql://postgres-olap:5432/olap_db")
    p.add_argument("--pg-user", default="olap")
    p.add_argument("--pg-password", default="olap")
    p.add_argument("--pg-schema", default=None, help="Optional target schema")
    p.add_argument("--table-prefix", default="stg_", help="Target table name prefix")
    return p.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName(f"sus-ingest-{args.dataset}")
        # Spark S3A configs are expected to be provided via spark-submit --conf from Airflow
        .getOrCreate()
    )

    # Build S3A path pattern
    base = f"s3a://{args.bucket}/{args.prefix}/{args.dataset}/dt={args.date}"
    glob_path = f"{base}/*.csv"

    matches = _list_paths_with_glob(spark, glob_path)
    if not matches:
        print(f"No input files found at {glob_path}. Skipping.")
        spark.stop()
        return 0

    print(f"Reading from: {glob_path}")
    # Read CSV(s)
    try:
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .option("escape", '"')
            .load(glob_path)
        )
    except Exception as e:
        print(f"Failed to read CSV from {glob_path}: {e}")
        spark.stop()
        # Non-zero to signal failure of the job
        sys.exit(1)

    if df.rdd.isEmpty():
        print("DataFrame is empty after read. Skipping write.")
        spark.stop()
        return 0

    # Cleanse
    df = cleanse_dataframe(df)

    # Prepare JDBC options
    target_table = f"{args.table_prefix}{args.dataset}"
    jdbc_opts = {
        "url": args.pg_url,
        "dbtable": target_table if not args.pg_schema else f"{args.pg_schema}.{target_table}",
        "user": args.pg_user,
        "password": args.pg_password,
        "driver": "org.postgresql.Driver",
    }

    # Write append (Spark will create the table if missing based on inferred schema)
    print(f"Writing to Postgres table: {jdbc_opts['dbtable']} at {args.pg_url}")
    (
        df.write.format("jdbc")
        .options(**jdbc_opts)
        .mode("append")
        .save()
    )

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
