"""Independently unpivot a validated XM hourly bronze payload with Spark."""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bronze")
    parser.add_argument("output")
    args = parser.parse_args()
    spark = SparkSession.builder.master("local[1]").appName("energy-hourly").getOrCreate()
    try:
        raw = spark.read.option("multiLine", True).json(args.bronze)
        entities = (
            raw.select(F.explode("Items").alias("item"))
            .select(F.to_date("item.Date").alias("day"), F.explode("item.HourlyEntities").alias("entity"))
        )
        wide = entities.select(
            "day", F.col("entity.Values.code").alias("code"),
            *(F.col(f"entity.Values.Hour{hour:02}").cast("double").alias(f"Hour{hour:02}")
              for hour in range(1, 25)),
        )
        long = wide.unpivot(["day", "code"], None, "slot", "value")
        long.withColumn("hour", F.substring("slot", 5, 2).cast("int")).drop("slot").write.parquet(args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
