import argparse
from datetime import datetime, timedelta

import pytz
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType, DateType

# ── SparkSession ──────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("omini-silver") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

S3_BRONZE = "s3://omini-financial-datalake/bronze/stocks/"
S3_SILVER = "s3://omini-financial-datalake/silver/stocks/"


# ── Section 1: Read Bronze ────────────────────────────────────────────────────
def read_bronze(mode: str):
    """
    Backfill → read all 5 years of bronze
    Daily    → read last 200 days only (enough for SMA 200)
    """
    if mode == "backfill":
        print("Reading ALL bronze data...")
        df = spark.read.parquet(S3_BRONZE)

    else:
        ist = pytz.timezone("Asia/Kolkata")
        today = datetime.now(ist).date()
        cutoff = today - timedelta(days=200)

        print(f"Reading bronze from {cutoff} onwards...")
        df = spark.read.parquet(S3_BRONZE) \
                  .filter(F.col("Date") >= F.lit(str(cutoff)))

    print(f"Rows loaded: {df.count()}")
    return df


# ── Section 2: Clean ──────────────────────────────────────────────────────────
def clean(df):
    """
    - Rename columns to snake_case
    - Cast to correct data types
    - Drop nulls in critical columns
    - Remove duplicates
    """
    print("Cleaning data...")

    df = df.withColumnRenamed("Date",      "date") \
           .withColumnRenamed("Symbol",    "symbol") \
           .withColumnRenamed("Open",      "open") \
           .withColumnRenamed("High",      "high") \
           .withColumnRenamed("Low",       "low") \
           .withColumnRenamed("Close",     "close") \
           .withColumnRenamed("Adj Close", "adj_close") \
           .withColumnRenamed("Volume",    "volume")

    df = df.withColumn("date",      F.col("date").cast(DateType())) \
           .withColumn("open",      F.col("open").cast(DoubleType())) \
           .withColumn("high",      F.col("high").cast(DoubleType())) \
           .withColumn("low",       F.col("low").cast(DoubleType())) \
           .withColumn("close",     F.col("close").cast(DoubleType())) \
           .withColumn("adj_close", F.col("adj_close").cast(DoubleType())) \
           .withColumn("volume",    F.col("volume").cast(LongType()))

    df = df.dropna(subset=["date", "symbol", "close"])
    df = df.dropDuplicates(["date", "symbol"])

    print("Cleaning complete.")
    return df


# ── Section 3: Technical Indicators ──────────────────────────────────────────
def add_indicators(df):
    """
    All indicators calculated per stock using Window functions.
    Window = partitionBy symbol, ordered by date.
    """
    print("Calculating indicators...")

    w = Window.partitionBy("symbol").orderBy("date")

    # ── Moving Averages ───────────────────────────────────────────────────────
    df = df.withColumn("sma_20",  F.avg("close").over(w.rowsBetween(-19,  0)))
    df = df.withColumn("sma_50",  F.avg("close").over(w.rowsBetween(-49,  0)))
    df = df.withColumn("sma_200", F.avg("close").over(w.rowsBetween(-199, 0)))

    # ── EMA (approximated as SMA for simplicity) ──────────────────────────────
    df = df.withColumn("ema_12", F.avg("close").over(w.rowsBetween(-11, 0)))
    df = df.withColumn("ema_26", F.avg("close").over(w.rowsBetween(-25, 0)))

    # ── MACD ──────────────────────────────────────────────────────────────────
    df = df.withColumn("macd", F.col("ema_12") - F.col("ema_26"))
    df = df.withColumn("macd_signal",    F.avg("macd").over(w.rowsBetween(-8, 0)))
    df = df.withColumn("macd_histogram", F.col("macd") - F.col("macd_signal"))

    # ── RSI 14 (Simplified) ───────────────────────────────────────────────────
    df = df.withColumn("price_change",
        F.col("close") - F.lag("close", 1).over(w))

    df = df.withColumn("gain",
        F.when(F.col("price_change") > 0, F.col("price_change")).otherwise(0.0))

    df = df.withColumn("loss",
        F.when(F.col("price_change") < 0, -F.col("price_change")).otherwise(0.0))

    df = df.withColumn("avg_gain", F.avg("gain").over(w.rowsBetween(-13, 0)))
    df = df.withColumn("avg_loss", F.avg("loss").over(w.rowsBetween(-13, 0)))

    df = df.withColumn("rsi_14",
        F.when(F.col("avg_loss") == 0, 100.0)
         .otherwise(100.0 - (100.0 / (1.0 + (F.col("avg_gain") / F.col("avg_loss"))))))

    # ── Bollinger Bands ───────────────────────────────────────────────────────
    df = df.withColumn("std_20", F.stddev("close").over(w.rowsBetween(-19, 0)))
    df = df.withColumn("bollinger_upper", F.col("sma_20") + (2 * F.col("std_20")))
    df = df.withColumn("bollinger_lower", F.col("sma_20") - (2 * F.col("std_20")))

    # ── Daily Return ──────────────────────────────────────────────────────────
    df = df.withColumn("daily_return",
        (F.col("close") - F.lag("close", 1).over(w)) /
         F.lag("close", 1).over(w))

    # ── Volume Ratio ──────────────────────────────────────────────────────────
    df = df.withColumn("volume_sma_20", F.avg("volume").over(w.rowsBetween(-19, 0)))
    df = df.withColumn("volume_ratio",  F.col("volume") / F.col("volume_sma_20"))

    # Drop intermediate columns
    df = df.drop("price_change", "gain", "loss", "avg_gain", "avg_loss", "std_20")

    print("Indicators complete.")
    return df


# ── Section 4: Write Silver ───────────────────────────────────────────────────
def write_silver(df, mode: str):
    """
    Add partition columns and write to S3.
    Backfill → write all rows
    Daily    → write today's rows only
    """
    print(f"Writing silver ({mode} mode)...")

    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.now(ist).date()

    df = df.withColumn("year",  F.year("date")) \
           .withColumn("month", F.month("date")) \
           .withColumn("day",   F.dayofmonth("date"))

    if mode == "daily":
        df = df.filter(F.col("date") == F.lit(str(today)))

    df.write \
      .partitionBy("year", "month", "day") \
      .mode("overwrite") \
      .parquet(S3_SILVER)

    print(f"Silver written → {S3_SILVER}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Omini Silver Layer Job")
    parser.add_argument(
        "--mode",
        choices=["daily", "backfill"],
        default="daily",
        help="daily = today only | backfill = all history"
    )
    args = parser.parse_args()

    print(f"========================================")
    print(f"Silver Job — Mode: {args.mode}")
    print(f"========================================")

    df = read_bronze(args.mode)
    df = clean(df)
    df = add_indicators(df)
    write_silver(df, args.mode)

    print("========================================")
    print("Silver job complete!")
    print("========================================")

    spark.stop()


if __name__ == "__main__":
    main()