import sys
from datetime import datetime, timedelta

import pytz
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType, DateType

## Glue boilerplate
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_BRONZE = "s3://omini-financial-datalake/bronze/stocks/"
S3_SILVER = "s3://omini-financial-datalake/silver/stocks/"

ist = pytz.timezone("Asia/Kolkata")
today = datetime.now(ist).date()
cutoff = today - timedelta(days=200)

print(f"Reading bronze from {cutoff} onwards...")

# ── Read Bronze ───────────────────────────────────────────────────────────────
df = spark.read.parquet(S3_BRONZE) \
          .filter(F.col("Date") >= F.lit(str(cutoff)))

print(f"Rows loaded: {df.count()}")

# ── Clean ─────────────────────────────────────────────────────────────────────
cols_to_drop = ["Dividends", "Stock Splits"]
df = df.drop(*[c for c in cols_to_drop if c in df.columns])

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

# ── Technical Indicators ──────────────────────────────────────────────────────
w = Window.partitionBy("symbol").orderBy("date")

df = df.withColumn("sma_20",  F.avg("close").over(w.rowsBetween(-19,  0)))
df = df.withColumn("sma_50",  F.avg("close").over(w.rowsBetween(-49,  0)))
df = df.withColumn("sma_200", F.avg("close").over(w.rowsBetween(-199, 0)))

df = df.withColumn("ema_12", F.avg("close").over(w.rowsBetween(-11, 0)))
df = df.withColumn("ema_26", F.avg("close").over(w.rowsBetween(-25, 0)))

df = df.withColumn("macd",          F.col("ema_12") - F.col("ema_26"))
df = df.withColumn("macd_signal",   F.avg("macd").over(w.rowsBetween(-8, 0)))
df = df.withColumn("macd_histogram",F.col("macd") - F.col("macd_signal"))

df = df.withColumn("price_change",  F.col("close") - F.lag("close", 1).over(w))
df = df.withColumn("gain",  F.when(F.col("price_change") > 0,  F.col("price_change")).otherwise(0.0))
df = df.withColumn("loss",  F.when(F.col("price_change") < 0, -F.col("price_change")).otherwise(0.0))
df = df.withColumn("avg_gain", F.avg("gain").over(w.rowsBetween(-13, 0)))
df = df.withColumn("avg_loss", F.avg("loss").over(w.rowsBetween(-13, 0)))
df = df.withColumn("rsi_14",
    F.when(F.col("avg_loss") == 0, 100.0)
     .otherwise(100.0 - (100.0 / (1.0 + (F.col("avg_gain") / F.col("avg_loss"))))))

df = df.withColumn("std_20",         F.stddev("close").over(w.rowsBetween(-19, 0)))
df = df.withColumn("bollinger_upper", F.col("sma_20") + (2 * F.col("std_20")))
df = df.withColumn("bollinger_lower", F.col("sma_20") - (2 * F.col("std_20")))

df = df.withColumn("daily_return",
    (F.col("close") - F.lag("close", 1).over(w)) / F.lag("close", 1).over(w))

df = df.withColumn("volume_sma_20", F.avg("volume").over(w.rowsBetween(-19, 0)))
df = df.withColumn("volume_ratio",  F.col("volume") / F.col("volume_sma_20"))

df = df.drop("price_change", "gain", "loss", "avg_gain", "avg_loss", "std_20")

print("Indicators complete.")

# ── Write Today Only ──────────────────────────────────────────────────────────
df = df.withColumn("year",  F.year("date")) \
       .withColumn("month", F.month("date")) \
       .withColumn("day",   F.dayofmonth("date"))

df = df.filter(F.col("date") == F.lit(str(today)))

df = df.coalesce(1)

df.write \
  .partitionBy("year", "month", "day") \
  .mode("overwrite") \
  .parquet(S3_SILVER)

print(f"Silver written for {today}")

job.commit()