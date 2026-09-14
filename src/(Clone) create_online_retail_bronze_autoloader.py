from pyspark import pipelines as dp


@dp.table(
    name="online_retail_autoloader_bronze",
    comment="Bronze layer: Raw online retail data ingested from parquet files using Auto Loader"
)
def online_retail_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/edu/kgt/datafile/online_retail_parquet/")
    )