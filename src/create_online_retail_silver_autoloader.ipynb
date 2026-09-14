from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="online_retail_autoloader_silver",
    comment="Silver layer: Cleaned online retail data with calculated TotalPrice, excluding cancelled invoices"
)
def silver_online_retail():
    """
    Silver layer transformation:
    - Filters out cancelled invoices (InvoiceNo starting with 'C')
    - Adds TotalPrice column (Quantity * UnitPrice)
    """
    return (
        spark.readStream.table("online_retail_autoloader_bronze")
        .filter(~F.col("InvoiceNo").startswith("C"))
        .withColumn("TotalPrice", F.col("Quantity") * F.col("UnitPrice"))
    )