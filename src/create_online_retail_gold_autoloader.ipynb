from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="online_retail_autoloader_gold",
    comment="Gold layer: Product-level aggregations from December 2010 onwards"
)
def gold_product_summary():
    """
    Gold layer materialized view:
    - Aggregates sales data by product (Description)
    - Filters data from December 1, 2010 onwards
    - Calculates total quantity, average unit price, and total revenue per product
    """
    return (
        spark.read.table("online_retail_autoloader_silver")
        .filter(F.col("InvoiceDate") >= "2010-12-01")
        .groupBy("Description")
        .agg(
            F.sum("Quantity").alias("sum_quantity"),
            F.round(F.avg("UnitPrice"), 2).alias("avg_unit_price"),
            F.round(F.sum("TotalPrice"), 2).alias("total_price")
        )
    )