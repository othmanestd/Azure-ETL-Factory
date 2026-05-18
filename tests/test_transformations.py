"""Unit tests for Silver layer retail transformations."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_timestamp, concat


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test-etl").getOrCreate()


@pytest.fixture
def sample_sales(spark):
    data = [
        ("s1", "2024-01-01", "10:00:00", "ST001", "SKU001", 2, 129.99, 0.0, 259.98, "online", "credit_card", "C001"),
        ("s1", "2024-01-01", "10:00:00", "ST001", "SKU001", 2, 129.99, 0.0, 259.98, "online", "credit_card", "C001"),
        ("s2", "2024-01-01", "11:00:00", "ST002", "SKU002", -1, 89.99, 0.1, 89.99, "in_store", "cash", "C002"),
        ("s3", "2024-01-01", "12:00:00", "ST003", "SKU003", 1, 39.99, 0.2, 39.99, "mobile_app", "mobile_pay", "C003"),
    ]
    columns = ["sale_id", "sale_date", "sale_time", "store_id", "product_id",
               "quantity", "unit_price", "discount_pct", "total_amount",
               "channel", "payment_method", "customer_id"]
    return spark.createDataFrame(data, columns)


def test_dedup_keeps_unique_sales(spark, sample_sales):
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    window = Window.partitionBy("sale_id").orderBy(col("sale_date").desc())
    result = sample_sales.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
    assert result.count() == 3


def test_negative_quantity_filtered(spark, sample_sales):
    result = sample_sales.filter(col("quantity") > 0)
    assert result.count() == 3
    assert result.filter(col("sale_id") == "s2").count() == 0


def test_discount_flag_applied(spark, sample_sales):
    result = sample_sales.withColumn(
        "is_discounted", when(col("discount_pct") > 0, True).otherwise(False)
    )
    discounted = result.filter(col("is_discounted") == True).count()
    assert discounted == 2


def test_net_amount_calculation(spark, sample_sales):
    result = sample_sales.withColumn("net_amount", col("total_amount") * (1 - col("discount_pct")))
    row = result.filter(col("sale_id") == "s3").collect()[0]
    assert round(row["net_amount"], 2) == round(39.99 * 0.8, 2)
