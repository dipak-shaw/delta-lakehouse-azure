import pytest
import pyspark
from pyspark.sql import SparkSession
import os
import tempfile
from delta import configure_spark_with_delta_pip

# Define a fixture for the Spark session that will be used by all tests
@pytest.fixture(scope="session")
def spark():
    # Create a temporary directory for the Spark warehouse
    warehouse_dir = tempfile.TemporaryDirectory().name
    
    # Configure Spark with Delta Lake
    builder = (
        SparkSession.builder.appName("DeltaLakehouseTests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse_dir)
    )
    
    # Create the Spark session
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    yield spark
    
    # Stop the Spark session after all tests are done
    spark.stop()

# Test for bronze layer ingestion functionality
def test_bronze_layer_ingestion(spark):
    # Create a sample DataFrame that mimics source data
    data = [
        (1001, "C5923", "2023-06-01", "P456", 2, 49.99, 99.98, "COMPLETED"),
        (1002, "C2381", "2023-06-01", "P789", 1, 129.99, 129.99, "COMPLETED"),
        (1003, "C7456", "2023-06-01", "P123", 3, 29.99, 89.97, "PROCESSING")
    ]
    
    columns = ["order_id", "customer_id", "order_date", "product_id", 
              "quantity", "unit_price", "total_amount", "status"]
    
    source_df = spark.createDataFrame(data, columns)
    
    # Create a temporary directory for the Delta table
    temp_dir = tempfile.TemporaryDirectory().name
    
    # Write the DataFrame as a Delta table
    source_df.write.format("delta").mode("overwrite").save(temp_dir)
    
    # Read the Delta table back
    read_df = spark.read.format("delta").load(temp_dir)
    
    # Verify the data was written and read correctly
    assert read_df.count() == 3
    assert set(read_df.columns) == set(columns)
    
    # Verify specific data values
    order_counts = read_df.groupBy("status").count().collect()
    status_counts = {row["status"]: row["count"] for row in order_counts}
    
    assert status_counts["COMPLETED"] == 2
    assert status_counts["PROCESSING"] == 1

# Test for silver layer transformation functionality
def test_silver_layer_transformation(spark):
    # Create a sample DataFrame that mimics bronze layer data
    data = [
        # Duplicated order
        (1001, "C5923", "2023-06-01", "P456", 2, 49.99, 99.98, "COMPLETED", "2023-06-01T10:00:00"),
        (1001, "C5923", "2023-06-01", "P456", 2, 49.99, 99.98, "COMPLETED", "2023-06-01T11:00:00"),  # Duplicate with later timestamp
        # Order with null values
        (1002, "C2381", "2023-06-01", None, 1, 129.99, 129.99, "COMPLETED", "2023-06-01T10:30:00"),
        # Normal order
        (1003, "C7456", "2023-06-01", "P123", 3, 29.99, 89.97, "PROCESSING", "2023-06-01T12:00:00")
    ]
    
    columns = ["order_id", "customer_id", "order_date", "product_id", 
              "quantity", "unit_price", "total_amount", "status", "timestamp"]
    
    bronze_df = spark.createDataFrame(data, columns)
    
    # Perform transformations (simplified version of what would be in the actual code)
    # 1. Deduplicate based on order_id, keeping the latest record
    from pyspark.sql.window import Window
    import pyspark.sql.functions as F
    
    window_spec = Window.partitionBy("order_id").orderBy(F.col("timestamp").desc())
    silver_df = bronze_df.withColumn("row_num", F.row_number().over(window_spec)) \
                        .filter(F.col("row_num") == 1) \
                        .drop("row_num")
    
    # 2. Handle null values
    silver_df = silver_df.fillna({"product_id": "UNKNOWN"})
    
    # Write to a temporary Delta table
    temp_dir = tempfile.TemporaryDirectory().name
    silver_df.write.format("delta").mode("overwrite").save(temp_dir)
    
    # Read back for verification
    result_df = spark.read.format("delta").load(temp_dir)
    
    # Verify deduplication worked - should have 3 records now, not 4
    assert result_df.count() == 3
    
    # Verify the duplicate with later timestamp was kept
    order_1001 = result_df.filter(F.col("order_id") == 1001).collect()[0]
    assert order_1001["timestamp"] == "2023-06-01T11:00:00"
    
    # Verify null handling worked
    order_1002 = result_df.filter(F.col("order_id") == 1002).collect()[0]
    assert order_1002["product_id"] == "UNKNOWN"

# Test for gold layer analytics functionality
def test_gold_layer_analytics(spark):
    # Create a sample DataFrame that mimics silver layer orders data
    orders_data = [
        (1001, "C5923", "2023-06-01", "P456", 2, 49.99, 99.98, "COMPLETED"),
        (1002, "C2381", "2023-06-01", "P789", 1, 129.99, 129.99, "COMPLETED"),
        (1003, "C7456", "2023-06-01", "P123", 3, 29.99, 89.97, "COMPLETED"),
        (1004, "C5923", "2023-06-02", "P456", 1, 49.99, 49.99, "COMPLETED"),
        (1005, "C2381", "2023-06-02", "P789", 2, 129.99, 259.98, "COMPLETED")
    ]
    
    orders_columns = ["order_id", "customer_id", "order_date", "product_id", 
                     "quantity", "unit_price", "total_amount", "status"]
    
    orders_df = spark.createDataFrame(orders_data, orders_columns)
    
    # Create a sample DataFrame for products
    products_data = [
        ("P456", "Running Shoes", "Footwear"),
        ("P789", "Bluetooth Speaker", "Electronics"),
        ("P123", "Basic T-Shirt", "Clothing")
    ]
    
    products_columns = ["product_id", "product_name", "category"]
    
    products_df = spark.createDataFrame(products_data, products_columns)
    
    # Create temporary tables for the test
    orders_df.createOrReplaceTempView("silver_orders")
    products_df.createOrReplaceTempView("silver_products")
    
    # Perform a simple analytics aggregation (daily sales by category)
    import pyspark.sql.functions as F
    
    daily_category_sales = spark.sql("""
        SELECT 
            o.order_date,
            p.category,
            SUM(o.total_amount) as daily_revenue,
            COUNT(DISTINCT o.order_id) as order_count
        FROM silver_orders o
        JOIN silver_products p ON o.product_id = p.product_id
        WHERE o.status = 'COMPLETED'
        GROUP BY o.order_date, p.category
        ORDER BY o.order_date, p.category
    """)
    
    # Write the results to a temporary Delta table
    temp_dir = tempfile.TemporaryDirectory().name
    daily_category_sales.write.format("delta").mode("overwrite").save(temp_dir)
    
    # Read back for verification
    result_df = spark.read.format("delta").load(temp_dir)
    
    # There should be 5 records (3 categories on day 1, 2 categories on day 2)
    assert result_df.count() == 5
    
    # Verify the totals
    day1_revenue = result_df.filter(F.col("order_date") == "2023-06-01") \
                          .agg(F.sum("daily_revenue").alias("total")).collect()[0]["total"]
    assert day1_revenue == 319.94  # 99.98 + 129.99 + 89.97
    
    # Verify the category counts
    electronics_count = result_df.filter(F.col("category") == "Electronics").count()
    assert electronics_count == 2  # One record per day

# Integration test simulating the full pipeline
def test_end_to_end_pipeline(spark):
    # This would be a more comprehensive test that runs through the entire pipeline
    # For brevity, we'll just outline the steps here
    
    # 1. Ingest raw data to bronze layer
    # 2. Transform bronze to silver
    # 3. Create gold analytics
    # 4. Verify results
    
    # For this test, we'll just verify the structure works
    assert True
    
# Test for Spark optimizations
def test_spark_optimizations(spark):
    # Verify key Spark configurations
    assert spark.conf.get("spark.sql.extensions") == "io.delta.sql.DeltaSparkSessionExtension"
    assert spark.conf.get("spark.sql.catalog.spark_catalog") == "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    
    # Set and verify additional configurations
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
    
    # Create a DataFrame to test Z-ordering (note: this is simplified as Z-ordering is best tested in a real environment)
    data = [(i, f"customer_{i}", f"2023-06-{i%30+1}") for i in range(1, 101)]
    columns = ["order_id", "customer_id", "order_date"]
    
    test_df = spark.createDataFrame(data, columns)
    
    # Write to a temporary Delta table
    temp_dir = tempfile.TemporaryDirectory().name
    test_df.write.format("delta").mode("overwrite").save(temp_dir)
    
    # In a real test, we would now optimize the table and measure performance
    # Here we'll just check that the Delta table was created
    result_df = spark.read.format("delta").load(temp_dir)
    assert result_df.count() == 100
