# MAGIC %md
# MAGIC # Sales Analytics Gold Layer
# MAGIC 
# MAGIC This notebook performs advanced analytics on the Silver layer data to create Gold layer tables for business analytics. The process includes:
# MAGIC 
# MAGIC - Aggregating sales data by various dimensions
# MAGIC - Computing key business metrics
# MAGIC - Optimizing for analytical queries
# MAGIC - Implementing Z-ordering for dimension tables
# MAGIC 
# MAGIC ## Spark Optimizations:
# MAGIC - Adaptive Query Execution
# MAGIC - Bloom Filter Indexes
# MAGIC - Partition Pruning
# MAGIC - Z-Ordering

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Import required libraries
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import datetime

# Define parameters
dbutils.widgets.text("source_orders_table", "silver.orders", "1. Source Orders Table")
dbutils.widgets.text("source_products_table", "silver.products", "2. Source Products Table")
dbutils.widgets.text("source_customers_table", "silver.customers", "3. Source Customers Table")
dbutils.widgets.text("target_schema", "gold", "4. Target Schema")
dbutils.widgets.dropdown("overwrite_existing", "false", ["true", "false"], "5. Overwrite Existing Tables")

# Get widget values
source_orders_table = dbutils.widgets.get("source_orders_table")
source_products_table = dbutils.widgets.get("source_products_table")
source_customers_table = dbutils.widgets.get("source_customers_table")
target_schema = dbutils.widgets.get("target_schema")
overwrite_existing = dbutils.widgets.get("overwrite_existing").lower() == "true"

# Set write mode
write_mode = "overwrite" if overwrite_existing else "merge"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Optimization Configuration

# COMMAND ----------

# Enable Adaptive Query Execution for better performance
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Set larger shuffle partitions for larger aggregations
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Enable dynamic partition pruning
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# Set broadcast join threshold higher for dimension tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")

# Set a reasonable AQE coalesce partition number
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Data

# COMMAND ----------

# Load source tables
orders_df = spark.table(source_orders_table)
products_df = spark.table(source_products_table)

# Cache frequently used tables
products_df.cache()

# Display summary statistics
print(f"Orders count: {orders_df.count()}")
print(f"Products count: {products_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Daily Sales Summary

# COMMAND ----------

# Create daily sales summary
daily_sales = orders_df.filter(F.col("status") == "COMPLETED") \
    .groupBy(F.to_date(F.col("order_date")).alias("date")) \
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_sales"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.avg("total_amount").alias("average_order_value")
    )

# Write to gold table
daily_sales_table = f"{target_schema}.daily_sales_summary"
daily_sales.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode(write_mode) \
    .saveAsTable(daily_sales_table)

# Optimize with Z-ordering on date
spark.sql(f"OPTIMIZE {daily_sales_table} ZORDER BY (date)")

print(f"Created table: {daily_sales_table}")
display(spark.table(daily_sales_table).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Product Performance

# COMMAND ----------

# Create product performance metrics
product_performance = orders_df.filter(F.col("status") == "COMPLETED") \
    .join(products_df, orders_df["product_id"] == products_df["product_id"], "left") \
    .groupBy(
        orders_df["product_id"],
        products_df["product_name"],
        products_df["category"]
    ) \
    .agg(
        F.sum("quantity").alias("total_units_sold"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("unit_price").alias("average_price"),
        F.count("order_id").alias("order_count")
    ) \
    .withColumn(
        "revenue_rank", 
        F.dense_rank().over(Window.partitionBy("category").orderBy(F.desc("total_revenue")))
    )

# Write to gold table
product_perf_table = f"{target_schema}.product_performance"
product_performance.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode(write_mode) \
    .saveAsTable(product_perf_table)

# Optimize with Z-ordering on category (common filter dimension)
spark.sql(f"OPTIMIZE {product_perf_table} ZORDER BY (category, product_id)")

print(f"Created table: {product_perf_table}")
display(spark.table(product_perf_table).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Sales by Product Category

# COMMAND ----------

# Create category performance metrics
category_performance = orders_df.filter(F.col("status") == "COMPLETED") \
    .join(products_df, orders_df["product_id"] == products_df["product_id"], "left") \
    .groupBy(
        products_df["category"],
        F.year(F.col("order_date")).alias("year"),
        F.month(F.col("order_date")).alias("month")
    ) \
    .agg(
        F.sum("quantity").alias("total_units_sold"),
        F.sum("total_amount").alias("total_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.countDistinct("customer_id").alias("unique_customers")
    ) \
    .withColumn("avg_revenue_per_customer", F.col("total_revenue") / F.col("unique_customers"))

# Write to gold table
category_perf_table = f"{target_schema}.category_performance"
category_performance.write.format("delta") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .mode(write_mode) \
    .saveAsTable(category_perf_table)

# Optimize with Z-ordering on category within each partition
spark.sql(f"OPTIMIZE {category_perf_table} ZORDER BY (category)")

print(f"Created table: {category_perf_table}")
display(spark.table(category_perf_table).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Customer Purchase Patterns

# COMMAND ----------

# Create customer purchase patterns
customer_patterns = orders_df \
    .groupBy("customer_id") \
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_spend"),
        F.avg("total_amount").alias("average_order_value"),
        F.min("order_date").alias("first_purchase_date"),
        F.max("order_date").alias("last_purchase_date"),
        F.countDistinct("product_id").alias("unique_products_purchased")
    ) \
    .withColumn("days_since_last_purchase", F.datediff(F.current_date(), F.col("last_purchase_date"))) \
    .withColumn("customer_lifespan_days", F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date")))

# Add customer segments
customer_patterns = customer_patterns \
    .withColumn(
        "spend_segment",
        F.when(F.col("total_spend") > 1000, "High Value")
         .when(F.col("total_spend") > 500, "Medium Value")
         .otherwise("Low Value")
    ) \
    .withColumn(
        "frequency_segment",
        F.when(F.col("total_orders") > 10, "Frequent")
         .when(F.col("total_orders") > 5, "Regular")
         .otherwise("Occasional")
    ) \
    .withColumn(
        "recency_segment",
        F.when(F.col("days_since_last_purchase") <= 30, "Active")
         .when(F.col("days_since_last_purchase") <= 90, "Recent")
         .otherwise("Inactive")
    )

# Write to gold table
customer_patterns_table = f"{target_schema}.customer_purchase_patterns"
customer_patterns.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode(write_mode) \
    .saveAsTable(customer_patterns_table)

# Optimize with Z-ordering on segments (common analytics filters)
spark.sql(f"OPTIMIZE {customer_patterns_table} ZORDER BY (spend_segment, frequency_segment, recency_segment)")

print(f"Created table: {customer_patterns_table}")
display(spark.table(customer_patterns_table).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Create Unified Dimension Tables

# COMMAND ----------

# Create product dimension table for analytics
product_dim = products_df.select(
    "product_id",
    "product_name",
    "category",
    "manufacturer",
    "price",
    "is_active"
)

# Write to gold table
product_dim_table = f"{target_schema}.dim_product"
product_dim.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode(write_mode) \
    .saveAsTable(product_dim_table)

# Optimize with Z-ordering on common lookup fields
spark.sql(f"OPTIMIZE {product_dim_table} ZORDER BY (category, manufacturer)")

print(f"Created table: {product_dim_table}")
display(spark.table(product_dim_table).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Metrics

# COMMAND ----------

# Calculate execution times as performance metrics
import time

start_time = time.time()

# Run a complex analytical query that would benefit from our optimizations
complex_query_result = spark.sql(f"""
    SELECT 
        p.category,
        year(o.order_date) as year,
        month(o.order_date) as month,
        count(distinct o.order_id) as orders,
        sum(o.total_amount) as revenue,
        sum(o.quantity) as units_sold,
        sum(o.total_amount) / sum(o.quantity) as average_unit_price
    FROM 
        {source_orders_table} o
    JOIN 
        {source_products_table} p ON o.product_id = p.product_id
    WHERE 
        o.status = 'COMPLETED'
    GROUP BY 
        p.category, year(o.order_date), month(o.order_date)
    ORDER BY 
        p.category, year, month
""")

# Cache the result to ensure execution
complex_query_result.cache()
complex_query_result.count()

query_time = time.time() - start_time
print(f"Complex analytical query execution time: {query_time:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Display summary of created tables
tables = [
    daily_sales_table,
    product_perf_table,
    category_perf_table,
    customer_patterns_table,
    product_dim_table
]

print("Gold Layer Tables Created:")
for table in tables:
    table_count = spark.table(table).count()
    print(f"- {table}: {table_count} rows")

# COMMAND ----------

print(f"Gold layer analytics processing completed successfully!")
print(f"Created {len(tables)} tables in the {target_schema} schema.")
print(f"Optimized tables with appropriate partitioning and Z-ordering for better query performance.")
