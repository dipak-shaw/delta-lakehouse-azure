# MAGIC %md
# MAGIC # Delta Lake Ingestion Pipeline
# MAGIC 
# MAGIC This notebook demonstrates an optimized data ingestion process from various sources into a Delta Lake bronze layer.
# MAGIC 
# MAGIC ## Features:
# MAGIC - Auto-detection of schema
# MAGIC - Handling of schema evolution
# MAGIC - Optimized write configurations
# MAGIC - Error handling and data validation
# MAGIC - Support for different file formats (CSV, JSON, Parquet, etc.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Import required libraries
import pyspark.sql.functions as F
from pyspark.sql.types import *
from delta.tables import *
import datetime
import json

# COMMAND ----------

# Define configuration parameters
dbutils.widgets.text("data_source", "", "1. Data Source")
dbutils.widgets.text("source_format", "csv", "2. Source Format")
dbutils.widgets.text("target_table", "", "3. Target Table")
dbutils.widgets.text("partition_cols", "", "4. Partition Columns (comma-separated)")
dbutils.widgets.dropdown("mode", "incremental", ["full", "incremental"], "5. Load Mode")
dbutils.widgets.text("watermark_column", "", "6. Watermark Column")

# COMMAND ----------

# Get widget values
data_source = dbutils.widgets.get("data_source")
source_format = dbutils.widgets.get("source_format")
target_table = dbutils.widgets.get("target_table")
partition_cols = [col.strip() for col in dbutils.widgets.get("partition_cols").split(",") if col.strip()]
load_mode = dbutils.widgets.get("mode")
watermark_column = dbutils.widgets.get("watermark_column")

# Validate inputs
assert data_source, "Data source path must be provided"
assert target_table, "Target table name must be provided"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_latest_watermark(table_name):
    """Get the latest watermark value from the target table if it exists"""
    try:
        if spark.catalog._jcatalog.tableExists(table_name) and watermark_column:
            watermark_value = spark.sql(f"SELECT MAX({watermark_column}) as max_watermark FROM {table_name}").collect()[0]["max_watermark"]
            return watermark_value
        else:
            return None
    except Exception as e:
        print(f"Error getting watermark: {str(e)}")
        return None

def optimize_spark_read_options(format_type):
    """Get optimized read options based on format type"""
    common_options = {
        "mergeSchema": "true"
    }
    
    format_specific = {
        "csv": {
            "header": "true",
            "inferSchema": "true",
            "sep": ",",
            "nullValue": ""
        },
        "json": {
            "multiLine": "true",
            "primitivesAsString": "true"
        },
        "parquet": {
            "vectorizedReading": "true"
        }
    }
    
    return {**common_options, **(format_specific.get(format_type, {}))}

def optimize_spark_write_options():
    """Get optimized Delta write options"""
    return {
        "mergeSchema": "true",
        "optimizeWrite": "true",
        "autoOptimize": "true",
        "dataChange": "true"
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source Data

# COMMAND ----------

# Get optimized read options
read_options = optimize_spark_read_options(source_format)

# Read source data
try:
    # If incremental load and watermark column is provided
    if load_mode == "incremental" and watermark_column:
        last_watermark = get_latest_watermark(target_table)
        
        if last_watermark:
            print(f"Incremental load from watermark: {last_watermark}")
            df = spark.read.format(source_format) \
                .options(**read_options) \
                .load(data_source) \
                .filter(F.col(watermark_column) > last_watermark)
        else:
            print("No prior watermark found. Loading all data.")
            df = spark.read.format(source_format) \
                .options(**read_options) \
                .load(data_source)
    else:
        df = spark.read.format(source_format) \
            .options(**read_options) \
            .load(data_source)
            
    # Add metadata columns
    df = df.withColumn("_ingest_timestamp", F.current_timestamp()) \
           .withColumn("_source_file", F.input_file_name())
           
    print(f"Successfully read {df.count()} records from source")
except Exception as e:
    raise Exception(f"Error reading source data: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Lake

# COMMAND ----------

# Get optimized write options
write_options = optimize_spark_write_options()

# Create or append to target table
try:
    write_mode = "overwrite" if load_mode == "full" else "append"
    
    if partition_cols:
        df.write.format("delta") \
          .options(**write_options) \
          .partitionBy(*partition_cols) \
          .mode(write_mode) \
          .saveAsTable(target_table)
    else:
        df.write.format("delta") \
          .options(**write_options) \
          .mode(write_mode) \
          .saveAsTable(target_table)
          
    print(f"Successfully wrote data to {target_table} in {write_mode} mode")
except Exception as e:
    raise Exception(f"Error writing to target table: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Table

# COMMAND ----------

# Optimize the table if it's not too small
try:
    row_count = spark.sql(f"SELECT COUNT(1) as count FROM {target_table}").collect()[0]["count"]
    
    # Only optimize if we have a significant amount of data
    if row_count > 100000:
        # Z-order the table if we have columns that would benefit
        if partition_cols:
            z_order_cols = ",".join(partition_cols[:2])  # Take first two partition columns
            spark.sql(f"OPTIMIZE {target_table} ZORDER BY ({z_order_cols})")
            print(f"Table optimized with Z-ORDER on columns: {z_order_cols}")
        else:
            spark.sql(f"OPTIMIZE {target_table}")
            print("Table optimized")
except Exception as e:
    print(f"Warning: Could not optimize table: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Basic validation
try:
    final_count = spark.sql(f"SELECT COUNT(1) as count FROM {target_table}").collect()[0]["count"]
    print(f"Validation: Target table has {final_count} rows")
    
    # Check if any data was actually loaded in incremental mode
    if load_mode == "incremental" and watermark_column and last_watermark:
        new_records = spark.sql(f"SELECT COUNT(1) as count FROM {target_table} WHERE {watermark_column} > '{last_watermark}'").collect()[0]["count"]
        print(f"Incremental load added {new_records} new records")
except Exception as e:
    print(f"Warning: Could not validate data: {str(e)}")

# COMMAND ----------

# Print a summary of the table
display(spark.sql(f"DESCRIBE DETAIL {target_table}"))

# COMMAND ----------

print(f"Ingestion to {target_table} completed successfully!")
