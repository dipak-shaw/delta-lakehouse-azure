# MAGIC %md
# MAGIC # Silver Layer Transformation
# MAGIC 
# MAGIC This notebook applies data transformations to move data from the Bronze layer to the Silver layer:
# MAGIC 
# MAGIC - Data cleansing
# MAGIC - Schema enforcement
# MAGIC - Data validation and quality checks
# MAGIC - Deduplication
# MAGIC - Business key identification
# MAGIC 
# MAGIC ## Spark Optimizations Used:
# MAGIC - Adaptive Query Execution
# MAGIC - Dynamic Partition Pruning
# MAGIC - Predicate Pushdown
# MAGIC - Cache Management

# COMMAND ----------

# Import required libraries
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from delta.tables import *
import datetime

# COMMAND ----------

# Define parameters
dbutils.widgets.text("source_table", "", "1. Source Table (Bronze)")
dbutils.widgets.text("target_table", "", "2. Target Table (Silver)")
dbutils.widgets.text("primary_keys", "", "3. Primary Keys (comma-separated)")
dbutils.widgets.dropdown("deduplication", "true", ["true", "false"], "4. Apply Deduplication")
dbutils.widgets.dropdown("apply_quality_rules", "true", ["true", "false"], "5. Apply Quality Rules")

# COMMAND ----------

# Get widget values
source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
primary_keys = [col.strip() for col in dbutils.widgets.get("primary_keys").split(",") if col.strip()]
deduplication = dbutils.widgets.get("deduplication").lower() == "true"
apply_quality_rules = dbutils.widgets.get("apply_quality_rules").lower() == "true"

# Validate inputs
assert source_table, "Source table must be provided"
assert target_table, "Target table must be provided"
if deduplication:
    assert primary_keys, "Primary keys must be provided for deduplication"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Optimization Configuration

# COMMAND ----------

# Enable Adaptive Query Execution
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Enable Dynamic Partition Pruning
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# Memory configuration for improved performance
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")

# Configure broadcast join threshold 
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Function Definitions

# COMMAND ----------

def define_data_quality_rules():
    """Define data quality rules to apply to the dataset"""
    return {
        # Format: column_name: [(rule_name, rule_function, error_message)]
        "email": [
            ("valid_format", lambda df, col: df.filter(~F.col(col).rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")), "Invalid email format")
        ],
        "phone": [
            ("valid_format", lambda df, col: df.filter(~F.col(col).rlike(r"^\+?[0-9]{10,15}$")), "Invalid phone number format")
        ],
        "date_columns": [
            ("future_date", lambda df, col: df.filter(F.col(col) > F.current_date()), "Date is in the future")
        ]
    }

def apply_quality_rules_to_column(df, column, rules, quality_tracking_list):
    """Apply quality rules to a specific column"""
    for rule_name, rule_func, error_msg in rules:
        invalid_records = rule_func(df, column)
        invalid_count = invalid_records.count()
        
        if invalid_count > 0:
            quality_tracking_list.append({
                "column": column,
                "rule": rule_name,
                "invalid_count": invalid_count,
                "message": error_msg
            })
            
            # Flag records with issues
            df = df.withColumn(
                "_data_quality_issues",
                F.when(
                    rule_func(df, column).select(F.col("*")).limit(1).rdd.isEmpty(),
                    F.col("_data_quality_issues")
                ).otherwise(
                    F.concat_ws(", ", F.col("_data_quality_issues"), F.lit(f"{column}:{rule_name}"))
                )
            )
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source Data

# COMMAND ----------

# Read bronze table with optimization for Delta tables
try:
    bronze_df = spark.read.format("delta").table(source_table)
    print(f"Successfully read {bronze_df.count()} records from {source_table}")
    
    # Cache the DataFrame if it's not too large
    if bronze_df.count() < 1000000:
        bronze_df.cache()
        print("DataFrame cached for optimized processing")
    
    # Show schema
    bronze_df.printSchema()
except Exception as e:
    raise Exception(f"Error reading source table: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations

# COMMAND ----------

# Start with the bronze data
silver_df = bronze_df

# Add data quality tracking column
silver_df = silver_df.withColumn("_data_quality_issues", F.lit(""))

# Apply schema enforcement and data cleansing transformations

# 1. Handle common data cleansing tasks
try:
    # Trim string fields
    for field in silver_df.schema.fields:
        if field.dataType == StringType():
            silver_df = silver_df.withColumn(field.name, F.trim(F.col(field.name)))
    
    # Convert date strings to proper date types if needed
    # date_columns = ["order_date", "shipping_date"]
    # for date_col in date_columns:
    #     if date_col in silver_df.columns:
    #         silver_df = silver_df.withColumn(date_col, F.to_date(F.col(date_col)))
            
    # Handle nulls for required fields
    # required_columns = ["customer_id", "order_id"]
    # for req_col in required_columns:
    #     if req_col in silver_df.columns:
    #         null_count = silver_df.filter(F.col(req_col).isNull()).count()
    #         if null_count > 0:
    #             print(f"Warning: {null_count} records have null values in required column {req_col}")
    
    print("Basic data cleansing completed")
except Exception as e:
    print(f"Warning: Error during data cleansing: {str(e)}")

# COMMAND ----------

# Apply data quality rules if enabled
quality_issues = []

if apply_quality_rules:
    try:
        # Get data quality rules
        quality_rules = define_data_quality_rules()
        
        # Apply rules to each configured column
        for column, rules in quality_rules.items():
            if column == "date_columns":
                # Special handling for date column rules
                date_columns = [field.name for field in silver_df.schema.fields 
                               if isinstance(field.dataType, DateType) or field.name.lower().endswith('_date')]
                
                for date_col in date_columns:
                    if date_col in silver_df.columns:
                        silver_df = apply_quality_rules_to_column(silver_df, date_col, rules, quality_issues)
            elif column in silver_df.columns:
                silver_df = apply_quality_rules_to_column(silver_df, column, rules, quality_issues)
        
        # Summarize quality issues
        if quality_issues:
            print("Data quality issues found:")
            for issue in quality_issues:
                print(f"  - Column '{issue['column']}': {issue['message']} ({issue['invalid_count']} records)")
        else:
            print("No data quality issues found")
            
    except Exception as e:
        print(f"Warning: Error applying quality rules: {str(e)}")

# COMMAND ----------

# Apply deduplication if enabled
if deduplication and primary_keys:
    try:
        print(f"Applying deduplication based on keys: {', '.join(primary_keys)}")
        
        # Use window function to identify duplicates
        window_spec = Window.partitionBy(*primary_keys).orderBy(F.col("_ingest_timestamp").desc())
        
        # Assign row numbers and keep the latest record for each key
        silver_df = silver_df.withColumn("row_num", F.row_number().over(window_spec)) \
                         .filter(F.col("row_num") == 1) \
                         .drop("row_num")
        
        print("Deduplication completed")
    except Exception as e:
        print(f"Warning: Error during deduplication: {str(e)}")

# COMMAND ----------

# Add Silver layer metadata
silver_df = silver_df.withColumn("_silver_processed_timestamp", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Layer

# COMMAND ----------

# Optimize write configuration
write_options = {
    "mergeSchema": "true",
    "optimizeWrite": "true",
    "autoOptimize": "true"
}

# Write to Silver table
try:
    # Create or replace table (consider using merge/upsert for incremental updates)
    silver_df.write.format("delta") \
        .options(**write_options) \
        .mode("overwrite") \
        .saveAsTable(target_table)
        
    print(f"Successfully wrote {silver_df.count()} records to {target_table}")
except Exception as e:
    raise Exception(f"Error writing to target table: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize the Table

# COMMAND ----------

# Optimize the Silver table for better read performance
try:
    # Identify columns for Z-ordering based on common query patterns
    # Use primary keys or other commonly filtered columns
    z_order_cols = primary_keys[:2] if primary_keys and len(primary_keys) > 0 else []
    
    if z_order_cols:
        z_order_sql = ", ".join(z_order_cols)
        spark.sql(f"OPTIMIZE {target_table} ZORDER BY ({z_order_sql})")
        print(f"Table optimized with Z-ORDER on columns: {z_order_sql}")
    else:
        spark.sql(f"OPTIMIZE {target_table}")
        print("Table optimized")
except Exception as e:
    print(f"Warning: Could not optimize table: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Statistics and Table Description

# COMMAND ----------

# Collect and display table statistics
try:
    # Show table details
    display(spark.sql(f"DESCRIBE DETAIL {target_table}"))
    
    # Show data profile summary
    summary_df = silver_df.summary()
    display(summary_df)
    
    # If there were data quality issues, summarize them
    if quality_issues:
        quality_df = spark.createDataFrame(quality_issues)
        display(quality_df)
except Exception as e:
    print(f"Warning: Could not generate statistics: {str(e)}")

# COMMAND ----------

print(f"Silver layer transformation to {target_table} completed successfully!")
