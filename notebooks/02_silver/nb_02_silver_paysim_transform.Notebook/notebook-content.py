# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "03b3ab34-e351-4757-ac65-28db43180f57",
# META       "default_lakehouse_name": "lh_finance_core",
# META       "default_lakehouse_workspace_id": "aa5bab7a-005d-4922-95fa-0edc2e6626e2",
# META       "known_lakehouses": [
# META         {
# META           "id": "03b3ab34-e351-4757-ac65-28db43180f57"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### **import library**

# CELL ********************

# Import PySpark SQL functions (like col, sum, max, when, etc.)
# We use "F" as a short alias to make code cleaner (e.g., F.col("column_name"))
from pyspark.sql import functions as F

# Import Window functions
# Used for operations like row_number(), rank(), lead(), lag() over partitions
from pyspark.sql.window import Window

# Import datetime module
# Used to work with dates and timestamps (e.g., current time, formatting dates)
from datetime import datetime

# Import uuid module
# Used to generate unique IDs (for example, unique transaction IDs or batch IDs)
import uuid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Read pipeline parameters**

# CELL ********************

# ===============================
# CELL-02: Read pipeline parameters (Enterprise)
# ===============================

# Function to safely read notebook parameters (Databricks widgets)
# If the parameter does not exist, return None instead of failing
def _get_param(name):
    try:
        return dbutils.widgets.get(name)  # Get widget value
    except Exception:
        return None  # Return None if widget is missing

# Read all expected parameters from the notebook widgets
p_env             = _get_param("p_env")               # Environment (DEV/UAT/PROD)
p_load_type       = _get_param("p_load_type")         # Load type (FULL/INCR)
p_source_file     = _get_param("p_source_file")       # Input source file name
p_pipeline_run_id = _get_param("p_pipeline_run_id")   # Unique pipeline execution ID

# Detect if this notebook is being run manually (not from pipeline)
# Conditions for manual run:
# - pipeline_run_id is None
# - pipeline_run_id is empty
# - pipeline_run_id equals "MANUAL_RUN"
is_manual = (
    (p_pipeline_run_id is None) or 
    (str(p_pipeline_run_id).strip() == "") or 
    (str(p_pipeline_run_id).upper() == "MANUAL_RUN")
)

# If manual execution, assign default DEV values
if is_manual:
    print("⚠️ Manual notebook run detected — using dev defaults")
    
    p_env = "DEV"  # Default environment
    p_load_type = "FULL"  # Default load type
    p_source_file = "paysim_transactions_full_20260214.csv"  # Default file
    p_pipeline_run_id = "MANUAL_RUN"  # Default run ID

# If pipeline execution, validate required parameters
else:
    # Create dictionary of required parameters
    required = {
        "p_env": p_env,
        "p_load_type": p_load_type,
        "p_source_file": p_source_file,
        "p_pipeline_run_id": p_pipeline_run_id
    }

    # Check for missing or empty values
    missing = [k for k, v in required.items() if v is None or str(v).strip() == ""]

    # If any required parameter is missing, raise error
    if missing:
        raise Exception(f"Missing required parameters: {missing}")

# Normalize values AFTER validation/default assignment
# Convert to uppercase for consistency
p_env = str(p_env).upper()
p_load_type = str(p_load_type).upper()

# Validate allowed environment values
if p_env not in ["DEV", "UAT", "PROD"]:
    raise Exception(f"Invalid p_env: {p_env}")

# Validate allowed load type values
if p_load_type not in ["FULL", "INCR"]:
    raise Exception(f"Invalid p_load_type: {p_load_type}")

# Print final parameter values for verification/logging
print("✅ Parameters Loaded:")
print("p_env =", p_env)
print("p_load_type =", p_load_type)
print("p_source_file =", p_source_file)
print("p_pipeline_run_id =", p_pipeline_run_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Read Watermark (Silver Incremental Control)**

# CELL ********************

# ===============================
# CELL-03: Read Watermark (Silver Incremental Control)
# ===============================

# Define the pipeline and entity (table) names
# These are used to identify the correct watermark record
pipeline_name = "pl_finance_e2e_batch"
entity_name   = "paysim_transactions"

# Read the last successful load timestamp from the watermark table
# This timestamp tells us up to which point data was already processed
watermark_row = spark.sql(f"""
    SELECT last_success_ts
    FROM meta.etl_watermark
    WHERE pipeline_name = '{pipeline_name}'
      AND entity_name   = '{entity_name}'
""").limit(1).collect()   # Get only 1 row and convert result into a Python list

# If no record is found, stop execution
# This prevents accidental full reloads or incorrect incremental logic
if len(watermark_row) == 0:
    raise Exception(
        f"❌ No watermark record found for pipeline_name='{pipeline_name}' "
        f"and entity_name='{entity_name}'"
    )

# Extract the last_success_ts value from the returned row
last_success_ts = watermark_row[0]["last_success_ts"]

# Print the watermark value for logging and debugging
print("✅ Watermark loaded. last_success_ts =", last_success_ts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Standardize Column Names**
# 
# In Silver, we rename raw source columns into consistent, business-friendly names.
# This makes downstream modeling (Gold star schema) and reporting much easier and less error-prone.
# 
# We keep Bronze untouched. Silver becomes the canonical clean dataset.


# CELL ********************

# Silver Layer Transformation – Clean and Restructure Data

from pyspark.sql import functions as F

# Transform the Bronze DataFrame into a Silver DataFrame with selected columns and proper data types
df_s1 = (
    df_bronze.select(
        # Step and transaction type columns
        F.col("step").alias("step"),
        F.col("type").alias("transaction_type"),
        
        # Amount column, cast to double for precise calculations
        F.col("amount").cast("double").alias("amount"),

        # Origin customer details and balance before/after the transaction
        F.col("nameOrig").alias("origin_customer_id"),
        F.col("oldbalanceOrg").cast("double").alias("origin_balance_before"),
        F.col("newbalanceOrig").cast("double").alias("origin_balance_after"),

        # Destination customer details and balance before/after the transaction
        F.col("nameDest").alias("destination_customer_id"),
        F.col("oldbalanceDest").cast("double").alias("destination_balance_before"),
        F.col("newbalanceDest").cast("double").alias("destination_balance_after"),

        # Fraud flags, cast to integers to maintain consistency
        F.col("isFraud").cast("int").alias("is_fraud"),
        F.col("isFlaggedFraud").cast("int").alias("is_flagged_fraud"),

        # Ingestion metadata to ensure traceability through the pipeline
        F.col("_ingest_ts"),
        F.col("batch_id"),
        F.col("_source_file"),
        F.col("_ingest_mode")
    )
)

# Print the schema of the transformed Silver DataFrame to inspect the column types
df_s1.printSchema()

# Show the first 5 rows of the Silver DataFrame without truncating any values for detailed inspection
df_s1.show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Create Derived Business Columns**
# 
# This step enriches the Silver dataset with derived fields
# required for analytics and dimensional modeling.
# 
# Derived Columns:
# 
# - event_ts → Converted from `step` into a real timestamp
# - txn_id → Synthetic unique transaction identifier
# - transaction_direction → Debit/Credit style classification
# 
# These fields prepare the dataset for Gold layer modeling.


# CELL ********************

# Step 7: Further Transformation – Adding Event Timestamps and Transaction Direction

from pyspark.sql import functions as F

# Define a base timestamp representing the starting point (2020-01-01 00:00:00)
base_timestamp = F.to_timestamp(F.lit("2020-01-01 00:00:00"))

# Create the Silver DataFrame with additional transformations
df_s2 = (
    df_s1
    # Add event timestamp by adding 'step' (assumed to be in hours) to the base timestamp
    .withColumn("event_ts", base_timestamp + F.col("step") * F.expr("INTERVAL 1 HOURS"))
    
    # Generate a unique transaction ID for each row
    .withColumn("txn_id", F.monotonically_increasing_id())
    
    # Define transaction direction based on the transaction type
    .withColumn(
        "transaction_direction",
        F.when(F.col("transaction_type").isin("CASH_OUT", "TRANSFER"), F.lit("DEBIT"))
         .otherwise(F.lit("CREDIT"))
    )
)

# Print the schema of the transformed DataFrame
df_s2.printSchema()

# Show the first 5 rows of the transformed DataFrame without truncating any values
df_s2.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **DQ checks + split**
# 
# #### *Data Quality Validation & Reject Handling*
# 
# This step enforces basic data quality rules in the Silver layer.
# 
# We split records into:
# - Valid records → written to `silver.paysim_transactions_clean`
# - Invalid records → written to `dq.paysim_rejects` with rejection reasons
# 
# This ensures reporting and Gold modeling never consume bad data,
# while still preserving rejected records for investigation.


# CELL ********************

# Step 8: Data Quality Checks – Validate and Flag Invalid Transactions

from pyspark.sql import functions as F

# Define allowed transaction types for validation
allowed_types = ["CASH_IN", "CASH_OUT", "TRANSFER", "PAYMENT", "DEBIT"]

# Apply data quality (DQ) checks and create new columns for the reasons and status
df_dq = (
    df_s2
    # Add a new column "dq_reason" that concatenates multiple conditions
    .withColumn(
        "dq_reason",
        F.concat_ws(
            " | ",
            # Check for invalid amounts (null or non-positive)
            F.when((F.col("amount").isNull()) | (F.col("amount") <= 0), F.lit("INVALID_AMOUNT")),
            
            # Check for invalid transaction types (not in allowed types)
            F.when(~F.col("transaction_type").isin(allowed_types), F.lit("INVALID_TXN_TYPE")),
            
            # Check for missing customer IDs (either origin or destination missing)
            F.when(F.col("origin_customer_id").isNull() | F.col("destination_customer_id").isNull(), F.lit("MISSING_CUSTOMER_ID")),
            
            # Check for negative balances (for origin or destination before/after transaction)
            F.when(
                (F.col("origin_balance_before") < 0) | (F.col("origin_balance_after") < 0) |
                (F.col("destination_balance_before") < 0) | (F.col("destination_balance_after") < 0),
                F.lit("NEGATIVE_BALANCE")
            )
        )
    )
    # Create a "dq_status" column to flag valid or rejected rows based on "dq_reason"
    .withColumn(
        "dq_status",
        F.when((F.col("dq_reason").isNull()) | (F.col("dq_reason") == ""), F.lit("VALID"))
         .otherwise(F.lit("REJECT"))
    )
)

# Filter out the valid and rejected rows into separate DataFrames
df_valid = df_dq.filter(F.col("dq_status") == "VALID").drop("dq_reason")
df_reject = df_dq.filter(F.col("dq_status") == "REJECT")

# Print the count of valid and rejected rows for review
print("Valid rows:", df_valid.count())
print("Reject rows:", df_reject.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Outputs to Delta tables**

# CELL ********************

# Write Valid and Rejected Transactions to Separate Tables

# Write the valid transactions to the Silver layer as a clean Delta table
(df_valid.write.format("delta")
    .mode("overwrite")  # Overwrite existing data in the Silver table
    .saveAsTable("silver.paysim_transactions_clear"))  # Save to Silver layer

# Write the rejected transactions (due to data quality issues) to a separate reject table
(df_reject.write.format("delta")
    .mode("overwrite")  # Overwrite existing data in the reject table
    .saveAsTable("dq.paysim_rejects"))  # Save to the Rejects table for further investigation

# Print confirmation that the tables have been created successfully
print("Silver table created: silver.paysim_transactions_clear")
print("Reject table created: dq.paysim_rejects")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Quick Inspect Reject Reasons**

# CELL ********************

# Step 10: Analyze Data Quality Rejects – Count Reject Reasons

from pyspark.sql import functions as F

# Query the reject table to group by the rejection reason, count occurrences, and order by the most frequent reasons
spark.table("dq.paysim_rejects") \
    .groupBy("dq_reason") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert the Spark DataFrame to a Pandas DataFrame for better formatting
df_rejects = spark.table("dq.paysim_rejects")

# Display the first 10 rows in a nice tabular format
df_rejects.limit(10).toPandas()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
