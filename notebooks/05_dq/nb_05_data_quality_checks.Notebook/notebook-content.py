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

# #### **Data Quality Validation – Gold Layer**
# 
# #### **Purpose**
# This notebook validates the quality of Gold layer tables after the full pipeline execution.
# 
# #### Checks Performed**
# The notebook performs the following validations:
# 
# 1. Row count validation
# 2. Null key column validation
# 3. Duplicate transaction detection
# 4. Table availability verification
# 
# #### **Pipeline Position**
# This notebook runs after:
# 
# Bronze → Silver → Gold → Optimization
# 
# **Notebook**
# nb_05_data_quality_checks

# CELL ********************

# ============================================================
# STEP 1: Load Gold layer tables for validation
# ============================================================

# This step loads the required Gold layer tables into Spark DataFrames
# so that data quality validation checks can be performed.
# The Gold layer typically contains cleaned, curated, and business-ready data.

print("INFO: Starting data quality validation checks...")

# ------------------------------------------------------------
# Load Gold tables into Spark DataFrames
# ------------------------------------------------------------

# Fact table containing all transaction records
df_fact = spark.table("gold.fact_transactions")

# Dimension table containing account details
df_account = spark.table("gold.dim_account")

# Date dimension table used for time-based analysis
df_date = spark.table("gold.dim_date")

# ------------------------------------------------------------
# Confirm that tables were loaded successfully
# ------------------------------------------------------------

print("INFO: Tables loaded successfully")

# Count the number of records in each table
# This helps validate that data exists and can also be used
# as a quick sanity check before running further validation logic.

print("fact_transactions rows:", df_fact.count())
print("dim_account rows:", df_account.count())
print("dim_date rows:", df_date.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 2: Check for NULL values in critical key columns
# ============================================================

# Import Spark SQL functions module
# This provides useful functions like sum(), col(), isNull(), etc.
from pyspark.sql import functions as F

# ------------------------------------------------------------
# Data Quality Check: NULL values in key columns
# ------------------------------------------------------------
# In a star schema or dimensional model, fact tables usually
# contain foreign keys that reference dimension tables.
# These key columns should NOT contain NULL values because
# they break relationships between fact and dimension tables.

# The following code counts NULL values in important keys
# within the fact_transactions table.

null_checks = df_fact.select(
    
    # Count NULL values in date_key column
    # This key links fact_transactions to the dim_date table
    F.sum(F.col("date_key").isNull().cast("int")).alias("null_date_key"),
    
    # Count NULL values in origin_account_key
    # Represents the account that initiated the transaction
    F.sum(F.col("origin_account_key").isNull().cast("int")).alias("null_origin_account"),
    
    # Count NULL values in destination_account_key
    # Represents the account receiving the transaction
    F.sum(F.col("destination_account_key").isNull().cast("int")).alias("null_destination_account"),
    
    # Count NULL values in transaction_type_key
    # Links the transaction to its type (deposit, withdrawal, transfer, etc.)
    F.sum(F.col("transaction_type_key").isNull().cast("int")).alias("null_transaction_type")
)

# ------------------------------------------------------------
# Display the results
# ------------------------------------------------------------

print("INFO: Checking for NULL values in fact table keys")

# Show the count of NULL values for each key column
# Ideally, all values should be 0 in a properly maintained warehouse
null_checks.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 3: Detect duplicate transactions
# ============================================================

duplicate_txn = (
    df_fact
    .groupBy("txn_id")
    .count()
    .filter("count > 1")
)

duplicate_count = duplicate_txn.count()

print("INFO: Duplicate transaction count:", duplicate_count)

if duplicate_count > 0:
    print("WARNING: Duplicate transactions detected")
    duplicate_txn.show()
else:
    print("SUCCESS: No duplicate transactions found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 4: Validate fraud KPI aggregates
# ============================================================

# ------------------------------------------------------------
# Data Validation: Fraud KPI summary table
# ------------------------------------------------------------
# The fraud_summary_daily table contains aggregated fraud
# metrics calculated at a daily level. This table is typically
# used for dashboards, monitoring systems, and fraud analysis.

# Load the fraud summary table from the Gold layer
df_fraud = spark.table("gold.fraud_summary_daily")

# ------------------------------------------------------------
# Basic validation checks
# ------------------------------------------------------------

# Count the total number of rows in the fraud summary table
# This helps verify that the aggregation job has produced data
# and the table is not empty.
print("INFO: Fraud summary table row count:", df_fraud.count())

# Display the first 5 rows of the fraud summary table
# Useful for visually validating the aggregated metrics
# and confirming that the schema and values look correct.
df_fraud.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
