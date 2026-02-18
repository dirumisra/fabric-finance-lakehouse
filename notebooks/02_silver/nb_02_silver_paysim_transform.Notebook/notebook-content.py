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

# #### **Silver Layer – PaySim Data Transformation & Standardization**
# 
# #### **Objective**
# 
# Transform raw transaction data from the Bronze layer into a clean,
# validated, and business-ready dataset in the Silver layer.
# 
# The Silver layer ensures that all downstream analytics and modeling
# are based on standardized and high-quality data.
# 
# ---
# 
# #### **Source**
# 
# - Input Table: `bronze.paysim_transactions_raw`
# - Data Volume: ~6.3 million transaction records
# - Contains raw fields + ingestion metadata
# 
# ---
# 
# #### **Why Silver Layer Is Required**
# 
# Bronze data is stored exactly as received, without enforcing
# business rules or consistent naming standards.
# 
# If Bronze data is directly used for reporting:
# 
# - Column names may be inconsistent
# - Invalid records may impact analytics
# - Data types may not be enforced
# - Business logic becomes scattered across reports
# 
# The Silver layer solves this by centralizing data preparation.
# 
# ---
# 
# #### **Transformations to Be Applied**
# 
# #### **1. Column Standardization**
# - Convert column names to snake_case
# - Rename business fields to meaningful names
#   (e.g., nameOrig → origin_customer_id)
# 
# #### **2. Data Type Enforcement**
# - Explicitly cast numeric and timestamp columns
# - Ensure consistent schema for production reliability
# 
# #### **3. Derived Columns**
# - Create event timestamp from `step`
# - Generate transaction ID
# - Add business indicators where required
# 
# #### **4. Data Quality Validation**
# - Validate amount > 0
# - Validate transaction type in allowed list
# - Identify invalid or suspicious records
# 
# Invalid records will be written to:
# `dq.paysim_rejects`
# 
# Valid records will be written to:
# `silver.paysim_transactions_clean`
# 
# ---
# 
# #### **Outputs**
# 
# - `silver.paysim_transactions_clean`
# - `dq.paysim_rejects`
# 
# These outputs will serve as the foundation for
# Gold layer dimensional modeling and reporting.


# MARKDOWN ********************

# #### **Read Bronze Table**
# 
# This step loads the Bronze layer table into the Silver notebook.
# 
# Why?
# Silver transformations must always use Bronze as input,
# never raw files from landing.
# 
# This ensures:
# - Clear medallion separation
# - Reproducibility
# - Controlled transformation pipeline


# CELL ********************

# Step 5: Inspect and Preview Data in the Bronze Layer

from pyspark.sql import functions as F

# Load the Bronze table into a DataFrame
df_bronze = spark.table("bronze.paysim_transactions_raw")

# Print the total row count of the Bronze table
print("Bronze Row Count:", df_bronze.count())

# Display the schema (data types) of the Bronze table
df_bronze.printSchema()

# Show the first 5 rows of the DataFrame without truncating any values
df_bronze.show(5, truncate=False)

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
