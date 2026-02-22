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

# #### **Gold – Dimension Build: dim_transaction_type**
# 
# #### **Purpose**
# Create a conformed transaction type dimension to standardize transaction classifications
# across all reporting and analytics.
# 
# #### **Why this dimension is needed**
# Although transaction type exists as a string in the raw data, a dimension provides:
# - Consistent naming and grouping
# - Extra business attributes (direction, fraud-risk group)
# - Cleaner star schema joins for Power BI

# CELL ********************

from pyspark.sql import functions as F  # Import Spark SQL functions

# -------------------------------------------------------------------
# Step 1: Extract distinct transaction types from the Silver layer
# -------------------------------------------------------------------
# The Silver layer typically contains cleaned and standardized data.
# We extract unique transaction types to build a dimension table.
df_types = (
    spark.table("silver.paysim_transactions_clear")  # Load cleaned transaction data
    .select("transaction_type")                      # Keep only transaction_type column
    .distinct()                                     # Remove duplicates (lazy transformation)
)

# -------------------------------------------------------------------
# Step 2: Enrich transaction types with business attributes
# -------------------------------------------------------------------
# We are building a dimension table (dim_transaction_type)
# by deriving additional business classifications.

df_dim_txn_type = (
    df_types

    # Classify transaction as DEBIT or CREDIT based on business logic
    # DEBIT: Money going out
    # CREDIT: Money coming in
    .withColumn(
        "transaction_direction",
        F.when(
            F.col("transaction_type").isin("CASH_OUT", "TRANSFER", "DEBIT"),
            F.lit("DEBIT")
        ).otherwise(F.lit("CREDIT"))
    )

    # Assign risk category based on fraud/business rules
    # HIGH: Transactions commonly associated with fraud patterns
    # NORMAL: Other transaction types
    .withColumn(
        "risk_group",
        F.when(
            F.col("transaction_type").isin("TRANSFER", "CASH_OUT"),
            F.lit("HIGH")
        ).otherwise(F.lit("NORMAL"))
    )

    # Generate a surrogate key using xxhash64
    # This creates a deterministic hash-based numeric key
    # Cast to long for consistency in dimension table design
    .withColumn(
        "transaction_type_key",
        F.xxhash64(F.col("transaction_type")).cast("long")
    )

    # Reorder/select final columns for the dimension table
    .select(
        "transaction_type_key",
        "transaction_type",
        "transaction_direction",
        "risk_group"
    )
)

# -------------------------------------------------------------------
# Step 3: Write the dimension table to the Gold layer
# -------------------------------------------------------------------
# Gold layer contains curated, business-ready data models.
# We overwrite the table to rebuild the dimension (full refresh approach).

(
    df_dim_txn_type.write
        .format("delta")                         # Store as Delta Lake table
        .mode("overwrite")                       # Replace existing table
        .option("overwriteSchema", "true")       # Allow schema overwrite
        .saveAsTable("gold.dim_transaction_type")  # Save as managed table
)

print("Created: gold.dim_transaction_type")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **# Read the Gold layer dimension table and display all columns without truncating values**

# CELL ********************

spark.table("gold.dim_transaction_type").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Count the total number of records in the Gold dimension table**

# CELL ********************

spark.table("gold.dim_transaction_type").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold – Dimension Build: dim_date**
# 
# #### **Purpose**
# Create a reusable date dimension to support time-based analysis
# across all fact tables.
# 
# #### **Why needed**
# Using a dedicated date dimension enables:
# - Time intelligence in Power BI
# - Consistent date filtering
# - Partitioning strategy
# - Clean star schema design
# 
# ***Generate date range based on Silver data min/max event_ts.***

# CELL ********************

from pyspark.sql import functions as F  # Import Spark SQL functions

# -------------------------------------------------------------------
# Step 1: Get minimum and maximum transaction date from Silver table
# -------------------------------------------------------------------
# We are reading the transaction table from the Silver layer.
# The goal is to determine the full date range of available data.

date_range = (
    spark.table("silver.paysim_transactions_clear")  # Load Silver table
    
    # Convert event_ts (timestamp column) to date
    # Then calculate:
    # - Minimum date in dataset
    # - Maximum date in dataset
    .select(
        F.min(F.to_date("event_ts")).alias("min_date"),  # Earliest transaction date
        F.max(F.to_date("event_ts")).alias("max_date")   # Latest transaction date
    )
    
    # collect() is an ACTION (triggers Spark execution)
    # Since aggregation returns only one row, we extract first row using [0]
    .collect()[0]
)

# Extract values from the returned Row object
min_date = date_range["min_date"]  # Store minimum date
max_date = date_range["max_date"]  # Store maximum date

# Print results
print("Min Dat", min_date)
print("Max Dat", max_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Generate calendar:**

# CELL ********************

from pyspark.sql import functions as F  # Import Spark SQL functions

# -------------------------------------------------------------------
# Step 1: Create a sequence of dates from min_date to max_date
# -------------------------------------------------------------------
# - We use Spark SQL's sequence() function to generate all dates in the range
# - explode() transforms the array of dates into individual rows
# - This will serve as the base for our date dimension table

df_dates = spark.sql(f"""
SELECT explode(sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)) AS date
""")

# -------------------------------------------------------------------
# Step 2: Enrich the dates with common date attributes
# -------------------------------------------------------------------
# We are building a standard Date Dimension (dim_date) for analytics
df_dim_date = (
    df_dates

    # Create a numeric surrogate key in the format YYYYMMDD
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))

    # Extract individual date attributes
    .withColumn("year", F.year("date"))                   # Year part
    .withColumn("month", F.month("date"))                 # Month number (1-12)
    .withColumn("month_name", F.date_format("date", "MMMM"))  # Month name
    .withColumn("day", F.dayofmonth("date"))             # Day of month
    .withColumn("week_of_year", F.weekofyear("date"))     # Week number in year
    .withColumn("quarter", F.quarter("date"))            # Quarter number (1-4)
    .withColumn("day_of_week", F.date_format("date", "EEEE"))  # Day name (Monday, etc.)

    # Select final columns and rename 'date' to 'full_date'
    .select(
        "date_key",
        F.col("date").alias("full_date"),
        "year", "quarter", "month", "month_name",
        "week_of_year", "day", "day_of_week"
    )
)

# -------------------------------------------------------------------
# Step 3: Write the Date Dimension to Gold layer
# -------------------------------------------------------------------
# - Gold layer stores curated, business-ready dimension tables
# - Using Delta format allows ACID transactions and time travel
# - Mode "overwrite" ensures the table is fully refreshed
(df_dim_date.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("gold.dim_date"))

print("Created: gold.dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validate the date dimension**

# CELL ********************

from pyspark.sql import functions as F  # Import Spark SQL functions

# -------------------------------------------------------------------
# Step 1: Count the number of rows in the Date Dimension
# -------------------------------------------------------------------
# - This provides a quick check on whether the table was created correctly
# - Should match the number of days between min_date and max_date + 1
print("dim_date rows:", spark.table("gold.dim_date").count())

# -------------------------------------------------------------------
# Step 2: Verify the minimum and maximum dates in dim_date
# -------------------------------------------------------------------
# - Ensures that the date dimension covers the expected full range
# - Uses aggregation functions min() and max() on 'full_date' column
# - show(truncate=False) prints full date without truncation
spark.table("gold.dim_date").select(
    F.min("full_date").alias("min_date"),  # Earliest date in dimension
    F.max("full_date").alias("max_date")   # Latest date in dimension
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold – Dimension Build: dim_account**
# 
# #### **Purpose**
# Create a conformed account dimension from origin and destination accounts.
# 
# #### **Why needed**
# - Provides surrogate key
# - Enables proper star schema joins
# - Avoids joining fact table on raw string IDs
# - Foundation for SCD Type 2 profile dimension


# CELL ********************

# Build Account Dimension

from pyspark.sql import functions as F

# Step 1: Collect all unique account IDs from transactions
# We extract both sender (origin_customer_id) and receiver (destination_customer_id)
# Then we rename both columns to a common name: account_id
# UNION appends the two lists vertically
# DISTINCT removes duplicate account IDs

df_accounts = (
    spark.table("silver.paysim_transactions_clear")  # Read transactions table from Silver layer
    .select(F.col("origin_customer_id").alias("account_id"))  # Select sender accounts
    .union(
        spark.table("silver.paysim_transactions_clear")  # Read same table again
        .select(F.col("destination_customer_id").alias("account_id"))  # Select receiver accounts
    )
    .distinct()  # Keep only unique account IDs
)

# Step 2: Add Surrogate Key
# Generate a deterministic numeric key using xxhash64
# Same account_id will always produce the same account_key
# Cast to long for better performance in joins

df_dim_account = (
    df_accounts
    .withColumn(
        "account_key",
        F.xxhash64(F.col("account_id")).cast("long")  # Create surrogate key using hashing
    )
    .select("account_key", "account_id")  # Keep only required columns
)

# Step 3: Write Dimension Table to Gold Layer
# Save as Delta table
# Mode "overwrite" replaces existing table if it exists

(df_dim_account.write
 .format("delta")              # Save in Delta format
 .mode("overwrite")            # Overwrite existing data
 .saveAsTable("gold.dim_account")  # Save as Gold layer dimension table
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold – SCD Type 2: Account Profile Snapshot**
# 
# #### Purpose
# Create a “current profile snapshot” for each account based on Silver transactions.
# This snapshot will later be merged into the SCD2 dimension to preserve history.
# 
# #### **What we derive**
# - txn_count, total_amount
# - fraud involvement flag
# - activity_segment (LOW/MEDIUM/HIGH)
# - risk_tier (LOW/HIGH)


# CELL ********************

from pyspark.sql import functions as F

# -----------------------------------------------------------
# STEP 0: Load cleaned transaction data from Silver layer
# -----------------------------------------------------------
# This table contains transaction-level data with:
# - origin_customer_id (sender)
# - destination_customer_id (receiver)
# - amount
# - is_fraud flag
df_silver = spark.table("silver.paysim_transactions_clear")


# -----------------------------------------------------------
# STEP 1: Create OUTGOING transaction metrics per account
# -----------------------------------------------------------
# We group by origin_customer_id (sender account)
# and calculate:
# - total number of outgoing transactions
# - total outgoing amount
# - whether this account was involved in fraud (as sender)
df_origin = (
    df_silver
    .groupBy(F.col("origin_customer_id").alias("account_id"))
    .agg(
        F.count("*").alias("out_txn_count"),          # number of outgoing transactions
        F.sum("amount").alias("out_total_amount"),   # total money sent
        F.max("is_fraud").alias("out_fraud_flag")    # 1 if any outgoing txn was fraud
    )
)


# -----------------------------------------------------------
# STEP 2: Create INCOMING transaction metrics per account
# -----------------------------------------------------------
# We group by destination_customer_id (receiver account)
# and calculate:
# - total number of incoming transactions
# - total incoming amount
# - whether this account was involved in fraud (as receiver)
df_dest = (
    df_silver
    .groupBy(F.col("destination_customer_id").alias("account_id"))
    .agg(
        F.count("*").alias("in_txn_count"),           # number of incoming transactions
        F.sum("amount").alias("in_total_amount"),    # total money received
        F.max("is_fraud").alias("in_fraud_flag")     # 1 if any incoming txn was fraud
    )
)


# -----------------------------------------------------------
# STEP 3: Combine outgoing + incoming into one account profile
# -----------------------------------------------------------
# 1. Stack (union) both datasets together
# 2. Group again by account_id
# 3. Add outgoing + incoming values
# 4. Create overall totals and fraud flag
df_profile = (
    df_origin
    .unionByName(df_dest, allowMissingColumns=True)
    .groupBy("account_id")
    .agg(
        # Replace nulls with 0 before summing
        F.sum(F.coalesce(F.col("out_txn_count"), F.lit(0))).alias("out_txn_count"),
        F.sum(F.coalesce(F.col("in_txn_count"), F.lit(0))).alias("in_txn_count"),
        F.sum(F.coalesce(F.col("out_total_amount"), F.lit(0.0))).alias("out_total_amount"),
        F.sum(F.coalesce(F.col("in_total_amount"), F.lit(0.0))).alias("in_total_amount"),

        # If fraud happened even once, mark flag as 1
        F.max(F.coalesce(F.col("out_fraud_flag"), F.lit(0))).alias("out_fraud_flag"),
        F.max(F.coalesce(F.col("in_fraud_flag"), F.lit(0))).alias("in_fraud_flag")
    )

    # Total transactions = incoming + outgoing
    .withColumn("total_txn_count",
                F.col("out_txn_count") + F.col("in_txn_count"))

    # Total money movement
    .withColumn("total_amount",
                F.col("out_total_amount") + F.col("in_total_amount"))

    # If fraud occurred either incoming or outgoing → mark account as fraud involved
    .withColumn(
        "fraud_involved_flag",
        F.when(
            (F.col("out_fraud_flag") == 1) |
            (F.col("in_fraud_flag") == 1),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
)


# -----------------------------------------------------------
# STEP 4: Create Business Segments (Enterprise-style Bucketing)
# -----------------------------------------------------------
# We classify accounts into:
# - Activity segment (HIGH / MEDIUM / LOW)
# - Risk tier (HIGH if fraud involved, else LOW)
# Then we join with dimension table to get surrogate key.
df_snapshot = (
    df_profile

    # Activity segmentation based on total transactions
    .withColumn(
        "activity_segment",
        F.when(F.col("total_txn_count") >= 50, F.lit("HIGH"))
         .when(F.col("total_txn_count") >= 10, F.lit("MEDIUM"))
         .otherwise(F.lit("LOW"))
    )

    # Risk tier based on fraud involvement
    .withColumn(
        "risk_tier",
        F.when(F.col("fraud_involved_flag") == 1, F.lit("HIGH"))
         .otherwise(F.lit("LOW"))
    )

    # Join with Gold dimension table to get surrogate key (account_key)
    .join(
        spark.table("gold.dim_account"),
        on="account_id",
        how="inner"
    )

    # Select final columns for snapshot table
    .select(
        "account_key",
        "account_id",
        "total_txn_count",
        "total_amount",
        "activity_segment",
        "risk_tier",
        "fraud_involved_flag"
    )
)

# -----------------------------------------------------------
# STEP 5: Validate output
# -----------------------------------------------------------
print("Snapshot rows:", df_snapshot.count())
df_snapshot.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold - SCD Type 2 Merge: dim_account_profile_scd2**
# 
# #### **Purpose**
# Maintain historical account profile changes using Delta MERGE.
# 
# If account attributes change:
# - Expire old record
# - Insert new version
# - Preserve history
# 
# This enables time-based behavioral analysis.

# MARKDOWN ********************

# **Create SCD2 Table (First Time Only)**

# CELL ********************

from pyspark.sql import functions as F

# -----------------------------------------------------------
# STEP 1: Add SCD2 tracking columns
# -----------------------------------------------------------
# We are preparing the account snapshot for SCD2 handling.
# For each account row, we add:
# - effective_start_date: when this record becomes active
# - effective_end_date: null for current record (will be updated when replaced)
# - is_current: True if this record is the active version
df_scd2_init = (
    df_snapshot
    .withColumn("effective_start_date", F.current_timestamp())  # start of validity
    .withColumn("effective_end_date", F.lit(None).cast("timestamp"))  # no end yet
    .withColumn("is_current", F.lit(True))  # current record
)


# -----------------------------------------------------------
# STEP 2: Save as Delta table (initial load)
# -----------------------------------------------------------
# Using overwrite mode since this is the very first load.
(df_scd2_init.write
 .format("delta")                       # Delta Lake format for ACID & SCD2 handling
 .mode("overwrite")                     # first load, so replace if exists
 .saveAsTable("gold.dim_account_profile_scd2"))

print("Created: gold.dim_account_profile_scd2 (initial load)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gold.dim_account_profile_scd2").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **SCD Type 2 MERGE Logic**
# 
# This step keeps historical versions of account profiles.
# 
# Rules:
# - If account exists and attributes changed → expire old row + insert new version
# - If account exists and no change → no action
# - If account is new → insert
# 
# This enables time-travel analysis of account behavior and risk.
# 
# **Delta MERGE**

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# -----------------------------------------------------------
# STEP 1: Define target table (SCD2 dimension table)
# -----------------------------------------------------------
target_table = "gold.dim_account_profile_scd2"

# -----------------------------------------------------------
# STEP 2: Prepare source snapshot for this run
# -----------------------------------------------------------
# We add a merge timestamp that will:
# - Close old records (effective_end_date)
# - Start new records (effective_start_date)
df_src = (
    df_snapshot.withColumn("merge_run_ts", F.current_timestamp())
)

# -----------------------------------------------------------
# STEP 3: Load target Delta table
# -----------------------------------------------------------
# DeltaTable allows us to perform MERGE (upsert) operations
dt = DeltaTable.forName(spark, target_table)

# -----------------------------------------------------------
# STEP 4: Define change detection logic
# -----------------------------------------------------------
# We only expire records when:
# - The record is currently active (is_current = true)
# - AND any business column has changed
change_condition = """
t.is_current = true AND (
  t.total_txn_count <> s.total_txn_count OR
  round(t.total_amount,2) <> round(s.total_amount,2) OR
  t.activity_segment <> s.activity_segment OR
  t.risk_tier <> s.risk_tier OR
  t.fraud_involved_flag <> s.fraud_involved_flag
)
"""

# -----------------------------------------------------------
# STEP 5: Perform SCD2 MERGE
# -----------------------------------------------------------
# Logic:
# 1. If matched AND changed → expire old record
# 2. If not matched → insert new record
dt.alias("t").merge(
    df_src.alias("s"),
    # Match on business key + only active records
    "t.account_key = s.account_key AND t.is_current = true"

# CASE 1: If matched AND data changed → expire old record
).whenMatchedUpdate(
    condition=change_condition,
    set={
        "effective_end_date": "s.merge_run_ts",  # close record
        "is_current": "false"                    # mark as inactive
    }

# CASE 2: If no current record exists → insert new record    
).whenNotMatchedInsert(
    values={
        "account_key": "s.account_key",
        "account_id": "s.account_id",
        "total_txn_count": "s.total_txn_count",
        "total_amount": "s.total_amount",
        "activity_segment": "s.activity_segment",
        "risk_tier": "s.risk_tier",
        "fraud_involved_flag": "s.fraud_involved_flag",
        "effective_start_date": "s.merge_run_ts",   # new version start
        "effective_end_date": "cast(null as timestamp)",
        "is_current": "true"
    }
).execute()

print("SCD2 MERGE completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Find accounts where current row was expired in this run (effective_end_date set recently)**
# #### **We'll insert new current version from snapshot**


# CELL ********************

# -----------------------------------------------------------
# STEP 1: Identify accounts whose old versions were expired
# -----------------------------------------------------------
# We look inside the target SCD2 table and find:
# - Records that are no longer current (is_current = false)
# - AND have an effective_end_date (meaning they were closed)
#
# These accounts need a new active version inserted.
df_changed = (
    spark.table(target_table)
    .filter("is_current = false AND effective_end_date IS NOT NULL")
    .select("account_key")
    .distinct()   # Avoid duplicates
)


# -----------------------------------------------------------
# STEP 2: Create new active versions from latest snapshot
# -----------------------------------------------------------
# We:
# - Join changed accounts with latest snapshot (df_snapshot)
# - Create a new record version
# - Set effective_start_date to current time
# - Keep effective_end_date as NULL (still active)
# - Mark is_current = True
df_new_versions = (
    df_snapshot
    .join(df_changed, on="account_key", how="inner")
    .withColumn("effective_start_date", F.current_timestamp())  # start new version
    .withColumn("effective_end_date", F.lit(None).cast("timestamp"))  # no end yet
    .withColumn("is_current", F.lit(True))  # mark as current record
)


# -----------------------------------------------------------
# STEP 3: Append new versions into the Delta table
# -----------------------------------------------------------
# We use append mode because:
# - Old records are already expired
# - These are new SCD2 rows (new versions)
(df_new_versions.write
 .format("delta")
 .mode("append")
 .saveAsTable(target_table))

print("Inserted new current versions for changed accounts")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# -----------------------------------------------------------
# STEP 1: Read the SCD2 target table
# -----------------------------------------------------------
# This table contains:
# - Current active records (is_current = true)
# - Historical expired records (is_current = false)
df_scd2 = spark.table(target_table)


# -----------------------------------------------------------
# STEP 2: Count records by is_current flag
# -----------------------------------------------------------
# We group by is_current to see:
# - How many active records exist
# - How many historical records exist
(
    df_scd2
    .groupBy("is_current")
    .count()
    .show()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gold.dim_account_profile_scd2").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold – Fact Build: fact_transactions**
# 
# #### **Purpose**
# Create central fact table for transaction analytics.
# 
# This table joins to:
# - dim_date
# - dim_transaction_type
# - dim_account (origin & destination)
# - dim_account_profile_scd2 (current version)
# 
# This enables:
# - Fraud analysis
# - Volume trends
# - Account behavior analysis
# - Executive dashboards


# CELL ********************

from pyspark.sql import functions as F

# -----------------------------------------------------------
# STEP 1: Load source transaction data from Silver layer
# -----------------------------------------------------------
df_silver = spark.table("silver.paysim_transactions_clear")


# -----------------------------------------------------------
# STEP 2: Build Fact Table by joining all necessary dimensions
# -----------------------------------------------------------
df_fact = (
    df_silver.alias("s")

    # 2a) Join with Date Dimension (gold.dim_date)
    # Match event timestamp (event_ts) with full_date in dim_date
    .join(
        spark.table("gold.dim_date").alias("d"),
        F.to_date("s.event_ts") == F.col("d.full_date"),
        "left"
    )

    # 2b) Join with Transaction Type Dimension (gold.dim_transaction_type)
    # Match transaction_type string to its surrogate key
    .join(
        spark.table("gold.dim_transaction_type").alias("t"),
        "transaction_type",
        "left"
    )

    # 2c) Join with Origin Account Dimension (gold.dim_account)
    # Map origin_customer_id to account surrogate key
    .join(
        spark.table("gold.dim_account").alias("a1"),
        F.col("s.origin_customer_id") == F.col("a1.account_id"),
        "left"
    )

    # 2d) Join with Destination Account Dimension (gold.dim_account)
    # Map destination_customer_id to account surrogate key
    .join(
        spark.table("gold.dim_account").alias("a2"),
        F.col("s.destination_customer_id") == F.col("a2.account_id"),
        "left"
    )

    # 2e) Select Fact Table Columns
    .select(
        F.col("d.date_key"),                               # FK to dim_date
        F.col("t.transaction_type_key"),                   # FK to dim_transaction_type
        F.col("a1.account_key").alias("origin_account_key"),       # FK to origin account
        F.col("a2.account_key").alias("destination_account_key"),  # FK to destination account
        F.col("s.amount"),                                 # Transaction amount
        F.col("s.is_fraud"),                               # Fraud flag
        F.col("s.event_ts"),                               # Original event timestamp
        F.col("s.batch_id")                               # Batch ID for traceability
    )
)


# -----------------------------------------------------------
# STEP 3: Write Fact Table to Gold layer as Delta Table
# -----------------------------------------------------------
(df_fact.write
 .format("delta")       # Delta format for ACID and versioning
 .mode("overwrite")     # Overwrite for initial load
 .saveAsTable("gold.fact_transactions"))

print("Created: gold.fact_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gold.fact_transactions").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold – Aggregate: fraud_summary_daily**
# 
# Purpose:
# Provide daily fraud KPIs for fraud department and risk management.
# Metrics:
# - total_transactions
# - fraud_transactions
# - fraud_amount
# - fraud_rate


# CELL ********************

from pyspark.sql import functions as F

# -----------------------------------------------------------
# STEP 1: Load Fact Table (Transactions) from Gold layer
# -----------------------------------------------------------
df_fact = spark.table("gold.fact_transactions")


# -----------------------------------------------------------
# STEP 2: Aggregate daily fraud statistics
# -----------------------------------------------------------
df_fraud_daily = (
    df_fact
    .groupBy("date_key")  # Group by date
    .agg(
        # Count total transactions for each day
        F.count("*").alias("total_transactions"),

        # Count fraudulent transactions for each day
        F.sum("is_fraud").alias("fraud_transactions"),

        # Sum the amounts of fraudulent transactions only
        F.sum(F.when(F.col("is_fraud") == 1, F.col("amount")).otherwise(0)).alias("fraud_amount")
    )
    # Add fraud rate column (fraud transactions / total transactions)
    .withColumn(
        "fraud_rate",
        F.col("fraud_transactions") / F.col("total_transactions")
    )
)


# -----------------------------------------------------------
# STEP 3: Write the Daily Fraud Summary to Gold layer
# -----------------------------------------------------------
(df_fraud_daily.write
 .format("delta")          # Write in Delta format for versioning and ACID compliance
 .mode("overwrite")        # Overwrite the table for this run
 .saveAsTable("gold.fraud_summary_daily"))

print("Created: gold.fraud_summary_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gold.fraud_summary_daily").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold – Aggregate: exec_kpi_daily**
# 
# Purpose:
# Provide executive-level KPIs for transaction monitoring.
# 
# Metrics:
# - total_transactions
# - total_amount
# - avg_transaction_amount
# - active_accounts
# - debit_amount
# - credit_amount


# CELL ********************

from pyspark.sql import functions as F

# -----------------------------------------------------------
# STEP 1: Load Fact Table (Transactions) from Gold layer
# -----------------------------------------------------------
df_fact = spark.table("gold.fact_transactions")


# -----------------------------------------------------------
# STEP 2: Aggregate Executive KPIs on Daily Level
# -----------------------------------------------------------
df_exec_kpi = (
    df_fact
    .groupBy("date_key")  # Grouping by the date
    .agg(
        # Total number of transactions for the day
        F.count("*").alias("total_transactions"),

        # Total transaction amount for the day
        F.sum("amount").alias("total_amount"),

        # Average transaction amount for the day
        F.avg("amount").alias("avg_transaction_amount"),

        # Number of distinct active accounts (origin account) for the day
        F.countDistinct("origin_account_key").alias("active_accounts"),

        # Gross transaction amount where:
        # - Transaction type exists (is not null)
        # - Amount is positive
        F.sum(
            F.when(F.col("transaction_type_key").isNotNull() & 
                   (F.col("amount") > 0), F.col("amount"))
        ).alias("gross_amount")
    )
)


# -----------------------------------------------------------
# STEP 3: Write the KPI Aggregation to Gold layer
# -----------------------------------------------------------
(df_exec_kpi.write
 .format("delta")         # Using Delta format for ACID compliance and versioning
 .mode("overwrite")       # Overwrite for the latest daily data
 .saveAsTable("gold.exec_kpi_daily"))  # Save results to the target table

print("Created: gold.exec_kpi_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gold.exec_kpi_daily").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold – Aggregate: ops_data_quality_daily**
# 
# Purpose:
# Provide daily data quality and pipeline health metrics.
# 
# Metrics:
# - total_rows
# - valid_rows
# - reject_rows
# - reject_rate

# CELL ********************

from pyspark.sql import functions as F

# -----------------------------------------------------------
# STEP 1: Load Source Tables
# -----------------------------------------------------------

# Bronze layer (raw ingestion data)
df_bronze = spark.table("bronze.paysim_transactions_raw")

# Silver layer (validated & cleaned records)
df_silver_valid = spark.table("silver.paysim_transactions_clear")

# Rejected records captured by DQ checks
df_reject = spark.table("dq.paysim_rejects")


# -----------------------------------------------------------
# STEP 2: Calculate Daily Valid Row Counts
# -----------------------------------------------------------
# Count how many records passed validation per day
df_valid_daily = (
    df_silver_valid
    .groupBy(F.to_date("event_ts").alias("event_date"))
    .agg(
        F.count("*").alias("valid_rows")
    )
)


# -----------------------------------------------------------
# STEP 3: Calculate Daily Rejected Row Counts
# -----------------------------------------------------------
# Count how many records failed validation per day
df_reject_daily = (
    df_reject
    .groupBy(F.to_date("event_ts").alias("event_date"))
    .agg(
        F.count("*").alias("reject_rows")
    )
)

# -----------------------------------------------------------
# STEP 4: Combine Valid + Reject Counts
# -----------------------------------------------------------
df_ops = (
    df_valid_daily
    .join(df_reject_daily, on="event_date", how="left")  # keep all valid dates
    .fillna({"reject_rows": 0})  # if no rejects, set to 0

    # Total rows processed
    .withColumn("total_rows",
                F.col("valid_rows") + F.col("reject_rows"))

    # Reject rate = rejected / total processed
    .withColumn("reject_rate",
                F.col("reject_rows") / F.col("total_rows"))
)

# -----------------------------------------------------------
# STEP 5: Write Daily Data Quality Summary to Gold layer
# -----------------------------------------------------------
(df_ops.write
 .format("delta")
 .mode("overwrite")  # Overwrite for full daily refresh
 .saveAsTable("gold.ops_data_quality_daily"))

print("Created: gold.ops_data_quality_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gold.ops_data_quality_daily").count()

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
