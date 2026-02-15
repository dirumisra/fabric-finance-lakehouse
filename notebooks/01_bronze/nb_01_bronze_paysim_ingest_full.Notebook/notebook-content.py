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

# #### **Load CSV File and Perform Initial Data Validation (Row Count, Schema, Preview)**

# MARKDOWN ********************

# #### **Bronze Layer – Full Load Ingestion (PaySim)**
# 
# #### **Purpose**
# This notebook ingests raw PaySim transaction data from the Landing zone
# and writes it to the Bronze layer in Delta format.
# 
# #### **Why Bronze?**
# The Bronze layer stores raw data as-is, with additional ingestion metadata
# to enable auditing, traceability, and reprocessing.
# 
# #### **Ingestion Metadata Columns Added**
# - _ingest_ts → Timestamp of ingestion
# - _batch_id → Unique identifier of this pipeline run
# - _source_file → File name from landing zone
# - _ingest_mode → FULL (initial load) or INCR (incremental load)
# 
# This ensures:
# - Data lineage
# - Auditability
# - Pipeline traceability
# - Enterprise compliance readiness

# CELL ********************

# Importing necessary PySpark functions
from pyspark.sql import functions as F

# Define the file path for the CSV data
file_path = "Files/landing/paysim/paysim_transactions_full_20260214.csv"

# Read the CSV file into a DataFrame with options for headers and schema inference
df = (
    spark.read
    .option("header", "true")  # Treat the first row as header (column names)
    .option("inferSchema", "true")  # Automatically infer data types for columns
    .csv(file_path)  # Load the data from the CSV file
)

# Print the number of rows in the DataFrame
print("Rows:", df.count())

# Display the schema of the DataFrame (data types for each column)
df.printSchema()

# Show the first 5 rows of the DataFrame, truncating long values for readability
df.show(5, truncate=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Bronze Layer – Persist Raw Transactions with Ingestion Metadata**
# 
# #### **Objective**
# Persist raw PaySim transaction data into the Bronze layer using Delta Lake
# while enriching it with ingestion metadata for traceability and auditing.
# 
# #### **Why Are We Adding Metadata Columns?**
# 
# In enterprise data platforms, raw data must always be traceable.
# If a pipeline fails, reruns, or audits are required, we must know:
# 
# - When the data was ingested
# - Which pipeline run loaded the data
# - Which source file generated the data
# - Whether the load was FULL or INCREMENTAL
# 
# #### **Metadata Columns Added**
# 
# - _ingest_ts     → Timestamp of ingestion
# - _batch_id      → Unique identifier for this pipeline execution
# - _source_file   → Name of the file loaded from landing zone
# - _ingest_mode   → FULL (initial baseline load)
# 
# #### **Why Use UUID for Batch ID?**
# 
# A UUID ensures that each ingestion run is uniquely identifiable,
# even if the same file is reprocessed multiple times.
# 
# #### **Why Delta Format?**
# 
# Delta Lake provides:
# - ACID transactions
# - Schema enforcement
# - Time travel capability
# - Scalability for millions of rows
# 
# #### **Why Mode = "overwrite"?**
# 
# Since this is the first full load,
# we overwrite any existing Bronze table to establish a clean baseline.
# Future incremental loads will use append mode.


# CELL ********************

# Step 2: Transform and Load Data into Bronze Delta Table with Metadata

# Import PySpark functions and Python UUID library
from pyspark.sql import functions as F
import uuid

# Generate a unique batch ID for this ingestion
batch_id = str(uuid.uuid4())

# Define the source file name
source_file = "paysim_transactions_full_20260214.csv"

# Add ingestion metadata columns to the DataFrame
df_bronze = (
    df
    .withColumn("_ingest_ts", F.current_timestamp())  # Timestamp when the data is ingested
    .withColumn("batch_id", F.lit(batch_id))          # Unique batch ID for this load
    .withColumn("_source_file", F.lit(source_file))   # Track the source file name
    .withColumn("_ingest_mode", F.lit("FULL"))        # Ingestion mode (FULL or INCREMENTAL)
)

# Write the DataFrame to a Delta table in the Bronze layer
(
    df_bronze.write
    .format("delta")                 # Use Delta Lake format
    .mode("overwrite")               # Overwrite existing data for this table
    .saveAsTable("bronze.paysim_transactions_raw")  # Save as managed Delta table
)

# Print confirmation of successful table creation
print("Bronze table created:", "bronze.paysim_transactions_raw")
print("Batch ID:", batch_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Raw Validation** 

# CELL ********************

spark.table("bronze.paysim_transactions_raw").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **SUCCESS Log Entry**

# CELL ********************

# Step 3: Log Pipeline Execution Details into Pipeline Run Log Table

# Import PySpark functions and UUID library
from pyspark.sql import functions as F
import uuid

# Generate a unique run ID for this pipeline execution
run_id = str(uuid.uuid4())

# Get the total number of rows loaded into the Bronze table
rows_loaded = spark.table("bronze.paysim_transactions_raw").count()

# Prepare a log record as a tuple (must match log table schema order)
log_entry = [(
    run_id,                   # Unique pipeline run ID
    "pl_ingest_paysim",       # Pipeline name
    batch_id,                 # Batch ID from ingestion step
    "DEV",                    # Environment name
    None,                     # start_ts (will be populated later)
    None,                     # end_ts (will be populated later)
    "SUCCESS",                # Pipeline execution status
    rows_loaded,              # Total rows read
    rows_loaded,              # Total rows written
    None,                     # error_message (None since success)
    None                      # created_at (will be populated later)
)]

# Get schema from existing pipeline log table to ensure structure consistency
log_schema = spark.table("log.pipeline_run_log").schema

# Create DataFrame using predefined schema and populate timestamp fields
df_log = spark.createDataFrame(log_entry, schema=log_schema) \
    .withColumn("start_ts", F.current_timestamp()) \
    .withColumn("end_ts", F.current_timestamp()) \
    .withColumn("created_at", F.current_timestamp())

# Append the log record into the Delta log table
df_log.write.format("delta").mode("append").saveAsTable("log.pipeline_run_log")

# Print confirmation message
print("Pipeline run logged successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Operational Logging – Record Bronze Ingestion Run**
# 
# #### **Purpose**
# Record the outcome of the Bronze ingestion step into `log.pipeline_run_log`
# so that every execution is traceable with status, timestamps, batch id,
# and row counts.
# 
# #### **Why this matters**
# In enterprise platforms, every pipeline execution must be auditable.
# This log enables:
# - Monitoring success/failure
# - Debugging issues
# - SLA and run history tracking
# - Building an Ops dashboard in Power BI later
# 


# CELL ********************

# Step 3: Capture and Store Pipeline Execution Log with Run ID

# Import required libraries
from pyspark.sql import functions as F
import uuid

# Generate a unique run ID for this pipeline execution
run_id = str(uuid.uuid4())

# Get total number of rows currently available in the Bronze table
rows_loaded = spark.table("bronze.paysim_transactions_raw").count()

# Create a single log entry matching the pipeline log table structure
log_entry = [(
    run_id,                 # Unique pipeline run identifier
    "pl_ingest_paysim",     # Pipeline name
    batch_id,               # Batch ID generated during ingestion
    "DEV",                  # Environment (DEV / TEST / PROD)
    None,                   # start_ts (will be populated below)
    None,                   # end_ts (will be populated below)
    "SUCCESS",              # Execution status
    rows_loaded,            # Total rows read
    rows_loaded,            # Total rows written
    None,                   # Error message (None since successful run)
    None                    # created_at (will be populated below)
)]

# Fetch schema from existing log table to maintain structure consistency
log_schema = spark.table("log.pipeline_run_log").schema

# Create DataFrame for logging and populate timestamp columns
df_log = spark.createDataFrame(log_entry, schema=log_schema) \
    .withColumn("start_ts", F.current_timestamp()) \
    .withColumn("end_ts", F.current_timestamp()) \
    .withColumn("created_at", F.current_timestamp())

# Append the log record into the Delta log table
df_log.write.format("delta").mode("append").saveAsTable("log.pipeline_run_log")

# Print confirmation message with run ID
print("Pipeline run logged successfully. run_id =", run_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Verify log row**

# CELL ********************

spark.table("log.pipeline_run_log").orderBy(F.col("created_at").desc()).show(5,truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Control Table Update – Mark FULL Load Completed**
# 
# #### **Purpose**
# Update `meta.ingestion_control` so the system remembers:
# - The FULL load has completed successfully
# - The last processed batch id
# - The watermark to continue from next time (incremental mode)
# 
# #### **Why this matters**
# Without updating this control table:
# - The next run will not know where to start
# - Incremental ingestion cannot work reliably
# - Re-runs may create duplicates

# CELL ********************


from pyspark.sql import functions as F

# Retrieve watermark value from Bronze table
# (PaySim "step" column behaves like a time indicator)
max_step = spark.table("bronze.paysim_transactions_raw") \
    .agg(F.max("step").alias("max_step")) \
    .collect()[0]["max_step"]

# Load the ingestion control metadata table
control_df = spark.table("meta.ingestion_control")

# Update control table fields for this specific pipeline/entity
updated_control = (
    control_df

    # Switch load type from FULL to INCR
    .withColumn(
        "load_type",
        F.when(
            (F.col("pipeline_name") == "pl_ingest_paysim") &
            (F.col("entity_name") == "paysim_transactions"),
            F.lit("INCR")
        ).otherwise(F.col("load_type"))
    )

    # Store last processed batch ID
    .withColumn(
        "last_batch_id",
        F.when(
            (F.col("pipeline_name") == "pl_ingest_paysim") &
            (F.col("entity_name") == "paysim_transactions"),
            F.lit(batch_id)
        ).otherwise(F.col("last_batch_id"))
    )

    # Store last processed source file name
    .withColumn(
        "last_source_file",
        F.when(
            (F.col("pipeline_name") == "pl_ingest_paysim") &
            (F.col("entity_name") == "paysim_transactions"),
            F.lit(source_file)
        ).otherwise(F.col("last_source_file"))
    )

    # Update timestamp of control record modification
    .withColumn(
        "updated_at",
        F.when(
            (F.col("pipeline_name") == "pl_ingest_paysim") &
            (F.col("entity_name") == "paysim_transactions"),
            F.current_timestamp()
        ).otherwise(F.col("updated_at"))
    )

    # Track which notebook/job updated the control record
    .withColumn(
        "updated_by",
        F.when(
            (F.col("pipeline_name") == "pl_ingest_paysim") &
            (F.col("entity_name") == "paysim_transactions"),
            F.lit("nb_01_bronze_paysim_ingest_full")
        ).otherwise(F.col("updated_by"))
    )
)

# Overwrite control table (safe since it is a small metadata table)
(
    updated_control.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("meta.ingestion_control")
)

# Print confirmation with latest watermark value
print("Control table updated. max_step =", max_step)

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
