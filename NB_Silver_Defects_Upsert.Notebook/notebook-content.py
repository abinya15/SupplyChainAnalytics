# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e6f6a2f7-ec0c-456b-8583-db1153a8ab3b",
# META       "default_lakehouse_name": "LH_SupplyChain_Silver",
# META       "default_lakehouse_workspace_id": "90e5cf56-0097-43d4-ac6d-df47fa8484c0",
# META       "known_lakehouses": [
# META         {
# META           "id": "89661cd3-4bdf-494e-95d1-34d05deb9b93"
# META         },
# META         {
# META           "id": "9b825699-e489-464c-ada5-9ac51c03794a"
# META         },
# META         {
# META           "id": "e6f6a2f7-ec0c-456b-8583-db1153a8ab3b"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# -------------------------------------------------------------
# VERIFY: confirm this session's default Lakehouse is Silver,
# and that silver_defect_logs / silver_defect_quarantine exist here
# -------------------------------------------------------------
print("Current default database/lakehouse:", spark.catalog.currentDatabase())
spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col, to_date, current_timestamp, input_file_name, when, lit, 
    trim, initcap, upper, sha2, concat_ws, avg, stddev, abs
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# -------------------------------------------------------------
# 0. ENABLE AUTOMATIC SCHEMA DRIFT / EVOLUTION
# -------------------------------------------------------------
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# -------------------------------------------------------------
# 1. READ RAW FROM BRONZE
# -------------------------------------------------------------
bronze_path = "abfss://Supplychain_Analytics_Dev@onelake.dfs.fabric.microsoft.com/LH_SupplyChain_Bronze.Lakehouse/Files/raw/supply_chain/defects/2026/08/defects.json"

raw_df = spark.read.option("multiline", "true").json(bronze_path) \
    .withColumn("_source_file", input_file_name()) \
    .withColumn("_ingested_at", current_timestamp())

standardized_df = raw_df \
    .withColumn("Supplier", trim(col("Supplier"))) \
    .withColumn("PlantRegion", upper(trim(col("PlantRegion")))) \
    .withColumn("DefectType", initcap(trim(col("DefectType")))) \
    .withColumn("Date", to_date(col("Date"), "yyyy-MM-dd")) \
    .withColumn("CostImpact", col("CostImpact").cast("double"))

# -------------------------------------------------------------
# 2. STATEFUL ANOMALY DETECTION & QUALITY AUDITING
# -------------------------------------------------------------
anomaly_window = Window.partitionBy("PlantRegion", "DefectType")

validated_df = standardized_df \
    .withColumn("group_avg", avg("CostImpact").over(anomaly_window)) \
    .withColumn("group_stddev", stddev("CostImpact").over(anomaly_window)) \
    .withColumn("is_outlier", 
        when(col("group_stddev") > 0, 
             abs(col("CostImpact") - col("group_avg")) > (3 * col("group_stddev"))
        ).otherwise(lit(False))
    ) \
    .withColumn("is_valid",
        (col("DefectID").isNotNull()) &
        (col("CostImpact").isNotNull()) &
        (col("CostImpact") >= 0) &
        (col("is_outlier") == False)
    )

clean_batch_df = validated_df.filter(col("is_valid") == True).drop("is_valid", "group_avg", "group_stddev", "is_outlier")
quarantine_batch_df = validated_df.filter(col("is_valid") == False).drop("is_valid", "group_avg", "group_stddev")

# -------------------------------------------------------------
# 3. ROUTE MALFORMED & ANOMALOUS DATA TO QUARANTINE
# -------------------------------------------------------------
if quarantine_batch_df.count() > 0:
    quarantine_batch_df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("silver_defect_quarantine")
    print(f"Quarantined {quarantine_batch_df.count()} corrupt or outlier records.")

# -------------------------------------------------------------
# 4. SLOWLY CHANGING DIMENSIONS TYPE 2 (SCD2) MERGE
# -------------------------------------------------------------
now = current_timestamp()
target_table_name = "silver_defect_logs"

incoming_df = clean_batch_df.dropDuplicates(["DefectID"]) \
    .withColumn("row_hash", sha2(concat_ws("||", col("CostImpact"), col("PlantRegion"), col("DefectType"), col("Supplier")), 256)) \
    .withColumn("valid_from", now) \
    .withColumn("valid_to", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True))

if not spark.catalog.tableExists(target_table_name):
    incoming_df.write.format("delta") \
        .mode("overwrite") \
        .option("clusterBy", "PlantRegion,DefectType") \
        .saveAsTable(target_table_name)
    print(f"Initialized {target_table_name} with SCD2 and Liquid Clustering.")
else:
    delta_target = DeltaTable.forName(spark, target_table_name)
    target_df = delta_target.toDF()

    changed_records = incoming_df.alias("src") \
        .join(
            target_df.filter(col("is_current") == True).alias("tgt"),
            on="DefectID",
            how="inner"
        ) \
        .filter("src.row_hash != tgt.row_hash") \
        .select("src.*")

    staged_updates = changed_records.withColumn("merge_key", lit(None).cast("string"))
    staged_inserts = incoming_df.withColumn("merge_key", col("DefectID"))
    staged_batch = staged_updates.unionByName(staged_inserts)

    (
        delta_target.alias("target")
        .merge(
            staged_batch.alias("source"),
            "target.DefectID = source.merge_key AND target.is_current = true"
        )
        .whenMatchedUpdate(
            condition="target.row_hash != source.row_hash",
            set={
                "is_current": lit(False),
                "valid_to": col("source.valid_from")
            }
        )
        .whenNotMatchedInsert(
            values={
                "DefectID": "source.DefectID",
                "Supplier": "source.Supplier",
                "PlantRegion": "source.PlantRegion",
                "DefectType": "source.DefectType",
                "Date": "source.Date",
                "CostImpact": "source.CostImpact",
                "row_hash": "source.row_hash",
                "valid_from": "source.valid_from",
                "valid_to": "source.valid_to",
                "is_current": "source.is_current",
                "_source_file": "source._source_file",
                "_ingested_at": "source._ingested_at"
            }
        )
        .execute()
    )
    print(f"Executed SCD2 Merge for batch into {target_table_name}.")

# -------------------------------------------------------------
# 5. STORAGE OPTIMIZATION & FILE COMPACTION
# -------------------------------------------------------------
spark.sql(f"OPTIMIZE {target_table_name}")
print("Storage layout compacted and optimized with V-Order.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_table = DeltaTable.forName(spark, "silver_defect_logs")
history_df = delta_table.history()

display(history_df.select("version", "timestamp", "operation", "operationMetrics"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp, lit

bad_records = [
    (None, "BadCorp", "Europe", "Assembly", "2026-08-30", 1200.0),
    ("DEF-FAIL-001", "ApexLogistics", "APAC", "Packaging", "2026-08-31", -850.0),
    ("DEF-FAIL-002", "GlobalParts", "NA", "Electrical", "2026-08-31", None),
]

columns = ["DefectID", "Supplier", "PlantRegion", "DefectType", "Date", "CostImpact"]
bad_df = (
    spark.createDataFrame(bad_records, columns)
    .withColumn("_source_file", lit("simulated_defect_test.json"))
    .withColumn("_ingested_at", current_timestamp())
)

bad_df.write.format("delta").mode("append").saveAsTable("silver_defect_quarantine")
print("Quarantine table successfully initialized with test failures.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when, concat_ws, array, lit

quarantine_df = spark.read.table("silver_defect_quarantine")

audit_inspected_df = quarantine_df.withColumn(
    "failure_reasons",
    concat_ws(
        "; ",
        array(
            when(col("DefectID").isNull(), lit("Missing DefectID")),
            when(col("CostImpact").isNull(), lit("Null CostImpact")),
            when(col("CostImpact") < 0, lit("Negative CostImpact"))
        )
    )
)

display(
    audit_inspected_df.select(
        "DefectID", "Supplier", "CostImpact", "failure_reasons", "_source_file", "_ingested_at"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
