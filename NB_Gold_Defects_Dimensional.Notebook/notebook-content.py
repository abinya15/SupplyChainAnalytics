# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9b825699-e489-464c-ada5-9ac51c03794a",
# META       "default_lakehouse_name": "LH_SupplyChain_Gold",
# META       "default_lakehouse_workspace_id": "90e5cf56-0097-43d4-ac6d-df47fa8484c0",
# META       "known_lakehouses": [
# META         {
# META           "id": "9b825699-e489-464c-ada5-9ac51c03794a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# -------------------------------------------------------------
# VERIFY: confirm this session's default Lakehouse is Gold
# -------------------------------------------------------------
print("Current default database/lakehouse:", spark.catalog.currentDatabase())
spark.sql("SHOW TABLES").show(truncate=False)

# Confirm silver_defect_logs actually exists at the Silver path
try:
    files = mssparkutils.fs.ls(
        "abfss://Supplychain_Analytics_Dev@onelake.dfs.fabric.microsoft.com/LH_SupplyChain_Silver.Lakehouse/Tables/"
    )
    print("Tables found in Silver Lakehouse:")
    for f in files:
        print(" -", f.name)
except Exception as e:
    print("Could not list Silver tables:", e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------------
# 0. CLEANUP: REMOVE STALE GOLD TABLES BEFORE REBUILD
# -------------------------------------------------------------
# Earlier runs left orphaned/misregistered Delta folders in Tables/.
# Dropping the metastore entry AND deleting the underlying folder before
# rewriting with saveAsTable() ensures a clean, correctly registered table.

gold_lakehouse_name = "LH_SupplyChain_Gold"
workspace_name = "Supplychain_Analytics_Dev"
gold_base_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{gold_lakehouse_name}.Lakehouse/Tables"

tables_to_reset = ["dim_supplier", "dim_region", "dim_date", "fact_defects"]

for t in tables_to_reset:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    path = f"{gold_base_path}/{t}"
    try:
        mssparkutils.fs.rm(path, recurse=True)
        print(f"Removed folder: {t}")
    except Exception as e:
        print(f"No folder to remove for {t} (already clean): {e}")

print("Cleanup complete. Ready to rebuild Gold tables.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col, monotonically_increasing_id, year, month, quarter, dayofmonth
)

# -------------------------------------------------------------
# 1. READ ACTIVE RECORDS FROM SILVER (via path, not cross-lakehouse table ref)
# -------------------------------------------------------------
silver_path = "abfss://Supplychain_Analytics_Dev@onelake.dfs.fabric.microsoft.com/LH_SupplyChain_Silver.Lakehouse/Tables/dbo/silver_defect_logs"

silver_active_df = spark.read.format("delta").load(silver_path) \
    .filter(col("is_current") == True)

# -------------------------------------------------------------
# 2. BUILD DIMENSIONS WITH SURROGATE KEYS
# -------------------------------------------------------------
dim_supplier = silver_active_df.select("Supplier").distinct() \
    .withColumn("SupplierKey", monotonically_increasing_id() + 1)

dim_region = silver_active_df.select("PlantRegion").distinct() \
    .withColumn("RegionKey", monotonically_increasing_id() + 1)

dim_date = silver_active_df.select("Date").distinct().filter(col("Date").isNotNull()) \
    .withColumn("DateKey", col("Date")) \
    .withColumn("Year", year(col("Date"))) \
    .withColumn("Quarter", quarter(col("Date"))) \
    .withColumn("Month", month(col("Date"))) \
    .withColumn("Day", dayofmonth(col("Date")))

# -------------------------------------------------------------
# 3. BUILD FACT TABLE
# -------------------------------------------------------------
fact_defects = silver_active_df \
    .join(dim_supplier, on="Supplier", how="left") \
    .join(dim_region, on="PlantRegion", how="left") \
    .select(
        col("DefectID"),
        col("SupplierKey"),
        col("RegionKey"),
        col("Date").alias("DateKey"),
        col("DefectType"),
        col("CostImpact"),
        col("_ingested_at").cast("date")
    )

# -------------------------------------------------------------
# 4. WRITE DELTA TABLES TO GOLD LAKEHOUSE (registered as metastore tables)
# -------------------------------------------------------------
gold_tables = {
    "dim_supplier": dim_supplier,
    "dim_region": dim_region,
    "dim_date": dim_date,
    "fact_defects": fact_defects
}

for table_name, df in gold_tables.items():
    (
        df.write.format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(table_name)
    )
    print(f"Successfully written to Gold: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List everything under Silver Lakehouse's Tables/ folder
base = "abfss://Supplychain_Analytics_Dev@onelake.dfs.fabric.microsoft.com/LH_SupplyChain_Silver.Lakehouse/Tables/dbo"
try:
    for f in mssparkutils.fs.ls(base):
        print(f.name, "| isDir:", f.isDir, "| size:", f.size)
except Exception as e:
    print("Tables/ folder itself not found:", e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
