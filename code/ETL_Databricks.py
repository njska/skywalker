# Databricks notebook source

spark.conf.set("fs.azure.account.auth.type.ddeg2devdlake001.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.ddeg2devdlake001.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.ddeg2devdlake001.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-04-19T13:31:59Z&st=2024-04-19T05:31:59Z&spr=https&sig=ffDPbH24HHO%2FrUKZ3fsT3q%2Fm3H2wxTqiQF%2B9Py9TJWU%3D")

# COMMAND ----------

https://ddeg2devdlake001.blob.core.windows.net/source/Directory_2024/



# COMMAND ----------




# Define the file path to your CSV file
csv_file_path = "abfs://source@ddeg2devdlake001.dfs.core.windows.net/Directory_2024/factaveragecosts.csv"


# COMMAND ----------


# Read CSV file into a DataFrame
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)




# COMMAND ----------

header = df.first()

# COMMAND ----------

data_rows = df.filter(df['_c0'] != header['_c0'])

# COMMAND ----------

df.show()

# COMMAND ----------



# Convert the header row to a list of column names
column_names = [header[i] for i in range(len(header))]


# COMMAND ----------

column_names

# COMMAND ----------

column_names = [str(col) for col in header]

# COMMAND ----------


# Set the DataFrame column names using the extracted header row
df_with_custom_headers = data_rows.toDF(*column_names)



# COMMAND ----------

columns_to_drop = ["_c0"]
df_without_columns = df.drop(*columns_to_drop)

# COMMAND ----------

df_with_custom_headers.printSchema()
df_with_custom_headers.show(truncate=False)

# COMMAND ----------

# Assuming df_with_custom_headers is your DataFrame
# Get the list of columns from the DataFrame
columns = df_with_custom_headers.columns

# Drop the first column from the DataFrame
# Assuming the first column is columns[0]
df_without_first_column = df_with_custom_headers.drop(columns[0])

# Show the DataFrame after dropping the first column
df_without_first_column.show()


# COMMAND ----------

   (df_without_first_column .write.format('parquet').mode('overwrite')\
     
        .saveAsTable("factaveragecosts"))

# COMMAND ----------

null_counts = {col: raw_data_df.filter(raw_data_df[col].isNull()).count() for col in raw_data_df.columns}
print("Null value counts:")
for col, count in null_counts.items():
    print(f"{col}: {count}")

# Check uniqueness of primary key
primary_key_cols = ["order_id"]  # Assuming 'order_id' is primary key
is_unique = raw_data_df.select(primary_key_cols).distinct().count() == raw_data_df.count()
print(f"Primary key (order_id) is unique: {is_unique}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hierrtlloc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hierprod

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hierpricestate

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hierpossite

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hierinvstatus

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hierhldy

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hierclnd

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hierinvloc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from facttransactions

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from factaveragecosts

# COMMAND ----------

# DBTITLE 1,Create a staging schema where the hierarchy table has been normalized into a table for each level and the staged fact table has foreign key relationships with those tables.
# MAGIC %sql
# MAGIC -- Create normalized hierarchy tables in SQL
# MAGIC CREATE TABLE fscldt_dimension (
# MAGIC     fscldt_id INT PRIMARY KEY,
# MAGIC     fscldt_label STRING
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE fsclwk_dimension (
# MAGIC     fsclwk_id INT PRIMARY KEY,
# MAGIC     fsclwk_label STRING
# MAGIC );
# MAGIC
# MAGIC -- Load data into fscldt_dimension and fsclwk_dimension tables
# MAGIC INSERT INTO fscldt_dimension (fscldt_id, fscldt_label)
# MAGIC SELECT DISTINCT fscldt_id, fscldt_label FROM clnd;
# MAGIC
# MAGIC INSERT INTO fsclwk_dimension (fsclwk_id, fsclwk_label)
# MAGIC SELECT DISTINCT fsclwk_id, fsclwk_label FROM clnd;
# MAGIC
# MAGIC -- Create staged fact table with foreign key relationships
# MAGIC CREATE TABLE staging_fact_table (
# MAGIC     order_id INT PRIMARY KEY,
# MAGIC     line_id INT,
# MAGIC     type STRING,
# MAGIC     dt DATE,
# MAGIC     pos_site_id INT,
# MAGIC     sku_id INT,
# MAGIC     fscldt_id INT,
# MAGIC     price_substate_id INT,
# MAGIC     sales_units INT,
# MAGIC     sales_dollars DOUBLE,
# MAGIC     discount_dollars DOUBLE,
# MAGIC     original_order_id INT,
# MAGIC     original_line_id INT,
# MAGIC     CONSTRAINT fk_fscldt FOREIGN KEY (fscldt_id) REFERENCES fscldt_dimension(fscldt_id),
# MAGIC     CONSTRAINT fk_fsclwk FOREIGN KEY (fsclwk_id) REFERENCES fsclwk_dimension(fsclwk_id)
# MAGIC );
# MAGIC
# MAGIC -- Load data into staging_fact_table
# MAGIC INSERT INTO staging_fact_table (order_id, line_id, type, dt, pos_site_id, sku_id, fscldt_id, price_substate_id, sales_units, sales_dollars, discount_dollars, original_order_id, original_line_id)
# MAGIC SELECT
# MAGIC     t.order_id,
# MAGIC     t.line_id,
# MAGIC     t.type,
# MAGIC     t.dt,
# MAGIC     t.pos_site_id,
# MAGIC     t.sku_id,
# MAGIC     c.fscldt_id,
# MAGIC     t.price_substate_id,
# MAGIC     t.sales_units,
# MAGIC     t.sales_dollars,
# MAGIC     t.discount_dollars,
# MAGIC     t.original_order_id,
# MAGIC     t.original_line_id
# MAGIC FROM
# MAGIC     facttransactions t
# MAGIC JOIN
# MAGIC     hierclnd c ON t.fscldt_id = c.fscldt_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create or replace the mview_weekly_sales view
# MAGIC CREATE OR REPLACE VIEW mview_weekly_sales AS
# MAGIC SELECT
# MAGIC     t.pos_site_id,
# MAGIC     t.sku_id,
# MAGIC     c.fsclwk_id,
# MAGIC     t.price_substate_id,
# MAGIC     t.type,
# MAGIC     SUM(t.sales_units) AS total_sales_units,
# MAGIC     SUM(t.sales_dollars) AS total_sales_dollars,
# MAGIC     SUM(t.discount_dollars) AS total_discount_dollars
# MAGIC FROM
# MAGIC  facttransactions t
# MAGIC JOIN
# MAGIC     hierclnd c ON t.fscldt_id = c.fscldt_id  -- Join to clnd to get fsclwk_id
# MAGIC GROUP BY
# MAGIC     t.pos_site_id,
# MAGIC     t.sku_id,
# MAGIC     c.fsclwk_id,
# MAGIC     t.price_substate_id,
# MAGIC     t.type;
# MAGIC

# COMMAND ----------


