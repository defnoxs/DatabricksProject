# Databricks notebook source
# MAGIC %md
# MAGIC #May 2023 Data (source)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/mnt/project/bronze'

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType, StringType, StructField, StructType, DateType
streets_schema = StructType([
    StructField('Crime_ID', StringType(), False), 
    StructField('Month', DateType(), True), 
    StructField('Reported_by', StringType(), True), 
    StructField('Falls_within', StringType(), True), 
    StructField('Longitude', DoubleType(), True), 
    StructField('Latitude', DoubleType(), True), 
    StructField('Location', StringType(), True), 
    StructField('LSOA_code', StringType(), True), 
    StructField('LSOA_name', StringType(), True), 
    StructField('Crime_type', StringType(), True), 
    StructField('Last_outcome_category', StringType(), True), 
    StructField('Context', StringType(), True)])

# COMMAND ----------

streets_df = spark.read.csv('/mnt/project/bronze/2023-05-bedfordshire-street.csv', header=True, schema=streets_schema)

# COMMAND ----------

display(streets_df)

# COMMAND ----------

from pyspark.sql.functions import *
streets_df = streets_df.select('Crime_ID', 'Falls_within', 'Location', 'LSOA_name', 'Crime_type', 'Last_outcome_category', 'Month') \
.withColumnRenamed('Crime_ID', 'crime_id') \
.withColumnRenamed('Falls_within', 'force_area') \
.withColumnRenamed('Location', 'location') \
.withColumnRenamed('LSOA_name', 'borough') \
.withColumnRenamed('Crime_type', 'crime') \
.withColumnRenamed('Last_outcome_category', 'latest_outcome') \
.withColumn('offence_date', date_format('Month','MM-yyyy')) \
.withColumn('timestamp', current_timestamp()) \
.withColumn("source_file", input_file_name())


# COMMAND ----------

streets_df2 = streets_df.withColumn("borough",expr("substring(borough, 1, length(borough)-5)")) \
.withColumn("location",expr("substring(location, 11, length(location)-1)")) \
.drop('Month')

# COMMAND ----------

final_df = streets_df2.where("latest_outcome != 'null'")

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS police_data

# COMMAND ----------

final_df.write.format("delta").option("path","/mnt/project/silver/crime_silver").saveAsTable("police_data.crime_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC #June 2023 data (update file)

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType, StringType, StructField, StructType, DateType
streets_schema = StructType([
    StructField('Crime_ID', StringType(), False), 
    StructField('Month', DateType(), True), 
    StructField('Reported_by', StringType(), True), 
    StructField('Falls_within', StringType(), True), 
    StructField('Longitude', DoubleType(), True), 
    StructField('Latitude', DoubleType(), True), 
    StructField('Location', StringType(), True), 
    StructField('LSOA_code', StringType(), True), 
    StructField('LSOA_name', StringType(), True), 
    StructField('Crime_type', StringType(), True), 
    StructField('Last_outcome_category', StringType(), True), 
    StructField('Context', StringType(), True)])

# COMMAND ----------

streets_df = spark.read.csv('/mnt/project/bronze/2023-06-bedfordshire-street.csv', header=True, schema=streets_schema)

# COMMAND ----------

from pyspark.sql.functions import *
streets_df = streets_df.select('Crime_ID', 'Falls_within', 'Location', 'LSOA_name', 'Crime_type', 'Last_outcome_category', 'Month') \
.withColumnRenamed('Crime_ID', 'crime_id') \
.withColumnRenamed('Falls_within', 'force_area') \
.withColumnRenamed('Location', 'location') \
.withColumnRenamed('LSOA_name', 'borough') \
.withColumnRenamed('Crime_type', 'crime') \
.withColumnRenamed('Last_outcome_category', 'latest_outcome') \
.withColumn('offence_date', date_format('Month','MM-yyyy')) \
.withColumn('timestamp', current_timestamp()) \
.withColumn("source_file", input_file_name())

# COMMAND ----------

streets_df2 = streets_df.withColumn("borough",expr("substring(borough, 1, length(borough)-5)")) \
.withColumn("location",expr("substring(location, 11, length(location)-1)")) \
.drop('Month')

# COMMAND ----------

update_df = streets_df2.where("latest_outcome != 'null'")

# COMMAND ----------

display(update_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Merge

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import *

deltaTable = DeltaTable.forPath(spark, '/mnt/project/silver/crime_silver')

deltaTable.alias('tgt') \
  .merge(
    update_df.alias('src'),
    'tgt.crime_id = src.crime_id'
  ) \
  .whenMatchedUpdate(set =
    {
      "crime_id": "src.crime_id",
      "force_area": "src.force_area",
      "location": "src.location",
      "borough": "src.borough",
      "crime": "src.crime",
      "latest_outcome": "src.latest_outcome",
      "offence_date": "src.offence_date",
      "timestamp": current_timestamp(),
      "source_file": "src.source_file"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
     "crime_id": "src.crime_id",
      "force_area": "src.force_area",
      "location": "src.location",
      "borough": "src.borough",
      "crime": "src.crime",
      "latest_outcome": "src.latest_outcome",
      "offence_date": "src.offence_date",
      "timestamp": current_timestamp(),
      "source_file": "src.source_file"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM police_data.crime_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY police_data.crime_silver

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver to Gold 

# COMMAND ----------

crime_df = spark.read.format("delta").load("/mnt/project/silver/crime_silver")

# COMMAND ----------

crime_borough = crime_df.select('borough').groupBy("borough").count().filter((crime_df.borough).isin(['Bedford', 'Luton', 'Central Bedfordshire'])).orderBy(desc("count"))

# COMMAND ----------

display(crime_borough)

# COMMAND ----------

crime_borough.write.format("delta").mode("overwrite").option("path","/mnt/project/gold/crime_gold_borough").saveAsTable("police_data.crime_gold_borough")

# COMMAND ----------

outcomes_per_crime= crime_df.select('crime', 'latest_outcome').groupBy("crime", "latest_outcome").count().orderBy(desc("count"))

# COMMAND ----------

display(outcomes_per_crime)

# COMMAND ----------

outcomes_per_crime.write.format("delta").mode("overwrite").option("path","/mnt/project/gold/crime_gold_outcomes").saveAsTable("police_data.crime_gold_outcomes")

# COMMAND ----------

# MAGIC %md
# MAGIC #SQL alternative (CREATE TABLE AS)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT borough, count(*) AS number_of_crimes
# MAGIC FROM police_data.crime_silver
# MAGIC WHERE borough IN ('Bedford', 'Luton', 'Central Bedfordshire')
# MAGIC GROUP BY borough
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT crime, latest_outcome, count(*) AS number_of_crimes
# MAGIC FROM police_data.crime_silver
# MAGIC GROUP BY crime, latest_outcome
# MAGIC ORDER BY 3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC #Visualisation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM police_data.crime_gold_borough

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM police_data.crime_gold_outcomes
