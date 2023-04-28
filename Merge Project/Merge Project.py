# Databricks notebook source
# MAGIC %md
# MAGIC #January Data (source)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/mnt/bronze/2023-01'

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType, StringType, StructField, StructType, DateType
streets_schema = StructType([
    StructField('Crime ID', StringType(), False), 
    StructField('Month', DateType(), True), 
    StructField('Reported by', StringType(), True), 
    StructField('Falls within', StringType(), True), 
    StructField('Longitude', DoubleType(), True), 
    StructField('Latitude', DoubleType(), True), 
    StructField('Location', StringType(), True), 
    StructField('LSOA code', StringType(), True), 
    StructField('LSOA name', StringType(), True), 
    StructField('Crime type', StringType(), True), 
    StructField('Last outcome category', StringType(), True), 
    StructField('Context', StringType(), True)])

# COMMAND ----------

streets_df = spark.read.csv('/mnt/bronze/2023-01/2023-01-bedfordshire-street.csv', header=True, schema=streets_schema)
#Added header & schema as the datatypes came back as strings. inferSchema is not efficient. 

# COMMAND ----------

display(streets_df)

# COMMAND ----------

from pyspark.sql.functions import *
streets_df = streets_df.select('Crime ID', 'Falls within', 'Location', 'LSOA name', 'Crime type', 'Last outcome category', 'Month') \
.withColumnRenamed('Crime ID', 'crime_id') \
.withColumnRenamed('Falls within', 'force_area') \
.withColumnRenamed('Location', 'location') \
.withColumnRenamed('LSOA name', 'borough') \
.withColumnRenamed('Crime type', 'crime') \
.withColumnRenamed('Last outcome category', 'latest_outcome') \
.withColumn('offence_date', date_format('Month','MM-yyyy')) \
.withColumn('timestamp', current_timestamp()) \
.withColumn("counter", monotonically_increasing_id())


# COMMAND ----------

streets_df2 = streets_df.withColumn("borough",expr("substring(borough, 1, length(borough)-5)")) \
.withColumn("location",expr("substring(location, 11, length(location)-1)")) \
.drop('Month')

# COMMAND ----------

from pyspark.sql import Window
w = Window.orderBy("counter")
# Use row number with the window specification
final_df = streets_df2.withColumn("index", row_number().over(w)).drop('counter').where("latest_outcome != 'null'")

# COMMAND ----------


display(final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS police_data

# COMMAND ----------

final_df.write.format("delta").option("path","/mnt/silver/crime_silver").saveAsTable("police_data.crime_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC #February data (update file)

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType, StringType, StructField, StructType, DateType
streets_schema = StructType([
    StructField('Crime ID', StringType(), False), 
    StructField('Month', DateType(), True), 
    StructField('Reported by', StringType(), True), 
    StructField('Falls within', StringType(), True), 
    StructField('Longitude', DoubleType(), True), 
    StructField('Latitude', DoubleType(), True), 
    StructField('Location', StringType(), True), 
    StructField('LSOA code', StringType(), True), 
    StructField('LSOA name', StringType(), True), 
    StructField('Crime type', StringType(), True), 
    StructField('Last outcome category', StringType(), True), 
    StructField('Context', StringType(), True)])

# COMMAND ----------

streets_df = spark.read.csv('/mnt/bronze/2023-02/2023-02-bedfordshire-street.csv', header=True, schema=streets_schema)

# COMMAND ----------

from pyspark.sql.functions import *
streets_df = streets_df.select('Crime ID', 'Falls within', 'Location', 'LSOA name', 'Crime type', 'Last outcome category', 'Month') \
.withColumnRenamed('Crime ID', 'crime_id') \
.withColumnRenamed('Falls within', 'force_area') \
.withColumnRenamed('Location', 'location') \
.withColumnRenamed('LSOA name', 'borough') \
.withColumnRenamed('Crime type', 'crime') \
.withColumnRenamed('Last outcome category', 'latest_outcome') \
.withColumn('offence_date', date_format('Month','MM-yyyy')) \
.withColumn('timestamp', current_timestamp()) \
.withColumn("counter", monotonically_increasing_id())

# COMMAND ----------

streets_df2 = streets_df.withColumn("borough",expr("substring(borough, 1, length(borough)-5)")) \
.withColumn("location",expr("substring(location, 11, length(location)-1)")) \
.drop('Month')

# COMMAND ----------

from pyspark.sql import Window
w = Window.orderBy("counter")
# Use row number with the window specification
update_df = streets_df2.withColumn("index", row_number().over(w)).drop('counter').where("latest_outcome != 'null'")

# COMMAND ----------

display(update_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Merge

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import *

deltaTable = DeltaTable.forPath(spark, '/mnt/silver/crime_silver')

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
      "index": "src.index"
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
      "index": "src.index"
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

crime_df = spark.read.format("delta").load("/mnt/silver/crime_silver")

# COMMAND ----------

crime_borough = crime_df.select('borough').groupBy("borough").count().filter((crime_df.borough).isin(['Bedford', 'Luton', 'Central Bedfordshire'])).orderBy(desc("count"))

# COMMAND ----------

display(crime_borough)

# COMMAND ----------

crime_borough.write.format("delta").mode("overwrite").option("path","/mnt/gold/crime_gold_borough").saveAsTable("police_data.crime_gold_borough")

# COMMAND ----------



# COMMAND ----------

outcomes_per_crime= crime_df.select('crime', 'latest_outcome').groupBy("crime", "latest_outcome").count().orderBy(desc("count"))

# COMMAND ----------

display(outcomes_per_crime)

# COMMAND ----------

outcomes_per_crime.write.format("delta").mode("overwrite").option("path","/mnt/gold/crime_gold_outcomes").saveAsTable("police_data.crime_gold_outcomes")

# COMMAND ----------

# MAGIC %md
# MAGIC #SQL alternative (CREATE TABLE AS)

# COMMAND ----------

# %sql 
# SELECT borough, count(*) AS number_of_crimes
# FROM police_data.crime_silver
# WHERE borough IN ('Bedford', 'Luton', 'Central Bedfordshire')
# GROUP BY borough
# ORDER BY 2 DESC

# COMMAND ----------

# %sql 
# SELECT crime, latest_outcome, count(*) AS number_of_crimes
# FROM police_data.crime_silver
# GROUP BY crime, latest_outcome
# ORDER BY 3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC #Visualisation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM police_data.crime_gold_borough
