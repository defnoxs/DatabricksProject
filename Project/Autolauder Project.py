# Databricks notebook source
# MAGIC %md
# MAGIC #May Data followed by Autoloaded June Data

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

  bronze_crime = (spark.readStream \
                        .format('cloudFiles')
                        .option('cloudFiles.format', 'csv')
                        .schema(streets_schema)
                        .option('Header',True)
                        .load('/mnt/project/bronze/demo/raw_files/'))

# COMMAND ----------

from pyspark.sql.functions import *
display(bronze_crime.select(count('*')))

# COMMAND ----------

bronze_crime.writeStream.format('delta') \
    .option('checkpointLocation', '/mnt/project/bronze/demo/checkpoint') \
    .outputMode('append') \
    .start('/mnt/project/bronze/demo/streaming_results')


# COMMAND ----------

# MAGIC %md
# MAGIC #Bronze to Silver after Autoloading update
# MAGIC ##Once file AutoLoaded -> run all commands from this tab down to update Silver->Gold

# COMMAND ----------

crimes_updated = spark.read.format('delta').load('/mnt/project/bronze/demo/streaming_results')

# COMMAND ----------

from pyspark.sql.functions import *
crimes_df = crimes_updated.select('Crime_ID', 'Falls_within', 'Location', 'LSOA_name', 'Crime_type', 'Last_outcome_category', 'Month') \
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

final_df = crimes_df.withColumn("borough",expr("substring(borough, 1, length(borough)-5)")) \
.withColumn("location",expr("substring(location, 11, length(location)-1)")) \
.drop('Month').where("latest_outcome != 'null'")

# COMMAND ----------


display(final_df)

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").option("path","/mnt/project/silver/streaming/crime_silver").saveAsTable("police_data.autoload_crime_silver")

# COMMAND ----------

display(final_df.select(count('*')))

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver to Gold

# COMMAND ----------

crime_df = spark.read.format("delta").load("/mnt/project/silver/streaming/crime_silver")

# COMMAND ----------

crime_borough = crime_df.select('borough').groupBy("borough").count().filter((crime_df.borough).isin(['Bedford', 'Luton', 'Central Bedfordshire'])).orderBy(desc("count"))

# COMMAND ----------

display(crime_borough)

# COMMAND ----------

crime_borough.write.format("delta").mode("overwrite").option("path","/mnt/project/gold/streaming/crime_gold_borough").saveAsTable("police_data.autoload_gold_borough")

# COMMAND ----------

outcomes_per_crime= crime_df.select('crime', 'latest_outcome').groupBy("crime", "latest_outcome").count().orderBy(desc("count"))

# COMMAND ----------

display(outcomes_per_crime)

# COMMAND ----------

outcomes_per_crime.write.format("delta").mode("overwrite").option("path","/mnt/project/gold/streaming/crime_gold_outcomes").saveAsTable("police_data.autoload_gold_outcomes")

# COMMAND ----------

# MAGIC %md
# MAGIC #Visualisation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM police_data.autoload_gold_borough
