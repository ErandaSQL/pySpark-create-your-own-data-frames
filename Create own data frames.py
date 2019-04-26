# Databricks notebook source
#suppose you want to create dataframs from scratch
#First i'm creating a list to store list of values
#Lists and arrays are similar with few differences, therefore I will use lists

#First set of values,[] marks the start and the end and () seperate value groups
languageList = [(1,'python','DS'),(2,'R','ST'),(3,'sas','ST'),(4,'scala','BD')]

#traverse through the list and print values
for item in languageList:
  print(item)

#below im creating a dataframe, with values and column names
languageDF = spark.createDataFrame(languageList,['id','language','use'])
languageDF.show()

#Suppose we have another table to store 'use' values

#below im creating a dataframe, with values and column names
useValues = [(1,'DS','Data Science'),(2,'ST','Statistics'),(3,'BD','Big Data')]
useDF = spark.createDataFrame(useValues,['id','use','useDescr'])
useDF.show()

#you want to show language and useDescr
#first, let's try data frame method to join two tables 
#the syntax is <<new data frame>>=<<data frame>>.join(<<data frame,<<common field>>,<<join type>>)
#based on the join type(especially the left join) positioning of data frames are important
languageUseDF = languageDF.join(useDF, languageDF.use == useDF.use,'inner')
languageUseDF.select('language','useDescr').show()
#if you dont see results, you can scroll down in the results frame to see all results

#SQL method
#spark.sql('select language,useDescr from languageDF LD inner join useDF UD on LD.use=UD.use').show()
# but your code doesn't work, you will get an error. why is that? 
# the reson is, for spark SQL to access dataframes, dataframes should be in catalog first
#print(spark.catalog.listTables())

languageDF.createOrReplaceTempView('languageDF')

print(spark.catalog.listTables())
#languageDF.createView('languageDF')
#useDF.createOrReplaceTempView('useDF')
#spark.sql('select language,useDescr from languageDF LD inner join useDF UD on LD.use=UD.use').show()

#incase you want to delete any DF from catalog
#spark.catalog.dropTempView('languageDF')
#spark.catalog.dropTempView('useDF')


# COMMAND ----------


languageDF.createOrReplaceTempView('languageDF')
useDF.createOrReplaceGlobalTempView('useDF')

print(spark.catalog.listTables)
#languageDF.createView('languageDF')
#useDF.createOrReplaceTempView('useDF')
#spark.sql('select language,useDescr from languageDF LD inner join useDF UD on LD.use=UD.use').show()

#incase you want to delete any DF from catalog
#spark.catalog.dropTempView('languageDF')
#spark.catalog.dropTempView('useDF')

# COMMAND ----------

spark.session.stop()
print(spark.catalog.listTables)


# COMMAND ----------

# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.getOrCreate()

print(my_spark.catalog.listTables)

my_spark.session.stop()
print(my_spark.catalog.listTables)
