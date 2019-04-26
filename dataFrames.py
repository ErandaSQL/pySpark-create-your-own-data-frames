
#suppose you want to create dataframs from scratch
#First, i'm creating a list to store list of values
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

#if you want to show language and useDescr
#first, let's try data frame method to join two tables 
#the syntax is <<new data frame>>=<<data frame>>.join(<<data frame,<<common field>>,<<join type>>)
#based on the join type(especially the left join) positioning of data frames are important
languageUseDF = languageDF.join(useDF, languageDF.use == useDF.use,'inner')
languageUseDF.select('language','useDescr').show()
#In data bricks, if you dont see results, you can scroll down in the results frame to see all results

#SQL method
spark.sql('select language,useDescr from languageDF LD inner join useDF UD on LD.use=UD.use').show()
# but your code doesn't work, you will get an error. why is that? 
# the reson is, for spark SQL to access dataframes, dataframes should be in catalog first
# when you do this you can't see languageDF
print(spark.catalog.listTables())
#let's add data frame to catalog
languageDF.createOrReplaceTempView('languageDF')
#now you can see the table
print(spark.catalog.listTables())
#mind you, this is just a view of the existing table

#do the same for useDF
useDF.createOrReplaceTempView('useDF')

#now SQL should work
spark.sql('select language,useDescr from languageDF LD inner join useDF UD on LD.use=UD.use').show()


#incase you want to delete any DF from catalog
#spark.catalog.dropTempView('languageDF')
#spark.catalog.dropTempView('useDF')

#suppose you want to share the same data frame in multiple sessions\ multiple notebooks, then you have to create a global table or a view
languageDF.createOrReplaceGlobalTempView('languageDF')
useDF.createOrReplaceGlobalTempView('useDF')

#to test this, open a new notebook from pySpark by going to databricks home https://community.cloud.databricks.com
# then chose notebook and enter below code to refer the data frame 
spark.sql('SELECT * FROM global_temp.languageDF').show()
