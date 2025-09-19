# PySpark 
# create spark session
from pyspark.sql import SparkSession as ss
spark=ss.builder.appName("pyspark practice").getOrCreate()
spark
# create DataFrame 
data=[(101,"arjun","chennai"),(102,"sachon","hyderabad"),(103,"vamsi","bangalore"),(None,"rohit","nellore")] 
cols=["id","name","city"]
dataframe=spark.createDataFrame(data,cols)
dataframe.show()
OutPut:
+----+------+---------+
|  id|  name|     city|
+----+------+---------+
| 101| arjun|  chennai|
| 102|sachon|hyderabad|
| 103| vamsi|bangalore|
|null| rohit|  nellore|

from pyspark.sql import SparkSession as ss
spark=ss.builder.appName("read external file").getOrCreate()
spark 

# How to READ CSV File using PySpark
df=spark.read.option('header','true').option("inferSchema","true").csv('/Volumes/workspace/default/filed/Explore_workspace_default_sales_data_first_2025_07_15_16_27_01.csv')
df.show()

# How to Check column data types
df.printSchema()
Example:
|-- Branch_ID: string (nullable = true)
 |-- Dealer_ID: string (nullable = true)
 |-- Model_ID: string (nullable = true)
 |-- Revenue: integer (nullable = true)
 |-- Units_Sold: integer (nullable = true)
 |-- Date_ID: string (nullable = true)
 |-- Month: integer (nullable = true)
 |-- Year: integer (nullable = true)
 |-- BranchName: string (nullable = true)
 |-- DealerName: string (nullable = true)
 |-- Product_Name: string (nullable = true)
 |-- Date: string (nullable = true)
# groupBy 
df.groupBy("Product_Name").sum("Units_Sold").show()
# filter 
df.filter(df.Product_Name=='Jeep').show()
# How to change col datatype
#using cil,cast 
from pyspark.sql.functions import col,cast
#|-- Branch_ID: string (nullable = true)
df.withColumn("Branch_Id",col("Branch_Id").cast('int')) # branch_id is str,it is changed to int
# Create new DF with existing DF using SELECT Method..
df2=df.select("Branch_ID","BranchName","DealerName","Product_Name","Revenue","Units_Sold","Date") # here select few col's from existing DF
df2.show()
# How to create new file using existing DF with OVERWRITE Method
df2.write.mode("overwrite").option("header","true").csv('/Volumes/workspace/default/filed/overwrite_files/car_sales_data.csv')
# Joins IN Pyspark
#Inner Join
df2.join(df3,df2.Product_Name==df3.Product_Name,"inner").show(3)
#Left Outer Join
df2.join(df3,df2.Product_Name==df3.Product_Name,"leftouter").show(3)
#Right Outer Join
df2.join(df3,df2.Product_Name==df3.Product_Name,"rightouter").show(3)
#Full Outer Join
df2.join(df3,df2.Product_Name==df3.Product_Name,"fullouter").show(3)
# create VIEWS
df.createOrReplaceTempView("sales_view") # Temp View created..
spark.sql( " select * from sales_view   ")# sales_view table name
df.createOrReplaceGlobalTempView("global_view") # Global temp view
spark.sql("SELECT * FROM global_temp.global_view").show() # global_view is table name and global_temp referenced keyword


