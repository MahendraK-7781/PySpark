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



