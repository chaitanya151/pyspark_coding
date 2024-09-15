'''Google collab notebook'''
!pip install pyspark py4j

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("App").getOrCreate()
print(f"Spark session created {spark}")
spark.getActiveSession()

'''Struct type use case'''
data=[(1,('A-424','Noida','India')),(2,('M.15','Unnao','India'))]
schema = StructType([
     StructField('AddId', IntegerType(), True),
     StructField('Address', StructType([
         StructField('Add1', StringType(), True),
         StructField('City', StringType(), True),
         StructField('Country', StringType(), True)
         ]))
     ])
dfST=spark.createDataFrame(data,schema)
display(dfST)
dfST.show()

dfST.select("*", dfST.Address.Add1.alias("Add1"), dfST.Address.Add1.alias("city")).show()

'''Map Type use case'''

data1=[(1,{'Laptop':'Apple',"Mobile":"OnePlus","HeadPhones":"boat"}),(2,{'Laptop':'Apple',"Mobile":"OnePlus"})]
from pyspark.sql.types import StructField, StructType, StringType, MapType
schema1 = StructType([
    StructField('EmpId', IntegerType(), True),
    StructField('Items', MapType(StringType(),StringType()),True) # Key, value datatype is defined. Items can have multiple key, value.
])

dfMT=spark.createDataFrame(data1,schema1)
display(dfMT)
dfMT.show(truncate=False)

dfMT.select("*", explode("Items")).show()
