'''
Used google collab for the execution of the code.
'''
!pip install pyspark py4j

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("sql_problems_in_spark").getOrCreate()

spark.getActiveSession()

data = [('India','SL','India'),('SL','Aus','Aus'),
        ('SA','Eng','Eng'),('Eng','NZ','NZ'),('Aus','India','India')]
schema = "team_1 string, team_2 string, winner string"
df_1 = spark.createDataFrame(data, schema)
df_1.show()

#win_flag
df_win_1 = df_1.withColumn("win_flag", when(col("team_1") == col("winner"), 1).otherwise(0)).withColumnRenamed("team_1","team").drop("team_2")
df_win_2 = df_1.withColumn("win_flag", when(col("team_2") == col("winner"), 1).otherwise(0)).withColumnRenamed("team_2","team").drop("team_1")
df_win_1.show()
df_win_2.show()

#union on dataframes
df_union = df_win_1.unionAll(df_win_2)
df_union.show()

#matches_played
df_matches_played = df_union.groupBy("team").agg(count("win_flag")\
                                                 .alias("No_of_matches_played"), sum("win_flag").alias("no_of_matches_won")\
                                                 ,(count("win_flag")-sum("win_flag")).alias("no_of_matches_lost"))\
                                                 .orderBy(desc("no_of_matches_won"))
df_matches_played.show()
