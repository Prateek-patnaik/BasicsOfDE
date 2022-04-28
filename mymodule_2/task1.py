# import lit as lit
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import flatten,explode
# from pyspark.sql.types import StructType, StructField, StringType,ArrayType
# from pyspark.sql.functions import split,create_map,regexp_replace
# import col
# spark = SparkSession \
#     .builder \
#     .appName("Python Spark SQL basic example") \
#     .config("spark.jars", "postgresql-42.2.12.jar") \
#     .getOrCreate()
#
# df_episode = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/postgres") \
#     .option("dbtable", "(SELECT * FROM episode) as temp") \
#     .option("user", "postgres") \
#     .option("password", "admin") \
#     .option("driver", "org.postgresql.Driver") \
#     .load()
# df_title = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/postgres") \
#     .option("dbtable", "(SELECT * FROM titledb) as temp") \
#     .option("user", "postgres") \
#     .option("password", "admin") \
#     .option("driver", "org.postgresql.Driver") \
#     .load()
#
# schema = StructType([StructField("id",StringType(),True),StructField("primarygenre",ArrayType(StringType()),True)])
#
# df_episode.show()
# # df2=df_episode.select(df_episode.id,explode(df_episode.primarygenre))
# df2=df_episode.select(split(df_episode.primarygenre,",").alias("genrearray"))
# df2=df2.withColumn("separray",df2.genrearray[0])
# df2.show()
# df2.printSchema()
#


from pyspark.sql import SparkSession,types
from pyspark.sql.functions import explode,split
from pyspark.sql import functions as F


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "postgresql-42.2.12.jar") \
    .getOrCreate()

#Read Table 1
df1 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "(SELECT * FROM episode) as temp") \
    .option("user", "postgres") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .load()

#Read table 2
df2 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "(SELECT * FROM titledb) as temp") \
    .option("user", "postgres") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .load()


#split the column using explode (it is a string , so we use split )
#use regex to remove special characters

df3=df1.withColumn("genre_name",explode(split(df1.primarygenre,',')))
df4=df3.withColumn("genre_name", F.regexp_replace(F.regexp_replace("genre_name", "\\{", ""),"\\}", "")).dropna("any")
df4.show(truncate=False)

#Joining Two Tables
df5=df4.join(df2,df2.id==df4.genre_name,"inner").select((df4.id),(df2.title))
print("this is df4")
df5.show(truncate=False,n=100)

#Grouping the rows
df6 = df5.groupBy('id').agg(F.collect_list('title').alias('title'))
df6.show(truncate=False)


# save to postgresql
df6.select("id","title").write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "result_table") \
    .option("user", "postgres").option("password", "admin").save()