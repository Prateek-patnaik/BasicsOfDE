from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "postgresql-42.2.12.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "(SELECT * FROM cities) as temp") \
    .option("user", "postgres") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()