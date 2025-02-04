from pyspark.sql import SparkSession

# Création de la session Spark avec MongoDB
spark = SparkSession.builder \
    .appName("MongoDB_Spark") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1")\
    .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017/github_issues") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/github_issues") \
    .config("spark.executor.memory", "1G") \
    .config("spark.executor.cores", "1") \
    .config("spark.cores.max", "4") \
    .config("spark.driver.memory", "2G") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Lire les données depuis MongoDB
df = spark.read.format("mongodb") \
    .option("database", "github_issues") \
    .option("collection", "issues") \
    .load()

df.show()
