from pyspark.sql import SparkSession

# Création de la session Spark avec connexion au Master du cluster
spark = SparkSession.builder \
    .appName("MongoDB_Spark") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017/github_issues") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/github_issues") \
    .config("spark.executor.memory", "1G") \
    .config("spark.executor.cores", "1") \
    .config("spark.cores.max", "4") \
    .config("spark.driver.memory", "2G") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Vérifier si la connexion au cluster est bien établie
print("🚀 Spark connecté au master:", spark.sparkContext.master)
print("🖥️ Nombre de workers disponibles:", spark.sparkContext.defaultParallelism)

try:
    # Lire les données depuis MongoDB
    df = spark.read.format("mongodb") \
        .option("database", "github_issues") \
        .option("collection", "issues") \
        .load()

    # Afficher les données
    df.show()

finally:
    # Arrêter la session Spark
    spark.stop()
    print("✅ Spark session stopped.")
