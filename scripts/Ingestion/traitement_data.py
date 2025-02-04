from pyspark.sql import SparkSession

# Ajouter explicitement le connecteur MongoDB pour Spark
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
    .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/github_issues.issues") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/github_issues.issues") \
    .getOrCreate()

print(spark.version)  # Vérifier que Spark fonctionne bien
# Test : Lire les données MongoDB
df = spark.read.format("mongodb").load()
df.show(5)  # Affiche les 5 premières lignes
