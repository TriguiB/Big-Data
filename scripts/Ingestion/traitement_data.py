from pyspark.sql import SparkSession
from pyspark.sql import Row

# Création de la session Spark avec connexion au Master du cluster
spark = SparkSession.builder \
    .appName("MongoDB_Spark") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017/github_issues") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/github_issues") \
    .config("spark.executor.memory", "1G") \
    .config("spark.executor.cores", "1") \
    .config("spark.cores.max", "4") \
    .config("spark.driver.memory", "2G") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Vérifier la connexion au cluster Spark
print("🚀 Spark connecté au master:", spark.sparkContext.master)
print("🖥️ Nombre de workers disponibles:", spark.sparkContext.defaultParallelism)

try:
    # Créer un DataFrame avec des données de test
    data = [
        Row(issue_id=1, title="Test issue 1", status="open", user="user1"),
        Row(issue_id=2, title="Test issue 2", status="closed", user="user2"),
        Row(issue_id=3, title="Test issue 3", status="open", user="user3")
    ]
    df = spark.createDataFrame(data)

    # Afficher les données à insérer
    df.show()

    # Écrire les données dans la collection MongoDB
    df.write.format("mongodb") \
        .option("database", "github_issues") \
        .option("collection", "issues") \
        .mode("append") \
        .save()

    print("✅ Données insérées avec succès dans MongoDB.")

    # Lire les données depuis MongoDB pour vérifier l'insertion
    df_read = spark.read.format("mongodb") \
        .option("database", "github_issues") \
        .option("collection", "issues") \
        .load()
    
    # Afficher les données lues
    df_read.show()

finally:
    # Arrêter la session Spark
    spark.stop()
    print("✅ Spark session stopped.")
