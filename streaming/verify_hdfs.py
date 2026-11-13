from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Connexion à Spark
spark = (
    SparkSession.builder
    .appName("FireWatch-HDFS-Verification")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Lire les fichiers Parquet depuis HDFS
df = spark.read.parquet("hdfs://namenode:9000/raw/detections")

# Afficher le nombre total d'événements
total = df.count()

print("\n========== VERIFICATION FIREWATCH ==========")
print(f"Nombre total d'evenements archives : {total}")

# Compter les événements par année et par pays
print("\nNombre d'evenements par annee et pays :")

df.groupBy("year", "country") \
  .count() \
  .orderBy("year", "country") \
  .show(30, truncate=False)

# Afficher la structure des données
print("\nStructure des donnees :")
df.printSchema()

print("============================================")

spark.stop()
