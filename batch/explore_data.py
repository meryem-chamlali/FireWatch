from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when

# ==================================================
# 1. INITIALISATION DE SPARK
# ==================================================

spark = (
    SparkSession.builder
    .appName("FireWatch-Batch-Exploration")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

input_path = "hdfs://namenode:9000/raw/detections"

try:

    # ==================================================
    # 2. LECTURE DES DONNEES DEPUIS HDFS
    # ==================================================

    df = spark.read.parquet(input_path).cache()

    print("\n========== EXPLORATION FIREWATCH ==========")

    # ==================================================
    # 3. NOMBRE TOTAL D'EVENEMENTS
    # ==================================================

    total = df.count()

    print(f"\nNombre total d'evenements : {total}")

    # ==================================================
    # 4. AFFICHAGE DES EXEMPLES
    # ==================================================

    print("\nExemples de donnees :")
    df.show(10, truncate=False)

    # ==================================================
    # 5. STRUCTURE DES DONNEES
    # ==================================================

    print("\nStructure des donnees :")
    df.printSchema()

    # ==================================================
    # 6. STATISTIQUES DESCRIPTIVES
    # ==================================================

    print("\nStatistiques descriptives :")

    df.select(
        "latitude",
        "longitude",
        "bright_ti4",
        "bright_ti5",
        "frp"
    ).describe().show()

    # ==================================================
    # 7. VERIFICATION DES VALEURS MANQUANTES
    # ==================================================

    print("\nValeurs manquantes :")

    missing = df.select([
        spark_sum(
            when(col(c).isNull(), 1).otherwise(0)
        ).alias(c)
        for c in df.columns
    ])

    missing.show(truncate=False)

    # ==================================================
    # 8. EVENEMENTS PAR PAYS
    # ==================================================

    print("\nEvenements par pays :")

    df.groupBy("country") \
        .count() \
        .orderBy("country") \
        .show()

    # ==================================================
    # 9. EVENEMENTS PAR ANNEE ET PAR PAYS
    # ==================================================

    print("\nEvenements par annee et pays :")

    df.groupBy("year", "country") \
        .count() \
        .orderBy("year", "country") \
        .show(30, truncate=False)

    # ==================================================
    # 10. VERIFICATION DES IDENTIFIANTS ET DES DOUBLONS
    # ==================================================

    print("\nVerification des identifiants nuls :")

    null_ids = df.filter(
        col("event_id").isNull()
    ).count()

    print(f"Nombre d'identifiants nuls : {null_ids}")

    print("\nVerification des doublons par event_id :")

    duplicates = (
    df.filter(col("event_id").isNotNull())
    .groupBy("event_id")
    .count()
    .filter(col("count") > 1)
)

    # Nombre d'identifiants apparaissant plusieurs fois
    duplicate_ids = duplicates.count()

    print(
        f"Nombre d'identifiants dupliques : {duplicate_ids}"
    )

    if duplicate_ids > 0:

        print("\nExemples de doublons :")
        duplicates.show(10, truncate=False)

        # Nombre de lignes dupliquees en surplus
        duplicate_rows = (
            duplicates
            .agg(
                spark_sum(col("count") - 1)
                .alias("duplicate_rows")
            )
            .first()["duplicate_rows"]
        )

        print(
            "Nombre total de lignes dupliquees en surplus :",
            duplicate_rows
        )

    else:
        print("Aucun doublon detecte.")

    # Frequence des doublons
    print("\nFrequence des doublons :")

    duplicates.groupBy("count") \
        .count() \
        .orderBy("count") \
        .show()

    # ==================================================
    # 11. VERIFICATION DES COORDONNEES ET DU FRP
    # ==================================================

    print("\nVerification des donnees invalides :")

    invalid = df.filter(
        col("latitude").isNull() |
        col("longitude").isNull() |
        col("frp").isNull() |
        (col("latitude") < -90) |
        (col("latitude") > 90) |
        (col("longitude") < -180) |
        (col("longitude") > 180) |
        (col("frp") < 0)
    )

    invalid_count = invalid.count()

    print(
        "Nombre de lignes avec des coordonnees "
        "ou valeurs FRP invalides :",
        invalid_count
    )

    if invalid_count > 0:
        print("\nExemples de donnees invalides :")
        invalid.show(10, truncate=False)

    # ==================================================
    # 12. VERIFICATION DES ANNEES ET DES PAYS
    # ==================================================

    print("\nVerification des annees et des pays :")

    unexpected = df.filter(
        (~col("year").between(2019, 2025)) |
        (~col("country").isin("spain", "portugal")) |
        col("year").isNull() |
        col("country").isNull()
    )

    unexpected_count = unexpected.count()

    print(
        "Nombre de lignes avec une annee "
        "ou un pays inattendu :",
        unexpected_count
    )

    if unexpected_count > 0:
        unexpected.show(10, truncate=False)

    # ==================================================
    # 13. FIN DE L'EXPLORATION
    # ==================================================

    print("\n==========================================")
    print("Exploration des donnees terminee !")
    print("Aucune donnee n'a ete supprimee de HDFS Raw.")

finally:

    # ==================================================
    # 14. LIBERATION DES RESSOURCES
    # ==================================================

    if "df" in locals():
        df.unpersist()

    spark.stop()