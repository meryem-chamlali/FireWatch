import argparse
import csv
import hashlib
import json
import logging
import math
import re
import time

from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data"

DEFAULT_TOPIC = "firms.detections"
DEFAULT_BOOTSTRAP = "localhost:9092"

COUNTRIES = ("spain", "portugal")
EXPECTED_YEARS = set(range(2019, 2026))

# Colonnes attendues dans les fichiers NASA FIRMS
REQUIRED_FIELDS = {
    "latitude", "longitude", "bright_ti4", "scan", "track",
    "acq_date", "acq_time", "satellite", "instrument",
    "confidence", "version", "bright_ti5", "frp",
    "daynight", "type"
}

NUMERIC_FIELDS = {
    "latitude", "longitude", "bright_ti4", "scan",
    "track", "bright_ti5", "frp"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("FireWatchProducer")


# ============================================================
# 1. CONVERSION ET NETTOYAGE DES DONNEES
# ============================================================

def convert_value(field, value):
    """Convertit les valeurs numériques et traite les valeurs manquantes."""

    if value is None:
        return None

    value = value.strip()

    if value == "" or value.lower() in {
        "nan", "none", "null", "na"
    }:
        return None

    if field in NUMERIC_FIELDS:
        number = float(value)

        if not math.isfinite(number):
            return None

        return number

    if field == "type":
        return int(value)

    return value


# ============================================================
# 2. VALIDATION DES COORDONNEES
# ============================================================

def validate_coordinates(data):
    """Vérifie la validité des coordonnées géographiques."""

    latitude = data.get("latitude")
    longitude = data.get("longitude")

    if latitude is None or longitude is None:
        raise ValueError("Coordonnées géographiques manquantes")

    if not -90 <= latitude <= 90:
        raise ValueError("Latitude invalide")

    if not -180 <= longitude <= 180:
        raise ValueError("Longitude invalide")


# ============================================================
# 3. CONSTRUCTION D'UN EVENEMENT
# ============================================================

def build_event(row, country, year):
    """Nettoie une ligne CSV et construit un événement JSON."""

    data = {
        key.strip().lower(): convert_value(
            key.strip().lower(), value
        )
        for key, value in row.items()
        if key is not None
    }

    validate_coordinates(data)

    # Préparation de la date et de l'heure d'acquisition
    event_timestamp = None

    if data.get("acq_date") and data.get("acq_time") is not None:

        time_value = str(data["acq_time"]).split(".")[0].zfill(4)

        acquisition = datetime.strptime(
            f"{data['acq_date']} {time_value}",
            "%Y-%m-%d %H%M"
        )

        event_timestamp = acquisition.isoformat()

    # Ajout des métadonnées
    data["country"] = country
    data["year"] = year

    # Identifiant stable pour reconnaître une même détection
    raw_identity = json.dumps(
        data,
        sort_keys=True,
        ensure_ascii=False
    )

    event_id = hashlib.sha256(
        raw_identity.encode("utf-8")
    ).hexdigest()

    return {
        "event_id": event_id,
        "source": "NASA_FIRMS_VIIRS",
        "event_timestamp": event_timestamp,
        **data
    }


# ============================================================
# 4. IDENTIFICATION AUTOMATIQUE DU PAYS ET DE L'ANNEE
# ============================================================

def get_country_and_year(file_path):
    """Déduit automatiquement le pays et l'année du fichier."""

    country = file_path.parent.name.lower()

    if country not in COUNTRIES:
        raise ValueError(
            f"Dossier pays non reconnu : {file_path.parent.name}"
        )

    pattern = r"viirs-jpss1_(20\d{2})_(Spain|Portugal)\.csv"

    match = re.fullmatch(pattern, file_path.name, re.IGNORECASE)

    if not match:
        raise ValueError(
            f"Nom de fichier non reconnu : {file_path.name}"
        )

    year = int(match.group(1))
    filename_country = match.group(2).lower()

    if filename_country != country:
        raise ValueError(
            f"Le pays du fichier ne correspond pas à son dossier : "
            f"{file_path.name}"
        )

    if year not in EXPECTED_YEARS:
        raise ValueError(f"Année non autorisée : {year}")

    return country, year


# ============================================================
# 5. RECHERCHE AUTOMATIQUE DES 14 FICHIERS CSV
# ============================================================

def find_csv_files():
    """Recherche les 7 fichiers de chaque pays."""

    files = []

    for country in COUNTRIES:

        country_dir = DATA_DIR / country

        if not country_dir.exists():
            raise FileNotFoundError(
                f"Dossier introuvable : {country_dir}"
            )

        country_files = sorted(
            country_dir.glob("viirs-jpss1_*.csv")
        )

        if len(country_files) != 7:
            raise ValueError(
                f"Le dossier {country} doit contenir 7 fichiers. "
                f"Nombre trouvé : {len(country_files)}"
            )

        found_years = set()

        for file_path in country_files:
            _, year = get_country_and_year(file_path)
            found_years.add(year)

        if found_years != EXPECTED_YEARS:
            raise ValueError(
                f"Années incorrectes dans {country} : "
                f"{sorted(found_years)}"
            )

        files.extend(country_files)

    if len(files) != 14:
        raise ValueError(
            f"14 fichiers attendus, {len(files)} trouvés."
        )

    return files


# ============================================================
# 6. CALLBACK DE CONFIRMATION DE LIVRAISON
# ============================================================

def delivery_report(error, message):
    """Confirme si Kafka a reçu le message."""

    if error:
        delivery_report.failed += 1

        logger.error(
            "Échec de livraison : %s",
            error
        )

    else:
        delivery_report.delivered += 1


delivery_report.delivered = 0
delivery_report.failed = 0


# ============================================================
# 7. TRAITEMENT D'UN FICHIER CSV
# ============================================================

def process_file(
    file_path,
    producer,
    topic,
    interval,
    limit_per_file
):
    """Lit un fichier et envoie ses événements à Kafka."""

    country, year = get_country_and_year(file_path)

    processed = 0
    rejected = 0

    logger.info("--------------------------------------")
    logger.info("Fichier : %s", file_path.name)
    logger.info("Pays : %s | Année : %s", country, year)

    with file_path.open(
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.DictReader(file)

        if not reader.fieldnames:
            raise ValueError("Fichier CSV sans en-têtes")

        reader.fieldnames = [
            name.strip().lower()
            for name in reader.fieldnames
        ]

        missing = REQUIRED_FIELDS - set(reader.fieldnames)

        if missing:
            raise ValueError(
                f"Colonnes manquantes dans {file_path.name} : "
                f"{sorted(missing)}"
            )

        logger.info("Structure CSV valide.")

        for row_number, row in enumerate(reader, start=2):

            if not row or None in row:
                rejected += 1
                logger.warning(
                    "Ligne %s rejetée : structure incorrecte",
                    row_number
                )
                continue

            try:
                event = build_event(row, country, year)

                message = json.dumps(
                    event,
                    ensure_ascii=False,
                    allow_nan=False
                )

                # Envoi vers Kafka
                producer.produce(
                    topic=topic,
                    key=event["event_id"],
                    value=message,
                    callback=delivery_report
                )

                # Déclenche les callbacks disponibles
                producer.poll(0)

                processed += 1

                logger.info(
                    "%s : détection préparée %s",
                    file_path.name,
                    processed
                )

                if interval > 0:
                    time.sleep(interval)

                if (
                    limit_per_file > 0
                    and processed >= limit_per_file
                ):
                    break

            except BufferError:
                # Attend de libérer de la place dans la file Kafka
                producer.poll(1)

                try:
                    producer.produce(
                        topic=topic,
                        key=event["event_id"],
                        value=message,
                        callback=delivery_report
                    )
                    processed += 1

                except Exception as error:
                    rejected += 1
                    logger.error(
                        "Erreur d'envoi ligne %s : %s",
                        row_number,
                        error
                    )

            except (ValueError, TypeError, OverflowError) as error:
                rejected += 1

                logger.warning(
                    "Ligne %s rejetée dans %s : %s",
                    row_number,
                    file_path.name,
                    error
                )

    logger.info(
        "Fichier terminé : %s | Préparés : %s | Rejetés : %s",
        file_path.name,
        processed,
        rejected
    )

    return processed, rejected


# ============================================================
# 8. PRODUCTEUR PRINCIPAL
# ============================================================

def run_producer(
    bootstrap,
    topic,
    interval,
    limit_per_file
):

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "firewatch-producer",
        "acks": "all",
        "enable.idempotence": True
    })

    total_processed = 0
    total_rejected = 0

    delivery_report.delivered = 0
    delivery_report.failed = 0

    try:

        # Recherche automatique des 14 fichiers
        files = find_csv_files()

        logger.info("======================================")
        logger.info("DEMARRAGE DE L'INGESTION FIREWATCH")
        logger.info("Fichiers trouvés : %s", len(files))
        logger.info("Topic Kafka : %s", topic)
        logger.info("Serveur Kafka : %s", bootstrap)
        logger.info("======================================")

        # Traitement de chaque fichier
        for file_path in files:

            processed, rejected = process_file(
                file_path,
                producer,
                topic,
                interval,
                limit_per_file
            )

            total_processed += processed
            total_rejected += rejected

        logger.info("Attente de la livraison des messages...")

        remaining = producer.flush(60)

        if remaining:
            logger.warning(
                "%s message(s) encore en attente.",
                remaining
            )

    finally:
        # Ferme proprement le producteur
        producer.flush(10)

    # Bilan final
    logger.info("")
    logger.info("======================================")
    logger.info("BILAN FINAL DE L'INGESTION")
    logger.info("======================================")
    logger.info("Fichiers traités : %s", len(files))
    logger.info("Messages préparés : %s", total_processed)
    logger.info("Messages livrés : %s", delivery_report.delivered)
    logger.info("Lignes rejetées : %s", total_rejected)
    logger.info("Échecs de livraison : %s", delivery_report.failed)
    logger.info("======================================")


# ============================================================
# 9. EXECUTION DU PROGRAMME
# ============================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Producteur automatique NASA FIRMS vers Kafka - FireWatch"
        )
    )

    parser.add_argument(
        "--bootstrap",
        default=DEFAULT_BOOTSTRAP,
        help="Adresse du serveur Kafka"
    )

    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Nom du topic Kafka"
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=0,
        help="Délai entre deux messages en secondes"
    )

    parser.add_argument(
        "--limit-per-file",
        type=int,
        default=0,
        help="Nombre maximal de lignes par fichier (0 = toutes)"
    )

    args = parser.parse_args()

    if args.interval < 0:
        parser.error("--interval doit être positif ou nul")

    if args.limit_per_file < 0:
        parser.error("--limit-per-file doit être positif ou nul")

    run_producer(
        bootstrap=args.bootstrap,
        topic=args.topic,
        interval=args.interval,
        limit_per_file=args.limit_per_file
    )
