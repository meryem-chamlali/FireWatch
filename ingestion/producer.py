
import argparse
import csv
import hashlib
import json
import logging
import time
from datetime import datetime

from confluent_kafka import Producer


# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Topic Kafka
TOPIC_NAME = "firms.detections"

# Colonnes NASA FIRMS
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


def convert_value(field, value):
    """Convertit les valeurs numériques."""

    if value is None or value.strip() == "":
        return None

    value = value.strip()

    if field in NUMERIC_FIELDS:
        return float(value)

    if field == "type":
        return int(value)

    return value


def build_event(row, country):
    """Nettoie une ligne et construit un événement JSON."""

    data = {
        key.strip().lower(): convert_value(
            key.strip().lower(), value
        )
        for key, value in row.items()
        if key is not None
    }

    # Vérification des coordonnées
    latitude = data.get("latitude")
    longitude = data.get("longitude")

    if latitude is None or longitude is None:
        raise ValueError("Coordonnées manquantes")

    if not -90 <= latitude <= 90:
        raise ValueError("Latitude invalide")

    if not -180 <= longitude <= 180:
        raise ValueError("Longitude invalide")

    # Préparation de la date et de l'heure
    event_timestamp = None

    if data.get("acq_date") and data.get("acq_time") is not None:
        time_value = str(data["acq_time"]).split(".")[0].zfill(4)

        acquisition = datetime.strptime(
            f"{data['acq_date']} {time_value}",
            "%Y-%m-%d %H%M"
        )

        event_timestamp = acquisition.isoformat()

    # Ajout du pays
    data["country"] = country.lower()

    # Création d'un identifiant stable
    raw_identity = json.dumps(
        data, sort_keys=True, ensure_ascii=False
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


def run_producer(csv_path, country, bootstrap, interval, max_rows):

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "firewatch-producer"
    })

    delivered = 0
    failed = 0
    rejected = 0

    def delivery_report(error, message):
        nonlocal delivered, failed

        if error:
            failed += 1
            logging.error("Échec d'envoi : %s", error)
        else:
            delivered += 1

    logging.info("Connexion à Kafka : %s", bootstrap)
    logging.info("Topic : %s", TOPIC_NAME)
    logging.info("Pays : %s", country)

    try:
        with open(
            csv_path, "r", encoding="utf-8-sig", newline=""
        ) as file:

            reader = csv.DictReader(file)

            # Normalisation des en-têtes
            if not reader.fieldnames:
                raise ValueError("CSV sans en-têtes")

            reader.fieldnames = [
                name.strip().lower()
                for name in reader.fieldnames
            ]

            missing = REQUIRED_FIELDS - set(reader.fieldnames)

            if missing:
                raise ValueError(
                    f"Colonnes manquantes : {missing}"
                )

            logging.info("Fichier CSV valide.")

            processed = 0

            for row in reader:

                if not row or None in row:
                    rejected += 1
                    continue

                try:
                    event = build_event(row, country)

                    message = json.dumps(
                        event, ensure_ascii=False
                    ).encode("utf-8")

                    producer.produce(
                        topic=TOPIC_NAME,
                        key=event["event_id"],
                        value=message,
                        callback=delivery_report
                    )

                    producer.poll(0)
                    processed += 1

                    logging.info(
                        "Détection préparée : %s",
                        processed
                    )

                    if interval > 0:
                        time.sleep(interval)

                    if max_rows > 0 and processed >= max_rows:
                        break

                except (ValueError, TypeError) as error:
                    rejected += 1
                    logging.warning(
                        "Ligne rejetée : %s", error
                    )

    finally:
        logging.info("Attente de la livraison des messages...")
        remaining = producer.flush(30)

        if remaining:
            logging.warning(
                "%s message(s) encore en attente.", remaining
            )

    logging.info("========== BILAN ==========")
    logging.info("Messages livrés : %s", delivered)
    logging.info("Messages rejetés : %s", rejected)
    logging.info("Échecs de livraison : %s", failed)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Producteur NASA FIRMS vers Kafka - FireWatch"
    )

    parser.add_argument("--csv", required=True)
    parser.add_argument(
        "--country",
        required=True,
        choices=["spain", "portugal"]
    )
    parser.add_argument(
        "--bootstrap",
        default="localhost:9092"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0
    )

    args = parser.parse_args()

    run_producer(
        csv_path=args.csv,
        country=args.country,
        bootstrap=args.bootstrap,
        interval=args.interval,
        max_rows=args.max_rows
    )
