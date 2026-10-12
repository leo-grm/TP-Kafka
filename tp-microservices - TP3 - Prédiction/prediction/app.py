import json
import os
import time

import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# pas de flask ici : le service ne fait que lire kafka, prédire, écrire dans kafka

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC_DEMANDES = "demandes-prediction"
TOPIC_PREDICTIONS = "predictions"
GROUP_ID = os.environ.get("GROUP_ID", "prediction-group")

# mêmes colonnes et même ordre que dans entrainer.py
COLONNES = ["age", "anciennete", "nombre_commandes", "montant_total",
            "montant_moyen", "jours_depuis_derniere_commande"]

# le modèle est chargé une seule fois au démarrage, pas à chaque message
model = joblib.load("modele_commande.pkl")


def connecter():
    # même principe qu'au TP2 : on réessaie tant que kafka n'est pas prêt
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_DEMANDES,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
            )
            return consumer, producer
        except KafkaError:
            print("kafka pas encore dispo, nouvel essai dans 5s...")
            time.sleep(5)


consumer, producer = connecter()
print(f"connecté à kafka, en écoute sur '{TOPIC_DEMANDES}' (groupe {GROUP_ID})")

for message in consumer:
    data = message.value

    # 3.4 : une ligne, les 6 colonnes du modèle (client_id n'en fait pas partie)
    X = pd.DataFrame([{c: data[c] for c in COLONNES}])

    # 3.5
    prediction = model.predict(X)[0]
    probabilite = model.predict_proba(X)[0][1]

    print(f"[partition {message.partition} | offset {message.offset}] "
          f"Client : {data['client_id']} - Prediction : {prediction} - Probabilite : {probabilite:.2f}")

    # 3.6 : int() et float() car json ne sait pas écrire les types numpy
    resultat = {
        "client_id": data["client_id"],
        "prediction": int(prediction),
        "probabilite_commande": float(probabilite),
    }
    producer.send(TOPIC_PREDICTIONS, key=data["client_id"], value=resultat)
