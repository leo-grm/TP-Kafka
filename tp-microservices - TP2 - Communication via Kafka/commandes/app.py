import json
import os
from datetime import date

from flask import Flask, jsonify, request
from kafka import KafkaProducer
from kafka.errors import KafkaError

app = Flask(__name__)

# kafka:9092 depuis un conteneur, localhost:9094 depuis la machine (cf docker-compose)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = "commandes-creees"

commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80}
]

producer = None


def get_producer():
    # on crée le producer au premier besoin plutôt qu'au démarrage :
    # si kafka n'est pas encore prêt, l'appli démarre quand même
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            # kafka ne transporte que des bytes, donc dict -> json -> bytes
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
        )
    return producer


@app.route('/commandes', methods=['GET'])
def get_commandes():
    return jsonify(commandes)


@app.route('/commandes/<int:commande_id>', methods=['GET'])
def get_commande(commande_id):
    for commande in commandes:
        if commande["id"] == commande_id:
            return jsonify(commande)
    return jsonify({"erreur": "Commande introuvable"}), 404


@app.route('/commandes', methods=['POST'])
def creer_commande():
    data = request.get_json(silent=True) or {}

    # on vérifie qu'on a bien les 3 champs
    manquants = [c for c in ("client_id", "produit", "montant") if c not in data]
    if manquants:
        return jsonify({"erreur": f"Champs manquants : {', '.join(manquants)}"}), 400

    commande = {
        "id": max(c["id"] for c in commandes) + 1,
        "client_id": data["client_id"],
        "produit": data["produit"],
        "montant": data["montant"],
        # date du jour par défaut, mais on peut en passer une pour tester plusieurs mois
        "date": data.get("date", date.today().isoformat()),
    }

    # le message envoyé dans kafka, format de l'énoncé + la date (pour le CA par mois en 1.7)
    evenement = {
        "commande_id": commande["id"],
        "client_id": commande["client_id"],
        "produit": commande["produit"],
        "montant": commande["montant"],
        "date": commande["date"],
    }

    try:
        # la clé = client_id : toutes les commandes d'un même client vont dans la
        # même partition, donc elles sont lues dans l'ordre
        future = get_producer().send(TOPIC, key=commande["client_id"], value=evenement)
        # send() est asynchrone, get() attend que kafka confirme la réception
        metadata = future.get(timeout=10)
    except KafkaError as e:
        return jsonify({"erreur": f"Kafka indisponible : {e}"}), 503

    # on n'enregistre la commande que si l'événement est bien parti
    commandes.append(commande)

    return jsonify({
        "commande": commande,
        "evenement": evenement,
        "kafka": {"topic": metadata.topic, "partition": metadata.partition, "offset": metadata.offset},
    }), 201


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=True)
