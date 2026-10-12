import json
import os
import threading
import time
from datetime import date

from flask import Flask, jsonify, request
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

app = Flask(__name__)

# kafka:9092 depuis un conteneur, localhost:9094 depuis la machine (cf docker-compose)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = "commandes-creees"
# TP3 : les demandes de prédiction partent dans leur propre topic
TOPIC_DEMANDES = "demandes-prediction"
# ... et les résultats reviennent par celui-ci
TOPIC_PREDICTIONS = "predictions"
CHAMPS_PREDICTION = ("client_id", "age", "anciennete", "nombre_commandes",
                     "montant_total", "montant_moyen", "jours_depuis_derniere_commande")

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


# tester : Invoke-RestMethod -Method Post -Uri http://localhost:8081/predictions -ContentType "application/json" -Body '{"client_id": 25, "age": 32, "anciennete": 3, "nombre_commandes": 8, "montant_total": 1250, "montant_moyen": 156.25, "jours_depuis_derniere_commande": 12}'
@app.route('/predictions', methods=['POST'])
def demander_prediction():
    data = request.get_json(silent=True) or {}

    # un message incomplet ferait planter le service prediction, donc on bloque ici
    manquants = [c for c in CHAMPS_PREDICTION if c not in data]
    if manquants:
        return jsonify({"erreur": f"Champs manquants : {', '.join(manquants)}"}), 400

    try:
        get_producer().send(TOPIC_DEMANDES, key=data["client_id"], value=data).get(timeout=10)
    except KafkaError as e:
        return jsonify({"erreur": f"Kafka indisponible : {e}"}), 503

    # 202 = demande acceptée mais pas encore traitée : le résultat arrivera plus tard par kafka
    return jsonify({"demande": data}), 202


def afficher_predictions():
    # commandes devient aussi consommateur, même code que le consumer de clients au TP2
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_PREDICTIONS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id="commandes-group",
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            break
        except KafkaError:
            print("kafka pas encore dispo, nouvel essai dans 5s...")
            time.sleep(5)

    for message in consumer:
        r = message.value
        print(f"prédiction reçue : client {r['client_id']} -> {r['prediction']} "
              f"(probabilité de commande {r['probabilite_commande']:.2f})")


if __name__ == '__main__':
    threading.Thread(target=afficher_predictions, daemon=True).start()

    port = int(os.environ.get("PORT", 8080))
    # use_reloader=False : sinon flask lance le script 2 fois, donc 2 consumers (cf clients)
    app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)
