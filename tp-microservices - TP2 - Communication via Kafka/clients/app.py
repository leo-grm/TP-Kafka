import json
import os
import threading
import time
from collections import Counter

from flask import Flask, jsonify
from kafka import KafkaConsumer
from kafka.errors import KafkaError


# tester GET : curl.exe http://localhost:8080/clients/1

app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = "commandes-creees"
GROUP_ID = os.environ.get("GROUP_ID", "clients-group")

clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"}
]

# nb de commandes par client_id, rempli au fil des messages kafka
nb_commandes = Counter()


def trouver_client(client_id):
    for client in clients:
        if client["id"] == client_id:
            return client
    return None


def creer_consumer():
    # kafka met quelques secondes à démarrer, donc on réessaie tant qu'il répond pas
    while True:
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                # kafka retient où en est le groupe (offset) : au redémarrage on reprend là où on s'était arrêté
                group_id=GROUP_ID,
                # si le groupe n'a encore rien lu, on part du début du topic
                auto_offset_reset="earliest",
                # bytes -> json -> dict, l'inverse du serializer côté commandes
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except KafkaError:
            print("kafka pas encore dispo, nouvel essai dans 5s...")
            time.sleep(5)


def consommer():
    consumer = creer_consumer()
    print(f"connecté à kafka, en écoute sur '{TOPIC}' (groupe {GROUP_ID})")

    # boucle infinie : on bloque ici en attendant les messages
    for message in consumer:
        evt = message.value
        client = trouver_client(evt["client_id"])
        nom = client["nom"] if client else f"client inconnu (id {evt['client_id']})"

        nb_commandes[evt["client_id"]] += 1

        print(f"[partition {message.partition} | offset {message.offset}] "
              f"commande {evt['commande_id']} : {nom} - {evt['produit']} - {evt['montant']} €")
        print("  total par client : " + ", ".join(
            f"{(trouver_client(cid) or {'nom': cid})['nom']} = {n}" for cid, n in nb_commandes.items()
        ))


@app.route('/clients', methods=['GET'])
def get_clients():
    return jsonify(clients)


@app.route('/clients/<int:client_id>', methods=['GET'])
def get_client(client_id):
    client = trouver_client(client_id)
    if client:
        return jsonify(client)
    # Gestion des erreurs (2.2) : client inexistant -> 404
    return jsonify({"erreur": "Client introuvable"}), 404


# pour voir les compteurs sans lire les logs
@app.route('/clients/nb-commandes', methods=['GET'])
def get_nb_commandes():
    return jsonify([
        {"client_id": cid, "nom": (trouver_client(cid) or {}).get("nom"), "nb_commandes": n}
        for cid, n in nb_commandes.items()
    ])


if __name__ == '__main__':
    # le consumer tourne dans un thread à part, sinon il bloquerait flask
    # daemon=True : le thread s'arrête avec l'appli
    threading.Thread(target=consommer, daemon=True).start()

    # use_reloader=False : en debug flask relance le script une 2e fois,
    # ce qui créerait un 2e consumer
    app.run(host='0.0.0.0', port=8080, debug=True, use_reloader=False)
