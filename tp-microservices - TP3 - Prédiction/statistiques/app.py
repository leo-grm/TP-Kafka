import json
import os
import threading
import time
from collections import Counter

from flask import Flask, jsonify
from kafka import KafkaConsumer
from kafka.errors import KafkaError


# tester : curl.exe http://localhost:8082/stats/produits/PC
#          curl.exe http://localhost:8082/stats/mois/2026-09

app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = "commandes-creees"
GROUP_ID = os.environ.get("GROUP_ID", "statistiques-group")

# nb de commandes par produit, et chiffre d'affaires par mois ("2026-09")
nb_par_produit = Counter()
ca_par_mois = Counter()


def creer_consumer():
    # même principe que dans clients : on attend que kafka soit prêt
    while True:
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except KafkaError:
            print("kafka pas encore dispo, nouvel essai dans 5s...")
            time.sleep(5)


def consommer():
    consumer = creer_consumer()
    print(f"connecté à kafka, en écoute sur '{TOPIC}' (groupe {GROUP_ID})")

    for message in consumer:
        evt = message.value
        # "2026-09-28" -> "2026-09" ; les vieux événements sans date tombent dans "inconnu"
        mois = evt.get("date", "inconnu")[:7]

        nb_par_produit[evt["produit"]] += 1
        ca_par_mois[mois] += evt["montant"]

        print(f"[partition {message.partition} | offset {message.offset}] "
              f"commande {evt['commande_id']} : {evt['produit']} - {evt['montant']} € ({mois})")


@app.route('/stats/produits/<produit>', methods=['GET'])
def stats_produit(produit):
    return jsonify({"produit": produit, "nb_commandes": nb_par_produit[produit]})


@app.route('/stats/mois/<mois>', methods=['GET'])
def stats_mois(mois):
    return jsonify({"mois": mois, "chiffre_affaires": ca_par_mois[mois]})


if __name__ == '__main__':
    threading.Thread(target=consommer, daemon=True).start()
    app.run(host='0.0.0.0', port=8080, debug=True, use_reloader=False)
