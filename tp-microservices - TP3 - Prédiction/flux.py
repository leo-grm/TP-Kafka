import random
import time

import requests

# flux infini de demandes de prédiction (3.7)
# lancer depuis la machine : python flux.py     (Ctrl+C pour arrêter)

URL = "http://localhost:8081/predictions"

client_id = 0
while True:
    # les client_id se suivent : pratique pour vérifier qu'aucune demande n'est perdue
    client_id += 1
    nb = random.randint(1, 30)
    moyen = round(random.uniform(10, 300), 2)

    demande = {
        "client_id": client_id,
        "age": random.randint(18, 80),
        "anciennete": random.randint(0, 10),
        "nombre_commandes": nb,
        "montant_total": round(nb * moyen, 2),
        "montant_moyen": moyen,
        "jours_depuis_derniere_commande": random.randint(0, 365),
    }
    requests.post(URL, json=demande)
    print("demande envoyée pour le client", client_id)

    time.sleep(0.5)
