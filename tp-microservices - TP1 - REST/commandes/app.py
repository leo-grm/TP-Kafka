import os

import requests
from flask import Flask, jsonify

app = Flask(__name__)

# Adresse du microservice clients (modifiable par variable d'environnement)
CLIENTS_URL = os.environ.get("CLIENTS_URL", "http://localhost:8080")

commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80}
]


@app.route('/commandes', methods=['GET'])
def get_commandes():
    return jsonify(commandes)


@app.route('/commandes/<int:commande_id>', methods=['GET'])
def get_commande(commande_id):
    # 1. Récupérer la commande
    commande = None
    for c in commandes:
        if c["id"] == commande_id:
            commande = c
            break
    # Gestion des erreurs (2.2) : commande inexistante -> 404
    if commande is None:
        return jsonify({"erreur": "Commande introuvable"}), 404

    # 2. Récupérer son client_id
    client_id = commande["client_id"]

    # 3. Appeler le microservice clients
    # Gestion des erreurs (2.2) : microservice clients injoignable ou trop lent -> 503
    try:
        reponse = requests.get(f"{CLIENTS_URL}/clients/{client_id}", timeout=5)
    except requests.exceptions.RequestException:
        return jsonify({"erreur": "Microservice clients indisponible"}), 503

    # Gestion des erreurs (2.2) : client inexistant -> 404
    if reponse.status_code == 404:
        return jsonify({"erreur": f"Client {client_id} introuvable"}), 404

    # Gestion des erreurs (2.2) : autre erreur renvoyée par clients -> 502
    if reponse.status_code != 200:
        return jsonify({"erreur": "Réponse invalide du microservice clients"}), 502

    # 4. Récupérer les informations du client
    client = reponse.json()

    # 5. Retourner la commande avec les informations du client
    return jsonify({
        "id": commande["id"],
        "produit": commande["produit"],
        "montant": commande["montant"],
        "client": client
    })


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=True)
