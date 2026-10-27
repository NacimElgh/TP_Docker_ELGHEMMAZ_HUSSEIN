# Microservice "commandes" : expose les commandes et interroge le microservice "clients" en REST
import os

import requests  # bibliothèque pour faire des appels HTTP vers un autre service
from flask import Flask, jsonify

app = Flask(__name__)

# Adresse du microservice clients. Dans Docker, "clients" est le nom du conteneur,
# résolu par le réseau Docker. On peut la changer avec la variable d'environnement CLIENTS_URL.
CLIENTS_URL = os.environ.get("CLIENTS_URL", "http://clients:8080")

# Données en mémoire ; client_id fait le lien avec le microservice clients
commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80},
]


# GET /commandes : renvoie toutes les commandes
@app.route("/commandes", methods=["GET"])
def get_commandes():
    return jsonify(commandes)


# GET /commandes/<id> : renvoie une commande enrichie des informations de son client
@app.route("/commandes/<int:commande_id>", methods=["GET"])
def get_commande(commande_id):
    # 1. Recherche de la commande (None si elle n'existe pas)
    commande = next((c for c in commandes if c["id"] == commande_id), None)
    if commande is None:
        return jsonify({"erreur": f"Commande {commande_id} introuvable"}), 404

    # 2-3. Appel REST au microservice clients avec le client_id de la commande
    try:
        r = requests.get(f"{CLIENTS_URL}/clients/{commande['client_id']}", timeout=3)
    except requests.exceptions.RequestException:
        # Le service clients est arrêté ou injoignable : 503 (service indisponible)
        return jsonify({"erreur": "Microservice clients indisponible"}), 503

    # Le client n'existe pas
    if r.status_code == 404:
        return jsonify({"erreur": f"Client {commande['client_id']} introuvable"}), 404
    # Toute autre erreur côté clients : 502 (mauvaise réponse d'un service appelé)
    if r.status_code != 200:
        return jsonify({"erreur": "Erreur du microservice clients"}), 502

    # 4-5. Réponse combinant la commande et les informations du client
    return jsonify({
        "id": commande["id"],
        "produit": commande["produit"],
        "montant": commande["montant"],
        "client": r.json(),
    })


if __name__ == "__main__":
    # Écoute sur toutes les interfaces, port 8080 (publié en 8081 sur la machine)
    app.run(host="0.0.0.0", port=8080)
