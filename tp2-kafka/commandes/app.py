# Microservice "commandes" (TP2) : reçoit des commandes en REST et publie un événement dans Kafka.
# Il n'appelle plus directement le service clients : la communication devient asynchrone.
import json
import os
import time
from datetime import datetime

from flask import Flask, jsonify, request
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Adresse du broker Kafka ("kafka" = nom du service dans docker-compose)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
# Topic dans lequel on publie les commandes créées
TOPIC = "commandes-creees"

app = Flask(__name__)
# Liste des commandes reçues (en mémoire)
commandes = []


def creer_producteur():
    # Kafka peut mettre quelques secondes à démarrer : on réessaie jusqu'à ce qu'il réponde
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                # Kafka transporte des octets : on convertit la clé et la valeur (dict -> JSON -> octets)
                key_serializer=lambda k: str(k).encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print("Kafka indisponible, nouvel essai dans 3 s...")
            time.sleep(3)


# Connexion au broker au démarrage du service
producteur = creer_producteur()


# GET /commandes : liste des commandes déjà créées
@app.route("/commandes", methods=["GET"])
def get_commandes():
    return jsonify(commandes)


# POST /commandes : crée une commande à partir du JSON envoyé et publie l'événement
@app.route("/commandes", methods=["POST"])
def creer_commande():
    data = request.get_json(silent=True) or {}
    # Vérification des champs obligatoires : sinon 400 (requête invalide)
    for champ in ("client_id", "produit", "montant"):
        if champ not in data:
            return jsonify({"erreur": f"Champ manquant : {champ}"}), 400

    # Structure du message JSON envoyé dans Kafka
    evenement = {
        "commande_id": len(commandes) + 1,
        "client_id": int(data["client_id"]),
        "produit": data["produit"],
        "montant": float(data["montant"]),
        # date ajoutée pour permettre le calcul du chiffre d'affaires par mois
        "date": data.get("date", datetime.now().strftime("%Y-%m-%d")),
    }
    commandes.append(evenement)

    # la clé client_id détermine la partition du message
    producteur.send(TOPIC, key=evenement["client_id"], value=evenement)
    # flush() force l'envoi immédiat (sinon les messages sont envoyés par lots)
    producteur.flush()
    print("Evenement publie dans", TOPIC, ":", evenement)
    # 201 = ressource créée
    return jsonify(evenement), 201


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
