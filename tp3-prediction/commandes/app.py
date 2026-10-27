# Microservice "commandes" (TP3) : envoie des demandes de prédiction dans Kafka
# et affiche les résultats renvoyés par le microservice prediction.
import json
import os
import random
import threading
import time

from flask import Flask, jsonify, request
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
# Topic où l'on envoie les demandes, et topic où l'on lit les résultats
TOPIC_DEMANDES = "demandes-prediction"
TOPIC_RESULTATS = "predictions"
# Nombre de demandes par seconde envoyées en continu (0 = désactivé)
DEBIT_SIMULATION = float(os.environ.get("DEBIT_SIMULATION", "0"))

app = Flask(__name__)


def creer_producteur():
    # On réessaie tant que Kafka n'est pas prêt
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                key_serializer=lambda k: str(k).encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print("Kafka indisponible, nouvel essai dans 3 s...")
            time.sleep(3)


producteur = creer_producteur()


def client_aleatoire():
    # Génère les caractéristiques d'un client fictif (pour les tests et la simulation)
    nb = random.randint(0, 40)
    moyen = round(random.uniform(10, 400), 2)
    return {
        "client_id": random.randint(1, 1000),
        "age": random.randint(18, 75),
        "anciennete": random.randint(0, 15),
        "nombre_commandes": nb,
        "montant_total": round(nb * moyen, 2),
        "montant_moyen": moyen,
        "jours_depuis_derniere_commande": random.randint(0, 365),
    }


def publier(evenement):
    # Envoie une demande de prédiction ; la clé client_id choisit la partition
    producteur.send(TOPIC_DEMANDES, key=evenement["client_id"], value=evenement)
    producteur.flush()
    print("Demande de prediction envoyee :", evenement)


def ecouter_resultats():
    # Tourne en tâche de fond (thread) : lit le topic "predictions" et affiche chaque résultat
    while True:
        try:
            consommateur = KafkaConsumer(
                TOPIC_RESULTATS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="commandes-group",
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            break
        except NoBrokersAvailable:
            time.sleep(3)
    for message in consommateur:
        r = message.value
        print(f"RESULTAT client {r['client_id']} : prediction={r['prediction']}, "
              f"probabilite={r['probabilite_commande']} (par {r.get('instance')})")


def simuler():
    # Bonus : flux infini de demandes, DEBIT_SIMULATION demandes par seconde
    while True:
        publier(client_aleatoire())
        time.sleep(1 / DEBIT_SIMULATION)


# POST /predictions : envoie une demande avec les caractéristiques fournies en JSON
@app.route("/predictions", methods=["POST"])
def demander_prediction():
    data = request.get_json(silent=True) or {}
    champs = ["client_id", "age", "anciennete", "nombre_commandes", "montant_total",
              "montant_moyen", "jours_depuis_derniere_commande"]
    manquants = [c for c in champs if c not in data]
    if manquants:
        return jsonify({"erreur": f"Champs manquants : {manquants}"}), 400
    publier(data)
    # 202 = demande acceptée ; le résultat arrivera plus tard, de façon asynchrone
    return jsonify({"statut": "demande envoyee", "demande": data}), 202


# GET /test : envoie une demande pour un client aléatoire (pratique depuis le navigateur)
@app.route("/test", methods=["GET"])
def test():
    evenement = client_aleatoire()
    publier(evenement)
    return jsonify({"statut": "demande envoyee", "demande": evenement}), 202


if __name__ == "__main__":
    # daemon=True : les threads s'arrêtent avec le programme principal
    threading.Thread(target=ecouter_resultats, daemon=True).start()
    if DEBIT_SIMULATION > 0:
        threading.Thread(target=simuler, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
