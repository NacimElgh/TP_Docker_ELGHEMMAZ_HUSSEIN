# Microservice "prediction" (TP3) : lit des demandes dans Kafka, applique le modèle de
# Machine Learning et publie le résultat dans un autre topic Kafka.
import json
import os
import socket
import time

import joblib  # sert à charger le modèle sauvegardé (.pkl)
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
# Topic d'entrée (demandes envoyées par commandes) et topic de sortie (résultats)
TOPIC_DEMANDES = "demandes-prediction"
TOPIC_RESULTATS = "predictions"
# Nom du conteneur : permet de voir quelle instance a traité chaque demande
INSTANCE = socket.gethostname()

# Caractéristiques attendues par le modèle, dans l'ordre utilisé à l'entraînement
COLONNES = ["age", "anciennete", "nombre_commandes", "montant_total",
            "montant_moyen", "jours_depuis_derniere_commande"]

# Le modèle est chargé une seule fois au démarrage
modele = joblib.load("modele_commande.pkl")


def connecter():
    # Crée le consommateur (lecture des demandes) et le producteur (envoi des résultats).
    # On réessaie tant que Kafka n'est pas prêt.
    while True:
        try:
            consommateur = KafkaConsumer(
                TOPIC_DEMANDES,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                # Toutes les instances partagent ce groupe : chaque demande n'est traitée
                # qu'une seule fois, par une seule instance (répartition de charge)
                group_id="prediction-group",
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            producteur = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                key_serializer=lambda k: str(k).encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            return consommateur, producteur
        except NoBrokersAvailable:
            print("Kafka indisponible, nouvel essai dans 3 s...")
            time.sleep(3)


consommateur, producteur = connecter()
print(f"Service prediction pret (instance={INSTANCE})")

# Boucle infinie : une prédiction par événement reçu
for message in consommateur:
    data = message.value
    # Construction de l'entrée du modèle : un DataFrame d'une ligne avec les bonnes colonnes
    X = pd.DataFrame([{c: data[c] for c in COLONNES}])

    # Classe prédite (0 = ne commandera pas, 1 = commandera)
    prediction = modele.predict(X)[0]
    # Probabilité de la classe 1 (le client passe une commande)
    probabilite = modele.predict_proba(X)[0][1]

    print("Client :", data["client_id"])
    print("Prediction :", prediction)
    print("Probabilite :", round(float(probabilite), 2))

    # Événement résultat ; int() et float() convertissent les types numpy en types JSON
    resultat = {
        "client_id": data["client_id"],
        "prediction": int(prediction),
        "probabilite_commande": round(float(probabilite), 2),
        "instance": INSTANCE,
    }
    # Publication du résultat, destiné au microservice commandes
    producteur.send(TOPIC_RESULTATS, key=data["client_id"], value=resultat)
    producteur.flush()
