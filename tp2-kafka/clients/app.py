# Microservice "clients" (TP2) : consomme les événements "commandes-creees" depuis Kafka
# et tient le compte des commandes passées par chaque client.
import json
import os
import socket
import time
from collections import Counter

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
# Groupe de consommateurs : les instances d'un même groupe se partagent les messages
GROUP_ID = os.environ.get("GROUP_ID", "clients-group")
TOPIC = "commandes-creees"

# Clients connus, indexés par id pour retrouver le nom à partir du client_id
clients = {
    1: {"id": 1, "nom": "Alice", "pays": "France"},
    2: {"id": 2, "nom": "Alex", "pays": "Belgique"},
    3: {"id": 3, "nom": "Madeleine", "pays": "France"},
}
# Compteur du nombre de commandes par client_id
nb_commandes = Counter()


def creer_consommateur():
    # On réessaie tant que Kafka n'est pas prêt
    while True:
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                # Au tout premier démarrage du groupe, on lit le topic depuis le début.
                # Ensuite, Kafka mémorise où le groupe s'est arrêté (offset) : rien n'est perdu
                # si le service est arrêté puis relancé.
                auto_offset_reset="earliest",
                # Conversion inverse du producteur : octets -> JSON -> dict
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except NoBrokersAvailable:
            print("Kafka indisponible, nouvel essai dans 3 s...")
            time.sleep(3)


consommateur = creer_consommateur()
# gethostname() donne l'identifiant du conteneur : utile pour distinguer les instances
print(f"Consommateur clients pret (groupe={GROUP_ID}, instance={socket.gethostname()})")

# Boucle infinie : attend les messages et traite chacun dès son arrivée
for message in consommateur:
    e = message.value
    client = clients.get(e["client_id"], {"nom": f"inconnu ({e['client_id']})"})
    nb_commandes[e["client_id"]] += 1
    # La partition montre comment Kafka répartit les messages entre consommateurs
    print(f"[partition {message.partition}] Commande {e['commande_id']} : "
          f"client={client['nom']}, produit={e['produit']}, montant={e['montant']}")
    print("  Total commandes par client :",
          {clients.get(cid, {"nom": cid})["nom"]: n for cid, n in nb_commandes.items()})
