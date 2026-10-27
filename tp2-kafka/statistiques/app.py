# Microservice "statistiques" (TP2, partie 1.7) : 2e consommateur du topic "commandes-creees".
# Il est dans un groupe différent de "clients", donc il reçoit lui aussi TOUS les messages.
import json
import os
import time
from collections import Counter, defaultdict

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
GROUP_ID = os.environ.get("GROUP_ID", "statistiques-group")
TOPIC = "commandes-creees"

# Nombre de commandes par produit
commandes_par_produit = Counter()
# Chiffre d'affaires par mois ("AAAA-MM" -> total des montants)
ca_par_mois = defaultdict(float)


def creer_consommateur():
    # On réessaie tant que Kafka n'est pas prêt
    while True:
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except NoBrokersAvailable:
            print("Kafka indisponible, nouvel essai dans 3 s...")
            time.sleep(3)


consommateur = creer_consommateur()
print(f"Consommateur statistiques pret (groupe={GROUP_ID})")

# Traitement de chaque commande reçue et mise à jour des statistiques
for message in consommateur:
    e = message.value
    mois = e.get("date", "inconnu")[:7]  # AAAA-MM
    commandes_par_produit[e["produit"]] += 1
    ca_par_mois[mois] += e["montant"]
    print(f"Commande {e['commande_id']} recue ({e['produit']}, {e['montant']}, {mois})")
    print("  Commandes par produit :", dict(commandes_par_produit))
    print("  Chiffre d'affaires par mois :", dict(ca_par_mois))
