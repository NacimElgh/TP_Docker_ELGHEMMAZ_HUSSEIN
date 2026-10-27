"""Entraîne un modèle de remplacement et le sauvegarde dans modele_commande.pkl.

Les données historiques n'étant pas fournies, on génère des données simulées.
Ce script n'est exécuté qu'une fois (à la construction de l'image Docker),
jamais à chaque événement Kafka.
"""
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

COLONNES = ["age", "anciennete", "nombre_commandes", "montant_total",
            "montant_moyen", "jours_depuis_derniere_commande"]

rng = np.random.default_rng(42)
n = 2000
df = pd.DataFrame({
    "age": rng.integers(18, 75, n),
    "anciennete": rng.integers(0, 15, n),
    "nombre_commandes": rng.integers(0, 40, n),
    "montant_moyen": rng.uniform(10, 400, n).round(2),
    "jours_depuis_derniere_commande": rng.integers(0, 365, n),
})
df["montant_total"] = (df["nombre_commandes"] * df["montant_moyen"]).round(2)

# Règle simulée : un client fidèle et récent a plus de chances de commander
score = (0.08 * df["nombre_commandes"] + 0.1 * df["anciennete"]
         - 0.012 * df["jours_depuis_derniere_commande"] + rng.normal(0, 0.8, n))
y = (score > 0).astype(int)

X_train, X_test, y_train, y_test = train_test_split(df[COLONNES], y, test_size=0.2, random_state=42)
modele = RandomForestClassifier(n_estimators=100, max_depth=6, random_state=42)
modele.fit(X_train, y_train)
print(f"Precision sur le jeu de test : {modele.score(X_test, y_test):.2f}")

joblib.dump(modele, "modele_commande.pkl")
print("Modele sauvegarde dans modele_commande.pkl")
