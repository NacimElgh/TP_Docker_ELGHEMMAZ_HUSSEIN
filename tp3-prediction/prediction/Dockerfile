# Image de base : Python 3.12 en version légère ("slim")
FROM python:3.12-slim

# Dossier de travail dans le conteneur
WORKDIR /app

# Dépendances d'abord, pour profiter du cache Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie de tout le dossier : app.py, entrainer_modele.py et le .pkl s'il existe
COPY . .

# Entraîne le modèle une seule fois, à la construction de l'image, s'il n'est pas déjà fourni.
# Le modèle n'est donc jamais réentraîné à chaque événement Kafka.
RUN [ -f modele_commande.pkl ] || python entrainer_modele.py

# Démarre le service de prédiction (consommateur Kafka)
CMD ["python", "app.py"]
