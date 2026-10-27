# Image de base : Python 3.12 en version légère ("slim")
FROM python:3.12-slim

# Dossier de travail dans le conteneur (toutes les commandes suivantes s'y exécutent)
WORKDIR /app

# On copie d'abord requirements.txt seul : Docker garde cette étape en cache
# et ne réinstalle pas les dépendances si seul le code change
COPY requirements.txt .

# Installation des bibliothèques Python listées dans requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code du microservice
COPY app.py .

# Commande lancée au démarrage du conteneur
CMD ["python", "app.py"]
