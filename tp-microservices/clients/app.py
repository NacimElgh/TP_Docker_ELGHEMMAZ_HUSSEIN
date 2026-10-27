# Microservice "clients" : expose la liste des clients via une API REST (Flask)
from flask import Flask, jsonify

# Création de l'application web Flask
app = Flask(__name__)

# Données en mémoire
clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"},
]


# GET /clients : renvoie tous les clients au format JSON
@app.route("/clients", methods=["GET"])
def get_clients():
    return jsonify(clients)


# GET /clients/<id> : renvoie un seul client ; <int:client_id> convertit l'id en entier
@app.route("/clients/<int:client_id>", methods=["GET"])
def get_client(client_id):
    for c in clients:
        if c["id"] == client_id:
            return jsonify(c)
    # Aucun client trouvé 
    return jsonify({"erreur": f"Client {client_id} introuvable"}), 404


if __name__ == "__main__":
    # host="0.0.0.0" : écoute sur toutes les interfaces, indispensable dans un conteneur
    # (avec 127.0.0.1, le serveur ne serait pas joignable depuis l'extérieur du conteneur)
    app.run(host="0.0.0.0", port=8080)
