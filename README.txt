Commandes utiles a tout moment
------------------------------
docker ps
    Liste les conteneurs en cours d'execution.
docker ps -a
    Liste aussi les conteneurs arretes (utile si un conteneur a plante).
docker logs NOM
    Affiche les messages (print, erreurs) d'un conteneur.
docker images
    Liste les images construites.
docker system prune
    Supprime les conteneurs arretes, reseaux et images inutilises (libere de la place).


=====================================================
TP1 - API REST (dossier tp-microservices)
=====================================================

cd H:\Desktop\Docker\tp-microservices
    Se place dans le dossier du TP1.

Construire et lancer
docker build -t microservice-clients ./clients
    Construit l'image du microservice clients a partir de son Dockerfile.
docker build -t microservice-commandes ./commandes
    Construit l'image du microservice commandes.
docker network create tp-reseau
    Cree un reseau Docker ou les conteneurs se joignent par leur nom.
docker run -d --name clients --network tp-reseau -p 8080:8080 microservice-clients
    Lance clients en arriere-plan, accessible sur le port 8080 de la machine.
docker run -d --name commandes --network tp-reseau -p 8081:8080 microservice-commandes
    Lance commandes, accessible sur le port 8081 ; il joint clients via http://clients:8080.

Verifier
docker ps
    Doit afficher les conteneurs clients et commandes
curl.exe http://localhost:8080/clients
    Renvoie tous les clients.
curl.exe http://localhost:8080/clients/1
    Renvoie le client 1 (Alice).	
curl.exe http://localhost:8081/commandes
    Renvoie toutes les commandes.
curl.exe http://localhost:8081/commandes/1
    Renvoie la commande 1 avec son client : appel REST commandes -> clients

Tester les erreurs
curl.exe http://localhost:8080/clients/100
    Client inexistant : erreur 404.
curl.exe http://localhost:8081/commandes/100
    Commande inexistante : erreur 404.
docker stop clients
    Arrete le microservice clients.
curl.exe http://localhost:8081/commandes/1
    Clients arrete : commandes renvoie l'erreur 503 (service indisponible).
docker start clients
    Relance le microservice clients.

Arreter
docker rm -f clients commandes
    Arrete et supprime les deux conteneurs (libere les ports 8080 et 8081).
docker network rm tp-reseau
    Supprime le reseau du TP1.


=====================================================
TP2 - KAFKA (dossier tp2-kafka)
=====================================================

cd H:\Desktop\Docker\tp2-kafka
    Se place dans le dossier du TP2.

Lancer
docker compose up -d --build
    Construit les images et lance Kafka, commandes, clients et statistiques.
docker compose ps
    Liste les 4 conteneurs du TP ; kafka doit etre "healthy"

Creer une commande
Invoke-RestMethod -Method Post -Uri http://localhost:8081/commandes -ContentType "application/json" -Body '{"client_id":1,"produit":"PC","montant":1200}'
    Cree une commande ; commandes publie l'evenement dans le topic commandes-creees.
Invoke-RestMethod -Method Post -Uri http://localhost:8081/commandes -ContentType "application/json" -Body '{"client_id":2,"produit":"Livre","montant":30,"date":"2026-09-15"}'
    Cree une commande avec une date choisie (pour le CA par mois).
curl.exe http://localhost:8081/commandes
    Liste les commandes creees.

Observer
docker compose logs -f commandes clients statistiques
    Affiche en direct les messages des 3 services, Ctrl+C pour quitter.
docker compose logs statistiques
    Affiche les statistiques : commandes par produit, CA par mois
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic commandes-creees
    Decrit le topic (nombre de partitions).
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic commandes-creees --from-beginning
    Affiche tous les messages stockes dans le topic. Ctrl+C pour quitter.

Partie 1.6 - Arreter un microservice
docker compose stop clients
    Arrete clients ; creer ensuite quelques commandes.
docker compose start clients
    Relance clients : il lit les commandes manquees, rien n'est perdu.
docker compose logs -f clients
    Montre les commandes rattrapees

Partie 8 - Groupes de consommateurs
$env:CLIENTS_GROUP="groupe-A"
    Place les consommateurs clients dans le groupe groupe-A.
docker compose up -d --scale clients=2
    Lance 2 instances de clients dans le meme groupe : elles se partagent les messages.
docker compose logs -f clients
    Montre quelle instance (et quelle partition) traite chaque message
Remove-Item Env:CLIENTS_GROUP
    Revient au groupe par defaut clients-group.
docker compose up -d --scale clients=1
    Revient a une seule instance de clients.

Arreter
docker compose down
    Arrete et supprime tous les conteneurs et le reseau du TP2.


=====================================================
TP3 - PREDICTION (dossier tp3-prediction)
=====================================================

cd H:\Desktop\Docker\tp3-prediction
    Se place dans le dossier du TP3.

Lancer
docker compose up -d --build
    Construit les images (et entraine le modele), puis lance Kafka, commandes et prediction.
docker compose ps
    Liste les conteneurs du TP

Envoyer une demande de prediction
curl.exe localhost:8081/test
    Envoie une demande pour un client aleatoire.
curl.exe -X POST localhost:8081/predictions -H "Content-Type: application/json" --data "@exemple.json"
    Envoie la demande du client de l'enonce (fichier exemple.json).

Observer
docker compose logs -f
    Affiche la demande, la prediction et le RESULTAT recu par commandes

Bonus - flux infini et K instances
docker compose down
    Arrete le TP3 avant de le relancer avec les nouveaux reglages.
$env:DEBIT="2"
    Regle le flux automatique a 2 demandes par seconde.
docker compose up -d --scale prediction=3
    Lance K=3 instances de prediction qui se partagent les demandes (K max = 6).
docker compose logs -f prediction
    Montre la repartition des demandes entre les instances

Bonus - resilience a la panne
docker stop tp3-prediction-prediction-1
    Arrete une instance : les autres reprennent ses demandes
docker start tp3-prediction-prediction-1
    Relance l'instance arretee.

Arreter
docker compose down
    Arrete et supprime tous les conteneurs et le reseau du TP3.
Remove-Item Env:DEBIT
    Desactive le flux automatique pour les prochains lancements.
