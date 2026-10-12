# TP microservices : REST, Kafka, prédiction

Code des 3 TP. Chaque TP repart du précédent et ajoute quelque chose, donc chaque dossier contient tout ce qu'il faut et se lance seul.

| Dossier | Contenu |
|---|---|
| `tp-microservices - TP1 - REST` | 2 microservices Flask (`clients`, `commandes`) qui se parlent en REST |
| `tp-microservices - TP2 - Communication via Kafka` | Les mêmes, mais ils communiquent par Kafka. Ajout d'un service `statistiques` |
| `tp-microservices - TP3 - Prédiction` | Le TP2 + un microservice `prediction` qui utilise un modèle scikit-learn |

Le TP3 est le plus complet, c'est celui qui est détaillé ici.

## Prérequis

- Docker Desktop, démarré
- Python avec `requests` (`pip install requests`), uniquement pour lancer `flux.py`

Les 3 TP utilisent les mêmes ports (8080, 8081...). Il faut en arrêter un avant d'en lancer un autre.

## TP3 : ce que ça fait

```
flux.py --POST /predictions--> commandes --[demandes-prediction]--> prediction x K
                                   ^                                      |
                                   +-----------[predictions]--------------+
```

- `commandes` reçoit une demande de prédiction (âge, ancienneté, nombre de commandes... d'un client) et la publie dans Kafka.
- `prediction` lit la demande, applique le modèle et publie le résultat : le client va-t-il commander (0 ou 1), avec quelle probabilité.
- `commandes` lit le résultat et l'affiche dans ses logs.
- `prediction` tourne en K exemplaires qui se partagent les demandes. K se règle dans le fichier `.env` (3 par défaut).

Le modèle est déjà entraîné (`prediction/modele_commande.pkl`). Il a été créé avec `prediction/entrainer.py`, sur des données inventées.

## TP3 : démarrer

Depuis le dossier du TP3 :

```
cd "tp-microservices - TP3 - Prédiction"
docker compose up -d --build
```

Sous Windows, `lancer tous les conteneurs.bat` fait la même chose. Le premier lancement prend quelques minutes (installation de pandas et scikit-learn dans l'image).

Ensuite, lancer le flux infini de demandes (une toutes les 0,5 s, Ctrl+C pour arrêter) :

```
python flux.py
```

## TP3 : voir que ça marche

Les prédictions calculées par les K instances :

```
docker compose logs -f prediction
```

Exemple avec K = 3. Chaque instance ne lit que ses partitions :

```
prediction-2  | [partition 4 | offset 12] Client : 68 - Prediction : 1 - Probabilite : 0.72
prediction-2  | [partition 5 | offset 8] Client : 73 - Prediction : 1 - Probabilite : 0.92
prediction-1  | [partition 2 | offset 5] Client : 74 - Prediction : 0 - Probabilite : 0.18
prediction-3  | [partition 1 | offset 13] Client : 77 - Prediction : 1 - Probabilite : 0.69
prediction-2  | [partition 5 | offset 9] Client : 78 - Prediction : 0 - Probabilite : 0.06
prediction-1  | [partition 3 | offset 14] Client : 79 - Prediction : 0 - Probabilite : 0.23
prediction-1  | [partition 3 | offset 15] Client : 80 - Prediction : 1 - Probabilite : 0.57
prediction-3  | [partition 1 | offset 14] Client : 81 - Prediction : 0 - Probabilite : 0.04
prediction-3  | [partition 0 | offset 21] Client : 82 - Prediction : 0 - Probabilite : 0.28
prediction-1  | [partition 2 | offset 6] Client : 83 - Prediction : 1 - Probabilite : 0.94
prediction-3  | [partition 1 | offset 15] Client : 84 - Prediction : 0 - Probabilite : 0.25
```

Les résultats reçus par `commandes` :

```
docker logs -f commandes
```

```
prédiction reçue : client 83 -> 1 (probabilité de commande 0.94)
prédiction reçue : client 84 -> 0 (probabilité de commande 0.25)
```

Dans le navigateur, avec kafka-ui :

- Tout : http://localhost:8090
- Messages du topic des résultats : http://localhost:8090/ui/clusters/tp-kafka/all-topics/predictions/messages
- Groupe des instances de prédiction (membres, lag) : http://localhost:8090/ui/clusters/tp-kafka/consumer-groups/prediction-group

## TP3 : changer K, simuler une panne

Changer le nombre d'instances : modifier `K` dans `.env`, puis relancer.

```
docker compose up -d
```

Simuler une panne : arrêter une instance pendant que `flux.py` tourne.

```
docker compose ps
docker stop <nom d'une instance prediction>
```

Pendant environ 45 s, les demandes de cette instance attendent dans Kafka. Ensuite Kafka redonne ses partitions aux autres instances, qui rattrapent le retard. Aucune demande n'est perdue, quelques-unes peuvent être traitées deux fois.

## TP3 : arrêter

```
docker compose down
```

Ou `couper.bat`. Les messages Kafka sont supprimés avec les conteneurs.

## TP1 et TP2

TP2, même principe que le TP3 :

```
cd "tp-microservices - TP2 - Communication via Kafka"
docker compose up -d --build
```

TP1, pas de docker compose : lancer `lancer les deux conteneurs.bat` (ou les commandes `docker` qu'il contient), puis `supprimer et couper.bat` pour tout enlever. Pour tester :

```
curl http://localhost:8081/commandes/1
```

## Pour aller plus loin

Le fichier `ARCHITECTURE.txt` des dossiers TP2 et TP3 décrit l'architecture, les événements et le vocabulaire Kafka.
