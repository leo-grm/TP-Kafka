# 📬 Mémo — Microservices, REST, Docker & Kafka en Python

> Résumé de ce que j'ai vu entre le **TP1** (REST) et le **TP2** (Kafka).
> Objectif : comprendre *à quoi sert chaque chose* et *comment on la fait*.

---

## 🗺️ Sommaire

1. [L'idée de départ : pourquoi Kafka ?](#1--lidée-de-départ--pourquoi-kafka-)
2. [Le vocabulaire Kafka en une phrase](#2--le-vocabulaire-kafka-en-une-phrase)
3. [Les règles d'or à retenir](#3--les-règles-dor-à-retenir)
4. [Les groupes de consommateurs](#4--les-groupes-de-consommateurs)
5. [Kafka dans Docker](#5--kafka-dans-docker)
6. [Kafka en Python : le code](#6--kafka-en-python--le-code)
7. [Commandes utiles](#7--commandes-utiles)
8. [Pièges rencontrés](#8--pièges-rencontrés)
9. [REST ou Kafka : lequel choisir ?](#9--rest-ou-kafka--lequel-choisir-)
10. [Rappels TP1 : REST & Docker](#10--rappels-tp1--rest--docker)

---

## 1. 💡 L'idée de départ : pourquoi Kafka ?

| | 📞 **TP1 — REST** (synchrone) | 📮 **TP2 — Kafka** (asynchrone) |
|---|---|---|
| **Image mentale** | Un **coup de fil** : j'appelle et j'attends qu'on décroche | Une **boîte aux lettres** : je dépose et je repars |
| **Qui connaît qui ?** | `commandes` doit connaître l'adresse de `clients` | Chacun ne connaît que Kafka |
| **Si le destinataire est éteint** | ❌ Erreur (503) | ✅ Le message attend, il sera lu plus tard |
| **Nombre de destinataires** | 1 appel = 1 destinataire | 1 message = autant de lecteurs qu'on veut |
| **Réponse** | ✅ On a une réponse immédiate | ❌ Pas de réponse, on annonce juste un fait |

```
TP1 :  commandes ──── GET /clients/1 ────▶ clients      (j'attends la réponse)
                 ◀─── {"nom": "Alice"} ───

TP2 :  commandes ── publie ──▶ [ KAFKA ] ── lit ──▶ clients
                                          └─ lit ──▶ statistiques
```

> [!TIP]
> **Un événement = un fait passé.** « Une commande *a été* créée ». On ne demande rien à personne, on annonce. Ceux que ça intéresse écoutent.

---

## 2. 📖 Le vocabulaire Kafka en une phrase

| Terme | Ça sert à… | Dans mon TP |
|---|---|---|
| 🖥️ **Broker** | Le serveur Kafka : il reçoit, **stocke sur disque** et distribue les messages | le conteneur `kafka` |
| 📂 **Topic** | Une « file » de messages avec un nom, sur un thème | `commandes-creees` |
| 🧩 **Partition** | Un morceau d'un topic. Permet de **répartir** la lecture entre plusieurs consommateurs | 2 partitions : 0 et 1 |
| ✉️ **Message** | Une **clé** (optionnelle) + une **valeur** (le contenu, ici du JSON) | clé = `client_id`, valeur = la commande |
| ✍️ **Producteur** | Celui qui **écrit** dans un topic | `commandes` |
| 👀 **Consommateur** | Celui qui **lit** un topic | `clients`, `statistiques` |
| 🔢 **Offset** | Le numéro d'un message dans sa partition = **marque-page** du lecteur | `[partition 1 \| offset 3]` |
| 👥 **Groupe** (`group_id`) | Une « équipe » de consommateurs qui partage un marque-page | `clients-group`, `statistiques-group` |
| ⏳ **Lag** | Nombre de messages **pas encore lus** par un groupe | lag = 3 quand `clients` était arrêté |
| 🔄 **Rebalancing** | Kafka **redistribue** les partitions quand un consommateur arrive ou part | les ~45 s d'attente au redémarrage |
| ⚙️ **KRaft** | Mode de Kafka **sans Zookeeper** (un conteneur de moins) | `apache/kafka` dans le compose |

---

## 3. ⭐ Les règles d'or à retenir

> [!IMPORTANT]
> **1. Lire ne supprime pas.** Les messages restent dans Kafka après lecture. Chaque groupe avance juste son marque-page (offset).

> [!IMPORTANT]
> **2. Rien n'est perdu si un consommateur est éteint.** Au redémarrage, il reprend à son offset → il rattrape tout, sans doublon. *(vu en 1.6)*

> [!IMPORTANT]
> **3. Même clé ➜ même partition ➜ ordre garanti.** L'ordre n'est garanti **qu'à l'intérieur d'une partition**. Donc si l'ordre compte pour un client, on met `client_id` comme clé.

> [!IMPORTANT]
> **4. Kafka ne transporte que des octets.** Il faut **sérialiser** (dict ➜ JSON ➜ bytes) côté producteur et **désérialiser** (bytes ➜ JSON ➜ dict) côté consommateur.

> [!WARNING]
> **5. Kafka garde les messages, pas les calculs.** Mes compteurs (`Counter`) sont en mémoire : ils repartent de 0 quand le service redémarre. Pour les garder, il faudrait une base de données.

---

## 4. 👥 Les groupes de consommateurs

C'est **la** notion clé. Le `group_id` décide si les consommateurs **se partagent** les messages ou **reçoivent tout**.

### 🟢 Même groupe ➜ on se **partage** le travail

```
                     ┌── partition 0 ──▶ conso-a1 ┐
topic commandes ─────┤                            ├─ groupe-A
                     └── partition 1 ──▶ conso-a2 ┘

➜ chaque message est lu par UN SEUL des deux
```

- **Sert à :** lancer plusieurs copies du **même service** pour aller plus vite quand il y a beaucoup de messages (*load balancing*).
- **Limite :** au maximum **1 consommateur actif par partition**. 2 partitions = 2 consommateurs utiles, le 3ᵉ attendrait sans rien faire.

### 🔵 Groupes différents ➜ chacun reçoit **tout**

```
                     ┌──▶ clients       (clients-group)       reçoit TOUT
topic commandes ─────┤
                     └──▶ statistiques  (statistiques-group)  reçoit TOUT

➜ chaque groupe a son propre marque-page
```

- **Sert à :** des services **différents** qui ont besoin des **mêmes événements** pour faire des choses différentes (compter par client ≠ calculer un CA).

> [!TIP]
> **Moyen mnémotechnique :**
> 👥 **même groupe = une équipe** qui se répartit le courrier
> 🏢 **groupes différents = des services différents** qui reçoivent chacun une copie du courrier

---

## 5. 🐳 Kafka dans Docker

### Le piège des deux adresses (listeners)

Quand on se connecte, Kafka répond : *« pour me joindre, utilise telle adresse »*. Or cette adresse n'est **pas la même** selon d'où on vient :

| Depuis… | Adresse | Pourquoi |
|---|---|---|
| 🐳 un **autre conteneur** | `kafka:9092` | le DNS Docker connaît le nom `kafka` |
| 💻 **ma machine** (Windows) | `localhost:9094` | ma machine ne connaît pas le nom `kafka` |

```yaml
KAFKA_LISTENERS:            PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
```

➡️ Dans le code Python, l'adresse vient d'une **variable d'environnement**, pour marcher dans les deux cas :

```python
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
```
```yaml
environment:
  KAFKA_BOOTSTRAP_SERVERS: kafka:9092   # dans le docker-compose
```

### Docker Compose en bref

| Commande | Effet |
|---|---|
| `docker compose up -d` | démarre tout en arrière-plan |
| `docker compose up -d --build` | idem, en **reconstruisant** les images (après une modif du code) |
| `docker compose down` | arrête et **supprime** les conteneurs (⚠️ et les messages Kafka, sans volume) |

> [!NOTE]
> Compose crée **automatiquement un réseau** commun : tous les services se joignent par leur nom (`kafka`, `clients`…), comme le `docker network create` du TP1, mais sans rien taper.

---

## 6. 🐍 Kafka en Python : le code

📦 Bibliothèque : **`kafka-python`** (`pip install kafka-python`)

### ✍️ Producteur (écrire)

```python
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # dict -> bytes
    key_serializer=lambda k: str(k).encode("utf-8"),
)

evenement = {"commande_id": 4, "client_id": 1, "produit": "PC", "montant": 1200}

future = producer.send("commandes-creees", key=evenement["client_id"], value=evenement)
metadata = future.get(timeout=10)   # attend la confirmation de Kafka
print(metadata.partition, metadata.offset)
```

| Élément | Rôle |
|---|---|
| `send()` | envoie **en arrière-plan** (asynchrone), renvoie une *future* |
| `future.get()` | **attend** que Kafka confirme ➜ on sait que c'est bien arrivé |
| `key=` | choisit la partition ➜ garantit l'ordre pour une même clé |

### 👀 Consommateur (lire)

```python
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "commandes-creees",
    bootstrap_servers="kafka:9092",
    group_id="clients-group",           # mon équipe / mon marque-page
    auto_offset_reset="earliest",       # 1re fois : lire depuis le début
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),  # bytes -> dict
)

for message in consumer:                # boucle infinie, attend les messages
    evt = message.value
    print(message.partition, message.offset, evt["produit"])
```

| Paramètre | Rôle |
|---|---|
| `group_id` | décide du **partage** (même groupe) ou de la **copie** (groupes différents) |
| `auto_offset_reset="earliest"` | si le groupe n'a **jamais rien lu** : partir du début. (`"latest"` = seulement les nouveaux messages) |
| `for message in consumer` | **bloque** et attend les messages indéfiniment |

### 🧵 Consommateur + API Flask dans le même service

La boucle `for` bloque ➜ on la met dans un **thread** pour que Flask puisse répondre en parallèle :

```python
import threading

if __name__ == '__main__':
    threading.Thread(target=consommer, daemon=True).start()   # Kafka en fond
    app.run(host='0.0.0.0', port=8080, debug=True, use_reloader=False)
```

- `daemon=True` ➜ le thread s'arrête avec l'appli
- `use_reloader=False` ➜ sinon Flask (en debug) lance le script **2 fois** = 2 consommateurs 😬

### 🔁 Attendre que Kafka soit prêt

Kafka met quelques secondes à démarrer. Pour ne pas planter au lancement :

```python
from kafka.errors import KafkaError

while True:
    try:
        return KafkaConsumer(...)
    except KafkaError:
        time.sleep(5)   # on réessaie
```

---

## 7. 🛠️ Commandes utiles

```powershell
# voir les logs d'un service en direct (Ctrl+C pour quitter)
docker logs -f clients

# état d'un groupe : offset, lag, qui lit quelle partition
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group clients-group

# lire tout un topic à la main (avec les clés)
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic commandes-creees --from-beginning --property print.key=true

# écrire à la main dans un topic
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic commandes-creees

# créer un topic avec 2 partitions
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic mon-topic --partitions 2 --replication-factor 1

# envoyer du JSON en POST depuis PowerShell
Invoke-RestMethod -Method Post -Uri http://localhost:8081/commandes -ContentType "application/json" -Body '{"client_id": 1, "produit": "PC", "montant": 1200}'
```

🌐 **kafka-ui** ➜ http://localhost:8090 : topics, messages, groupes et lag, en visuel.

---

## 8. 🪤 Pièges rencontrés

| 😵 Symptôme | 🔍 Cause | ✅ Solution |
|---|---|---|
| `ImportError: NoBrokersAvailable` | `kafka-python` 3.x a supprimé cette erreur | attraper `KafkaError` (classe mère de toutes les erreurs) |
| Le conteneur ne joint pas Kafka avec `localhost` | dans un conteneur, `localhost` = **lui-même** | utiliser le nom du conteneur : `kafka:9092` |
| Les `print()` n'apparaissent pas dans `docker logs` | Python garde la sortie en mémoire | `ENV PYTHONUNBUFFERED=1` dans le Dockerfile |
| Rattrapage lent (~45 s) après `docker stop` | l'ancien consommateur n'a pas prévenu Kafka ➜ **rebalancing** | attendre, c'est normal |
| Compteurs à 0 après redémarrage | état en mémoire, pas dans Kafka | stocker en base si on veut le garder |
| Messages disparus après `docker compose down` | Kafka sans **volume** Docker | ajouter un volume (pas nécessaire pour un TP) |
| Un consommateur du groupe ne reçoit rien | plus de consommateurs que de partitions | augmenter le nombre de partitions |

---

## 9. ⚖️ REST ou Kafka : lequel choisir ?

```diff
+ REST  ➜ j'ai besoin d'une RÉPONSE tout de suite
+         « donne-moi les infos du client 1 »

+ Kafka ➜ j'ANNONCE un fait, je n'attends rien
+         « la commande 4 a été créée » ➜ qui veut l'entend
```

| Utiliser **REST** 📞 | Utiliser **Kafka** 📮 |
|---|---|
| lire une donnée (`GET`) | notifier plusieurs services d'un événement |
| besoin d'une réponse immédiate | le destinataire peut être éteint un moment |
| 1 appelant ➜ 1 destinataire | 1 émetteur ➜ N lecteurs |
| simple à mettre en place | découpler les services, absorber de gros volumes |

> [!NOTE]
> Les deux cohabitent très bien : dans mon TP2, chaque service garde son **API REST** pour qu'un humain le consulte, mais les services se parlent **entre eux** via Kafka.

---

## 10. 🔙 Rappels TP1 : REST & Docker

### Flask en 5 lignes

```python
from flask import Flask, jsonify
app = Flask(__name__)

@app.route('/clients/<int:client_id>', methods=['GET'])
def get_client(client_id):
    ...
    return jsonify({"erreur": "Client introuvable"}), 404   # code HTTP en 2e valeur

app.run(host='0.0.0.0', port=8080)   # 0.0.0.0 = accessible depuis l'extérieur du conteneur
```

### Appeler une autre API (`requests`)

```python
try:
    r = requests.get(f"{CLIENTS_URL}/clients/1", timeout=5)
except requests.exceptions.RequestException:
    return jsonify({"erreur": "service indisponible"}), 503
```

### Codes HTTP utilisés

| Code | Sens |
|---|---|
| 🟢 `200` | OK |
| 🟢 `201` | Créé (après un POST) |
| 🟠 `400` | Requête mal formée (champ manquant) |
| 🟠 `404` | Introuvable |
| 🔴 `502` | Le service appelé a renvoyé une erreur |
| 🔴 `503` | Le service appelé est injoignable |

### Docker : l'essentiel

| Notion | À retenir |
|---|---|
| **Image** vs **conteneur** | l'image = la recette, le conteneur = le plat qui tourne |
| `-p 8081:8080` | **port machine : port conteneur**. Chaque conteneur a ses propres ports ➜ deux services peuvent écouter sur 8080 à l'intérieur |
| Réseau créé (`docker network create`) | les conteneurs se joignent **par leur nom** (DNS Docker) |
| `EXPOSE` | simple documentation, n'ouvre rien (c'est `-p` qui ouvre) |
| Ordre du Dockerfile | copier `requirements.txt` **avant** `app.py` ➜ le `pip install` reste en cache |
| `-e VAR=valeur` | passer une config au conteneur sans toucher au code |
