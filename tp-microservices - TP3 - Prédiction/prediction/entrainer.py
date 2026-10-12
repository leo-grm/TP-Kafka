import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# à lancer une seule fois, depuis le dossier prediction : python entrainer.py
# le microservice, lui, ne fait que charger le .pkl

COLONNES = ["age", "anciennete", "nombre_commandes", "montant_total",
            "montant_moyen", "jours_depuis_derniere_commande"]

# pas de vraies données historiques, donc on invente 2000 clients
rng = np.random.default_rng(0)
n = 2000

df = pd.DataFrame({
    "age": rng.integers(18, 80, n),
    "anciennete": rng.integers(0, 10, n),
    "nombre_commandes": rng.integers(1, 30, n),
    "montant_moyen": rng.uniform(10, 300, n).round(2),
    "jours_depuis_derniere_commande": rng.integers(0, 365, n),
})
df["montant_total"] = df["nombre_commandes"] * df["montant_moyen"]

# règle inventée : celui qui commande souvent et récemment recommande, + un peu de hasard
score = df["nombre_commandes"] / 30 - df["jours_depuis_derniere_commande"] / 365 + rng.normal(0, 0.2, n)
y = (score > 0).astype(int)

X_train, X_test, y_train, y_test = train_test_split(df[COLONNES], y, random_state=0)

model = RandomForestClassifier(n_estimators=50, max_depth=5, random_state=0)
model.fit(X_train, y_train)
print("précision sur les données de test :", model.score(X_test, y_test))

joblib.dump(model, "modele_commande.pkl")
