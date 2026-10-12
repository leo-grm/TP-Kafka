FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# le modèle est embarqué dans l'image, à côté du code
COPY app.py modele_commande.pkl ./

CMD ["python", "app.py"]
