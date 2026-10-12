FROM python:3.12-slim

WORKDIR /app

# pour voir les print() tout de suite dans docker logs
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8080

CMD ["python", "app.py"]
