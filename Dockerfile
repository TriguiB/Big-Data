# Utilise l'image Bitnami Spark
FROM bitnami/spark:latest
# Définir le répertoire de travail
WORKDIR /app

# Installer Python et pip (nécessaire pour Streamlit et MLflow)
USER root
RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir --upgrade pip

# Copier les dépendances et les installer avec pip
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code source dans le conteneur
COPY . /app/

EXPOSE 8501

# Lancer un terminal pour coder dans le conteneur
CMD ["bash"]