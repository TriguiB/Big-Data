import streamlit as st
import mlflow
import mlflow.keras
import joblib
import pandas as pd
from scipy import sparse
import numpy as np

# Configuration de MLflow Tracking URI
mlflow.set_tracking_uri("../mlruns")
model_name = "MyNNModel"

# Colonnes attendues pour le one-hot encoding de la langue
LANGUAGE_COLUMNS = ["Python", "Java", "JavaScript", "C++", "Ruby", "Go", "PHP", "Other"]

def load_model_and_artifacts():
    # Charger le modèle depuis le MLflow Model Registry en stage "Production"
    model = mlflow.keras.load_model(f"models:/{model_name}/Production")
    
    # Charger le vectorizer et le label encoder depuis les fichiers sauvegardés
    vectorizer_path = "../models/tfidf_vectorizer.pkl"
    vectorizer = joblib.load(vectorizer_path)
    
    labelencoder_path = "../models/label_encoder.pkl"
    le = joblib.load(labelencoder_path)
    
    return model, vectorizer, le

def predict(model, vectorizer, le, text_input, language_input):
    # Transformer le texte avec le vectorizer
    test_tfidf = vectorizer.transform([text_input])
    
    # Convertir la langue en une représentation one-hot basée sur LANGUAGE_COLUMNS
    test_language_feature = pd.get_dummies([language_input]).reindex(columns=LANGUAGE_COLUMNS, fill_value=0)
    test_language_feature = test_language_feature.values.astype(float)
    test_language_feature_sparse = sparse.csr_matrix(test_language_feature)
    
    # Combiner les features de texte et de langue
    test_input_sparse = sparse.hstack([test_tfidf, test_language_feature_sparse])
    # Conversion en dense pour la prédiction
    test_input = test_input_sparse.toarray()
    
    # Obtenir les probabilités de prédiction et convertir en classe
    pred_prob = model.predict(test_input)
    pred_class_index = np.argmax(pred_prob, axis=1)[0]
    pred_label = le.inverse_transform([pred_class_index])[0]
    return pred_label

# --- Interface Streamlit ---
st.title("Prédiction de la durée d'un problème sur GitHub avec NN")
st.write("Entrez le titre, le corps du problème et le langage de programmation pour prédire la classe de durée.")

# Champs d'entrée
title = st.text_input("Titre du problème")
body = st.text_area("Corps du problème")
language = st.selectbox("Langage de programmation", LANGUAGE_COLUMNS)

# Charger le modèle et les artefacts (une seule fois)
model, vectorizer, le = load_model_and_artifacts()

if st.button("Prédire"):
    if title and body:
        text_input = title + " " + body
        prediction = predict(model, vectorizer, le, text_input, language)
        st.write(f"Classe de durée prédite : {prediction}")
    else:
        st.write("Veuillez entrer un titre et un corps pour la prédiction.")