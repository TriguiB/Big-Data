import streamlit as st
import mlflow
import mlflow.sklearn
import joblib
import pandas as pd
from scipy import sparse

# Configuration de MLflow Tracking URI
mlflow.set_tracking_uri("../mlruns")
model_name = "MyLogisticModel"

# Colonnes attendues pour le one-hot encoding de la langue
LANGUAGE_COLUMNS = ["Python", "Java", "JavaScript", "C++", "Ruby", "Go", "PHP", "Other"]

# Correspondance entre les classes et la durée en jours
DURATION_CLASSES = {
    1: "0 jour (Très rapide)",
    2: "1-2 jours (Rapide)",
    3: "3-5 jours (Moyenne)",
    4: "6-8 jours (Lente)",
    5: "9+ jours (Très lente)"
}

def load_model_and_vectorizer():
    """Charge le modèle et le vectorizer depuis MLflow."""
    model = mlflow.sklearn.load_model(f"models:/{model_name}/Production")

    # Charger le vectorizer depuis le fichier sauvegardé
    vectorizer_path = "../models/tfidf_vectorizer.pkl"
    vectorizer = joblib.load(vectorizer_path)

    return model, vectorizer

def predict(model, vectorizer, text_input, language_input):
    """Effectue la prédiction de la classe de durée."""
    # Transformer le texte avec le vectorizer
    test_tfidf = vectorizer.transform([text_input])

    # Convertir la langue en une représentation one-hot
    test_language_feature = pd.get_dummies([language_input]).reindex(columns=LANGUAGE_COLUMNS, fill_value=0)
    test_language_feature = test_language_feature.values.astype(float)
    test_language_feature_sparse = sparse.csr_matrix(test_language_feature)

    # Combiner les features de texte et de langue
    test_input = sparse.hstack([test_tfidf, test_language_feature_sparse])

    # Obtenir la prédiction
    prediction = model.predict(test_input)[0]

    # Retourner la classe et la durée correspondante
    return prediction, DURATION_CLASSES.get(prediction, "Inconnu")

# --- Interface Streamlit ---
st.title("⏳ Prédiction de la classe de durée pour résoudre un issue GitHub")
st.write("Entrez le titre, le corps du problème et le langage de programmation pour prédire la classe de durée.")

# Champs d'entrée utilisateur
title = st.text_input("📌 Titre du problème")
body = st.text_area("📝 Corps du problème")
language = st.selectbox("💻 Langage de programmation", LANGUAGE_COLUMNS)

# Charger le modèle et le vectorizer
model, vectorizer = load_model_and_vectorizer()

if st.button("🔮 Prédire"):
    if title and body:
        text_input = title + " " + body
        prediction, duration_info = predict(model, vectorizer, text_input, language)

        # Affichage du résultat
        st.success(f"📊 **Classe de durée prédite : {prediction}**")
        st.markdown(f"⏱️ **Durée estimée : {duration_info}**")
    else:
        st.warning("⚠️ Veuillez entrer un titre et un corps pour la prédiction.")