import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import joblib
import os

def train():
    # 1. Chargement (Assurez-vous que le nom correspond au fichier surveillé par Airflow)
    data_path = 'data/raw_data.csv'
    if not os.path.exists(data_path):
        print(f"Erreur : {data_path} introuvable.")
        return

    df = pd.read_csv(data_path)

    # 2. Nettoyage spécifique au dataset Maintenance
    # On supprime les IDs uniques (UDI et Product ID) qui ne servent pas à la prédiction
    df_cleaned = df.drop(['UDI', 'Product ID'], axis=1)
    
    # Encodage de la colonne 'Type' (L, M, H) en chiffres
    le = LabelEncoder()
    df_cleaned['Type'] = le.fit_transform(df_cleaned['Type'])

    # 3. Séparation Features/Target
    # La cible est 'Machine failure'
    X = df_cleaned.drop('Machine failure', axis=1)
    y = df_cleaned['Machine failure']
    
    # 4. Entraînement
    model = RandomForestClassifier(n_estimators=100)
    model.fit(X, y)
    
    # 5. Sauvegarde
    os.makedirs('models', exist_ok=True)
    joblib.dump(model, 'models/model.pkl')
    print(f"Modèle entraîné avec {len(df)} lignes. Sauvegardé dans models/model.pkl")

if __name__ == "__main__":
    train()