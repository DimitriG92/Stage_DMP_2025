import pandas as pd
from sqlalchemy import create_engine, text

# Configuration PostgreSQL
DB_USER = 'postgres'
DB_PASSWORD = 'MotDePasseDVF'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'tj-paris-5-ans'
TABLE_NAME = 'tj-paris-5-ans'

# Chemin vers le fichier CSV (à adapter selon ton système ou WSL)
CSV_PATH = '/mnt/c/Users/dgard/Downloads/tj-paris-5-ans.csv'

# Connexion à la base PostgreSQL
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Suppression de la table si elle existe
with engine.begin() as conn:
    conn.execute(text(f'DROP TABLE IF EXISTS "{TABLE_NAME}"'))

# Lecture par blocs pour éviter les erreurs de mémoire
chunksize = 10000
first_chunk = True

for chunk in pd.read_csv(CSV_PATH, sep=',', encoding='utf-8', dtype=str, chunksize=chunksize):
    # Nettoyage éventuel des noms de colonnes (supprime les espaces)
    chunk.columns = [col.strip().replace(" ", "_").lower() for col in chunk.columns]

    chunk.to_sql(TABLE_NAME, engine, if_exists='replace' if first_chunk else 'append', index=False)
    first_chunk = False
    print(f"{len(chunk)} lignes insérées...")

print("Base de données initialisée avec succès.")

