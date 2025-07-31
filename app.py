# Pour lancer le serveur : python app.py
# Puis acceder au site sur localhost:5000

from flask import Flask, request, send_from_directory, jsonify, Response
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import date, timedelta
import io
import csv

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.precision', 0)

app = Flask(__name__, static_url_path='')

config = pd.read_csv('config.csv', header=None)
id = "postgres"  # config[0][0]
pwd = "MotDePasseDVF"  # config[0][1]
host = "localhost"  # config[0][2]
db = "tj-paris-5-ans"  # config[0][3] [dvf2, licitor-tj-paris, licitor-tj-i-1-mois, tj-paris-1-an, tj-paris-5-ans]
engine = create_engine('postgresql://%s:%s@%s/%s'%(id, pwd, host, db))

# Chargement des natures de culture plus besoin

@app.route('/stats/<ville>')
def stats_ville(ville):
    date_min = (date.today() - timedelta(days=10_000)).isoformat()
    ville_pattern = f'%{ville}%'

    query = """
        SELECT "nom_commune", 
               "valeur_fonciere",
               "lot1_surface_carrez",
               "date_mutation",
               "type_local",
               "adresse_numero", "adresse_suffixe", "adresse_nom_voie", "adresse_code_voie", "code_postal",
               "nombre_lots"
        FROM dvf_2020_2024
        WHERE "nom_commune" ILIKE %(ville_pattern)s
        AND "type_local" IN ('Appartement', 'Maison')
        AND "nature_mutation" = 'Adjudication'
        AND CAST("nombre_lots" AS INTEGER) = 1
        AND "date_mutation" >= %(date_min)s
    """
    """
    
    """
    df = pd.read_sql_query(query, engine, params={
        'ville_pattern': ville_pattern,
        'date_min': date_min
    })
    
    if df.empty:
        return jsonify({
        "ville": ville.upper(),
        "nombre_transactions": 0,
        "prix_m2_moyen": None,
        "nb_biens_revendus_plus_50000": 0,
        "zventes": []
    })

    # Convertir surface et valeur_fonciere
    df['surface'] = pd.to_numeric(df['lot1_surface_carrez'], errors='coerce')
    df['valeur_fonciere'] = pd.to_numeric(df['valeur_fonciere'], errors='coerce')

    # Nettoyage
    #df = df[df['surface'].notnull() & (df['surface'] > 0)]
    df = df[df['valeur_fonciere'].notnull()]

    # Calcul du prix au m²
    df['prix_m2'] = df['valeur_fonciere'] / df['surface']
    #df = df[(df['prix_m2'] > 1000) & (df['prix_m2'] < 25000)]
    #df = df[(df['surface'] > 5) & (df['surface'] < 1000)]

    # Adresse complète
    adresse_cols = ["adresse_numero", "adresse_suffixe", "adresse_nom_voie", "adresse_code_voie", "code_postal"]
    df['adresse'] = df[adresse_cols].fillna('').astype(str).agg(' '.join, axis=1).str.replace(r'\s+', ' ', regex=True).str.strip()

    # Clé pour identifier un bien
    df['cle_bien'] = df['adresse'] + ' | ' + df['surface'].astype(str)

    # Conversion de la date
    df['date'] = pd.to_datetime(df['date_mutation'], errors='coerce')
    df = df[df['date'].notnull()]

    # Détection des plus-values
    plus_values = 0
    for _, group in df.groupby('cle_bien'):
        ventes = group.sort_values('date')
        ventes_list = ventes[['date', 'valeur_fonciere']].values
        for i in range(1, len(ventes_list)):
            date1, prix1 = ventes_list[i -1]
            date2, prix2 = ventes_list[i]
            if (date2 - date1).days > 10 and (prix2 - prix1) >= 50_000:
                plus_values += 1

    # Moyenne du prix au m²
    prix_m2_moyen = df['prix_m2'].mean()

    # Préparation des données de sortie
    ventes = df[['adresse', 'date_mutation', 'type_local', 'valeur_fonciere', 'surface', 'prix_m2']].copy()
    ventes['prix_m2'] = ventes['prix_m2'].round(2)
    ventes['valeur_fonciere'] = ventes['valeur_fonciere'].round(0)
    ventes = ventes.to_dict(orient='records')

    return jsonify({
        'ville': ville.upper(),
        'nombre_transactions': len(df),
        'prix_m2_moyen': round(prix_m2_moyen, 2) if not pd.isna(prix_m2_moyen) else None,
        'nb_biens_revendus_plus_50000': plus_values,
        'zventes': ventes
    })


@app.route('/plus_values/<ville>')
def plus_values_ville(ville):
    from datetime import date, timedelta
    import pandas as pd

    date_min = (date.today() - timedelta(days=10_000)).isoformat()
    ville_pattern = f'%{ville}%'

    query = """
        SELECT 
            nom_commune,
            valeur_fonciere,
			lot1_surface_carrez,
			date_mutation,
			type_local,
            adresse_numero, adresse_suffixe, adresse_nom_voie, adresse_code_voie, code_postal,
			nombre_lots
        FROM dvf_2020_2024
        WHERE nom_commune ILIKE %(ville_pattern)s
          AND type_local IN ('Appartement', 'Maison')
          AND nature_mutation = 'Vente'
          AND CAST("nombre_lots" AS INTEGER) = 1
          AND date_mutation >= %(date_min)s
    """

    df = pd.read_sql_query(query, engine, params={
        'ville_pattern': ville_pattern,
        'date_min': date_min
    })

    if df.empty:
        return jsonify({
        "ville": ville.upper(),
        "nombre_transactions": 0,
        "prix_m2_moyen": None,
        "nb_biens_revendus_plus_50000": 0,
        "zventes": []
    })

    # Convertir les types
    df['valeur_fonciere'] = df['valeur_fonciere'].astype(float)
    df['lot1_surface_carrez'] = df['lot1_surface_carrez'].astype(float)

    # Nettoyage de base
    df = df[df['lot1_surface_carrez'] > 0]

    # Construire l'adresse complète
    df['adresse'] = (
        df['adresse_numero'].fillna('').astype(str) + ' ' +
        df['adresse_suffixe'].fillna('').astype(str) + ' ' +
        df['adresse_nom_voie'].fillna('').astype(str) + ' ' +
        df['code_postal'].fillna('').astype(str)
    ).str.replace(r'\s+', ' ', regex=True).str.strip()

    # Clé unique pour un bien (adresse + surface)
    df['cle_bien'] = df['adresse'] + ' | ' + df['lot1_surface_carrez'].astype(str)

    # Conversion de la date mutation
    df['date'] = pd.to_datetime(df['date_mutation'], errors='coerce')
    df = df[df['date'].notnull()]

    # Calcul du prix au m² et filtrage des valeurs aberrantes
    df['prix_m2'] = df['valeur_fonciere'] / df['lot1_surface_carrez']
    df = df[(df['prix_m2'] > 1000) & (df['prix_m2'] < 25000)]
    df = df[(df['lot1_surface_carrez'] > 5) & (df['lot1_surface_carrez'] < 1000)]

    # Statistiques globales
    nb_ventes_valides = len(df)
    prix_m2_moyen = round(df['prix_m2'].mean(), 2) if nb_ventes_valides > 0 else None

    # Détection des plus-values sur reventes
    resultats = []
    for cle, group in df.groupby('cle_bien'):
        ventes = group.sort_values('date')
        ventes_list = ventes[['date', 'valeur_fonciere']].values

        for i in range(1, len(ventes_list)):
            date1, prix1 = ventes_list[i - 1]
            date2, prix2 = ventes_list[i]
            if (date2 - date1).days > 30 and (prix2 - prix1) >= 20_000:
                ventes_sel = ventes.iloc[[i - 1, i]].copy()
                ventes_sel = ventes_sel[[
                    'nom_commune', 'date_mutation', 'type_local', 'adresse',
                    'prix_m2', 'lot1_surface_carrez', 'valeur_fonciere'
                ]]
                ventes_sel.rename(columns={'lot1_surface_carrez': 'surface'}, inplace=True)
                ventes_sel['prix_m2'] = ventes_sel['prix_m2'].round(2)
                resultats.extend(ventes_sel.to_dict(orient='records'))

    return jsonify({
        'ville': ville.upper(),
        'nombre_ventes_valides': nb_ventes_valides,
        'prix_m2_moyen': prix_m2_moyen,
        'nombre_plus_values': len(resultats) // 2,
        'zplus_values': resultats,
    })


@app.route('/plus_values_creation_piece/<ville>')
def plus_values_creation_piece(ville):
    from datetime import date, timedelta
    import pandas as pd

    date_min = (date.today() - timedelta(days=1000)).isoformat()
    ville_pattern = f'%{ville}%'

    query = """
        SELECT 
            nom_commune,
            valeur_fonciere,
			lot1_surface_carrez,
			date_mutation,
			type_local,
            adresse_numero, adresse_suffixe, adresse_nom_voie, adresse_code_voie, code_postal,
			nombre_lots,
            nombre_pieces_principales
        FROM dvf_2020_2024
        WHERE nom_commune ILIKE %(ville_pattern)s
          AND type_local IN ('Appartement', 'Maison')
          AND nature_mutation = 'Vente'
          AND CAST("nombre_lots" AS INTEGER) = 1
          AND date_mutation >= %(date_min)s
    """

    df = pd.read_sql_query(query, engine, params={
        'ville_pattern': ville_pattern,
        'date_min': date_min
    })

    if df.empty:
        return jsonify({
        "ville": ville.upper(),
        "nombre_transactions": 0,
        "prix_m2_moyen": None,
        "nb_biens_revendus_plus_50000": 0,
        "zventes": []
    })

    # Convertir les types
    df['valeur_fonciere'] = df['valeur_fonciere'].astype(float)
    df['lot1_surface_carrez'] = df['lot1_surface_carrez'].astype(float)
    df['nombre_pieces_principales'] = pd.to_numeric(df['nombre_pieces_principales'], errors='coerce')

    # Nettoyage de base
    df = df[df['lot1_surface_carrez'] > 0]

    # Construire l'adresse complète
    df['adresse'] = (
        df['adresse_numero'].fillna('').astype(str) + ' ' +
        df['adresse_suffixe'].fillna('').astype(str) + ' ' +
        df['adresse_nom_voie'].fillna('').astype(str) + ' ' +
        df['code_postal'].fillna('').astype(str)
    ).str.replace(r'\s+', ' ', regex=True).str.strip()

    # Clé unique pour un bien (adresse)
    df['cle_bien'] = df['adresse']

    # Conversion de la date mutation
    df['date'] = pd.to_datetime(df['date_mutation'], errors='coerce')
    df = df[df['date'].notnull()]

    # Calcul du prix au m² et filtrage des valeurs aberrantes
    df['prix_m2'] = df['valeur_fonciere'] / df['lot1_surface_carrez']
    df = df[(df['prix_m2'] > 1000) & (df['prix_m2'] < 25000)]
    df = df[(df['lot1_surface_carrez'] > 5) & (df['lot1_surface_carrez'] < 1000)]

    # Statistiques globales
    nb_ventes_valides = len(df)
    prix_m2_moyen = round(df['prix_m2'].mean(), 2) if nb_ventes_valides > 0 else None

    # Détection des plus-values sur reventes
    resultats = []
    for cle, group in df.groupby('cle_bien'):
        ventes = group.sort_values('date')
        ventes_list = ventes[['date', 'valeur_fonciere', 'lot1_surface_carrez', 'nombre_pieces_principales']].values

        for i in range(1, len(ventes_list)):
            date1, prix1, surface1, pieces1 = ventes_list[i - 1]
            date2, prix2, surface2, pieces2 = ventes_list[i]

            if (
                (date2 - date1).days > 30 and
                (prix2 - prix1) >= 20_000 and
                pieces2 == pieces1 + 1 and
                surface2 >= surface1 * 0.95 and
                surface2 <= surface1 * 1.05
                #surface2 == surface1
            ):
                
                ventes_sel = ventes.iloc[[i - 1, i]].copy()
                ventes_sel = ventes_sel[[
                    'nom_commune', 'date_mutation', 'type_local', 'adresse',
                    'prix_m2', 'lot1_surface_carrez', 'valeur_fonciere', 'nombre_pieces_principales'
                ]]
                ventes_sel.rename(columns={'lot1_surface_carrez': 'surface'}, inplace=True)
                ventes_sel['prix_m2'] = ventes_sel['prix_m2'].round(2)
                resultats.extend(ventes_sel.to_dict(orient='records'))

    return jsonify({
        'ville': ville.upper(),
        'nombre_ventes_valides': nb_ventes_valides,
        'prix_m2_moyen': prix_m2_moyen,
        'nombre_plus_values': len(resultats) // 2,
        'zplus_values': resultats,
    })


@app.route('/prix_m2_par_ville')
def prix_m2_par_ville():
    import pandas as pd

    query = """
        SELECT 
            nom_commune,
            valeur_fonciere::float AS valeur_fonciere,
            lot1_surface_carrez::float AS surface,
            date_mutation,
            type_local,
            adresse_numero,
            adresse_suffixe,
            adresse_nom_voie,
            adresse_code_voie,
            code_postal,
            nombre_lots
        FROM dvf_2020_2024
        WHERE 
            type_local IN ('Appartement', 'Maison')
            AND nature_mutation = 'Vente'
            AND CAST("nombre_lots" AS INTEGER) = 1
    """
    """
    AND lot1_surface_carrez IS NOT NULL
    AND lot1_surface_carrez::text ~ '^[0-9]+(\\.[0-9]+)?$'
    AND valeur_fonciere IS NOT NULL
    AND valeur_fonciere::text ~ '^[0-9]+(\\.[0-9]+)?$'
    AND CAST("lot1_surface_carrez" AS FLOAT) > 0
    """

    df = pd.read_sql_query(query, engine)

    # Calcul du prix au m²
    df['prix_m2'] = df['valeur_fonciere'] / df['surface']

    # Filtrage valeurs aberrantes
    df = df[(df['prix_m2'] > 1000) & (df['prix_m2'] < 25000)]
    df = df[(df['surface'] > 5) & (df['surface'] < 1000)]

    # Supprimer doublons : même date, prix, et nom de voie
    df['nom_voie'] = df['adresse_nom_voie'].str.lower().str.strip()
    dupes = df.duplicated(subset=['date_mutation', 'valeur_fonciere', 'nom_voie'], keep=False)
    df = df[~dupes]

    # Normaliser les noms pour regrouper Paris, Lyon, Marseille
    df['nom_commune'] = df['nom_commune'].replace(
    regex={
        r'^Paris \d{1,2}(er|e)? Arrondissement$': 'Paris',
        r'^Marseille \d{1,2}(er|e)? Arrondissement$': 'Marseille',
        r'^Lyon \d{1,2}(er|e)? Arrondissement$': 'Lyon'
        }
    )

    # Agrégation par ville
    grouped = df.groupby('nom_commune').agg(
        prix_m2_moyen=('prix_m2', 'mean'),
        nb_ventes=('prix_m2', 'count')
    ).reset_index()

    grouped['prix_m2_moyen'] = grouped['prix_m2_moyen'].round(2)
    grouped = grouped[grouped['nb_ventes'] > 30]  # 30

    # Tri décroissant
    result = grouped.sort_values(by='prix_m2_moyen', ascending=False)

    
    # Création du CSV en mémoire
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['nom_commune', 'prix_m2_moyen', 'nb_ventes'])

    for _, row in result.iterrows():
        writer.writerow([row['nom_commune'], row['prix_m2_moyen'], row['nb_ventes']])

    output.seek(0)
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': 'attachment; filename=prix_m2_par_ville.csv'})
    

    return Response(
        json.dumps(result.to_dict(orient='records'), ensure_ascii=False),
        content_type='application/json; charset=utf-8'
    )


@app.route('/classement_plus_values')
def classement_plus_values():
    import pandas as pd
    from datetime import date, timedelta

    date_min = (date.today() - timedelta(days=10_000)).isoformat()

    query = """
        SELECT 
            nom_commune,
            valeur_fonciere,
            lot1_surface_carrez,
            date_mutation,
            type_local,
            adresse_numero, adresse_suffixe, adresse_nom_voie, adresse_code_voie, code_postal,
            nombre_lots,
            nombre_pieces_principales
        FROM dvf_2020_2024
        WHERE 
            type_local IN ('Appartement', 'Maison')
            AND nature_mutation = 'Vente'
            AND CAST(nombre_lots AS INTEGER) = 1
            AND date_mutation >= %(date_min)s
    """
    """
    AND LEFT(code_postal, 2) IN ('75', '77', '78', '91', '92', '93', '94', '95')
    nombre_pieces_principales
    """
    df = pd.read_sql_query(query, engine, params={'date_min': date_min})

    df['valeur_fonciere'] = df['valeur_fonciere'].astype(float)
    df['surface'] = df['lot1_surface_carrez'].astype(float)
    df['nombre_pieces_principales'] = pd.to_numeric(df['nombre_pieces_principales'], errors='coerce')

    # Nettoyage
    df = df[df['surface'].notnull() & (df['surface'] > 0)]
    df = df[df['valeur_fonciere'].notnull()]
    df['prix_m2'] = df['valeur_fonciere'] / df['surface']

    df = df[(df['prix_m2'] > 1000) & (df['prix_m2'] < 25000)]
    df = df[(df['surface'] > 5) & (df['surface'] < 1000)]

    adresse_cols = ["adresse_numero", "adresse_suffixe", "adresse_nom_voie", "adresse_code_voie", "code_postal"]
    df['adresse'] = df[adresse_cols].fillna('').astype(str).agg(' '.join, axis=1).str.replace(r'\s+', ' ', regex=True).str.strip()
    df['cle_bien'] = df['adresse']
    df['date'] = pd.to_datetime(df['date_mutation'], errors='coerce')
    df = df[df['date'].notnull()]

    # Calcul des plus-values par ville
    result = []

    for ville, group in df.groupby('nom_commune'):
        ventes = group.copy().sort_values('date')
        nb_ventes = len(ventes)
        if nb_ventes < 30:
            continue

        plus_values = 0
        for _, g in ventes.groupby('cle_bien'):
            ventes_list = g[['date', 'valeur_fonciere', 'surface', 'nombre_pieces_principales']].values
            for i in range(1, len(ventes_list)):
                date1, prix1, surface1, pieces1 = ventes_list[i - 1]
                date2, prix2, surface2, pieces2 = ventes_list[i]
                
                if (
                    (date2 - date1).days > 30 and
                    (prix2 - prix1) >= 20_000 and
                    pieces2 == pieces1 + 1 and
                    surface2 >= surface1 * 0.95 and surface2 <= surface1 * 1.05
                ):    
                    plus_values += 1

        ratio = plus_values / nb_ventes if nb_ventes else 0
        prix_m2_moyen = ventes['prix_m2'].mean() if not ventes['prix_m2'].empty else None

        result.append({
            'nom_commune': ville,
            "prix_m2_moyen": round(prix_m2_moyen, 2) if prix_m2_moyen else None,
            'nb_ventes': nb_ventes,
            'nb_plus_values': plus_values,
            'ratio_plus_value': round(ratio, 4)
        })

    result = sorted(result, key=lambda x: x['nb_plus_values'], reverse=True)

    """
    # Création du CSV en mémoire
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['nom_commune', 'prix_m2_moyen', 'nb_ventes', 'nb_plus_values', 'ratio_plus_value'])

    for row in result:
        writer.writerow([
            row['nom_commune'],
            row.get('prix_m2_moyen', ''),
            row['nb_ventes'],
            row['nb_plus_values'],
            f"{row['ratio_plus_value']:.2f}"
        ])

    output.seek(0)
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': 'attachment; filename=classement_plus_values_creation_piece.csv'})
    """
                    
    return Response(
        json.dumps(result, ensure_ascii=False),
        content_type='application/json; charset=utf-8'
    )
    

@app.route('/classement_adjudications')
def classement_adjudications():
    import pandas as pd
    import io
    import csv
    from flask import Response

    query = """
        SELECT DISTINCT
            nom_commune,
            date_mutation,
            valeur_fonciere,
            adresse_numero, adresse_suffixe, adresse_nom_voie, adresse_code_voie, code_postal
        FROM dvf_2020_2024
        WHERE nature_mutation = 'Adjudication'
    """

    df = pd.read_sql_query(query, engine)

    # Construction d’une adresse unique pour éviter les doublons
    adresse_cols = ["adresse_numero", "adresse_suffixe", "adresse_nom_voie", "adresse_code_voie", "code_postal"]
    df['adresse'] = df[adresse_cols].fillna('').astype(str).agg(' '.join, axis=1).str.replace(r'\s+', ' ', regex=True).str.strip()

    # On compte une seule adjudication par bien (adresse + date) pour éviter les doublons
    df['cle'] = df['adresse'] + ' | ' + df['date_mutation'].astype(str)
    df = df.drop_duplicates(subset=['cle'])

    # Comptage par commune
    result = (
        df['nom_commune']
        .value_counts()
        .reset_index()
        .rename(columns={'index': 'nom_commune', 'nom_commune': 'nb_adjudications'})
        .sort_values(by='nb_adjudications', ascending=False)
    )

    # Export CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(result.columns)
    for row in result.itertuples(index=False):
        writer.writerow(row)

    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=classement_adjudications.csv'}
    )

##########
@app.route('/licitor_classement_adjudications')
def licitor_classement_adjudications():
    import pandas as pd
    import io
    import csv
    from flask import Response

    query = """
        SELECT
            "ville",
            "surface",
            "prix_d'adjudication"
        FROM "tj-paris-5-ans"

        WHERE "type_de_bien" IN ('un-appartement', 'une-maison', 'un-studio', 'un-duplex', 'un-logement', 
        'une-chambre-de-service', 'une-piece', 'une-habitation', 'une-unite-d-habitation', 
        'un-appartement-en-duplex', 'une-propriete', 'un-bien')

    """

    df = pd.read_sql_query(query, engine)

    # Nettoyage : conversion virgule → point
    df['surface'] = df['surface'].str.replace(',', '.', regex=False)
    df['prix_d\'adjudication'] = df['prix_d\'adjudication'].astype(str).str.replace(',', '.', regex=False)

    # Conversion en float
    df['surface'] = pd.to_numeric(df['surface'], errors='coerce')
    df['prix_d\'adjudication'] = pd.to_numeric(df['prix_d\'adjudication'], errors='coerce')

    # Suppression des lignes incomplètes ou aberrantes
    df = df.dropna(subset=['ville', 'surface', 'prix_d\'adjudication'])
    df = df[(df['surface'] > 5) & (df['surface'] < 1000)]
    df = df[df['prix_d\'adjudication'] > 1000]

    # Calcul prix au m²
    df['prix_m2'] = df['prix_d\'adjudication'] / df['surface']

    # Agrégation
    grouped = df.groupby('ville').agg(
        prix_m2_moyen=('prix_m2', 'mean'),
        nb_adjudications=('prix_m2', 'count')
    ).reset_index()

    grouped['prix_m2_moyen'] = grouped['prix_m2_moyen'].round(2)

    # Si tu veux voir toutes les villes, commente cette ligne :
    # grouped = grouped[grouped['nb_adjudications'] >= 5]

    # Tri
    result = grouped.sort_values(by='prix_m2_moyen', ascending=False)

    # Export CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(result.columns)
    for row in result.itertuples(index=False):
        writer.writerow(row)

    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=prix_m2_adjudications.csv'}
    )



if __name__ == '__main__':
	app.run(debug=True)
