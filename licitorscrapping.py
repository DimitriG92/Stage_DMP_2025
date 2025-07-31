import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import csv
import time
import random


jours_fr = {
    "Monday": "lundi",
    "Tuesday": "mardi",
    "Wednesday": "mercredi",
    "Thursday": "jeudi",
    "Friday": "vendredi",
    "Saturday": "samedi",
    "Sunday": "dimanche"
}


mois_fr = {
    "January": "janvier",
    "February": "fevrier",
    "March": "mars",
    "April": "avril",
    "May": "mai",
    "June": "juin",
    "July": "juillet",
    "August": "aout",
    "September": "septembre",
    "October": "octobre",
    "November": "novembre",
    "December": "decembre"
}


# Table de correspondance nom → code
dep_map = {
    "Ain": "01", "Aisne": "02", "Allier": "03", "Alpes-de-Haute-Provence": "04",
    "Hautes-Alpes": "05", "Alpes-Maritimes": "06", "Ardèche": "07", "Ardennes": "08",
    "Ariège": "09", "Aube": "10", "Aude": "11", "Aveyron": "12", "Bouches-du-Rhône": "13",
    "Calvados": "14", "Cantal": "15", "Charente": "16", "Charente-Maritime": "17",
    "Cher": "18", "Corrèze": "19", "Corse-du-Sud": "2A", "Haute-Corse": "2B",
    "Côte-d'Or": "21", "Côtes-d'Armor": "22", "Creuse": "23", "Dordogne": "24",
    "Doubs": "25", "Drôme": "26", "Eure": "27", "Eure-et-Loir": "28", "Finistère": "29",
    "Gard": "30", "Haute-Garonne": "31", "Gers": "32", "Gironde": "33", "Hérault": "34",
    "Ille-et-Vilaine": "35", "Indre": "36", "Indre-et-Loire": "37", "Isère": "38",
    "Jura": "39", "Landes": "40", "Loir-et-Cher": "41", "Loire": "42", "Haute-Loire": "43",
    "Loire-Atlantique": "44", "Loiret": "45", "Lot": "46", "Lot-et-Garonne": "47",
    "Lozère": "48", "Maine-et-Loire": "49", "Manche": "50", "Marne": "51",
    "Haute-Marne": "52", "Mayenne": "53", "Meurthe-et-Moselle": "54", "Meuse": "55",
    "Morbihan": "56", "Moselle": "57", "Nièvre": "58", "Nord": "59", "Oise": "60",
    "Orne": "61", "Pas-de-Calais": "62", "Puy-de-Dôme": "63", "Pyrénées-Atlantiques": "64",
    "Hautes-Pyrénées": "65", "Pyrénées-Orientales": "66", "Bas-Rhin": "67",
    "Haut-Rhin": "68", "Rhône": "69", "Haute-Saône": "70", "Saône-et-Loire": "71",
    "Sarthe": "72", "Savoie": "73", "Haute-Savoie": "74", "Paris": "75",
    "Seine-Maritime": "76", "Seine-et-Marne": "77", "Yvelines": "78", "Deux-Sèvres": "79",
    "Somme": "80", "Tarn": "81", "Tarn-et-Garonne": "82", "Var": "83", "Vaucluse": "84",
    "Vendée": "85", "Vienne": "86", "Haute-Vienne": "87", "Vosges": "88", "Yonne": "89",
    "Territoire de Belfort": "90", "Essonne": "91", "Hauts-de-Seine": "92",
    "Seine-Saint-Denis": "93", "Val-de-Marne": "94", "Val-d'Oise": "95",

    # DROM
    "Guadeloupe": "971", "Martinique": "972", "Guyane": "973", "La Réunion": "974",
    "Mayotte": "976"
}


# Liste des TJ
tj_liste = [
    "tj-paris", "tj-versailles", "tj-nanterre", "tj-bobigny", "tj-creteil", "tj-pontoise", "tj-evry", "tj-melun", "tj-meaux", "tj-fontainebleau",
    "tj-douai", "tj-lille", "tj-arras", "tj-valenciennes", "tj-dunkerque", "tj-bethune", "tj-boulogne-sur-mer", "tj-cambrai", "tj-avesnes-sur-helpe", "tj-saint-omer",
    "tj-amiens", "tj-beauvais", "tj-compiegne", "tj-senlis", "tj-laon", "tj-soissons", "tj-saint-quentin", "tj-abbeville",
    "tj-reims", "tj-troyes", "tj-charleville-mezieres", "tj-chaumont", "tj-chalons-en-champagne",
    "tj-nancy", "tj-bar-le-duc", "tj-verdun", "tj-epinal", "tj-val-de-briey",
    "tj-mulhouse", "tj-saverne",
    "tj-besancon", "tj-belfort", "tj-vesoul", "tj-lons-le-saunier", "tj-montbeliard",
    "tj-dijon", "tj-auxerre", "tj-nevers", "tj-macon", "tj-sens", "tj-chalon-sur-saone",
    "tj-rouen", "tj-evreux", "tj-le-havre", "tj-dieppe", "tj-bernay",
    "tj-caen", "tj-cherbourg", "tj-alencon", "tj-lisieux", "tj-avranches", "tj-coutances", "tj-argentan",
    "tj-rennes", "tj-quimper", "tj-brest", "tj-saint-brieuc", "tj-vannes", "tj-lorient", "tj-saint-malo", "tj-dinan",
    "tj-angers", "tj-nantes", "tj-le-mans", "tj-laval", "tj-la-roche-sur-yon", "tj-saint-nazaire", "tj-saumur", "tj-les-sables-d-olonne",
    "tj-orleans", "tj-bourges", "tj-chartres", "tj-blois", "tj-tours", "tj-chateauroux", "tj-montargis",
    "tj-limoges", "tj-gueret", "tj-tulle", "tj-brive-la-gaillarde",
    "tj-clermont-ferrand", "tj-moulins", "tj-le-puy-en-velay", "tj-aurillac", "tj-montlucon", "tj-cusset",
    "tj-poitiers", "tj-niort", "tj-angouleme", "tj-la-rochelle", "tj-saintes",
    "tj-bordeaux", "tj-pau", "tj-agen", "tj-perigueux", "tj-mont-de-marsan", "tj-bayonne", "tj-bergerac", "tj-dax", "tj-libourne", "tj-marmande",
    "tj-toulouse", "tj-albi", "tj-rodez", "tj-millau", "tj-cahors", "tj-montauban", "tj-auch", "tj-castres", "tj-tarbes", "tj-foix", "tj-saint-gaudens",
    "tj-lyon", "tj-grenoble", "tj-chambery", "tj-albertville", "tj-annecy", "tj-thonon-les-bains", "tj-bourg-en-bresse",
    "tj-saint-etienne", "tj-valence", "tj-privas", "tj-bonneville", "tj-villefranche-sur-saone", "tj-roanne", "tj-vienne", "tj-bourgoin-jallieu",
    "tj-aix-en-provence", "tj-marseille", "tj-draguignan", "tj-toulon", "tj-nice", "tj-grasse", "tj-digne-les-bains", "tj-gap",
    "tj-avignon", "tj-carpentras", "tj-tarascon",
    "tj-bastia", "tj-ajaccio",
    "tj-montpellier", "tj-nimes", "tj-perpignan", "tj-carcassonne", "tj-mende", "tj-beziers", "tj-narbonne", "tj-ales"
]


tj_outre_mer = [
    "tj-pointe-a-pitre", "tj-fort-de-france", "tj-saint-pierre", "tj-saint-denis", "tj-saint-pierre-et-miquelon"
]


tj_préférés = [
    "tj-paris", "tj-versailles", "tj-caen", "tj_lisieux", "tj-thonon-les-bains"
]

tj_idf = [
    "tj-paris", "tj-versailles", "tj-nanterre", "tj-bobigny", "tj-creteil", "tj-pontoise", "tj-evry", "tj-melun", "tj-meaux", "tj-fontainebleau",
]

tj_interessants = [
    "tj-paris", "tj-versailles", "tj-caen", "tj_lisieux", "tj-thonon-les-bains", "tj-lille", "tj-nancy", "tj-bordeaux", "tj-bayonne", "tj-lyon",
    "tj-annecy", "tj-aix-en-provence", "tj-marseille", "tj-toulon", "tj-nice", "tj-montpellier"
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


def extract_annonce_links(soup):
    links = soup.find_all("a", href=True)
    annonce_links = []
    for a in links:
        href = a['href']
        if "/annonce/" in href:
            full_url = href if href.startswith("http") else "https://www.licitor.com" + href
            annonce_links.append(full_url)
    return sorted(list(set(annonce_links)))


# Fonction pour récupérer tous les liens d'annonces pour une date donnée (avec pagination)
def get_all_annonce_links(base_url):
    all_links = []
    page = 1
    first_page_links = []

    while True:
        url = f"{base_url}?p={page}"
        response = requests.get(url, headers=headers, timeout=10)  #  warning : http call
        time.sleep(random.uniform(1, 2))

        print("warning : http call (get link)")

        if response.status_code != 200:
            break
        soup = BeautifulSoup(response.text, "html.parser")
        current_links = extract_annonce_links(soup)
        if not current_links:
            break
        if page == 1:
            first_page_links = current_links
        else:
            if current_links == first_page_links:
                break
        all_links.extend(current_links)
        page += 1

    return list(set(all_links))


def extraire_info_annonce(url, date, tribunal_nom):
    response = requests.get(url, headers=headers, timeout=10)  # warning : http call
    print("warning : http call (get info)")

    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    date_vente = date.strftime("%Y-%m-%d")
    tribunal = tribunal_nom

    # Type de bien
    type_bien = ""
    if "/vente-aux-encheres/" in url:
        try:
            type_bien = url.split("/vente-aux-encheres/")[1].split("/")[0].strip().lower()
        except IndexError:
            pass
    if not type_bien or len(type_bien) > 50:
        h1 = soup.find("h1")
        if h1:
            type_bien = h1.get_text(strip=True).split("à")[0].lower()

    # Adresse
    adresse = ""
    ville = ""
    departement_nom = ""
    departement_code = ""

    adresse_div = soup.find("div", class_="Location")
    if adresse_div:
        rue = adresse_div.find("p", class_="Street")
        city = adresse_div.find("p", class_="City")
        if rue:
            adresse = rue.get_text(strip=True)
        if city:
            ville_raw = city.get_text(" ", strip=True)
            ville_match = re.match(r"^(.*?)\s*\((.*?)\)", ville_raw)
            if ville_match:
                ville = ville_match.group(1).strip()
                departement_nom = ville_match.group(2).strip()
            else:
                ville = ville_raw.strip()
                if ville.lower().startswith("paris"):
                    departement_nom = "Paris"

    if departement_nom in dep_map:
        departement_code = dep_map[departement_nom]

    # Surface & occupation
    surface = ""
    occupation = ""
    souslot = soup.find("div", class_="FirstSousLot")
    if souslot:
        p = souslot.find("p")
        if p:
            text = p.get_text(" ", strip=True).lower()
            match = re.search(r"(\d{1,4}(?:,\d{1,2})?)\s*m²", text)
            if match:
                surface = match.group(1)
            if "inoccup" in text:
                occupation = "inoccupé"
            elif "occup" in text:
                occupation = "occupé"
            elif "loué" in text:
                occupation = "loué"

    # Prix : mise à prix et adjudication ou carence
    mise_a_prix = ""
    prix_adjudication = ""

    # Recherche explicite du bloc "Carence d'enchères"
    for h3 in soup.find_all("h3"):
        if "carence" in h3.get_text(strip=True).lower():
            prix_adjudication = ""
            next_tag = h3.find_next_sibling("h4")
            if next_tag:
                match_mise = re.search(r"mise à prix\s*:\s*([\d\s]+)", next_tag.get_text(strip=True).lower())
                if match_mise:
                    mise_a_prix = match_mise.group(1).strip()
            break

    # Sinon, on cherche la mise à prix et adjudication normalement
    if prix_adjudication == "":
        h1 = soup.find("h1")
        if h1:
            h1_text = h1.get_text(" ", strip=True).lower()
            match_mise = re.search(r"mise à prix\s*:\s*([\d\s]+)", h1_text)
            if match_mise:
                mise_a_prix = match_mise.group(1).replace("\xa0", " ").strip()

        for tag in soup.find_all(["h3", "p", "strong", "span", "div"]):
            text = tag.get_text(" ", strip=True).lower()
            if "adjudication" in text and "€" in text:
                match = re.search(r"adjudication\s*:\s*([\d\s]+)", text)
                if match:
                    prix_adjudication = match.group(1).strip()
            elif "adjugé" in text and "€" in text:
                prix_adjudication = tag.get_text(strip=True)


    return {
        "Date de la Vente aux enchère": date_vente,
        "Tribunal": tribunal,
        "Type de bien": type_bien,
        "Département": departement_code,
        "Ville": ville.strip(),
        "Adresse": adresse,
        "Occupation": occupation,
        "Surface": surface,
        "Prix d'adjudication": prix_adjudication,
        "Mise à prix": mise_a_prix,
        "URL": url
    }


# Générer tous les jeudis depuis le 1er janvier 2020
def generate_thursdays(start_date, end_date):
    thursdays = []
    current = start_date
    # Aller jusqu'au premier jeudi
    while current.weekday() != 3:  # 3 = jeudi
        current += timedelta(days=1)
    while current <= end_date:
        thursdays.append(current)
        current += timedelta(days=7)
    return thursdays

def generate_days(start_date, end_date):
    current = start_date
    days = []
    while current <= end_date:
        if current.weekday() < 5:  # 0 = lundi, 4 = vendredi
            days.append(current)
        current += timedelta(days=1)
    return days


def get_tj_entrypoints():
    url = "https://www.licitor.com/historique-des-adjudications.html"
    response = requests.get(url, headers=headers, timeout=10)
    soup = BeautifulSoup(response.text, "html.parser")

    entrypoints = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/ventes-judiciaires-immobilieres/tj-") and href.endswith(".html"):
            match = re.search(r"/ventes-judiciaires-immobilieres/(tj-[^/]+)/.*\.html", href)
            if match:
                tj_key = match.group(1)
                full_url = "https://www.licitor.com" + href
                if tj_key not in entrypoints:
                    entrypoints[tj_key] = full_url
    return entrypoints


def get_dates_with_audience(tj, entry_url, start_date, end_date, max_pages=20):
    visited_urls = set()
    urls_to_visit = [entry_url]
    dates = set()
    page_counter = 0

    mois_map = {
        'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4, 'mai': 5, 'juin': 6,
        'juillet': 7, 'août': 8, 'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12
    }

    while urls_to_visit and page_counter < max_pages:
        url = urls_to_visit.pop(0)
        if url in visited_urls:
            continue
        visited_urls.add(url)
        page_counter += 1

        try:
            response = requests.get(url, headers=headers, timeout=10)  # warning : http call
            time.sleep(random.uniform(1.5, 2.5))

            print("warning : http call (get dates)")
            if response.status_code != 200:
                continue
        except:
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        div = soup.find("div", id="traversing-hearings")
        if not div:
            continue

        new_dates_found = False
        for a in div.find_all("a", href=True):
            href = a["href"]
            match = re.search(r"/([a-zéûî]+)-(\d{1,2})-([a-zéûî]+)-(\d{4})\.html", href)
            if match:
                _, day, mois_fr, year = match.groups()
                mois_fr = mois_fr.lower()
                try:
                    dt = datetime(int(year), mois_map[mois_fr], int(day))
                    if start_date <= dt <= end_date:
                        if dt not in dates:
                            new_dates_found = True
                            dates.add(dt)
                except:
                    continue

        # Arrêt anticipé si aucune date utile sur cette page
        if not new_dates_found:  # and page_counter > 2:
            break

        # Pagination : on avance dans les pages
        for nav_class in ["Next"]:  # ["Previous", "Next"]
            li = div.find("li", class_=nav_class)
            if li:
                a = li.find("a", href=True)
                if a:
                    next_url = "https://www.licitor.com" + a["href"].split("#")[0]
                    if next_url not in visited_urls and next_url not in urls_to_visit:
                        urls_to_visit.append(next_url)

    return sorted(dates)


# Récupérer les annonces pour tous les jeudis
start_date = datetime(2020, 1, 1) #datetime(2024, 1, 1)
end_date = datetime(2025, 7, 10)  #datetime.today()
# days = generate_days(start_date, end_date)

resultats = []
entrypoints = get_tj_entrypoints()  # warning : http call
print("warning : http call (entrypoints found)")

# 1) Récupérer la liste des URLs d'annonces sur la page principale
for tj in ["tj-paris"]:  # ou tj_préférés
    time.sleep(2)  # sleep between tj

    if tj in entrypoints:
        url_point_entree = entrypoints[tj]

        print(f"🔎 Recherche des audiences pour {tj}...")
        dates = get_dates_with_audience(tj, url_point_entree, start_date, end_date)  # multiple warning : http call
        # print(dates)
        print(f"📅 {len(dates)} dates à traiter pour {tj}")

        for date in dates:
            # print(date)
            time.sleep(random.uniform(2, 3))  # sleep between dates

            weekday_en = date.strftime("%A")
            month_en = date.strftime("%B")
            day = str(date.day)
            year = str(date.year)
            weekday_fr = jours_fr[weekday_en]
            month_fr = mois_fr[month_en]

            formatted_date = f"{weekday_fr}-{day}-{month_fr}-{year}"

            url_page = f"https://www.licitor.com/ventes-judiciaires-immobilieres/{tj}/{formatted_date}.html"
            #print(url_page)
            try:
                annonce_links = get_all_annonce_links(url_page)  # warning : multiple http call

                print(f"{tj} {date.strftime('%Y-%m-%d')} : {len(annonce_links)} annonces")
                for link in annonce_links:
                    tribunal_label = tj.replace("tj-", "TJ ").replace("-", " ").title()

                    time.sleep(random.uniform(1, 2))
                    info = extraire_info_annonce(link, date, tribunal_label)  # warning : http call

                    resultats.append(info)
            except Exception as e:
                print(f"{date.strftime('%Y-%m-%d')} : Erreur ({e})")

# Export CSV 
if resultats:
    fieldnames = [
        "Date de la Vente aux enchère", "Tribunal", "Type de bien", "Département",
        "Ville", "Adresse", "Occupation", "Surface",
        "Prix d'adjudication", "Mise à prix", "URL"
    ]
    with open("annonces_licitor_tj_paris_5_an.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(resultats)
    print("✅ Export CSV terminé : annonces_licitor_tj_paris_5_an.csv")
else:
    print("❌ Aucune annonce trouvée.")

