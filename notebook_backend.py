# notebook_backend.py

import pandas as pd
import numpy as np
import re
from thefuzz import fuzz
import unidecode
import os

# --- Fonction principale ---
def process_files(path_sextan, path_izydesk):

    # --- 1. Lecture des fichiers ---
    data_sextan = pd.read_excel(path_sextan)
    data_izydesk = pd.read_excel(path_izydesk)

    # --- 2. Standardisation colonnes ---
    data_sextan.columns = data_sextan.columns.str.lower()
    data_izydesk.columns = data_izydesk.columns.str.lower()

    # --- 3. Mapping corners ---
    corner_mapping = {
        "toulouse": {"id_corner": "004", "nom_corner": "toulouse"},
        "garosud": {"id_corner": "001", "nom_corner": "garosud"},
        "nimes": {"id_corner": "002", "nom_corner": "nimes"},
        "rochplaza": {"id_corner": "003", "nom_corner": "rochplaza"}
    }
    nom_fichier = os.path.basename(path_izydesk).lower()
    id_corner, nom_corner = "", ""
    for keyword, values in corner_mapping.items():
        if keyword in nom_fichier:
            id_corner = values["id_corner"]
            nom_corner = values["nom_corner"]
            data_izydesk["id_corner"] = id_corner
            data_izydesk["nom corner"] = nom_corner
            break
    data_izydesk = data_izydesk[["id_corner", "nom corner"] + [col for col in data_izydesk.columns if col not in ["id_corner", "nom corner"]]]


    # Identification de la colonne correspondant au pattern de commande
    pattern = r"commandes du \d{2}/\d{2}/\d{4} au \d{2}/\d{2}/\d{4}"
    for col in data_izydesk.columns:
        if re.match(pattern, col):
            data_izydesk.rename(columns={col: "id_commande"}, inplace=True)
            break  # On s'arrête après avoir trouvé la colonne
    # Vérification et découpage sécurisé
    data_izydesk[["type paiement", "montant réglé"]] = data_izydesk["paiements"].str.split(":", n = 1, expand = True)

    # Suppression du signe € et conversion en float
    data_izydesk["montant réglé"] = data_izydesk["montant réglé"].str.replace("€", "", regex=False)
    data_izydesk["montant réglé"] = pd.to_numeric(data_izydesk["montant réglé"], errors="coerce")

    # data = data_izydesk.drop(columns = ["paiements"])
    # Réorganisation des colonnes pour une meilleure lisibilité
    colonnes = list(data_izydesk.columns)
    index_paiements = colonnes.index("type paiement")
    colonnes.insert(index_paiements + 1, colonnes.pop(colonnes.index("montant réglé")))
    data_izydesk = data_izydesk[colonnes]

    # Parcours du mapping pour trouver la correspondance
    for keyword, values in corner_mapping.items():
        if keyword in nom_fichier:
            data_izydesk["id_corner"] = values["id_corner"]
            data_izydesk["nom corner"] = values["nom_corner"]
            break  # On sort dès qu'on trouve le bon corner
        
    # Réorganisation des colonnes
    colonnes = ['id_corner', 'nom corner'] + [col for col in data_izydesk.columns if col not in ['id_corner', 'nom corner']]
    data_izydesk = data_izydesk[colonnes]

    # Fonction corrigée pour extraire et séparer les produits
    def extract_products_corrected(df, col_produits="produits"):
        exploded_sales = []
        for index, row in df.iterrows():
            if pd.notna(row[col_produits]):  # Vérifier que la colonne produit n'est pas vide
                produits = row[col_produits].split("\n")  # Séparer les produits
                for produit in produits:
                    match = re.match(r"(\d+)x (.+)", produit.strip())  # Extraire la quantité et le nom du produit
                    if match:
                        qty = int(match.group(1))
                        product_name = match.group(2).strip()
                        new_row = row.copy()
                        new_row["quantité"] = qty
                        new_row["produit"] = product_name
                        exploded_sales.append(new_row)
        return pd.DataFrame(exploded_sales)

    # Appliquer la correction
    data_izydesk = extract_products_corrected(data_izydesk)

    # Identifier les lignes où un seul produit a été commandé dans les données originales
    data_izydesk_single = data_izydesk[data_izydesk["produits"].str.count("\n") == 0].copy()

    # Extraire le nom et le prix de ces produits uniques
    def extract_single_product_price(row):
        match = re.match(r"(\d+)x (.+)", row["produits"].strip())
        if match:
            qty = int(match.group(1))
            product_name = match.group(2).strip()
            unit_ht = row["ht"] / qty if qty > 0 else row["ht"]
            unit_ttc = row["ttc"] / qty if qty > 0 else row["ttc"]
            return product_name, unit_ht, unit_ttc
        return None, None, None

    # Appliquer la fonction pour extraire les prix unitaires
    data_izydesk_single[["produit", "ht_unitaire", "ttc_unitaire"]] = data_izydesk_single.apply(
        lambda row: pd.Series(extract_single_product_price(row)), axis=1
    )

    # Identifier les produits n'ayant pas pu être extraits correctement (avec valeurs nulles)
    produits_non_extraits = data_izydesk_single[data_izydesk_single["produit"].isna()][["produits"]].drop_duplicates()

    # Supprimer les lignes avec valeurs nulles pour le DataFrame des prix unitaires
    df_prix_produits_cleaned = data_izydesk_single.dropna(subset=["produit"])[["produit", "ht_unitaire", "ttc_unitaire"]].drop_duplicates()

    # Supprimer l'ancienne colonne "produits"
    data_izydesk = data_izydesk.drop(columns=["produits"])

    # Fusionner les données éclatées avec les prix unitaires extraits
    data_izydesk_corrected = data_izydesk.merge(df_prix_produits_cleaned, on="produit", how="left")

    # Calculer les nouveaux montants HT, TTC et réglés par produit en fonction de la quantité
    data_izydesk_corrected["ht_total"] = data_izydesk_corrected["ht_unitaire"] * data_izydesk_corrected["quantité"]
    data_izydesk_corrected["ttc_total"] = data_izydesk_corrected["ttc_unitaire"] * data_izydesk_corrected["quantité"]
    data_izydesk_corrected["montant réglé total"] = data_izydesk_corrected["ttc_total"]
    data_izydesk_corrected["ht_unitaire"] = data_izydesk_corrected["ht_unitaire"].round(2)
    data_izydesk_corrected["ttc_unitaire"] = data_izydesk_corrected["ttc_unitaire"].round(2)
    data_izydesk_corrected["ht_total"] = data_izydesk_corrected["ht_total"].round(2)
    data_izydesk_corrected["ttc_total"] = data_izydesk_corrected["ttc_total"].round(2)
    data_izydesk_corrected["montant réglé total"] = data_izydesk_corrected["montant réglé total"].round(2)

    # Suppression des colonnes redondantes
    data_izydesk_corrected = data_izydesk_corrected.drop(columns=["ht", "ttc", "montant réglé", "paiements"])

    produits_sans_prix = data_izydesk_corrected[data_izydesk_corrected["ht_unitaire"].isna()][["produit"]].drop_duplicates()

    data_izydesk = data_izydesk_corrected

    pd.options.display.max_colwidth = None
    data_sextan = data_sextan.drop(columns=["unnamed: 0", "marque", "type", "catégorie", "prod. par", "nb portion", "nb sous-prod.", "stock", "prix ht", "prix ttc", "options"])

    data_sextan = data_sextan.rename(columns={
        "n°": "id_sextan",
        "nom": "produit_sextan",
        "coût unit.": "cout_unitaire"
    })

    # Suppression du symbole € et conversion en float avec 2 décimales
    data_sextan["cout_unitaire"] = data_sextan["cout_unitaire"].str.replace("€", "", regex=False).str.replace(",", ".")
    data_sextan["cout_unitaire"] = pd.to_numeric(data_sextan["cout_unitaire"], errors="coerce").round(2)
    data_sextan = data_sextan[
        ~data_sextan["produit_sextan"].str.contains("solanid|arena", case=False, na=False)
    ].reset_index(drop=True)

    pd.options.display.max_colwidth = None
    data_sextan = data_sextan.drop(columns=["unnamed: 0", "marque", "type", "catégorie", "prod. par", "nb portion", "nb sous-prod.", "stock", "prix ht", "prix ttc", "options"])
    data_sextan = data_sextan.rename(columns={
        "n°": "id_sextan",
        "nom": "produit_sextan",
        "coût unit.": "cout_unitaire"
    })

    # Suppression du symbole € et conversion en float avec 2 décimales
    data_sextan["cout_unitaire"] = data_sextan["cout_unitaire"].str.replace("€", "", regex=False).str.replace(",", ".")
    data_sextan["cout_unitaire"] = pd.to_numeric(data_sextan["cout_unitaire"], errors="coerce").round(2)
    data_sextan = data_sextan[
        ~data_sextan["produit_sextan"].str.contains("solanid|arena", case=False, na=False)
    ].reset_index(drop=True)

    def split_product_info(value):
        parts = value.split("|")  # Séparer par "|"
        parts = [p.strip() for p in parts]  # Nettoyer les espaces
        categorie = parts[0] if parts else ""
        produit = ""
        contenant = ""
        dlc = ""

        # Identifier les éléments
        if len(parts) > 1:
            produit = parts[1]

        if len(parts) > 2:
            # Essayer de deviner où est le contenant et où est la DLC
            if "j+" in parts[-1]:  # Vérifie si le dernier élément est une DLC
                dlc = parts[-1]
                contenant = " ".join(parts[2:-1])  # Tout le reste est le contenant
            else:
                contenant = " ".join(parts[2:])  # Tout est contenant si pas de DLC

        # Si la DLC est collée au contenant, la séparer
        if contenant and "j+" in contenant:
            parts = contenant.split()
            dlc = parts[-1]  # Dernier élément = DLC
            contenant = " ".join(parts[:-1])  # Tout sauf le dernier = contenant

        return pd.Series([categorie, produit, contenant, dlc])

    data_sextan[["categorie", "produit_sextan", "contenant", "dlc"]] = data_sextan["produit_sextan"].apply(split_product_info)

    data_sextan = data_sextan.applymap(lambda x: x.lower() if isinstance(x, str) else x)


    # Normaliser les colonnes produit pour minimiser les différences de casse et d'orthographe
    data_izydesk["produit"] = data_izydesk["produit"].astype(str).str.lower().str.strip()
    data_sextan["produit_sextan"] = data_sextan["produit_sextan"].astype(str).str.lower().str.strip()

    # Optimisation en limitant les comparaisons
    def find_best_match(produit, produits_sextan):
        best_match = None
        best_score = 0
        for prod in produits_sextan:
            score = fuzz.ratio(produit, prod)
            if score > best_score and score >= 80:
                best_match = prod
                best_score = score
        return best_match

    # Appliquer la correspondance avec une approche optimisée - 2mmin 12
    produits_sextan_list = data_sextan["produit_sextan"].unique()
    data_izydesk["produit_match"] = data_izydesk["produit"].apply(lambda x: find_best_match(x, produits_sextan_list))

    # Remplacer NaN dans 'produit_match' par la valeur de 'produit'
    data_izydesk["produit_match"] = data_izydesk.apply(
        lambda row: row["produit"] if pd.isna(row["produit_match"]) else row["produit_match"], axis=1
    )

    # Suppression des volumes dans les boissons
    data_izydesk["produit_match"] = data_izydesk["produit_match"].str.replace(r'\s*(\d{2,3}\s?cl)\b', '', regex=True).str.strip()

    # Afficher les lignes où le produit d'origine contenait 'pepsi max'
    verif_pepsi = data_izydesk[data_izydesk["produit"].str.contains("pepsi max", case=False, na=False)]

    # Lignes où produit_match a été modifié (différent du produit d'origine)
    modifs = data_izydesk[data_izydesk["produit"] != data_izydesk["produit_match"]][["produit", "produit_match"]]

    # Fusionner les DataFrames
    merged_data = data_izydesk.merge(
        data_sextan, left_on="produit_match", right_on="produit_sextan", how="left"
    )

    # Remplacer les NaN dans 'produit_sextan' par la valeur de 'produit_match'
    merged_data["produit_sextan"] = merged_data.apply(
        lambda row: row["produit_match"] if pd.isna(row["produit_sextan"]) else row["produit_sextan"], axis=1
    )

    # Créer une colonne pour indiquer si le produit existe ou non dans Sextan
    merged_data["produit_sextan_trouve"] = merged_data["produit_sextan"].notna()

    # Maintenant, merged_data a un booléen True/False qui montre la correspondance

    # Suppression de la colonne de correspondance temporaire
    merged_data.drop(columns=["produit_match"], inplace=True)

    # Suppression des doublons en conservant la première occurrence
    merged_data_cleaned = merged_data.drop_duplicates(subset=["id_commande", "date", "heure", "service", "produit"], keep="first").reset_index(drop=True)

    # Attribution de familles si NaN

    # Nettoyage pour homogénéiser
    merged_data["produit_sextan_clean"] = merged_data["produit_sextan"].apply(lambda x: unidecode.unidecode(x.lower()) if isinstance(x, str) else "")

    # Fonction pour attribuer la famille
    def attribuer_famille(row):
        produit = row["produit_sextan_clean"]

        # Si famille déjà remplie → on garde
        if pd.notna(row["famille"]):
            return row["famille"]

        # 1️⃣ Condition spéciale : tout ce qui contient "offert", "offre", "1 acheté"
        if re.search(r'offert|offre|1.*achete', produit):
            return "offre"

        # 2️⃣ Produits boissons doivent retourner leur propre nom sans volume
        if re.search(r'\b(pepsi max|pepsi|ice tea peche)\b', produit):
            return produit.strip()

        # 3️⃣ Muffin → dessert muffin
        if "muffin" in produit:
            return "dessert muffin"

        # 4️⃣ Cookie → dessert cookie
        if "cookie" in produit:
            return "dessert cookie"

        # 5️⃣ Brownie → dessert brownie
        if "brownie" in produit:
            return "dessert brownie"
        
        # 5️⃣ sojasun → dessert yaourt
        if re.search(r'sojasun|yaourt', produit):
            return "dessert yaourt"
        
        # 5️⃣ galette des rois / frangipane → dessert part de cake
        if re.search(r'frangipane|galette|gateau|buche', produit):
            return "dessert part de cake"

        # 6️⃣ Produits contenant "salade" ou "bowl" → trefle salade
        if re.search(r'salade|bowl', produit):
            return "trefle salade"
        
        # 6️⃣ Produits contenant "porc" → trefle porc
        if re.search(r'porc', produit):
            return "trefle porc"

        # 7️⃣ Verre de vin rouge
        if "verre de vin rouge" in produit:
            return "vin"

        # 8️⃣ Kit couverts inox
        if "kit couverts inox" in produit:
            return "kit couverts"

        # 9️⃣ gateaux
        if re.search(r'gateau', produit):
            return "dessert part de cake"

        # 9️⃣ Pain
        if re.search(r'pain individuel|petit pain|pain de la veille', produit):
            return "pain"
        
        if "pain polaire" in produit:
            return "snack"

        # 10️⃣ Anti-gaspi
        if "anti-gaspi" in produit:
            return "anti-gaspi"
        
        if "sac kraft" in produit:
            return "autre"

        # 11️⃣ Pizza, focaccia, petites faims → snack
        if re.search(r'pizza|focaccia|petites faims|pain polaire', produit):
            return "snack"

        # 12️⃣ Menu ou "+"
        if ("menu" in produit) or (("+" in produit) and not re.search(r'2 \+ 1', produit)):
            return "menu"

        # 13️⃣ Produit compte
        if "compte" in produit:
            return "produit compte"

        # Sinon → autre
        return row["produit_sextan"]

    # Application
    merged_data["famille"] = merged_data.apply(attribuer_famille, axis = 1)

    # Nettoyage colonne temporaire
    merged_data.drop(columns=["produit_sextan_clean"], inplace = True)

    # Convertir en string pour éviter les types mixtes (NaN souvent pose problème)
    merged_data["categorie"] = merged_data["categorie"].astype(str)

    # Nettoyage
    merged_data["produit_sextan_clean"] = merged_data["produit_sextan"].apply(lambda x: unidecode.unidecode(str(x).lower()) if pd.notna(x) else "")
    merged_data["famille_clean"] = merged_data["famille"].apply(lambda x: unidecode.unidecode(str(x).lower()) if pd.notna(x) else "")

    ### 1️⃣ Remplacement des catégories numériques
    mapping_categorie = {
        1: "entree",
        2: "plat",
        3: "dessert",
        "1.0": "entree",
        "2.0": "plat",
        "3.0": "dessert",
        "1": "entree",
        "2": "plat",
        "3": "dessert"
    }
    merged_data["categorie"] = merged_data["categorie"].replace(mapping_categorie)

    # Boissons via famille
    merged_data.loc[
        merged_data["famille_clean"].str.contains(r'pepsi|max|coca|ice tea|orangina|cristaline|badoit|eau|vin|schweppes|jus|the|boisson|cafe|minute maid|tropicana'),
        "categorie"
    ] = "boisson"

    # Nettoyage pour être sûr (mise en minuscule, suppression accents)
    merged_data["produit_clean"] = merged_data["produit"].apply(lambda x: unidecode.unidecode(str(x).lower()) if pd.notna(x) else "")

    # Appliquer les règles spécifiques anti-gaspi
    merged_data.loc[
        (merged_data["produit_clean"].str.contains(r"gaspi|anti-gaspi")) & 
        (merged_data["produit_clean"].str.contains(r"dessert|desserts")), 
        "famille"
    ] = "dessert"

    merged_data.loc[
        (merged_data["produit_clean"].str.contains(r"gaspi|anti-gaspi")) & 
        (merged_data["produit_clean"].str.contains(r"plat|plats")), 
        "famille"
    ] = "plat"

    merged_data.loc[
        (merged_data["produit_clean"].str.contains(r"gaspi|anti-gaspi")) & 
        (~merged_data["produit_clean"].str.contains("dessert|plat")), 
        "famille"
    ] = "anti-gaspi autre"

    # Optionnel : suppression colonne temporaire
    merged_data.drop(columns=["produit_clean"], inplace=True)

    # ✅ RECREATION DE FAMILLE_CLEAN après modif famille
    merged_data["famille_clean"] = merged_data["famille"].apply(lambda x: unidecode.unidecode(str(x).lower()) if pd.notna(x) else "")

    # Attribution des catégories
    merged_data.loc[merged_data["famille_clean"].str.contains(r'muffin'), "categorie"] = "dessert muffin"
    merged_data.loc[merged_data["famille_clean"].str.contains(r'cookie'), "categorie"] = "dessert cookie"
    merged_data.loc[merged_data["famille_clean"].str.contains(r'brownie'), "categorie"] = "dessert brownie"
    merged_data.loc[merged_data["famille_clean"].str.contains(r'fruit|dessert'), "categorie"] = "dessert"
    merged_data.loc[merged_data["famille_clean"].str.contains(r'snack|petite faim'), "categorie"] = "snack"
    merged_data.loc[merged_data["famille_clean"].str.contains(r'offre|event|menu|compte|pain|autre'), "categorie"] = "autre"

    # Salades & bowls
    merged_data.loc[merged_data["famille_clean"].str.contains(r'salade|bowl|porc|plat'), "categorie"] = "plat"

    # Kits couverts
    merged_data.loc[merged_data["famille_clean"].str.contains(r'kit couverts'), "categorie"] = "kit couverts"

    # Cas spécifique anti-gaspi autre
    merged_data.loc[
        merged_data["famille"].str.lower() == "anti-gaspi autre",
        "categorie"
    ] = "autre"

    # Règle spécifique : dessert gourmand pot transparent thermo
    merged_data.loc[
        merged_data["famille_clean"] == "dessert gourmand pot transparent thermo",
        "categorie"
    ] = "dessert"

    ### 3️⃣ Vérification sur produit_sextan_clean pour les boissons (au cas où famille serait vide)
    boissons_keywords = r'pepsi|max|coca|ice tea|orangina|cristaline|badoit|eau|vin|schweppes|jus|the|boisson|cafe|minute maid|tropicana'
    merged_data.loc[
        merged_data["categorie"].isna() &
        merged_data["produit_sextan_clean"].str.contains(boissons_keywords),
        "categorie"
    ] = "boisson"

    ### 4️⃣ Fallback : tout ce qui reste → "autre"
    merged_data["categorie"] = merged_data["categorie"].fillna("autre")

    # Nettoyage colonnes temporaires
    merged_data.drop(columns=["produit_sextan_clean", "famille_clean"], inplace=True)
    
    # Définir par défaut
    id_corner = ""
    nom_corner = ""
    for keyword, values in corner_mapping.items():
        if keyword in nom_fichier:
            id_corner = values["id_corner"]
            nom_corner = values["nom_corner"]
            data_izydesk["id_corner"] = id_corner
            data_izydesk["nom corner"] = nom_corner
            break
    # Réorganiser les colonnes
    colonnes = ['id_corner', 'nom corner'] + [col for col in data_izydesk.columns if col not in ['id_corner', 'nom corner']]
    data_izydesk = data_izydesk[colonnes]
        

    # --- 11. Export ---
    nom_export_izydesk = f"exports/izydesk_auto_{nom_corner}.xlsx"
    nom_export_merged = f"exports/merged_data_auto_{nom_corner}.xlsx"

    data_izydesk.to_excel(nom_export_izydesk, index=False)
    merged_data.to_excel(nom_export_merged, index=False)

    return data_izydesk, merged_data
