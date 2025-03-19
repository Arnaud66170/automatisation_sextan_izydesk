import pandas as pd
import numpy as np
import re
import unidecode
from thefuzz import fuzz
import os

# Liste des dossiers nécessaires
required_dirs = ["temp", "exports"]

# Vérification + création si besoin
for folder in required_dirs:
    if not os.path.exists(folder):
        os.makedirs(folder)
        print(f"Dossier créé : {folder}")
    else:
        print(f"Dossier déjà existant : {folder}")

def process_files(path_sextan, path_izydesk):
    # --- 1. Lecture des fichiers ---
    data_sextan = pd.read_excel(path_sextan)
    data_izydesk = pd.read_excel(path_izydesk)

    # --- 2. Normalisation colonnes ---
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
            id_corner, nom_corner = values["id_corner"], values["nom_corner"]
            data_izydesk["id_corner"] = id_corner
            data_izydesk["nom corner"] = nom_corner
            break

    data_izydesk = data_izydesk[["id_corner", "nom corner"] + [col for col in data_izydesk.columns if col not in ["id_corner", "nom corner"]]]

    # --- 4. Nettoyage et éclatement produits Izydesk ---
    def extract_products_corrected(df, col_produits="produits"):
        exploded_sales = []
        for index, row in df.iterrows():
            if pd.notna(row[col_produits]):
                produits = row[col_produits].split("\n")
                for produit in produits:
                    match = re.match(r"(\d+)x (.+)", produit.strip())
                    if match:
                        qty = int(match.group(1))
                        product_name = match.group(2).strip()
                        new_row = row.copy()
                        new_row["quantité"] = qty
                        new_row["produit"] = product_name
                        exploded_sales.append(new_row)
        return pd.DataFrame(exploded_sales)

    data_izydesk = extract_products_corrected(data_izydesk)

    # --- 5. Extraction prix unitaires ---
    data_izydesk_single = data_izydesk[data_izydesk["produits"].str.count("\n") == 0].copy()

    def extract_single_product_price(row):
        match = re.match(r"(\d+)x (.+)", row["produits"].strip())
        if match:
            qty = int(match.group(1))
            unit_ht = row["ht"] / qty if qty > 0 else row["ht"]
            unit_ttc = row["ttc"] / qty if qty > 0 else row["ttc"]
            return match.group(2).strip(), unit_ht, unit_ttc
        return None, None, None

    data_izydesk_single[["produit", "ht_unitaire", "ttc_unitaire"]] = data_izydesk_single.apply(
        lambda row: pd.Series(extract_single_product_price(row)), axis=1
    )

    prix_cleaned = data_izydesk_single.dropna(subset=["produit"])[["produit", "ht_unitaire", "ttc_unitaire"]].drop_duplicates()
    data_izydesk = data_izydesk.drop(columns=["produits"])
    data_izydesk = data_izydesk.merge(prix_cleaned, on="produit", how="left")

    data_izydesk["ht_total"] = data_izydesk["ht_unitaire"] * data_izydesk["quantité"]
    data_izydesk["ttc_total"] = data_izydesk["ttc_unitaire"] * data_izydesk["quantité"]
    data_izydesk["montant réglé total"] = data_izydesk["ttc_total"]

    data_izydesk = data_izydesk.drop(columns=["ht", "ttc", "montant réglé", "paiements"], errors = "ignore")

    # --- 6. Traitement Sextan ---
    data_sextan = data_sextan.drop(columns=["unnamed: 0", "marque", "type", "catégorie", "prod. par", "nb portion", "nb sous-prod.", "stock", "prix ht", "prix ttc", "options"])
    data_sextan = data_sextan.rename(columns={"n°": "id_sextan", "nom": "produit_sextan", "coût unit.": "cout_unitaire"})
    data_sextan["cout_unitaire"] = data_sextan["cout_unitaire"].str.replace("€", "").str.replace(",", ".").astype(float).round(2)
    data_sextan = data_sextan[~data_sextan["produit_sextan"].str.contains("solanid|arena", case=False, na=False)].reset_index(drop=True)
    data_sextan = data_sextan[~data_sextan["famille"].str.contains("ftv|lmf|solanid", case=False, na=False)].reset_index(drop=True)

    # --- 7. Correspondance produits ---
    data_izydesk["produit"] = data_izydesk["produit"].astype(str).str.lower().str.strip()
    data_sextan["produit_sextan"] = data_sextan["produit_sextan"].astype(str).str.lower().str.strip()

    def find_best_match(produit, produits_sextan):
        best_match, best_score = None, 0
        for prod in produits_sextan:
            score = fuzz.ratio(produit, prod)
            if score > best_score and score >= 80:
                best_match = prod
                best_score = score
        return best_match

    data_izydesk["produit_match"] = data_izydesk["produit"].apply(lambda x: find_best_match(x, data_sextan["produit_sextan"].unique()))
    data_izydesk["produit_match"] = data_izydesk.apply(lambda row: row["produit"] if pd.isna(row["produit_match"]) else row["produit_match"], axis=1)
    data_izydesk["produit_match"] = data_izydesk["produit_match"].str.replace(r'\s*(\d{2,3}\s?cl)\b', '', regex=True).str.strip()

    # --- 8. Fusion ---
    merged_data = data_izydesk.merge(data_sextan, left_on="produit_match", right_on="produit_sextan", how="left")
    merged_data["produit_sextan"] = merged_data.apply(lambda row: row["produit_match"] if pd.isna(row["produit_sextan"]) else row["produit_sextan"], axis=1)
    merged_data.drop(columns=["produit_match"], inplace=True)
    merged_data = merged_data.drop_duplicates(subset=["id_commande", "date", "heure", "service", "produit"], keep="first").reset_index(drop=True)

    # --- 9. Sauvegarde finale dynamique ---
    nom_export_izydesk = f"exports/izydesk_auto_{nom_corner}.xlsx"
    nom_export_merged = f"exports/merged_data_auto_{nom_corner}.xlsx"

    merged_data.to_excel(nom_export_merged, index=False)
    data_izydesk.to_excel(nom_export_izydesk, index=False)

    print(f"Exports réalisés :\n{nom_export_izydesk}\n{nom_export_merged}")

    return data_izydesk, merged_data
