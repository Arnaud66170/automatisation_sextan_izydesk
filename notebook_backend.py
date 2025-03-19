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

    # --- 4. Identifier la colonne commande ---
    pattern = r"commandes du \d{2}/\d{2}/\d{4} au \d{2}/\d{2}/\d{4}"
    for col in data_izydesk.columns:
        if re.match(pattern, col):
            data_izydesk.rename(columns={col: "id_commande"}, inplace=True)
            break

    # --- 5. Découper paiements ---
    data_izydesk[["type paiement", "montant réglé"]] = data_izydesk["paiements"].str.split(":", n=1, expand=True)
    data_izydesk["montant réglé"] = data_izydesk["montant réglé"].str.replace("€", "", regex=False)
    data_izydesk["montant réglé"] = pd.to_numeric(data_izydesk["montant réglé"], errors="coerce")

    # Réorganisation colonnes
    colonnes = list(data_izydesk.columns)
    index_paiements = colonnes.index("type paiement")
    colonnes.insert(index_paiements + 1, colonnes.pop(colonnes.index("montant réglé")))
    data_izydesk = data_izydesk[colonnes]

    # --- 6. Éclatement produits ---
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

    # --- 7. Extraction prix unitaires ---
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

    df_prix_produits_cleaned = data_izydesk_single.dropna(subset=["produit"])[["produit", "ht_unitaire", "ttc_unitaire"]].drop_duplicates()
    data_izydesk = data_izydesk.drop(columns=["produits"])
    data_izydesk = data_izydesk.merge(df_prix_produits_cleaned, on="produit", how="left")
    data_izydesk["ht_total"] = data_izydesk["ht_unitaire"] * data_izydesk["quantité"]
    data_izydesk["ttc_total"] = data_izydesk["ttc_unitaire"] * data_izydesk["quantité"]
    data_izydesk["montant réglé total"] = data_izydesk["ttc_total"]
    data_izydesk = data_izydesk.drop(columns=["ht", "ttc", "montant réglé", "paiements"], errors="ignore")

    # --- 8. Traitement Sextan ---
    data_sextan = data_sextan.drop(columns=["unnamed: 0", "marque", "type", "catégorie", "prod. par", "nb portion", "nb sous-prod.", "stock", "prix ht", "prix ttc", "options"], errors="ignore")
    data_sextan = data_sextan.rename(columns={"n°": "id_sextan", "nom": "produit_sextan", "coût unit.": "cout_unitaire"})
    data_sextan["cout_unitaire"] = data_sextan["cout_unitaire"].str.replace("€", "").str.replace(",", ".").astype(float).round(2)
    data_sextan = data_sextan[~data_sextan["produit_sextan"].str.contains("solanid|arena", case=False, na=False)].reset_index(drop=True)
    data_sextan = data_sextan[~data_sextan["famille"].str.contains("ftv|lmf|solanid", case=False, na=False)].reset_index(drop=True)

    # Split infos produit_sextan (categorie, contenant, dlc)
    def split_product_info(value):
        parts = value.split("|")
        parts = [p.strip() for p in parts]
        categorie = parts[0] if parts else ""
        produit = parts[1] if len(parts) > 1 else ""
        contenant = ""
        dlc = ""
        if len(parts) > 2:
            if "j+" in parts[-1]:
                dlc = parts[-1]
                contenant = " ".join(parts[2:-1])
            else:
                contenant = " ".join(parts[2:])
        if contenant and "j+" in contenant:
            parts_contenant = contenant.split()
            dlc = parts_contenant[-1]
            contenant = " ".join(parts_contenant[:-1])
        return pd.Series([categorie, produit, contenant, dlc])

    data_sextan[["categorie", "produit_sextan", "contenant", "dlc"]] = data_sextan["produit_sextan"].apply(split_product_info)
    data_sextan = data_sextan.applymap(lambda x: x.lower() if isinstance(x, str) else x)

    # --- 9. Correspondance produits ---
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

    # --- 10. Fusion ---
    merged_data = data_izydesk.merge(data_sextan, left_on="produit_match", right_on="produit_sextan", how="left")
    merged_data["produit_sextan"] = merged_data.apply(lambda row: row["produit_match"] if pd.isna(row["produit_sextan"]) else row["produit_sextan"], axis=1)
    merged_data["produit_sextan_trouve"] = merged_data["produit_sextan"].notna()
    merged_data.drop(columns=["produit_match"], inplace=True)
    merged_data = merged_data.drop_duplicates(subset=["id_commande", "date", "heure", "service", "produit"], keep="first").reset_index(drop=True)

    # --- 11. Export ---
    nom_export_izydesk = f"exports/izydesk_auto_{nom_corner}.xlsx"
    nom_export_merged = f"exports/merged_data_auto_{nom_corner}.xlsx"

    data_izydesk.to_excel(nom_export_izydesk, index=False)
    merged_data.to_excel(nom_export_merged, index=False)

    return data_izydesk, merged_data
