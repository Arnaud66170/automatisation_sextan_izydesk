import streamlit as st
import pandas as pd
import os
from io import BytesIO

def convert_df_to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()


# --- Interface Streamlit ---
st.title("Fusion automatique des données Sextan et Izydesk")

# --- Upload des fichiers ---
st.sidebar.header("Importer les fichiers")
file_sextan = st.sidebar.file_uploader("Importer le fichier Sextan", type=["xlsx"])
file_izydesk = st.sidebar.file_uploader("Importer le fichier Izydesk", type=["xlsx"])

# --- Traitement si les deux fichiers sont chargés ---
if file_sextan and file_izydesk:
    st.success("Les deux fichiers sont chargés. Traitement en cours...")

    # --- Sauvegarde temporaire des fichiers uploadés ---
    temp_sextan = os.path.join("temp", file_sextan.name)
    temp_izydesk = os.path.join("temp", file_izydesk.name)
    os.makedirs("temp", exist_ok=True)

    with open(temp_sextan, "wb") as f:
        f.write(file_sextan.getbuffer())

    with open(temp_izydesk, "wb") as f:
        f.write(file_izydesk.getbuffer())

    # --- Exécution du Notebook Backend ---
    from notebook_backend import process_files
    
    # Appel de la fonction principale avec les chemins temporaires
    izydesk_result, merged_result = process_files(temp_sextan, temp_izydesk)

    # --- Affichage des résultats ---
    st.subheader("Résultats consolidés")

    st.write("### Données Izydesk consolidées")
    st.dataframe(izydesk_result)

    st.write("### Données fusionnées consolidées")
    st.dataframe(merged_result)

    # --- Export ---
    st.download_button(
        label="Télécharger données Izydesk",
        data=convert_df_to_excel(izydesk_result),
        file_name="izydesk_auto.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.download_button(
        label="Télécharger données fusionnées",
        data=convert_df_to_excel(merged_result),
        file_name="merged_data_auto.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.warning("Veuillez importer les deux fichiers pour commencer le traitement.")