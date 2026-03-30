# =========================
# IMPORTS
# =========================
import sqlite3
from datetime import datetime, date, timedelta
from io import BytesIO

import pandas as pd
import streamlit as st
import altair as alt


# =========================
# CONFIG
# =========================
DB_PATH = "transport_controle.db"
DEFAULT_TOLERANCE = 0.45


NORMALISATION_SERVICES = {
    "DPD Business SP": "DPD Business",
    "DPD Business NP": "DPD Business",
    "DPD  Business NP": "DPD Business",
    "DPD HOME NP": "DPD HOME",
    "DPD Home NP": "DPD HOME",
    "DPD Home SP": "DPD HOME",
}


# =========================
# DB
# =========================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            filename TEXT,
            segment TEXT,
            nb_lignes INTEGER,
            nb_ok INTEGER,
            nb_ko INTEGER,
            nb_incomplet INTEGER,
            montant_facture_total REAL,
            montant_calcule_total REAL,
            ecart_total REAL,
            ecart_total_pos REAL,
            ecart_total_neg REAL,
            taux_conformite REAL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS run_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            numero_facture TEXT,
            reference_expedition TEXT,
            transporteur TEXT,
            service_code TEXT,
            pays_dest TEXT,
            cp_dest TEXT,
            poids_kg REAL,
            montant_facture_ht REAL,
            montant_calcule_ht REAL,
            ecart_ht REAL,
            statut TEXT,
            raison TEXT,
            date_facture TEXT
        )
    """)

    conn.commit()
    conn.close()


# =========================
# OUTILS
# =========================
def parse_date(val):
    if pd.isna(val) or val == "":
        return None
    if isinstance(val, (datetime, date)):
        return datetime(val.year, val.month, val.day)
    if isinstance(val, (int, float)) and val > 20000:
        return datetime(1899, 12, 30) + timedelta(days=float(val))
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(val), fmt)
        except ValueError:
            continue
    return None


def normaliser_cp(s):
    return str(s).strip().replace(" ", "").upper()


# =========================
# PALETTES – FACTURE
# =========================
def agreger_facture_brut_palette(df):
    df = df.copy()

    df["date_facture"] = df["date_facture"].apply(parse_date)
    df["cp_dest"] = df["cp_dest"].apply(normaliser_cp)

    group_cols = [
        "numero_facture",
        "reference_expedition",
        "date_facture",
        "transporteur",
        "service_code",
        "pays_dest",
        "cp_dest",
    ]

    def agg(g):
        return pd.Series({
            "nb_palettes": g["nb_palettes"].max(),
            "montant_ligne_ht": g["montant_ht"].sum(),
        })

    return df.groupby(group_cols, dropna=False).apply(agg).reset_index()


# =========================
# PALETTES – TARIFS
# =========================
def preparer_tarifs_palette(df):
    t = df.copy()

    t["cp_debut"] = t["cp_debut"].astype(str).str.strip()
    t["cp_fin"] = t["cp_fin"].astype(str).str.strip()

    for col in [
        "prix_base_ht",
        "taxe_km_ht_par_km",
        "taxe_gazoil_pct",
        "taxe_gestion PAL",
        "taxe_rdv_ht",
        "taxe_sécurité",
        "taxe_énergie",
    ]:
        t[col] = pd.to_numeric(t.get(col, 0), errors="coerce").fillna(0)

    t["date_debut"] = t["date_debut"].apply(parse_date)
    t["date_fin"] = t["date_fin"].apply(parse_date)

    return t


def tarif_actif(t, d):
    if d is None:
        return True
    if t["date_debut"] and d < t["date_debut"]:
        return False
    if t["date_fin"] and d > t["date_fin"]:
        return False
    return True


def trouver_tarif_palette(tarifs, row):
    cp = normaliser_cp(row["cp_dest"])
    nb_pal = row["nb_palettes"]

    cand = tarifs[
        (tarifs["transporteur"] == row["transporteur"]) &
        (tarifs["service_code"] == row["service_code"]) &
        (tarifs["pays_dest"] == row["pays_dest"])
    ]

    cand = cand[cand.apply(lambda r: tarif_actif(r, row["date_facture"]), axis=1)]

    def match_cp(r):
        if r["cp_debut"] == "" and r["cp_fin"] == "":
            return True
        return r["cp_debut"] <= cp <= r["cp_fin"]

    cand = cand[cand.apply(match_cp, axis=1)]
    cand = cand[(cand["nb_pal_min"] <= nb_pal) & (nb_pal <= cand["nb_pal_max"])]

    return None if cand.empty else cand.iloc[0]


# =========================
# CONTROLE PALETTES
# =========================
def controler_palettes(file, tolerance):
    xls = pd.ExcelFile(file)
    tarifs = preparer_tarifs_palette(pd.read_excel(xls, "tarifs_palette"))
    fact = agreger_facture_brut_palette(pd.read_excel(xls, "facture_palette_brut"))

    lignes = []

    for _, r in fact.iterrows():
        t = trouver_tarif_palette(tarifs, r)

        if t is None:
            lignes.append({**r, "statut": "INCOMPLET", "raison": "Aucun tarif trouvé"})
            continue

        base = t["prix_base_ht"]
        gestion = r["nb_palettes"] * t["taxe_gestion PAL"]
        rdv = t["taxe_rdv_ht"]

        calc = base + gestion + rdv
        ecart = r["montant_ligne_ht"] - calc

        if ecart < 0:
            statut = "OK"
        elif ecart <= tolerance:
            statut = "OK"
        else:
            statut = "KO"

        lignes.append({
            **r,
            "montant_calcule_ht": round(calc, 2),
            "ecart_ht": round(ecart, 2),
            "statut": statut,
            "raison": ""
        })

    return pd.DataFrame(lignes)


# =========================
# STREAMLIT
# =========================
def main():
    st.set_page_config("Contrôle transport", layout="wide")
    st.title("🚚 Contrôle factures transport – Palettes")

    init_db()

    tolerance = st.number_input("Tolérance (€)", 0.0, 10.0, DEFAULT_TOLERANCE, 0.05)

    file = st.file_uploader("Fichier Excel", type=["xlsx"])
    if file and st.button("Lancer le contrôle"):
        df = controler_palettes(file, tolerance)
        st.dataframe(df, use_container_width=True)


if __name__ == "__main__":
    main()
