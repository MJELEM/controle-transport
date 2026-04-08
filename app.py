import sqlite3
from datetime import datetime, date, timedelta
from io import BytesIO
from pathlib import Path
import re

import numpy as np
import pandas as pd
import streamlit as st
import altair as alt
import hashlib

# =========================
# UTILISATEURS
# =========================

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

USERS = {
    "mathieu": {
        "password": hash_password("Mathieu!2026"),
        "role": "admin",
    },
    "user": {
        "password": hash_password("userElem!2026"),
        "role": "user",
    },
}

def login():
    st.markdown("## 🔐 Connexion")

    username = st.text_input("Utilisateur")
    password = st.text_input("Mot de passe", type="password")

    if st.button("Se connecter"):
        if username in USERS:
            hashed = hash_password(password)
            if USERS[username]["password"] == hashed:
                st.session_state["authenticated"] = True
                st.session_state["user"] = username
                st.session_state["role"] = USERS[username]["role"]
                st.success("Connexion réussie")
                st.rerun()
            else:
                st.error("Mot de passe incorrect")
        else:
            st.error("Utilisateur inconnu")

# =========================
# CONFIG
# =========================
DB_PATH = "transport_controle.db"
DEFAULT_TOLERANCE_EUR = 0.45
TARIFS_PALETTE_MASTER_PATH = "tarifs_palette_master.xlsx"
TARIFS_COLIS_MASTER_PATH = "tarifs_colis_master.xlsx"

NORMALISATION_SERVICES = {
    "DPD Business SP": "DPD Business",
    "DPD Business NP": "DPD Business",
    "DPD  Business NP": "DPD Business",
    "DPD HOME NP": "DPD HOME",
    "DPD Home SP": "DPD HOME",
    "DPD Home NP": "DPD HOME",
}

DEBUG_POIDS_MATCH = False


# =========================
# DB
# =========================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
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
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS run_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            segment TEXT,
            numero_facture TEXT,
            reference_expedition TEXT,
            reference_client TEXT,
            date_facture TEXT,
            transporteur TEXT,
            service_code TEXT,
            pays_orig TEXT,
            cp_orig TEXT,
            pays_dest TEXT,
            cp_dest TEXT,
            poids_kg REAL,
            nb_palettes REAL,
            distance_km REAL,
            poids_total_kg REAL,
            montant_facture_ht REAL,
            montant_calcule_ht REAL,
            ecart_ht REAL,
            ecart_pos REAL,
            ecart_neg REAL,
            statut TEXT,
            raison TEXT,
            FOREIGN KEY(run_id) REFERENCES runs(id)
        )
        """
    )

    def try_alter(sql: str):
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    try_alter("ALTER TABLE run_lines ADD COLUMN reference_client TEXT")
    try_alter("ALTER TABLE run_lines ADD COLUMN pays_orig TEXT")
    try_alter("ALTER TABLE run_lines ADD COLUMN cp_orig TEXT")
    try_alter("ALTER TABLE run_lines ADD COLUMN nb_palettes REAL")
    try_alter("ALTER TABLE run_lines ADD COLUMN distance_km REAL")
    try_alter("ALTER TABLE run_lines ADD COLUMN poids_total_kg REAL")
    try_alter("ALTER TABLE runs ADD COLUMN ecart_total_pos REAL")
    try_alter("ALTER TABLE runs ADD COLUMN ecart_total_neg REAL")

    conn.commit()
    conn.close()


def save_run_and_lines(run_info: dict, df_lines: pd.DataFrame) -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO runs (
            created_at, filename, segment,
            nb_lignes, nb_ok, nb_ko, nb_incomplet,
            montant_facture_total, montant_calcule_total,
            ecart_total, ecart_total_pos, ecart_total_neg,
            taux_conformite
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_info["created_at"],
            run_info["filename"],
            run_info["segment"],
            run_info["nb_lignes"],
            run_info["nb_ok"],
            run_info["nb_ko"],
            run_info["nb_incomplet"],
            run_info["montant_facture_total"],
            run_info["montant_calcule_total"],
            run_info["ecart_total"],
            run_info.get("ecart_total_pos", 0.0),
            run_info.get("ecart_total_neg", 0.0),
            run_info["taux_conformite"],
        ),
    )
    run_id = cur.lastrowid

    df = df_lines.copy()
    df["segment"] = run_info["segment"]

    required_cols = [
        "segment",
        "numero_facture",
        "reference_expedition",
        "reference_client",
        "date_facture",
        "transporteur",
        "service_code",
        "pays_orig",
        "cp_orig",
        "pays_dest",
        "cp_dest",
        "poids_kg",
        "nb_palettes",
        "distance_km",
        "poids_total_kg",
        "montant_facture_ht",
        "montant_calcule_ht",
        "ecart_ht",
        "ecart_pos",
        "ecart_neg",
        "statut",
        "raison",
    ]

    for c in required_cols:
        if c not in df.columns:
            df[c] = "" if c in [
                "segment", "numero_facture", "reference_expedition", "reference_client",
                "date_facture", "transporteur", "service_code",
                "pays_orig", "cp_orig", "pays_dest", "cp_dest", "statut", "raison"
            ] else 0.0

    num_cols = [
        "poids_kg", "nb_palettes", "distance_km", "poids_total_kg",
        "montant_facture_ht", "montant_calcule_ht",
        "ecart_ht", "ecart_pos", "ecart_neg"
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    records = []
    for _, r in df.iterrows():
        values = [
            r.get("segment", ""),
            r.get("numero_facture", ""),
            r.get("reference_expedition", ""),
            r.get("reference_client", ""),
            r.get("date_facture", ""),
            r.get("transporteur", ""),
            r.get("service_code", ""),
            r.get("pays_orig", ""),
            r.get("cp_orig", ""),
            r.get("pays_dest", ""),
            r.get("cp_dest", ""),
            float(r.get("poids_kg", 0) or 0),
            float(r.get("nb_palettes", 0) or 0),
            float(r.get("distance_km", 0) or 0),
            float(r.get("poids_total_kg", 0) or 0),
            float(r.get("montant_facture_ht", 0) or 0),
            float(r.get("montant_calcule_ht", 0) or 0),
            float(r.get("ecart_ht", 0) or 0),
            float(r.get("ecart_pos", 0) or 0),
            float(r.get("ecart_neg", 0) or 0),
            r.get("statut", ""),
            r.get("raison", ""),
        ]
        records.append((run_id, *values))

    cols_sql = ["run_id"] + required_cols
    placeholders = ", ".join(["?"] * len(cols_sql))
    sql = f"INSERT INTO run_lines ({', '.join(cols_sql)}) VALUES ({placeholders})"
    cur.executemany(sql, records)

    conn.commit()
    conn.close()
    return run_id


def get_runs():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM runs ORDER BY created_at DESC", conn)
    conn.close()
    return df


def get_run_lines(run_id: int):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM run_lines WHERE run_id = ?", conn, params=(run_id,))
    conn.close()
    return df


def get_all_lines():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM run_lines", conn)
    conn.close()
    return df


# =========================
# UTILS
# =========================
def parse_date_any(val):
    if pd.isna(val) or val == "":
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, date):
        return datetime(val.year, val.month, val.day)

    if isinstance(val, (int, float, np.integer, np.floating)):
        if float(val) > 20000:
            return datetime(1899, 12, 30) + timedelta(days=float(val))
        return None

    s = str(val).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def normaliser_cp_text(val) -> str:
    if val is None:
        return ""
    if isinstance(val, float) and np.isnan(val):
        return ""
    s = str(val).strip().upper().replace(" ", "").replace(",", ".")
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    return s


def normaliser_service_code_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    s = s.str.replace(r"\s+", " ", regex=True)
    return s.replace(NORMALISATION_SERVICES)


def safe_num(val, default=0.0) -> float:
    try:
        if pd.isna(val):
            return default
        return float(val)
    except Exception:
        return default


def map_country(val: str) -> str:
    if pd.isna(val):
        return ""
    s = str(val).strip().upper()
    if s in ["B", "BEL", "BELGIQUE", "BE"]:
        return "BE"
    if s in ["F", "FR", "FRANCE"]:
        return "FR"
    if s in ["L", "LUX", "LU", "LUXEMBOURG"]:
        return "LU"
    if s in ["NL", "N", "PAYS-BAS", "PAYS BAS"]:
        return "NL"
    if len(s) == 2:
        return s
    return s[:2]


def pick_first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {str(c).lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def clean_numeric(val):
    if pd.isna(val):
        return np.nan
    s = str(val).replace(",", ".").strip()
    try:
        return float(s)
    except Exception:
        return np.nan


def normalize_headers_for_detection(cols) -> set[str]:
    out = set()
    for c in cols:
        s = str(c).strip().lower()
        s = s.replace("é", "e").replace("è", "e").replace("ê", "e")
        s = s.replace("à", "a").replace("ù", "u").replace("ô", "o")
        s = s.replace("ï", "i").replace("î", "i")
        s = re.sub(r"\s+", " ", s)
        out.add(s)
    return out


def match_cp_generic(cp_norm: str, cp_deb: str, cp_fin: str) -> bool:
    deb = normaliser_cp_text(cp_deb)
    fin = normaliser_cp_text(cp_fin)
    cp_norm = normaliser_cp_text(cp_norm)

    if deb == "" and fin == "":
        return True

    try:
        cp_int = int(cp_norm)
        try:
            return int(float(deb)) <= cp_int <= int(float(fin))
        except Exception:
            pass
    except Exception:
        pass

    return deb <= cp_norm <= fin


# =========================
# MASTER TARIFS
# =========================
def load_tarifs_palette_master() -> pd.DataFrame:
    p = Path(TARIFS_PALETTE_MASTER_PATH)
    if not p.exists():
        raise FileNotFoundError(
            f"Fichier manquant: '{TARIFS_PALETTE_MASTER_PATH}'. Place-le dans le dossier du projet."
        )
    xls = pd.ExcelFile(str(p))
    if "tarifs_palette" not in xls.sheet_names:
        raise ValueError(
            f"Le fichier '{TARIFS_PALETTE_MASTER_PATH}' doit contenir un onglet 'tarifs_palette'."
        )
    return pd.read_excel(xls, "tarifs_palette")


def load_tarifs_colis_master() -> pd.DataFrame:
    p = Path(TARIFS_COLIS_MASTER_PATH)
    if not p.exists():
        raise FileNotFoundError(
            f"Fichier manquant: '{TARIFS_COLIS_MASTER_PATH}'. Place-le dans le dossier du projet."
        )
    xls = pd.ExcelFile(str(p))
    if "tarifs" not in xls.sheet_names:
        raise ValueError(
            f"Le fichier '{TARIFS_COLIS_MASTER_PATH}' doit contenir un onglet 'tarifs'."
        )
    return pd.read_excel(xls, "tarifs")


# =========================
# DETECTION FACTURES BRUTES
# =========================
def is_probably_tfm_raw_excel(uploaded_file) -> bool:
    try:
        xls = pd.ExcelFile(uploaded_file)
        if "facture_palette_brut" in xls.sheet_names:
            return False

        tfm_markers_strong = {
            "article",
            "quantite tarif",
            "quantite",
            "nombre",
            "unite.1",
            "pays.1",
            "code postal.1",
            "km",
            "total",
        }

        tfm_markers_soft = {
            "date facture",
            "numero dossier",
            "n° facture",
            "no facture",
            "code postal",
            "pays",
            "unite",
            "poids 1",
            "poids",
        }

        for sheet in xls.sheet_names:
            try:
                df0 = pd.read_excel(xls, sheet_name=sheet, nrows=10)
                cols = normalize_headers_for_detection(df0.columns)

                strong_hits = len(cols.intersection(tfm_markers_strong))
                soft_hits = len(cols.intersection(tfm_markers_soft))

                if strong_hits >= 4:
                    return True
                if strong_hits >= 3 and soft_hits >= 1:
                    return True
            except Exception:
                continue

        return False
    except Exception:
        return False


def is_probably_geodis_raw_excel(uploaded_file) -> bool:
    try:
        xls = pd.ExcelFile(uploaded_file)
        if "facture_palette_brut" in xls.sheet_names:
            return False

        geodis_markers_strong = {
            "facture",
            "date_fact",
            "no_recep",
            "reference",
            "pays_d",
            "cp_d",
            "rate",
            "fuel",
            "km-taks",
        }

        geodis_markers_soft = {
            "pays_e",
            "cp_e",
            "poids reel",
            "poids brut",
            "nb_colis",
        }

        for sheet in xls.sheet_names:
            try:
                df0 = pd.read_excel(xls, sheet_name=sheet, nrows=10)
                cols = normalize_headers_for_detection(df0.columns)

                strong_hits = len(cols.intersection(geodis_markers_strong))
                soft_hits = len(cols.intersection(geodis_markers_soft))

                if strong_hits >= 4:
                    return True
                if strong_hits >= 3 and soft_hits >= 2:
                    return True
            except Exception:
                continue

        return False
    except Exception:
        return False


def is_probably_dpd_raw_excel(uploaded_file) -> bool:
    try:
        xls = pd.ExcelFile(uploaded_file)
        if "facture_lignes" in xls.sheet_names:
            return False

        for sheet in xls.sheet_names:
            try:
                df0 = pd.read_excel(xls, sheet_name=sheet, nrows=10)
                cols = normalize_headers_for_detection(df0.columns)

                markers = {
                    "invoice number",
                    "parcel number",
                    "scan date",
                    "product name",
                    "receiver zip code",
                    "total net amount",
                    "country",
                }

                if len(cols.intersection(markers)) >= 4:
                    return True
            except Exception:
                continue

        return False
    except Exception:
        return False
    

def is_probably_gls_raw_excel(uploaded_file) -> bool:
    try:
        xls = pd.ExcelFile(uploaded_file)
        if "facture_lignes" in xls.sheet_names:
            return False

        for sheet in xls.sheet_names:
            try:
                df0 = pd.read_excel(xls, sheet_name=sheet, nrows=10)
                cols = normalize_headers_for_detection(df0.columns)

                markers = {
                    "numero document",
                    "date document",
                    "produit",
                    "flux",
                    "code pays destinataire",
                    "code postal destinataire",
                    "reference gls",
                    "reference expediteur",
                    "poids taxation",
                    "total (d/c)",
                }

                if len(cols.intersection(markers)) >= 5:
                    return True
            except Exception:
                continue

        return False
    except Exception:
        return False
    

def is_probably_vmg_raw_excel(uploaded_file) -> bool:
    try:
        xls = get_excel_file_resilient(uploaded_file)

        if "facture_palette_brut" in xls.sheet_names:
            return False

        if "FACTURES" not in xls.sheet_names:
            return False

        df0 = load_excel_resilient(uploaded_file, sheet_name="FACTURES", header=None)

        sample_text = " ".join(
            str(v) for v in df0.head(30).fillna("").astype(str).values.flatten()
        ).lower()

        markers = [
            "n° odisce",
            "nom expéditeur",
            "pays dest.",
            "prix transports",
            "poids taxable",
            "palettes 80 x 120",
        ]

        hits = sum(1 for m in markers if m in sample_text)
        return hits >= 4

    except Exception:
        return False

def is_probably_gls_pal_raw_excel(uploaded_file) -> bool:
    try:
        xls = pd.ExcelFile(uploaded_file)

        if "facture_palette_brut" in xls.sheet_names:
            return False

        sheet = xls.sheet_names[0]
        df0 = pd.read_excel(xls, sheet_name=sheet, nrows=20)

        cols = {str(c).strip().upper() for c in df0.columns}

        markers = {
            "NO_FACTURE",
            "DATE_FACTURE",
            "NO_ENVOI",
            "REF_EXPED",
            "CP_EXPED",
            "PAYS_EXPED",
            "CP_DEST",
            "PAYS_DEST",
            "NB_PALETTE",
            "TYPE_FRAIS",
            "MONTANT_HT",
        }

        hits = len(cols.intersection(markers))

        if hits >= 8:
            return True

        # sécurité supplémentaire sur le contenu
        sample_text = " ".join(
            str(v) for v in df0.fillna("").astype(str).values.flatten()
        ).upper()

        if "FRAIS DE PORT (PICK&RETURN)" in sample_text:
            return True
        if "NB_PALETTE" in sample_text and "TYPE_FRAIS" in sample_text:
            return True

        return False

    except Exception:
        return False


# =========================
# CONVERTISSEURS
# =========================
def infer_type_ligne_tfm(article: str) -> str:
    a = str(article).upper()
    if "GAZOIL" in a or "GASOIL" in a:
        return "GAZOIL"
    if "PEAGE" in a or "PÉAGE" in a or "SURCHARGE" in a or "GESTION" in a or "RDV" in a:
        return "AUTRES"
    return "BASE"


def convert_tfm_palette_to_standard(file_bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    src = pd.read_excel(file_bytes)

    ref_col = pick_first_existing_col(
        src,
        ["Référence clients", "Reference clients", "Référence client", "Reference client", "Ref client", "Réf client"],
    )
    reference_client = src[ref_col].astype(str).str.strip() if ref_col else ""

    pays = src.get("Pays.1", "").apply(map_country)
    qt = pd.to_numeric(src.get("Quantité tarif"), errors="coerce").fillna(0)
    unit1 = src.get("Unité.1", "").astype(str).str.strip().str.upper()
    nombre = pd.to_numeric(src.get("Nombre"), errors="coerce").fillna(0)

    nb_palettes = pd.Series(0.0, index=src.index)
    nb_palettes[pays == "FR"] = nombre[pays == "FR"]

    nb_qt = qt.copy()
    nb_qt[unit1 == "DEMI"] = nb_qt[unit1 == "DEMI"] * 0.5
    nb_palettes[pays != "FR"] = nb_qt[pays != "FR"]

    col_poids = pick_first_existing_col(
        src,
        ["Poids 1", "Poids", "Poids réel", "Poids reel", "Poids brut", "KG"]
    )

    poids_total_kg = pd.Series(0.0, index=src.index)
    if col_poids:
        poids_reel = pd.to_numeric(src[col_poids], errors="coerce").fillna(0)
        poids_total_kg[pays == "FR"] = poids_reel[pays == "FR"]
    else:
        poids_total_kg[pays == "FR"] = qt[pays == "FR"]

    df_std = pd.DataFrame({
        "numero_facture": src.get("N° Facture", "").astype(str).str.strip() if "N° Facture" in src.columns else "",
        "date_facture": pd.to_datetime(src.get("Date facture", ""), errors="coerce"),
        "reference_expedition": src.get("Numéro dossier", "").astype(str).str.strip() if "Numéro dossier" in src.columns else "",
        "reference_client": reference_client,
        "transporteur": "TFM",
        "service_code": "TFM_PAL",
        "pays_orig": "",
        "cp_orig": "",
        "pays_dest": pays,
        "cp_dest": src.get("Code postal.1", "").apply(normaliser_cp_text) if "Code postal.1" in src.columns else "",
        "nb_palettes": nb_palettes,
        "poids_total_kg": poids_total_kg,
        "distance_km": pd.to_numeric(src.get("KM", 0), errors="coerce").fillna(0),
        "type_ligne": src.get("Article", "").apply(infer_type_ligne_tfm),
        "montant_ht": pd.to_numeric(src.get("Total", 0), errors="coerce").fillna(0),
    })
    return df_std, src


def infer_type_ligne_geodis(libelle: str) -> str:
    s = str(libelle).upper()
    if "FUEL" in s or "GAZO" in s or "GASOIL" in s:
        return "GAZOIL"
    if "KM" in s or "KILOM" in s or "DIST" in s or "TAXE KM" in s:
        return "KM"
    return "BASE"


def convert_geodis_palette_to_standard(file_bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    src = pd.read_excel(file_bytes)

    col_fact = pick_first_existing_col(src, ["FACTURE", "Facture"])
    col_date = pick_first_existing_col(src, ["DATE_FACT", "Date_fact", "DATE", "Date"])
    col_rec = pick_first_existing_col(src, ["NO_RECEP", "NO RECEP", "N° RECEP", "NO_REC", "NO RECEP."])
    col_refc = pick_first_existing_col(src, ["REFERENCE", "Référence", "Référence clients", "Reference clients"])

    col_pays_d = pick_first_existing_col(src, ["PAYS_D", "Pays_D", "PAYS", "Pays"])
    col_cp_d = pick_first_existing_col(src, ["CP_D", "CP", "Code postal", "Code Postal", "CODE_POSTAL"])

    col_pays_e = pick_first_existing_col(src, ["PAYS_E", "Pays_E", "PAYS_ORIG", "Pays_orig"])
    col_cp_e = pick_first_existing_col(src, ["CP_E", "CP_ORIG", "Code postal enlèvement", "Code Postal Enlevement"])

    col_nb = pick_first_existing_col(src, ["NB_COLIS", "NB COLIS", "NB_PAL", "NB PAL", "PALETTES", "NOMBRE PAL"])
    col_pds = pick_first_existing_col(src, ["POIDS REEL", "POIDS_REEL", "POIDS", "POIDS_BRUT", "POIDS BRUT"])

    col_rate = pick_first_existing_col(src, ["RATE", "Rate"])
    col_fuel = pick_first_existing_col(src, ["FUEL", "Fuel"])
    col_km = pick_first_existing_col(src, ["KM-taks", "KM TAKS", "KM_TAKS"])

    col_lib = pick_first_existing_col(src, ["LIBELLE", "Libellé", "DESIGNATION", "Designation", "ARTICLE", "Article"])
    col_amt = pick_first_existing_col(src, ["MONTANT", "Montant", "TOTAL", "Total", "AMOUNT", "Amount", "HT", "Montant HT"])

    missing_core = [name for name, col in [
        ("FACTURE", col_fact),
        ("DATE_FACT", col_date),
        ("NO_RECEP", col_rec),
        ("PAYS_D", col_pays_d),
        ("CP_D", col_cp_d),
    ] if col is None]
    if missing_core:
        raise ValueError(f"Facture GEODIS : colonnes obligatoires manquantes: {missing_core}")

    numero_facture = src[col_fact].astype(str).str.strip()
    date_facture = pd.to_datetime(src[col_date], errors="coerce")
    reference_expedition = src[col_rec].astype(str).str.strip()
    reference_client = src[col_refc].astype(str).str.strip() if col_refc else ""

    pays_dest = src[col_pays_d].astype(str).str.strip().str.upper()
    cp_dest = src[col_cp_d].apply(normaliser_cp_text)

    pays_orig = src[col_pays_e].astype(str).str.strip().str.upper() if col_pays_e else pd.Series("", index=src.index)
    cp_orig = src[col_cp_e].apply(normaliser_cp_text) if col_cp_e else pd.Series("", index=src.index)

    nb_palettes = pd.to_numeric(src[col_nb], errors="coerce").fillna(0) if col_nb else pd.Series(0.0, index=src.index)
    poids_total = pd.to_numeric(src[col_pds], errors="coerce").fillna(0) if col_pds else pd.Series(0.0, index=src.index)

    if col_pds and len(poids_total) > 0 and poids_total.max() <= 10:
        poids_total = poids_total * 1000

    rows = []

    def compute_service_code(i: int) -> str:
        if str(pays_orig.iloc[i]).upper() == "FR" and str(pays_dest.iloc[i]).upper() == "BE":
            return "GEODIS_RET_BE"
        return "GEODIS_PAL"

    if col_rate and col_fuel and col_km:
        rate = pd.to_numeric(src[col_rate], errors="coerce").fillna(0)
        fuel = pd.to_numeric(src[col_fuel], errors="coerce").fillna(0)
        km = pd.to_numeric(src[col_km], errors="coerce").fillna(0)

        for i in range(len(src)):
            common = {
                "numero_facture": numero_facture.iloc[i],
                "date_facture": date_facture.iloc[i],
                "reference_expedition": reference_expedition.iloc[i],
                "reference_client": reference_client.iloc[i] if isinstance(reference_client, pd.Series) else reference_client,
                "transporteur": "GEODIS",
                "service_code": compute_service_code(i),
                "pays_orig": pays_orig.iloc[i] if isinstance(pays_orig, pd.Series) else "",
                "cp_orig": cp_orig.iloc[i] if isinstance(cp_orig, pd.Series) else "",
                "pays_dest": pays_dest.iloc[i],
                "cp_dest": cp_dest.iloc[i],
                "nb_palettes": float(nb_palettes.iloc[i] or 0),
                "poids_total_kg": float(poids_total.iloc[i] or 0),
                "distance_km": 0.0,
            }
            added = False
            for typ, series in [("BASE", rate), ("GAZOIL", fuel), ("KM", km)]:
                amt = float(series.iloc[i] or 0)
                if amt != 0:
                    rows.append({**common, "type_ligne": typ, "montant_ht": amt})
                    added = True
            if not added:
                rows.append({**common, "type_ligne": "BASE", "montant_ht": 0.0})

        return pd.DataFrame(rows), src

    if not (col_lib and col_amt):
        raise ValueError(
            "Facture GEODIS : format non reconnu.\n"
            "- Soit colonnes RATE/FUEL/KM-taks\n"
            "- Soit colonnes libellé + montant"
        )

    montant = pd.to_numeric(src[col_amt], errors="coerce").fillna(0)
    lib = src[col_lib].astype(str).fillna("")

    for i in range(len(src)):
        rows.append({
            "numero_facture": numero_facture.iloc[i],
            "date_facture": date_facture.iloc[i],
            "reference_expedition": reference_expedition.iloc[i],
            "reference_client": reference_client.iloc[i] if isinstance(reference_client, pd.Series) else reference_client,
            "transporteur": "GEODIS",
            "service_code": compute_service_code(i),
            "pays_orig": pays_orig.iloc[i] if isinstance(pays_orig, pd.Series) else "",
            "cp_orig": cp_orig.iloc[i] if isinstance(cp_orig, pd.Series) else "",
            "pays_dest": pays_dest.iloc[i],
            "cp_dest": cp_dest.iloc[i],
            "nb_palettes": float(nb_palettes.iloc[i] or 0),
            "poids_total_kg": float(poids_total.iloc[i] or 0),
            "distance_km": 0.0,
            "type_ligne": infer_type_ligne_geodis(lib.iloc[i]),
            "montant_ht": float(montant.iloc[i] or 0),
        })

    return pd.DataFrame(rows), src


def convert_gls_colis_to_standard(file_bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    src = pd.read_excel(file_bytes)

    def find_col_loose(df: pd.DataFrame, candidates: list[str]) -> str | None:
        norm = {}
        for c in df.columns:
            k = str(c).strip().lower()
            k = (
                k.replace("è", "e")
                 .replace("é", "e")
                 .replace("ê", "e")
                 .replace("à", "a")
                 .replace("ù", "u")
                 .replace("ô", "o")
                 .replace("ï", "i")
                 .replace("î", "i")
            )
            k = re.sub(r"\s+", " ", k)
            norm[k] = c

        for cand in candidates:
            k = str(cand).strip().lower()
            k = (
                k.replace("è", "e")
                 .replace("é", "e")
                 .replace("ê", "e")
                 .replace("à", "a")
                 .replace("ù", "u")
                 .replace("ô", "o")
                 .replace("ï", "i")
                 .replace("î", "i")
            )
            k = re.sub(r"\s+", " ", k)
            if k in norm:
                return norm[k]
        return None

    def clean_num_series(s: pd.Series) -> pd.Series:
        return (
            s.astype(str)
             .str.replace(",", ".", regex=False)
             .str.strip()
             .replace({"": np.nan, "nan": np.nan, "None": np.nan})
             .pipe(pd.to_numeric, errors="coerce")
        )

    col_fact = find_col_loose(src, ["Numéro document", "Numero document"])
    col_date = find_col_loose(src, ["Date document"])
    col_product = find_col_loose(src, ["Produit"])
    col_flux = find_col_loose(src, ["Flux"])
    col_country_orig = find_col_loose(src, ["Code pays expéditeur", "Code pays expediteur"])
    col_country_dest = find_col_loose(src, ["Code pays destinataire"])
    col_cp_dest = find_col_loose(src, ["Code postal destinataire"])
    col_ref_gls = find_col_loose(src, ["Référence GLS", "Reference GLS"])
    col_ref_exp = find_col_loose(src, ["Référence expéditeur", "Reference expediteur"])
    col_weight = find_col_loose(src, ["Poids taxation"])
    col_total = find_col_loose(src, ["TOTAL (D/C)"])
    col_pick_return = find_col_loose(src, ["Pick & Return / Pick & Ship"])

    missing = [name for name, col in [
        ("Numéro document", col_fact),
        ("Date document", col_date),
        ("Flux", col_flux),
        ("Code pays expéditeur", col_country_orig),
        ("Code pays destinataire", col_country_dest),
        ("Code postal destinataire", col_cp_dest),
        ("Référence GLS", col_ref_gls),
        ("Poids taxation", col_weight),
        ("TOTAL (D/C)", col_total),
    ] if col is None]

    if missing:
        raise ValueError(f"Colonnes GLS manquantes: {missing}")

    flux = src[col_flux].astype(str).str.strip().str.upper()
    pays_orig = src[col_country_orig].astype(str).str.strip().str.upper()
    pays_dest = src[col_country_dest].astype(str).str.strip().str.upper()
    pick_return = clean_num_series(src[col_pick_return]).fillna(0) if col_pick_return else pd.Series(0.0, index=src.index)

    is_reprise = pick_return > 0

    service_code = pd.Series("", index=src.index, dtype="object")

    # DOM
    service_code[(flux == "DOM") & (~is_reprise)] = "DOM_EXP"
    service_code[(flux == "DOM") & (is_reprise)] = "DOM_REP"

    # EXP
    service_code[(flux == "EXP") & (~is_reprise)] = "EXP_EXP"
    service_code[(flux == "EXP") & (is_reprise)] = "EXP_REP"

    # Règle métier GLS :
    # toute expédition FR -> BE est une reprise marchandise
    # Toute entrée vers BE depuis l’étranger = reprise GLS
    service_code[
        (flux == "EXP") &
        (pays_dest == "BE") &
        (pays_orig != "BE")
    ] = "EXP_REP"

    df = pd.DataFrame({
        "numero_facture": src[col_fact].astype(str).str.strip(),
        "date_facture": pd.to_datetime(src[col_date], errors="coerce"),
        "reference_expedition": src[col_ref_gls].astype(str).str.strip(),
        "reference_client": src[col_ref_exp].astype(str).str.strip() if col_ref_exp else "",
        "transporteur": "GLS",
        "service_code": service_code,
        "pays_orig": pays_orig,
        "pays_dest": pays_dest,
        "cp_dest": src[col_cp_dest].apply(normaliser_cp_text),
        "poids_kg": clean_num_series(src[col_weight]).fillna(0),
        "montant_ligne_ht": clean_num_series(src[col_total]).fillna(0),
        "surcharge_pick_return": pick_return,
        "produit_source": src[col_product].astype(str).str.strip() if col_product else "",
        "flux_source": flux,
    })

    group_keys = [
        "numero_facture",
        "reference_expedition",
        "reference_client",
        "date_facture",
        "transporteur",
        "service_code",
        "pays_orig",
        "pays_dest",
        "cp_dest",
    ]

    df_final = (
        df.groupby(group_keys, dropna=False)
        .agg(
            poids_kg=("poids_kg", "max"),
            montant_ligne_ht=("montant_ligne_ht", "sum"),
            surcharge_pick_return=("surcharge_pick_return", "sum"),
            produit_source=("produit_source", "first"),
            flux_source=("flux_source", "first"),
        )
        .reset_index()
    )

    return df_final, src

def convert_dpd_colis_to_standard(file_bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    src = pd.read_excel(file_bytes)

    def find_col_loose(df: pd.DataFrame, candidates: list[str]) -> str | None:
        norm = {}
        for c in df.columns:
            k = str(c).strip().lower()
            k = re.sub(r"\s+", " ", k)
            norm[k] = c
        for cand in candidates:
            k = str(cand).strip().lower()
            k = re.sub(r"\s+", " ", k)
            if k in norm:
                return norm[k]
        return None

    def clean_numeric_series(s: pd.Series) -> pd.Series:
        return (
            s.astype(str)
             .str.replace(",", ".", regex=False)
             .str.replace("kg", "", case=False, regex=False)
             .str.strip()
             .replace({"": np.nan, "nan": np.nan, "None": np.nan})
             .pipe(pd.to_numeric, errors="coerce")
        )

    col_fact = find_col_loose(src, ["Invoice Number"])
    col_date = find_col_loose(src, ["Scan Date"])
    col_parcel = find_col_loose(src, ["Parcel Number"])
    col_ref = find_col_loose(src, ["Reference 1", "Reference"])
    col_service = find_col_loose(src, ["Product name"])
    col_country = find_col_loose(src, ["Country"])
    col_cp = find_col_loose(src, ["Receiver Zip code"])
    col_corrected_weight = find_col_loose(src, ["Corrected Weight"])
    col_invoicing_weight = find_col_loose(src, ["Invoicing Weight"])
    col_weight_generic = find_col_loose(src, ["Weight"])
    col_amount = find_col_loose(src, ["Total Net Amount"])
    col_relabel = find_col_loose(src, ["Relabeling Surcharge"])

    missing = [name for name, col in [
        ("Invoice Number", col_fact),
        ("Scan Date", col_date),
        ("Parcel Number", col_parcel),
        ("Country", col_country),
        ("Receiver Zip code", col_cp),
        ("Total Net Amount", col_amount),
    ] if col is None]

    if missing:
        raise ValueError(f"Colonnes DPD manquantes: {missing}")

    poids = pd.Series(np.nan, index=src.index, dtype="float64")

    if col_corrected_weight:
        poids = clean_numeric_series(src[col_corrected_weight])

    if col_invoicing_weight:
        poids = poids.fillna(clean_numeric_series(src[col_invoicing_weight]))

    if col_weight_generic:
        poids = poids.fillna(clean_numeric_series(src[col_weight_generic]))

    poids = poids.fillna(0.0)

    relabel = (
        clean_numeric_series(src[col_relabel]).fillna(0)
        if col_relabel else pd.Series(0.0, index=src.index)
    )

    df = pd.DataFrame({
        "numero_facture": src[col_fact].astype(str).str.strip(),
        "date_facture": pd.to_datetime(src[col_date], errors="coerce"),
        "reference_expedition": src[col_parcel].astype(str).str.strip(),
        "reference_client": src[col_ref].astype(str).str.strip() if col_ref else "",
        "transporteur": "DPD",
        "service_code": src[col_service].astype(str).str.strip() if col_service else "",
        "pays_orig": "",
        "pays_dest": src[col_country].astype(str).str.strip().str.upper(),
        "cp_dest": src[col_cp].apply(normaliser_cp_text),
        "poids_kg": poids,
        "montant_ligne_ht": pd.to_numeric(src[col_amount], errors="coerce").fillna(0),
        "surcharge_relabeling": relabel,
    })

    group_keys = [
        "numero_facture",
        "reference_expedition",
        "reference_client",
        "date_facture",
        "transporteur",
        "service_code",
        "pays_orig",
        "pays_dest",
        "cp_dest",
    ]

    df_final = (
        df.groupby(group_keys, dropna=False)
        .agg(
            poids_kg=("poids_kg", "max"),
            montant_ligne_ht=("montant_ligne_ht", "sum"),
            surcharge_relabeling=("surcharge_relabeling", "sum"),
        )
        .reset_index()
    )

    return df_final, src


def convert_vmg_palette_to_standard(file_bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convertit la facture brute VMG (onglet FACTURES) vers le format standard
    attendu par le moteur palettes : facture_palette_brut
    """
    src = load_excel_resilient(file_bytes, sheet_name="FACTURES", header=20).copy()

    # sécurisation noms de colonnes
    src.columns = [str(c).strip() for c in src.columns]

    # garder seulement les lignes exploitables
    if "Pos." in src.columns:
        src = src[src["Pos."].notna()].copy()

    if " Facturable oui / non" in src.columns:
        src = src[src[" Facturable oui / non"].astype(str).str.strip().str.lower().isin(["oui", "yes", "true", "1"])].copy()

    # colonnes utiles
    def num_col(name):
        return pd.to_numeric(src[name], errors="coerce").fillna(0) if name in src.columns else pd.Series(0.0, index=src.index)

    pal_60_80 = num_col("Palettes 60 x 80")
    pal_80_120 = num_col("Palettes 80 x 120")
    pal_100_120 = num_col("Palettes 100 x 120")
    pal_120_120 = num_col("Paletttes 120 x 120")

    nb_palettes = pal_60_80 + pal_80_120 + pal_100_120 + pal_120_120

    # poids
    poids_total_kg = num_col("Poids taxable")

    # montants VMG
    prix_transports = num_col("Prix transports")
    prix_divers = num_col("Prix divers")
    taxe_km = num_col("Taxe kilométrique")

    # fallback numéro facture à partir de l'en-tête
    top = load_excel_resilient(file_bytes, sheet_name="FACTURES", header=None)
    numero_facture_default = "VMG"
    try:
        vm_debut = str(top.iloc[7, 1]).strip() if pd.notna(top.iloc[7, 1]) else ""
        vm_fin = str(top.iloc[8, 1]).strip() if pd.notna(top.iloc[8, 1]) else ""
        if vm_debut or vm_fin:
            numero_facture_default = f"VMG_{vm_debut}_{vm_fin}"
    except Exception:
        pass

    # service_code simple pour commencer
    # on pourra raffiner plus tard si tu as plusieurs tarifs VMG distincts
    service_code = "VMG_PAL"

    rows = []

    for i in src.index:
        common = {
            "numero_facture": numero_facture_default,
            "date_facture": pd.to_datetime(src.at[i, "Date"], errors="coerce") if "Date" in src.columns else pd.NaT,
            "reference_expedition": str(src.at[i, "N° odisce"]).strip() if "N° odisce" in src.columns else "",
            "reference_client": str(src.at[i, "N° commande"]).strip() if "N° commande" in src.columns else "",
            "transporteur": "VMG",
            "service_code": service_code,
            "pays_orig": str(src.at[i, "Pays exp."]).strip().upper() if "Pays exp." in src.columns else "",
            "cp_orig": normaliser_cp_text(src.at[i, "CP exp"]) if "CP exp" in src.columns else "",
            "pays_dest": str(src.at[i, "Pays dest."]).strip().upper() if "Pays dest." in src.columns else "",
            "cp_dest": normaliser_cp_text(src.at[i, "CP dest"]) if "CP dest" in src.columns else "",
            "nb_palettes": float(nb_palettes.loc[i] or 0),
            "poids_total_kg": float(poids_total_kg.loc[i] or 0),
            "distance_km": 0.0,
        }

        # BASE
        if float(prix_transports.loc[i] or 0) != 0:
            rows.append({
                **common,
                "type_ligne": "BASE",
                "montant_ht": float(prix_transports.loc[i] or 0),
            })

        # KM
        if float(taxe_km.loc[i] or 0) != 0:
            rows.append({
                **common,
                "type_ligne": "KM",
                "montant_ht": float(taxe_km.loc[i] or 0),
            })

        # AUTRES
        if float(prix_divers.loc[i] or 0) != 0:
            rows.append({
                **common,
                "type_ligne": "AUTRES",
                "montant_ht": float(prix_divers.loc[i] or 0),
            })

        # si rien du tout, on garde quand même une ligne BASE à 0
        if (
            float(prix_transports.loc[i] or 0) == 0
            and float(prix_divers.loc[i] or 0) == 0
            and float(taxe_km.loc[i] or 0) == 0
        ):
            rows.append({
                **common,
                "type_ligne": "BASE",
                "montant_ht": 0.0,
            })

    df_std = pd.DataFrame(rows)
    return df_std, src

def convert_gls_palette_to_standard(file_bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    src = pd.read_excel(file_bytes).copy()

    src.columns = [str(c).strip() for c in src.columns]

    def clean_num_series(s: pd.Series) -> pd.Series:
        return (
            s.astype(str)
             .str.replace(",", ".", regex=False)
             .str.strip()
             .replace({"": np.nan, "nan": np.nan, "None": np.nan})
             .pipe(pd.to_numeric, errors="coerce")
        )

    # colonnes principales
    col_fact = "NO_FACTURE" if "NO_FACTURE" in src.columns else None
    col_date = "DATE_FACTURE" if "DATE_FACTURE" in src.columns else None
    col_envoi = "NO_ENVOI" if "NO_ENVOI" in src.columns else None
    col_ref = "REF_EXPED" if "REF_EXPED" in src.columns else None
    col_cp_exp = "CP_EXPED" if "CP_EXPED" in src.columns else None
    col_pays_exp = "PAYS_EXPED" if "PAYS_EXPED" in src.columns else None
    col_cp_dest = "CP_DEST" if "CP_DEST" in src.columns else None
    col_pays_dest = "PAYS_DEST" if "PAYS_DEST" in src.columns else None
    col_nb_pal = "NB_PALETTE" if "NB_PALETTE" in src.columns else None
    col_poids = "POIDS" if "POIDS" in src.columns else None
    col_type_frais = "TYPE_FRAIS" if "TYPE_FRAIS" in src.columns else None
    col_montant = "MONTANT_HT" if "MONTANT_HT" in src.columns else None

    missing = [name for name, col in [
        ("NO_FACTURE", col_fact),
        ("DATE_FACTURE", col_date),
        ("NO_ENVOI", col_envoi),
        ("PAYS_EXPED", col_pays_exp),
        ("CP_EXPED", col_cp_exp),
        ("PAYS_DEST", col_pays_dest),
        ("CP_DEST", col_cp_dest),
        ("TYPE_FRAIS", col_type_frais),
        ("MONTANT_HT", col_montant),
    ] if col is None]

    if missing:
        raise ValueError(f"Colonnes GLS PAL manquantes: {missing}")

    src["__montant__"] = clean_num_series(src[col_montant]).fillna(0.0)
    src["__poids__"] = clean_num_series(src[col_poids]).fillna(0.0) if col_poids else 0.0
    src["__nb_pal__"] = clean_num_series(src[col_nb_pal]).fillna(0.0) if col_nb_pal else 0.0
    src["__type_frais__"] = src[col_type_frais].astype(str).str.strip().str.upper()

    grp = src.groupby(col_envoi, dropna=False)
    is_palette_envoi = grp.apply(
        lambda g: (
            (pd.to_numeric(g["__nb_pal__"], errors="coerce").fillna(0.0) > 0).any()
            or g["__type_frais__"].str.contains("PALETTE", na=False).any()
            or g["__type_frais__"].str.contains("PICK&RETURN", na=False).any()
        )
    )

    palette_ids = set(is_palette_envoi[is_palette_envoi].index.tolist())
    src = src[src[col_envoi].isin(palette_ids)].copy()

    if src.empty:
        return pd.DataFrame(), pd.DataFrame()

    rows = []

    def compute_type_ligne(type_frais: str) -> str:
        s = str(type_frais).upper()

        if s == "FRAIS DE PORT":
            return "BASE"
        if "PICK&RETURN" in s:
            return "AUTRES"
        if "KILOMET" in s:
            return "KM"
        if "ENERG" in s:
            return "GAZOIL"
        if "SECUR" in s or "SÈCUR" in s:
            return "AUTRES"
        if "PALETTE" in s:
            return "AUTRES"
        if "ETIQUET" in s:
            return "AUTRES"
        if "NON EXECUTE" in s or "NON EXECUT" in s:
            return "AUTRES"

        return "AUTRES"

    for _, r in src.iterrows():
        montant = float(r.get("__montant__", 0.0) or 0.0)
        type_frais = str(r.get("__type_frais__", "")).upper()

        nb_pal = float(r.get("__nb_pal__", 0.0) or 0.0)
        if nb_pal == 0 and type_frais in ["FRAIS DE PORT", "FRAIS DE PORT (PICK&RETURN)"]:
            nb_pal = 1.0

        pick_return_amount = montant if "PICK&RETURN" in type_frais else 0.0

        rows.append({
            "numero_facture": str(r.get(col_fact, "")).strip(),
            "date_facture": pd.to_datetime(r.get(col_date, None), errors="coerce"),
            "reference_expedition": str(r.get(col_envoi, "")).strip(),
            "reference_client": str(r.get(col_ref, "")).strip() if col_ref else "",
            "transporteur": "GLS",
            "service_code": "GLS_PAL",
            "pays_orig": str(r.get(col_pays_exp, "")).strip().upper(),
            "cp_orig": normaliser_cp_text(r.get(col_cp_exp, "")),
            "pays_dest": str(r.get(col_pays_dest, "")).strip().upper(),
            "cp_dest": normaliser_cp_text(r.get(col_cp_dest, "")),
            "nb_palettes": nb_pal,
            "poids_total_kg": float(r.get("__poids__", 0.0) or 0.0),
            "distance_km": 0.0,
            "type_ligne": compute_type_ligne(type_frais),
            "montant_ht": montant,
            "surcharge_pick_return": pick_return_amount,
        })

    df_std = pd.DataFrame(rows)
    return df_std, src



CARRIERS = {
    "TFM": {
        "detect": is_probably_tfm_raw_excel,
        "convert": convert_tfm_palette_to_standard,
        "segment": "palettes",
    },
    "GEODIS": {
        "detect": is_probably_geodis_raw_excel,
        "convert": convert_geodis_palette_to_standard,
        "segment": "palettes",
    },
    "VMG": {
        "detect": is_probably_vmg_raw_excel,
        "convert": convert_vmg_palette_to_standard,
        "segment": "palettes",
    },
    "GLS_PAL": {
        "detect": is_probably_gls_pal_raw_excel,
        "convert": convert_gls_palette_to_standard,
        "segment": "palettes",
    },
    "DPD": {
        "detect": is_probably_dpd_raw_excel,
        "convert": convert_dpd_colis_to_standard,
        "segment": "colis",
    },
    "GLS": {
        "detect": is_probably_gls_raw_excel,
        "convert": convert_gls_colis_to_standard,
        "segment": "colis",
    },
}


def detect_carrier(uploaded_file) -> str | None:
    try:
        name = str(getattr(uploaded_file, "name", "")).upper()

        if "TFM" in name:
            return "TFM"
        if "GEODIS" in name:
            return "GEODIS"
        if "VMG" in name or "VANMIEGHEM" in name:
            return "VMG"
        if "GLS_PAL" in name or "GLS PAL" in name:
            return "GLS_PAL"
        if "DPD" in name:
            return "DPD"
        if "GLS" in name:
            return "GLS"

        for carrier, cfg in CARRIERS.items():
            try:
                uploaded_file.seek(0)
            except Exception:
                pass

            if cfg["detect"](uploaded_file):
                return carrier

        return None
    except Exception:
        return None


def convert_raw_invoice(uploaded_file) -> tuple[pd.DataFrame, pd.DataFrame, str, str]:
    carrier = detect_carrier(uploaded_file)
    if carrier is None:
        raise ValueError("Transporteur non reconnu (facture brute).")

    df_std, df_src = CARRIERS[carrier]["convert"](uploaded_file)
    segment = CARRIERS[carrier]["segment"]

    if "cp_dest" in df_std.columns:
        df_std["cp_dest"] = df_std["cp_dest"].apply(normaliser_cp_text)
    if "cp_orig" in df_std.columns:
        df_std["cp_orig"] = df_std["cp_orig"].apply(normaliser_cp_text)
    if "pays_dest" in df_std.columns:
        df_std["pays_dest"] = df_std["pays_dest"].astype(str).str.strip().str.upper()
    if "pays_orig" in df_std.columns:
        df_std["pays_orig"] = df_std["pays_orig"].astype(str).str.strip().str.upper()

    return df_std, df_src, carrier, segment


def build_excel_from_df(df_std: pd.DataFrame, df_source: pd.DataFrame, source_sheet_name: str, segment: str) -> bytes:
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        if segment == "palettes":
            df_std.to_excel(w, sheet_name="facture_palette_brut", index=False)
        else:
            df_std.to_excel(w, sheet_name="facture_lignes", index=False)

        df_source.to_excel(w, sheet_name=source_sheet_name, index=False)
    out.seek(0)
    return out.getvalue()


# =========================
# COLIS
# =========================
def controler_colis(file_bytes, filename: str, tolerance_eur: float) -> pd.DataFrame:
    xls = pd.ExcelFile(file_bytes)
    if "tarifs" not in xls.sheet_names or "facture_lignes" not in xls.sheet_names:
        raise ValueError("Pour COLIS, il faut les onglets 'tarifs' et 'facture_lignes'.")

    tarifs = pd.read_excel(xls, "tarifs")
    fact = pd.read_excel(xls, "facture_lignes")

    tarifs["transporteur"] = tarifs["transporteur"].astype(str).str.strip()
    fact["transporteur"] = fact["transporteur"].astype(str).str.strip()
    tarifs["service_code"] = normaliser_service_code_series(tarifs["service_code"])
    fact["service_code"] = normaliser_service_code_series(fact["service_code"])

    for col in ["pays_dest", "cp_debut", "cp_fin", "zone", "pays_orig"]:
        if col not in tarifs.columns:
            tarifs[col] = ""
        tarifs[col] = tarifs[col].astype(str).str.strip().str.upper()

    for col in ["date_debut", "date_fin"]:
        if col not in tarifs.columns:
            tarifs[col] = None

    tarifs["date_debut"] = tarifs["date_debut"].apply(lambda v: parse_date_any(v).date() if parse_date_any(v) else None)
    tarifs["date_fin"] = tarifs["date_fin"].apply(lambda v: parse_date_any(v).date() if parse_date_any(v) else None)
    tarifs["cp_debut"] = tarifs["cp_debut"].apply(normaliser_cp_text)
    tarifs["cp_fin"] = tarifs["cp_fin"].apply(normaliser_cp_text)

    for col in ["pays_dest", "cp_dest", "numero_facture", "reference_expedition", "reference_client", "pays_orig"]:
        if col in fact.columns:
            fact[col] = fact[col].astype(str).str.strip()

    if "cp_dest" in fact.columns:
        fact["cp_dest"] = fact["cp_dest"].apply(normaliser_cp_text)
    if "pays_dest" in fact.columns:
        fact["pays_dest"] = fact["pays_dest"].astype(str).str.strip().str.upper()
    if "pays_orig" in fact.columns:
        fact["pays_orig"] = fact["pays_orig"].astype(str).str.strip().str.upper()
    else:
        fact["pays_orig"] = ""

    def tarif_actif_colis(row, date_ref: datetime | None) -> bool:
        d_deb = row.get("date_debut", None)
        d_fin = row.get("date_fin", None)
        if date_ref is None:
            return True
        if pd.notna(d_deb) and d_deb is not None and date_ref.date() < d_deb:
            return False
        if pd.notna(d_fin) and d_fin is not None and date_ref.date() > d_fin:
            return False
        return True

    def trouver_tarif_colis(
        tarifs_df: pd.DataFrame,
        transporteur,
        service_code,
        pays_dest,
        cp_dest,
        poids_kg,
        date_ref,
        pays_orig="",
    ):
        cand = tarifs_df[
            (tarifs_df["transporteur"].astype(str).str.lower() == str(transporteur).lower())
            & (tarifs_df["service_code"].astype(str).str.lower() == str(service_code).lower())
        ].copy()
        if cand.empty:
            return None

        cand = cand[cand.apply(lambda r: tarif_actif_colis(r, date_ref), axis=1)]
        if cand.empty:
            return None

        p_orig = str(pays_orig).strip().upper()
        cand = cand[
            cand["pays_orig"].astype(str).str.upper().isin(["", p_orig])
        ]
        if cand.empty:
            return None

        p_dest = str(pays_dest).strip().upper()
        cp_norm = normaliser_cp_text(cp_dest)

        cand_pays = cand[cand["pays_dest"].astype(str).str.upper() == p_dest]
        cand_any = cand[cand["pays_dest"].astype(str) == ""]
        cand = pd.concat([cand_pays, cand_any]).drop_duplicates()
        if cand.empty:
            return None

        def in_poids(r):
            try:
                return float(r["poids_min_kg"]) <= float(poids_kg) <= float(r["poids_max_kg"])
            except Exception:
                return False

        cand = cand[cand.apply(in_poids, axis=1)]
        if cand.empty:
            return None

        cand_cp = cand[
            cand.apply(
                lambda r: match_cp_generic(cp_norm, r.get("cp_debut", ""), r.get("cp_fin", "")),
                axis=1,
            )
        ]
        if not cand_cp.empty:
            return cand_cp.iloc[0]

        cand_nocp = cand[
            (cand["cp_debut"].astype(str) == "") & (cand["cp_fin"].astype(str) == "")
        ]
        if not cand_nocp.empty:
            return cand_nocp.iloc[0]

        return None

    lignes = []
    for _, r in fact.iterrows():
        numero_facture = r.get("numero_facture", "")
        reference_expedition = r.get("reference_expedition", "")
        reference_client = r.get("reference_client", "")
        transporteur = r.get("transporteur", "")
        service_code = r.get("service_code", "")
        pays_orig = r.get("pays_orig", "")
        pays_dest = r.get("pays_dest", "")
        cp_dest = r.get("cp_dest", "")
        poids_kg = safe_num(r.get("poids_kg", 0), 0.0)
        montant_facture_ht = safe_num(r.get("montant_ligne_ht", 0), 0.0)
        surcharge_relabeling = safe_num(r.get("surcharge_relabeling", 0), 0.0)
        surcharge_pick_return = safe_num(r.get("surcharge_pick_return", 0), 0.0)
        date_facture = parse_date_any(r.get("date_facture", None))
        date_facture_str = date_facture.date().isoformat() if date_facture else ""

        trow = trouver_tarif_colis(
            tarifs,
            transporteur,
            service_code,
            pays_dest,
            cp_dest,
            poids_kg,
            date_facture,
            pays_orig,
        )

        if trow is None:
            raison = "Aucun tarif trouvé (transporteur/service/pays/CP/poids/date)"
            if surcharge_relabeling > 0:
                raison += " | surcharge relabeling"
            if surcharge_pick_return > 0:
                raison += " | pick & return"

            lignes.append(dict(
                numero_facture=numero_facture,
                reference_expedition=reference_expedition,
                reference_client=reference_client,
                date_facture=date_facture_str,
                transporteur=transporteur,
                service_code=service_code,
                pays_orig=pays_orig,
                cp_orig="",
                pays_dest=pays_dest,
                cp_dest=cp_dest,
                poids_kg=poids_kg,
                nb_palettes=0.0,
                distance_km=0.0,
                poids_total_kg=0.0,
                montant_facture_ht=montant_facture_ht,
                montant_calcule_ht=np.nan,
                ecart_ht=np.nan,
                ecart_pos=0.0,
                ecart_neg=0.0,
                surcharge_relabeling=surcharge_relabeling,
                surcharge_pick_return=surcharge_pick_return,
                statut="INCOMPLET",
                raison=raison,
            ))
            continue

        montant_calcule = safe_num(trow.get("prix_ht", 0), 0.0)
        ecart = round(montant_facture_ht - montant_calcule, 2)

        if ecart < 0:
            statut, raison = "OK", "Facturé moins que le tarif"
        else:
            if abs(ecart) <= tolerance_eur:
                statut, raison = "OK", ""
            else:
                statut, raison = "KO", f"Écart {ecart:.2f}€ > tolérance {tolerance_eur:.2f}€"

        if surcharge_relabeling > 0:
            if raison:
                raison += " | surcharge relabeling"
            else:
                raison = "surcharge relabeling"

        if surcharge_pick_return > 0:
            if raison:
                raison += " | pick & return"
            else:
                raison = "pick & return"

        lignes.append(dict(
            numero_facture=numero_facture,
            reference_expedition=reference_expedition,
            reference_client=reference_client,
            date_facture=date_facture_str,
            transporteur=transporteur,
            service_code=service_code,
            pays_orig=pays_orig,
            cp_orig="",
            pays_dest=pays_dest,
            cp_dest=cp_dest,
            poids_kg=poids_kg,
            nb_palettes=0.0,
            distance_km=0.0,
            poids_total_kg=0.0,
            montant_facture_ht=montant_facture_ht,
            montant_calcule_ht=montant_calcule,
            ecart_ht=ecart,
            ecart_pos=(ecart if ecart > 0 else 0.0),
            ecart_neg=(ecart if ecart < 0 else 0.0),
            surcharge_relabeling=surcharge_relabeling,
            surcharge_pick_return=surcharge_pick_return,
            statut=statut,
            raison=raison,
        ))

    return pd.DataFrame(lignes)


# =========================
# PALETTES
# =========================
def preparer_tarifs_palette(tarifs: pd.DataFrame) -> pd.DataFrame:
    t = tarifs.copy()

    text_cols = [
        "transporteur", "service_code", "pays_dest",
        "cp_debut", "cp_fin",
        "pays_orig", "cp_orig_debut", "cp_orig_fin",
        "sens_flux",
    ]
    for col in text_cols:
        if col not in t.columns:
            t[col] = ""
        t[col] = t[col].astype(str).str.strip()

    for col in ["cp_debut", "cp_fin", "cp_orig_debut", "cp_orig_fin"]:
        t[col] = t[col].apply(normaliser_cp_text)

    for col in ["nb_pal_min", "nb_pal_max"]:
        if col not in t.columns:
            t[col] = np.nan
        t[col] = t[col].apply(clean_numeric)

    for col in ["date_debut", "date_fin"]:
        if col not in t.columns:
            t[col] = None
    t["date_debut"] = t["date_debut"].apply(lambda v: parse_date_any(v).date() if parse_date_any(v) else None)
    t["date_fin"] = t["date_fin"].apply(lambda v: parse_date_any(v).date() if parse_date_any(v) else None)

    if "mode_calcul" not in t.columns:
        t["mode_calcul"] = "PAL"
    t["mode_calcul"] = t["mode_calcul"].astype(str).str.strip().str.upper()
    t.loc[t["mode_calcul"] == "", "mode_calcul"] = "PAL"

    for col in ["poids_min_kg", "poids_max_kg"]:
        if col not in t.columns:
            t[col] = np.nan
        t[col] = t[col].apply(clean_numeric)

    num_cols = [
        "prix_base_ht",
        "taxe_km_ht_par_km",
        "taxe_gazoil_pct",
        "taxe_gestion PAL",
        "taxe_rdv_ht",
        "taxe_sécurité",
        "taxe_énergie",
    ]
    for col in num_cols:
        if col not in t.columns:
            t[col] = 0.0
        t[col] = t[col].apply(clean_numeric).fillna(0.0)

    return t


def tarif_actif_palette(row, date_facture: datetime | None) -> bool:
    if date_facture is None or pd.isna(date_facture):
        return True

    try:
        d = date_facture.date()
    except Exception:
        return True

    d_deb = row.get("date_debut", None)
    d_fin = row.get("date_fin", None)

    if d_deb is not None and pd.notna(d_deb) and d < d_deb:
        return False
    if d_fin is not None and pd.notna(d_fin) and d > d_fin:
        return False

    return True


def agreger_facture_brut_palette(facture_brut: pd.DataFrame) -> pd.DataFrame:
    df = facture_brut.copy()

    for col in [
        "numero_facture", "reference_expedition", "reference_client",
        "transporteur", "service_code",
        "pays_dest", "cp_dest", "pays_orig", "cp_orig",
        "type_ligne",
    ]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(str).str.strip()

    if "surcharge_pick_return" not in df.columns:
        df["surcharge_pick_return"] = 0.0

    df["cp_dest"] = df["cp_dest"].apply(normaliser_cp_text)
    df["cp_orig"] = df["cp_orig"].apply(normaliser_cp_text)
    df["pays_dest"] = df["pays_dest"].astype(str).str.strip().str.upper()
    df["pays_orig"] = df["pays_orig"].astype(str).str.strip().str.upper()

    if "date_facture" not in df.columns:
        df["date_facture"] = None
    df["date_facture"] = df["date_facture"].apply(parse_date_any)
    df["date_facture"] = pd.to_datetime(df["date_facture"], errors="coerce")

    for col in ["montant_ht", "nb_palettes", "distance_km", "poids_total_kg", "surcharge_pick_return"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    group_keys = [
        "numero_facture",
        "reference_expedition",
        "reference_client",
        "date_facture",
        "transporteur",
        "service_code",
        "pays_dest",
        "cp_dest",
        "pays_orig",
        "cp_orig",
    ]

    def agg_group(g: pd.DataFrame) -> pd.Series:
        return pd.Series({
            "nb_palettes": g["nb_palettes"].max(),
            "distance_km": g["distance_km"].max(),
            "poids_total_kg": g["poids_total_kg"].max(),
            "montant_ligne_ht": g["montant_ht"].sum(),
            "surcharge_pick_return": g["surcharge_pick_return"].sum(),
        })

    return df.groupby(group_keys, dropna=False, sort=False).apply(agg_group).reset_index()


def filter_candidates(
    tarifs: pd.DataFrame,
    transporteur: str,
    service_code: str,
    pays_dest: str,
    cp_dest: str,
    date_facture: datetime | None,
    pays_orig: str = "",
    cp_orig: str = "",
) -> pd.DataFrame:
    t = tarifs[
        (tarifs["transporteur"].astype(str).str.lower() == str(transporteur).lower())
        & (tarifs["service_code"].astype(str).str.lower() == str(service_code).lower())
    ].copy()
    if t.empty:
        return t

    t = t[t.apply(lambda r: tarif_actif_palette(r, date_facture), axis=1)]
    if t.empty:
        return t

    p_dest = str(pays_dest).strip().upper()
    t = pd.concat([
        t[t["pays_dest"].astype(str).str.upper() == p_dest],
        t[t["pays_dest"].astype(str) == ""],
    ]).drop_duplicates()
    if t.empty:
        return t

    cp_dest_norm = normaliser_cp_text(cp_dest)
    t = t[
        t.apply(
            lambda r: match_cp_generic(cp_dest_norm, r.get("cp_debut", ""), r.get("cp_fin", "")),
            axis=1,
        )
    ]
    if t.empty:
        return t

    if str(service_code).upper() == "GEODIS_RET_BE":
        p_orig = str(pays_orig).strip().upper()
        cp_orig_norm = normaliser_cp_text(cp_orig)

        t = t[
            t.apply(
                lambda r: (
                    str(r.get("pays_orig", "")).upper() in ["", p_orig]
                    and match_cp_generic(cp_orig_norm, r.get("cp_orig_debut", ""), r.get("cp_orig_fin", ""))
                ),
                axis=1,
            )
        ]
        return t

    return t


def find_best_tarif_pal(t: pd.DataFrame, nb_palettes: float):
    nb = float(nb_palettes or 0)
    t_pal = t[t["mode_calcul"].astype(str).str.upper().isin(["PAL", ""])].copy()
    if t_pal.empty:
        return None

    def match_pal(r):
        mn = r.get("nb_pal_min", np.nan)
        mx = r.get("nb_pal_max", np.nan)
        if pd.isna(mn) and pd.isna(mx):
            return True
        try:
            return float(mn) <= nb <= float(mx)
        except Exception:
            return False

    t_pal = t_pal[t_pal.apply(match_pal, axis=1)]
    if t_pal.empty:
        return None
    return t_pal.iloc[0]


def find_best_tarif_poids(t: pd.DataFrame, nb_palettes: float, poids_total_kg: float):
    nb = float(nb_palettes or 0)
    w_raw = float(poids_total_kg or 0)
    w_match = int(np.ceil(w_raw)) if w_raw > 0 else 0

    t_poids = t[t["mode_calcul"].astype(str).str.upper() == "POIDS"].copy()
    if t_poids.empty:
        return None

    def match_pal_limit(r):
        mn = r.get("nb_pal_min", np.nan)
        mx = r.get("nb_pal_max", np.nan)

        if pd.isna(mn) and pd.isna(mx):
            return True

        if str(r.get("service_code", "")).upper() == "GEODIS_RET_BE":
            return True

        try:
            return float(mn) <= nb <= float(mx)
        except Exception:
            return False

    t_poids = t_poids[t_poids.apply(match_pal_limit, axis=1)]
    if t_poids.empty:
        return None

    if DEBUG_POIDS_MATCH:
        print("DEBUG POIDS MATCH - AVANT FILTRE POIDS:")
        cols_debug = [
            c for c in [
                "transporteur", "service_code", "mode_calcul",
                "nb_pal_min", "nb_pal_max",
                "poids_min_kg", "poids_max_kg",
                "cp_debut", "cp_fin", "pays_dest"
            ] if c in t_poids.columns
        ]
        try:
            print(t_poids[cols_debug].head(50).to_string(index=False))
        except Exception:
            print(t_poids.head(50))
        print("Poids brut =", w_raw, "| Poids match =", w_match, "| nb_pal =", nb)

    def match_poids(r):
        mn = r.get("poids_min_kg", np.nan)
        mx = r.get("poids_max_kg", np.nan)

        try:
            mn = float(mn) if not pd.isna(mn) else np.nan
            mx = float(mx) if not pd.isna(mx) else np.nan
        except Exception:
            return False

        if np.isnan(mn) and np.isnan(mx):
            return True
        if np.isnan(mn):
            return w_match <= mx
        if np.isnan(mx):
            return w_match >= mn

        return mn <= w_match <= mx

    t_poids_match = t_poids[t_poids.apply(match_poids, axis=1)]

    if DEBUG_POIDS_MATCH:
        print("DEBUG POIDS MATCH - APRES FILTRE POIDS:")
        try:
            print(t_poids_match[cols_debug].head(50).to_string(index=False))
        except Exception:
            print(t_poids_match.head(50))

    if t_poids_match.empty:
        return None

    return t_poids_match.iloc[0]


def choose_tarif_palette(
    tarifs_prep: pd.DataFrame,
    transporteur: str,
    service_code: str,
    pays_dest: str,
    cp_dest: str,
    nb_palettes: float,
    poids_total_kg: float,
    date_facture: datetime | None,
    pays_orig: str = "",
    cp_orig: str = "",
):
    t = filter_candidates(
        tarifs=tarifs_prep,
        transporteur=transporteur,
        service_code=service_code,
        pays_dest=pays_dest,
        cp_dest=cp_dest,
        date_facture=date_facture,
        pays_orig=pays_orig,
        cp_orig=cp_orig,
    )
    if t.empty:
        return None

    nb = float(nb_palettes or 0)
    w = float(poids_total_kg or 0)

    if str(service_code).upper() == "GEODIS_RET_BE":
        r_poids = find_best_tarif_poids(t, nb, w)
        if r_poids is not None:
            return r_poids
        r_pal = find_best_tarif_pal(t, nb)
        if r_pal is not None:
            return r_pal
        return None

    if str(service_code).upper() == "GEODIS_PAL" and nb < 2 and w > 0:
        r_poids = find_best_tarif_poids(t, nb, w)
        if r_poids is not None:
            return r_poids

        r_pal = find_best_tarif_pal(t, nb)
        if r_pal is not None:
            return r_pal

        return None

    has_poids = (t["mode_calcul"].astype(str).str.upper() == "POIDS").any()
    wants_poids = has_poids and (nb < 2) and (w > 0)

    if wants_poids:
        r_poids = find_best_tarif_poids(t, nb, w)
        if r_poids is not None:
            return r_poids

        r_pal = find_best_tarif_pal(t, nb)
        if r_pal is not None:
            return r_pal

        return None

    r_pal = find_best_tarif_pal(t, nb)
    if r_pal is not None:
        return r_pal

    if has_poids and w > 0:
        r_poids = find_best_tarif_poids(t, nb, w)
        if r_poids is not None:
            return r_poids

    return None


def diagnose_no_tarif_palette(
    tarifs_prep: pd.DataFrame,
    transporteur: str,
    service_code: str,
    pays_dest: str,
    cp_dest: str,
    nb_palettes: float,
    poids_total_kg: float,
    date_facture: datetime | None,
    pays_orig: str = "",
    cp_orig: str = "",
) -> str:
    t1 = tarifs_prep[
        (tarifs_prep["transporteur"].astype(str).str.lower() == str(transporteur).lower())
        & (tarifs_prep["service_code"].astype(str).str.lower() == str(service_code).lower())
    ].copy()
    if t1.empty:
        return f"INCOMPLET: aucun tarif avec transporteur='{transporteur}' et service_code='{service_code}'"

    if date_facture is not None:
        t1 = t1[t1.apply(lambda r: tarif_actif_palette(r, date_facture), axis=1)]
        if t1.empty:
            return "INCOMPLET: tarifs trouvés mais hors période de validité"

    p_dest = str(pays_dest).strip().upper()
    t2 = pd.concat([
        t1[t1["pays_dest"].astype(str).str.upper() == p_dest],
        t1[t1["pays_dest"].astype(str) == ""],
    ]).drop_duplicates()
    if t2.empty:
        return f"INCOMPLET: aucun tarif pour pays_dest='{p_dest}'"

    cp_dest_norm = normaliser_cp_text(cp_dest)
    t3 = t2[
        t2.apply(
            lambda r: match_cp_generic(cp_dest_norm, r.get("cp_debut", ""), r.get("cp_fin", "")),
            axis=1,
        )
    ]
    if t3.empty:
        return f"INCOMPLET: aucun tarif ne couvre cp_dest='{cp_dest_norm}'"

    if str(service_code).upper() == "GEODIS_RET_BE":
        p_orig = str(pays_orig).strip().upper()
        cp_orig_norm = normaliser_cp_text(cp_orig)

        t4 = t3[
            t3.apply(
                lambda r: (
                    str(r.get("pays_orig", "")).upper() in ["", p_orig]
                    and match_cp_generic(cp_orig_norm, r.get("cp_orig_debut", ""), r.get("cp_orig_fin", ""))
                ),
                axis=1,
            )
        ]

        if t4.empty:
            return (
                "INCOMPLET: aucun tarif retour GEODIS ne matche "
                f"(service_code=GEODIS_RET_BE, pays_orig='{p_orig}', cp_orig='{cp_orig_norm}')"
            )

        if find_best_tarif_poids(t4, nb_palettes, poids_total_kg) is None:
            poids_match = int(np.ceil(float(poids_total_kg or 0))) if float(poids_total_kg or 0) > 0 else 0
            return (
                "INCOMPLET: aucune tranche POIDS retour GEODIS ne matche "
                f"(service_code=GEODIS_RET_BE, poids_brut={poids_total_kg}, poids_match={poids_match}, nb_pal={nb_palettes})"
            )

        return "INCOMPLET: cause non identifiée (service_code=GEODIS_RET_BE)"

    nb = float(nb_palettes or 0)
    w = float(poids_total_kg or 0)
    w_match = int(np.ceil(w)) if w > 0 else 0

    if str(service_code).upper() == "GEODIS_PAL" and nb < 2 and w > 0:
        r_poids = find_best_tarif_poids(t3, nb, w)
        if r_poids is not None:
            return "INCOMPLET: cause non identifiée (GEODIS_PAL - POIDS)"

        r_pal = find_best_tarif_pal(t3, nb)
        if r_pal is not None:
            return "INCOMPLET: cause non identifiée (GEODIS_PAL - fallback PAL)"

        return (
            "INCOMPLET: aucune tranche GEODIS_PAL ne matche "
            f"(nb_pal={nb}, poids_brut={w}, poids_match={w_match})"
        )

    has_poids = (t3["mode_calcul"].astype(str).str.upper() == "POIDS").any()
    wants_poids = has_poids and (nb < 2) and (w > 0)

    if wants_poids:
        if find_best_tarif_poids(t3, nb, w) is None:
            return (
                "INCOMPLET: aucune tranche POIDS ne matche "
                f"(service_code='{service_code}', nb_pal={nb}, poids_brut={w}, poids_match={w_match})"
            )
        return f"INCOMPLET: cause non identifiée (service_code='{service_code}')"

    if find_best_tarif_pal(t3, nb) is None:
        return (
            "INCOMPLET: aucune tranche PAL ne matche "
            f"(service_code='{service_code}', nb_pal={nb})"
        )

    return f"INCOMPLET: cause non identifiée (service_code='{service_code}')"


def controler_palettes(file_bytes, filename: str, tolerance_eur: float) -> pd.DataFrame:
    xls = pd.ExcelFile(file_bytes)
    sheets = [s.strip() for s in xls.sheet_names]

    if "facture_palette_brut" not in sheets:
        raise ValueError(
            "Onglet manquant: 'facture_palette_brut'.\n"
            "➡️ Ceci ressemble à une facture brute. Va dans l’onglet Convertisseur."
        )

    facture_brut = pd.read_excel(xls, "facture_palette_brut")

    found_tarifs = None
    lower_map = {s.lower(): s for s in xls.sheet_names}
    for v in ["tarifs_palette", "tarifs palette", "tarifs_palettes", "tarifs-palettes"]:
        if v.lower() in lower_map:
            found_tarifs = lower_map[v.lower()]
            break

    if found_tarifs:
        tarifs = pd.read_excel(xls, found_tarifs)
    else:
        tarifs = load_tarifs_palette_master()

    tarifs_prep = preparer_tarifs_palette(tarifs)
    fact_lignes = agreger_facture_brut_palette(facture_brut)

    lignes = []
    for _, r in fact_lignes.iterrows():
        numero_facture = r.get("numero_facture", "")
        reference_expedition = r.get("reference_expedition", "")
        reference_client = r.get("reference_client", "")
        transporteur = r.get("transporteur", "")
        service_code = r.get("service_code", "")
        pays_dest = r.get("pays_dest", "")
        cp_dest = r.get("cp_dest", "")
        pays_orig = r.get("pays_orig", "")
        cp_orig = r.get("cp_orig", "")
        nb_pal = safe_num(r.get("nb_palettes", 0), 0.0)
        dist_km = safe_num(r.get("distance_km", 0), 0.0)
        poids_total_kg = safe_num(r.get("poids_total_kg", 0), 0.0)
        montant_facture_ht = safe_num(r.get("montant_ligne_ht", 0), 0.0)
        surcharge_pick_return = safe_num(r.get("surcharge_pick_return", 0), 0.0)

        date_facture = r.get("date_facture", None)
        date_facture_dt = None
        if isinstance(date_facture, pd.Timestamp) and not pd.isna(date_facture):
            date_facture_dt = date_facture.to_pydatetime()
        elif isinstance(date_facture, datetime):
            date_facture_dt = date_facture

        date_facture_str = date_facture_dt.date().isoformat() if date_facture_dt else ""

        trow = choose_tarif_palette(
            tarifs_prep,
            transporteur,
            service_code,
            pays_dest,
            cp_dest,
            nb_pal,
            poids_total_kg,
            date_facture_dt,
            pays_orig,
            cp_orig,
        )

        if trow is None:
            raison_diag = diagnose_no_tarif_palette(
                tarifs_prep,
                transporteur,
                service_code,
                pays_dest,
                cp_dest,
                nb_pal,
                poids_total_kg,
                date_facture_dt,
                pays_orig,
                cp_orig,
            )

            if surcharge_pick_return > 0:
                raison_diag += " | pick & return"

            lignes.append(dict(
                numero_facture=numero_facture,
                reference_expedition=reference_expedition,
                reference_client=reference_client,
                date_facture=date_facture_str,
                transporteur=transporteur,
                service_code=service_code,
                pays_orig=pays_orig,
                cp_orig=cp_orig,
                pays_dest=pays_dest,
                cp_dest=cp_dest,
                poids_kg=0.0,
                nb_palettes=nb_pal,
                distance_km=dist_km,
                poids_total_kg=poids_total_kg,
                montant_facture_ht=montant_facture_ht,
                montant_calcule_ht=np.nan,
                ecart_ht=np.nan,
                ecart_pos=0.0,
                ecart_neg=0.0,
                surcharge_pick_return=surcharge_pick_return,
                statut="INCOMPLET",
                raison=raison_diag,
            ))
            continue

        base = safe_num(trow.get("prix_base_ht", 0), 0.0)
        taxe_km_fixe = safe_num(trow.get("taxe_km_ht_par_km", 0), 0.0)
        taxe_gazoil_fixe = safe_num(trow.get("taxe_gazoil_pct", 0), 0.0)
        taxe_rdv = safe_num(trow.get("taxe_rdv_ht", 0), 0.0)
        taxe_gestion_unit = safe_num(trow.get("taxe_gestion PAL", 0), 0.0)
        montant_gestion = taxe_gestion_unit * nb_pal
        taxe_secu = safe_num(trow.get("taxe_sécurité", 0), 0.0)
        taxe_energie = safe_num(trow.get("taxe_énergie", 0), 0.0)

        montant_calcule = round(
            base + taxe_km_fixe + taxe_gazoil_fixe + taxe_rdv + montant_gestion + taxe_secu + taxe_energie,
            2,
        )
        ecart = round(montant_facture_ht - montant_calcule, 2)

        if ecart < 0:
            statut, raison = "OK", "Facturé moins que le tarif"
        else:
            if abs(ecart) <= tolerance_eur:
                statut, raison = "OK", ""
            else:
                statut, raison = "KO", f"Écart {ecart:.2f}€ > tolérance {tolerance_eur:.2f}€"

        if surcharge_pick_return > 0:
            if raison:
                raison += " | pick & return"
            else:
                raison = "pick & return"

        lignes.append(dict(
            numero_facture=numero_facture,
            reference_expedition=reference_expedition,
            reference_client=reference_client,
            date_facture=date_facture_str,
            transporteur=transporteur,
            service_code=service_code,
            pays_orig=pays_orig,
            cp_orig=cp_orig,
            pays_dest=pays_dest,
            cp_dest=cp_dest,
            poids_kg=0.0,
            nb_palettes=nb_pal,
            distance_km=dist_km,
            poids_total_kg=poids_total_kg,
            montant_facture_ht=montant_facture_ht,
            montant_calcule_ht=montant_calcule,
            ecart_ht=ecart,
            ecart_pos=(ecart if ecart > 0 else 0.0),
            ecart_neg=(ecart if ecart < 0 else 0.0),
            surcharge_pick_return=surcharge_pick_return,
            statut=statut,
            raison=raison,
        ))

    return pd.DataFrame(lignes)


# =========================
# REPORT / DASHBOARD
# =========================
def build_run_info(df_res: pd.DataFrame, filename: str, segment: str) -> dict:
    df = df_res.copy()
    for c in ["montant_facture_ht", "montant_calcule_ht", "ecart_ht", "ecart_pos", "ecart_neg"]:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    total = len(df)
    nb_ok = int((df["statut"] == "OK").sum())
    nb_ko = int((df["statut"] == "KO").sum())
    nb_inc = int((df["statut"] == "INCOMPLET").sum())

    mt_fact = float(df["montant_facture_ht"].sum())
    mt_calc = float(df["montant_calcule_ht"].sum())
    mt_ecart = float(df["ecart_ht"].sum())
    mt_pos = float(df["ecart_pos"].sum())
    mt_neg = float(df["ecart_neg"].sum())
    taux = (nb_ok / total * 100) if total else 0.0

    return dict(
        created_at=datetime.now().isoformat(timespec="seconds"),
        filename=filename,
        segment=segment,
        nb_lignes=int(total),
        nb_ok=nb_ok,
        nb_ko=nb_ko,
        nb_incomplet=nb_inc,
        montant_facture_total=mt_fact,
        montant_calcule_total=mt_calc,
        ecart_total=mt_ecart,
        ecart_total_pos=mt_pos,
        ecart_total_neg=mt_neg,
        taux_conformite=float(taux),
    )


def build_excel_report(df_res: pd.DataFrame) -> bytes:
    df = df_res.copy()
    for c in ["montant_facture_ht", "montant_calcule_ht", "ecart_ht", "ecart_pos", "ecart_neg"]:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    synth = (
        df.groupby(["transporteur", "statut"], dropna=False)
        .agg(
            nb_lignes=("numero_facture", "count"),
            montant_facture_total=("montant_facture_ht", "sum"),
            montant_calcule_total=("montant_calcule_ht", "sum"),
            ecart_total=("ecart_ht", "sum"),
            ecart_total_pos=("ecart_pos", "sum"),
            ecart_total_neg=("ecart_neg", "sum"),
        )
        .reset_index()
    )

    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="rapport_controle", index=False)
        synth.to_excel(writer, sheet_name="synthese_par_transporteur", index=False)
    out.seek(0)
    return out.getvalue()


def chart_statuts(df: pd.DataFrame):
    data = (
        df["statut"]
        .value_counts(dropna=False)
        .reset_index()
    )
    data.columns = ["statut", "count"]

    order = ["OK", "KO", "INCOMPLET"]

    return (
        alt.Chart(data)
        .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
        .encode(
            x=alt.X("statut:N", sort=order, title="Statut"),
            y=alt.Y("count:Q", title="Nombre de lignes"),
            color=alt.Color(
                "statut:N",
                scale=alt.Scale(
                    domain=["OK", "KO", "INCOMPLET"],
                    range=["#22c55e", "#ef4444", "#94a3b8"],
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("statut:N", title="Statut"),
                alt.Tooltip("count:Q", title="Nombre"),
            ],
        )
        .properties(height=280, title="Répartition des statuts")
    )


def chart_conformite_transporteur(df: pd.DataFrame):
    data = (
        df.groupby(["transporteur", "statut"], dropna=False)
        .size()
        .reset_index(name="nb_lignes")
    )

    return (
        alt.Chart(data)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("transporteur:N", title="Transporteur"),
            y=alt.Y("nb_lignes:Q", title="Nombre de lignes"),
            color=alt.Color(
                "statut:N",
                scale=alt.Scale(
                    domain=["OK", "KO", "INCOMPLET"],
                    range=["#22c55e", "#ef4444", "#94a3b8"],
                ),
                title="Statut",
            ),
            tooltip=[
                alt.Tooltip("transporteur:N", title="Transporteur"),
                alt.Tooltip("statut:N", title="Statut"),
                alt.Tooltip("nb_lignes:Q", title="Nombre"),
            ],
        )
        .properties(height=320, title="Conformité par transporteur")
    )


def chart_ecarts_transporteur(df: pd.DataFrame):
    data = (
        df.groupby("transporteur", dropna=False)
        .agg(ecart_pos=("ecart_pos", "sum"))
        .reset_index()
        .sort_values("ecart_pos", ascending=False)
    )

    return (
        alt.Chart(data)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("transporteur:N", sort="-y", title="Transporteur"),
            y=alt.Y("ecart_pos:Q", title="Montant à réclamer (€)"),
            color=alt.value("#2563eb"),
            tooltip=[
                alt.Tooltip("transporteur:N", title="Transporteur"),
                alt.Tooltip("ecart_pos:Q", title="À réclamer (€)", format=".2f"),
            ],
        )
        .properties(height=320, title="Écarts positifs par transporteur")
    )


def chart_surcharges_transporteur(df: pd.DataFrame):
    d = df.copy()

    if "surcharge_relabeling" not in d.columns:
        d["surcharge_relabeling"] = 0.0
    if "surcharge_pick_return" not in d.columns:
        d["surcharge_pick_return"] = 0.0

    data = (
        d.groupby("transporteur", dropna=False)
        .agg(
            relabel=("surcharge_relabeling", "sum"),
            pick_return=("surcharge_pick_return", "sum"),
        )
        .reset_index()
    )

    data_long = data.melt(
        id_vars="transporteur",
        value_vars=["relabel", "pick_return"],
        var_name="type_surcharge",
        value_name="montant",
    )

    label_map = {
        "relabel": "Relabeling",
        "pick_return": "Pick & Return",
    }
    data_long["type_surcharge"] = data_long["type_surcharge"].map(label_map)

    return (
        alt.Chart(data_long)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("transporteur:N", title="Transporteur"),
            y=alt.Y("montant:Q", title="Montant (€)"),
            color=alt.Color(
                "type_surcharge:N",
                scale=alt.Scale(
                    domain=["Relabeling", "Pick & Return"],
                    range=["#f59e0b", "#8b5cf6"],
                ),
                title="Type surcharge",
            ),
            xOffset="type_surcharge:N",
            tooltip=[
                alt.Tooltip("transporteur:N", title="Transporteur"),
                alt.Tooltip("type_surcharge:N", title="Type"),
                alt.Tooltip("montant:Q", title="Montant (€)", format=".2f"),
            ],
        )
        .properties(height=320, title="Surcharges par transporteur")
    )


def chart_top_anomalies(df: pd.DataFrame):
    d = df.copy()
    d["ecart_pos"] = pd.to_numeric(d["ecart_pos"], errors="coerce").fillna(0.0)

    data = (
        d[d["ecart_pos"] > 0]
        .sort_values("ecart_pos", ascending=False)
        .head(10)
        .copy()
    )

    if data.empty:
        data = pd.DataFrame({
            "reference_expedition": ["Aucune anomalie"],
            "ecart_pos": [0.0],
        })

    return (
        alt.Chart(data)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            y=alt.Y(
                "reference_expedition:N",
                sort="-x",
                title="Référence expédition"
            ),
            x=alt.X("ecart_pos:Q", title="Écart positif (€)"),
            color=alt.value("#dc2626"),
            tooltip=[
                alt.Tooltip("reference_expedition:N", title="Expédition"),
                alt.Tooltip("ecart_pos:Q", title="Écart positif (€)", format=".2f"),
            ],
        )
        .properties(height=340, title="Top 10 anomalies à réclamer")
    )

def chart_evolution_mensuelle_ecarts(df: pd.DataFrame):
    d = df.copy()

    if "date_facture" not in d.columns:
        return alt.Chart(pd.DataFrame({"mois": [], "montant": [], "type_ecart": []})).mark_line()

    d["date_facture"] = pd.to_datetime(d["date_facture"], errors="coerce")
    d = d[d["date_facture"].notna()].copy()

    if d.empty:
        return alt.Chart(pd.DataFrame({"mois": [], "montant": [], "type_ecart": []})).mark_line()

    for c in ["ecart_pos", "ecart_neg", "ecart_ht"]:
        if c not in d.columns:
            d[c] = 0.0
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0.0)

    d["mois"] = d["date_facture"].dt.to_period("M").astype(str)

    monthly = (
        d.groupby("mois", dropna=False)
        .agg(
            ecart_pos=("ecart_pos", "sum"),
            ecart_neg=("ecart_neg", "sum"),
            ecart_total=("ecart_ht", "sum"),
        )
        .reset_index()
        .sort_values("mois")
    )

    data_long = monthly.melt(
        id_vars="mois",
        value_vars=["ecart_pos", "ecart_neg", "ecart_total"],
        var_name="type_ecart",
        value_name="montant",
    )

    label_map = {
        "ecart_pos": "À réclamer",
        "ecart_neg": "Écart négatif",
        "ecart_total": "Écart total",
    }
    data_long["type_ecart"] = data_long["type_ecart"].map(label_map)

    return (
        alt.Chart(data_long)
        .mark_line(point=True, strokeWidth=3)
        .encode(
            x=alt.X("mois:N", title="Mois"),
            y=alt.Y("montant:Q", title="Montant (€)"),
            color=alt.Color(
                "type_ecart:N",
                scale=alt.Scale(
                    domain=["À réclamer", "Écart négatif", "Écart total"],
                    range=["#dc2626", "#2563eb", "#0f172a"],
                ),
                title="Type d'écart",
            ),
            tooltip=[
                alt.Tooltip("mois:N", title="Mois"),
                alt.Tooltip("type_ecart:N", title="Type"),
                alt.Tooltip("montant:Q", title="Montant (€)", format=".2f"),
            ],
        )
        .properties(height=340, title="Évolution mensuelle des écarts")
    )

def chart_evolution_mensuelle_conformite(df: pd.DataFrame):
    d = df.copy()

    if "date_facture" not in d.columns:
        return alt.Chart(pd.DataFrame({"mois": [], "taux_conformite": []})).mark_line()

    d["date_facture"] = pd.to_datetime(d["date_facture"], errors="coerce")
    d = d[d["date_facture"].notna()].copy()

    if d.empty:
        return alt.Chart(pd.DataFrame({"mois": [], "taux_conformite": []})).mark_line()

    if "statut" not in d.columns:
        d["statut"] = ""

    d["mois"] = d["date_facture"].dt.to_period("M").astype(str)
    d["is_ok"] = (d["statut"].astype(str) == "OK").astype(int)

    monthly = (
        d.groupby("mois", dropna=False)
        .agg(
            nb_lignes=("statut", "count"),
            nb_ok=("is_ok", "sum"),
        )
        .reset_index()
        .sort_values("mois")
    )

    monthly["taux_conformite"] = np.where(
        monthly["nb_lignes"] > 0,
        monthly["nb_ok"] / monthly["nb_lignes"] * 100,
        0.0,
    )

    return (
        alt.Chart(monthly)
        .mark_line(point=True, strokeWidth=3)
        .encode(
            x=alt.X("mois:N", title="Mois"),
            y=alt.Y("taux_conformite:Q", title="Taux de conformité (%)", scale=alt.Scale(domain=[0, 100])),
            color=alt.value("#16a34a"),
            tooltip=[
                alt.Tooltip("mois:N", title="Mois"),
                alt.Tooltip("nb_lignes:Q", title="Nb lignes"),
                alt.Tooltip("nb_ok:Q", title="Nb OK"),
                alt.Tooltip("taux_conformite:Q", title="Conformité (%)", format=".2f"),
            ],
        )
        .properties(height=340, title="Évolution mensuelle du taux de conformité")
    )

# =========================
# GRAPHIQUES DASHBOARD
# =========================

def chart_conformite_transporteur(df: pd.DataFrame):
    data = (
        df.groupby(["transporteur", "statut"], dropna=False)
        .size()
        .reset_index(name="nb_lignes")
    )

    return (
        alt.Chart(data)
        .mark_bar()
        .encode(
            x=alt.X("transporteur:N", title="Transporteur"),
            y=alt.Y("nb_lignes:Q", title="Nombre de lignes"),
            color=alt.Color("statut:N", title="Statut"),
            tooltip=["transporteur", "statut", "nb_lignes"],
        )
        .properties(height=300, title="Statuts par transporteur")
    )


def chart_ecarts_transporteur(df: pd.DataFrame):
    data = (
        df.groupby("transporteur", dropna=False)
        .agg(ecart_pos=("ecart_pos", "sum"))
        .reset_index()
        .sort_values("ecart_pos", ascending=False)
    )

    return (
        alt.Chart(data)
        .mark_bar()
        .encode(
            x=alt.X("transporteur:N", sort="-y", title="Transporteur"),
            y=alt.Y("ecart_pos:Q", title="Montant à réclamer (€)"),
            tooltip=[
                "transporteur",
                alt.Tooltip("ecart_pos:Q", format=".2f")
            ],
        )
        .properties(height=300, title="Écarts positifs par transporteur")
    )


def chart_surcharges_transporteur(df: pd.DataFrame):
    d = df.copy()

    if "surcharge_relabeling" not in d.columns:
        d["surcharge_relabeling"] = 0.0
    if "surcharge_pick_return" not in d.columns:
        d["surcharge_pick_return"] = 0.0

    data = (
        d.groupby("transporteur", dropna=False)
        .agg(
            relabel=("surcharge_relabeling", "sum"),
            pick_return=("surcharge_pick_return", "sum"),
        )
        .reset_index()
    )

    data_long = data.melt(
        id_vars="transporteur",
        value_vars=["relabel", "pick_return"],
        var_name="type_surcharge",
        value_name="montant",
    )

    return (
        alt.Chart(data_long)
        .mark_bar()
        .encode(
            x=alt.X("transporteur:N", title="Transporteur"),
            y=alt.Y("montant:Q", title="Montant (€)"),
            color=alt.Color("type_surcharge:N", title="Type surcharge"),
            xOffset="type_surcharge:N",
            tooltip=[
                "transporteur",
                "type_surcharge",
                alt.Tooltip("montant:Q", format=".2f"),
            ],
        )
        .properties(height=300, title="Surcharges par transporteur")
    )

# =========================
# EXPORT RECLAMATION
# =========================
def build_excel_reclamation(df: pd.DataFrame) -> bytes:
    d = df.copy()

    if "ecart_pos" not in d.columns:
        d["ecart_pos"] = 0.0
    if "statut" not in d.columns:
        d["statut"] = ""

    d["ecart_pos"] = pd.to_numeric(d["ecart_pos"], errors="coerce").fillna(0.0)

    df_reclam = d[
        (d["statut"].astype(str) == "KO") &
        (d["ecart_pos"] > 0)
    ].copy()

    cols_priority = [
        "date_facture",
        "transporteur",
        "numero_facture",
        "reference_expedition",
        "reference_client",
        "service_code",
        "pays_orig",
        "pays_dest",
        "cp_dest",
        "poids_kg",
        "nb_palettes",
        "montant_facture_ht",
        "montant_calcule_ht",
        "ecart_ht",
        "ecart_pos",
        "raison",
    ]

    cols_keep = [c for c in cols_priority if c in df_reclam.columns]
    other_cols = [c for c in df_reclam.columns if c not in cols_keep]

    df_reclam = df_reclam[cols_keep + other_cols]

    synth = pd.DataFrame({
        "indicateur": [
            "Nb lignes à réclamer",
            "Montant total à réclamer (€)",
        ],
        "valeur": [
            len(df_reclam),
            round(df_reclam["ecart_pos"].sum(), 2) if not df_reclam.empty else 0.0,
        ],
    })

    from io import BytesIO
    out = BytesIO()

    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        synth.to_excel(writer, sheet_name="synthese_reclamation", index=False)
        df_reclam.to_excel(writer, sheet_name="lignes_a_reclamer", index=False)

    out.seek(0)
    return out.getvalue()

def simuler_tarifs_colis(
    tarifs: pd.DataFrame,
    poids,
    pays_orig,
    pays_dest,
    cp_dest,
    date_ref
):
    results = []

    # nettoyage
    poids = float(poids or 0)
    pays_orig = str(pays_orig).upper().strip()
    pays_dest = str(pays_dest).upper().strip()
    cp_dest = str(cp_dest).strip()

    # parcourir tous les tarifs
    for _, t in tarifs.iterrows():

        try:
            # transporteur / service
            transporteur = str(t.get("transporteur", ""))
            service_code = str(t.get("service_code", ""))

            # pays destination
            if t.get("pays_dest") not in ["", pays_dest]:
                continue

            # pays origine (si utilisé)
            if "pays_orig" in tarifs.columns:
                if t.get("pays_orig") not in ["", pays_orig]:
                    continue

            # CP destination
            cp_deb = str(t.get("cp_debut", ""))
            cp_fin = str(t.get("cp_fin", ""))

            if cp_deb and cp_fin:
                try:
                    if not (int(cp_deb) <= int(cp_dest) <= int(cp_fin)):
                        continue
                except:
                    if not (cp_deb <= cp_dest <= cp_fin):
                        continue

            # poids
            pmin = float(t.get("poids_min_kg", 0))
            pmax = float(t.get("poids_max_kg", 999999))

            if not (pmin <= poids <= pmax):
                continue

            # date
            date_ok = True
            if "date_debut" in tarifs.columns and pd.notna(t.get("date_debut")):
                if date_ref < t["date_debut"]:
                    date_ok = False

            if "date_fin" in tarifs.columns and pd.notna(t.get("date_fin")):
                if date_ref > t["date_fin"]:
                    date_ok = False

            if not date_ok:
                continue

            # prix
            prix = float(t.get("prix_ht", 0))

            results.append({
                "transporteur": transporteur,
                "service_code": service_code,
                "prix_estime": prix
            })

        except Exception:
            continue

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)

    df = df.sort_values("prix_estime").reset_index(drop=True)
    df["rang"] = df.index + 1

    return df

def simuler_tarifs_palettes(
    tarifs: pd.DataFrame,
    nb_palettes,
    poids_total_kg,
    pays_orig,
    cp_orig,
    pays_dest,
    cp_dest,
    date_ref,
):
    tarifs_prep = preparer_tarifs_palette(tarifs)

    results = []

    nb_palettes = float(nb_palettes or 0)
    poids_total_kg = float(poids_total_kg or 0)

    pays_orig = str(pays_orig).strip().upper()
    cp_orig = normaliser_cp_text(cp_orig)
    pays_dest = str(pays_dest).strip().upper()
    cp_dest = normaliser_cp_text(cp_dest)

    couples = (
        tarifs_prep[["transporteur", "service_code"]]
        .drop_duplicates()
        .to_dict("records")
    )

    for item in couples:
        transporteur = str(item.get("transporteur", "")).strip()
        service_code = str(item.get("service_code", "")).strip()

        try:
            trow = choose_tarif_palette(
                tarifs_prep=tarifs_prep,
                transporteur=transporteur,
                service_code=service_code,
                pays_dest=pays_dest,
                cp_dest=cp_dest,
                nb_palettes=nb_palettes,
                poids_total_kg=poids_total_kg,
                date_facture=pd.to_datetime(date_ref) if date_ref else None,
                pays_orig=pays_orig,
                cp_orig=cp_orig,
            )

            if trow is None:
                continue

            base = safe_num(trow.get("prix_base_ht", 0))
            taxe_km = safe_num(trow.get("taxe_km_ht_par_km", 0))
            taxe_gazoil = safe_num(trow.get("taxe_gazoil_pct", 0))
            taxe_rdv = safe_num(trow.get("taxe_rdv_ht", 0))
            taxe_gestion_unit = safe_num(trow.get("taxe_gestion PAL", 0))
            taxe_secu = safe_num(trow.get("taxe_sécurité", 0))
            taxe_energie = safe_num(trow.get("taxe_énergie", 0))

            taxe_gestion = taxe_gestion_unit * nb_palettes

            prix = round(
                base + taxe_km + taxe_gazoil + taxe_rdv + taxe_gestion + taxe_secu + taxe_energie,
                2,
            )

            results.append({
                "transporteur": transporteur,
                "service_code": service_code,
                "mode_calcul": trow.get("mode_calcul", ""),
                "prix_estime": prix,
                "prix_base_ht": base,
                "taxe_km": taxe_km,
                "taxe_gazoil": taxe_gazoil,
                "taxe_rdv": taxe_rdv,
                "taxe_gestion": taxe_gestion,
                "taxe_securite": taxe_secu,
                "taxe_energie": taxe_energie,
            })

        except Exception:
            continue

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)

    df["prix_estime"] = pd.to_numeric(df["prix_estime"], errors="coerce").fillna(999999.0)

    df = df.sort_values(["prix_estime"]).reset_index(drop=True)
    df["rang"] = df.index + 1

    return df

import zipfile


def load_excel_resilient(file_obj_or_path, sheet_name=0, header=0) -> pd.DataFrame:
    """
    Lecture Excel robuste, utile pour certains fichiers VMG qui ont des octets
    en trop après la fin réelle du zip XLSX.
    """
    try:
        return pd.read_excel(file_obj_or_path, sheet_name=sheet_name, header=header)
    except zipfile.BadZipFile:
        # on tente une réparation simple en tronquant après le marqueur de fin ZIP
        if hasattr(file_obj_or_path, "read"):
            try:
                file_obj_or_path.seek(0)
            except Exception:
                pass
            data = file_obj_or_path.read()
        else:
            with open(file_obj_or_path, "rb") as f:
                data = f.read()

        end_sig = b"PK\x05\x06"
        idx = data.rfind(end_sig)
        if idx == -1:
            raise

        fixed = data[:idx + 22]
        return pd.read_excel(BytesIO(fixed), sheet_name=sheet_name, header=header)


def get_excel_file_resilient(file_obj_or_path) -> pd.ExcelFile:
    """
    Version robuste de pd.ExcelFile pour certains fichiers VMG.
    """
    try:
        return pd.ExcelFile(file_obj_or_path)
    except zipfile.BadZipFile:
        if hasattr(file_obj_or_path, "read"):
            try:
                file_obj_or_path.seek(0)
            except Exception:
                pass
            data = file_obj_or_path.read()
        else:
            with open(file_obj_or_path, "rb") as f:
                data = f.read()

        end_sig = b"PK\x05\x06"
        idx = data.rfind(end_sig)
        if idx == -1:
            raise

        fixed = data[:idx + 22]
        return pd.ExcelFile(BytesIO(fixed))

# =========================
# UI
# =========================
def login():
    st.markdown("## 🔐 Connexion")

    username = st.text_input("Utilisateur", key="login_user")
    password = st.text_input("Mot de passe", type="password", key="login_pwd")

    if st.button("Se connecter", key="login_btn"):
        if username in USERS:
            hashed = hash_password(password)
            if USERS[username]["password"] == hashed:
                st.session_state["authenticated"] = True
                st.session_state["user"] = username
                st.session_state["role"] = USERS[username]["role"]
                st.success("Connexion réussie")
                st.rerun()
            else:
                st.error("Mot de passe incorrect")
        else:
            st.error("Utilisateur inconnu")
def main():
    st.set_page_config(page_title="Contrôle factures transport", layout="wide")

    # =========================
    # AUTHENTIFICATION
    # =========================
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if not st.session_state["authenticated"]:
        login()
        return

    # =========================
    # HEADER + DESIGN PREMIUM
    # =========================
    st.markdown(
        """
        <style>

        html, body, [class*="css"]  {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
        }

        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 2rem;
            max-width: 100%;
        }

        h1, h2, h3 {
            margin-top: 0 !important;
            padding-top: 0 !important;
            line-height: 1.2;
            font-weight: 600;
        }

        .app-header {
            background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
            border: 1px solid #e5e7eb;
            border-radius: 18px;
            padding: 18px 20px 14px 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
            margin-bottom: 1rem;
        }

        .app-title {
            font-size: 1.9rem;
            font-weight: 700;
            letter-spacing: -0.02em;
            margin: 0;
            color: #111827;
        }

        .app-subtitle {
            margin-top: 6px;
            font-size: 0.95rem;
            color: #6b7280;
        }

        .user-badge {
            display: inline-block;
            padding: 8px 12px;
            border-radius: 999px;
            background: #eff6ff;
            border: 1px solid #bfdbfe;
            color: #1d4ed8;
            font-size: 0.82rem;
            font-weight: 600;
            text-align: center;
            white-space: nowrap;
        }

        /* KPI cards */
        div[data-testid="stMetric"] {
            background: linear-gradient(180deg, #ffffff 0%, #f9fafb 100%);
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            padding: 16px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        }

        div[data-testid="stMetricLabel"] {
            font-size: 0.8rem;
            color: #6b7280;
            font-weight: 600;
        }

        div[data-testid="stMetricValue"] {
            font-size: 1.4rem;
            font-weight: 700;
        }

        /* boutons */
        div.stButton > button {
            border-radius: 10px;
            border: 1px solid #d1d5db;
            padding: 0.35rem 0.8rem;
            font-size: 0.82rem;
            font-weight: 500;
            background: white;
        }

        div.stButton > button:hover {
            background: #f3f4f6;
        }

        div[data-testid="stDownloadButton"] > button {
            border-radius: 10px;
            font-weight: 600;
        }

        /* dataframe */
        div[data-testid="stDataFrame"] {
            border-radius: 16px;
            border: 1px solid #e5e7eb;
            overflow: hidden;
        }

        /* sidebar */
        section[data-testid="stSidebar"] {
            border-right: 1px solid #e5e7eb;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="app-header">', unsafe_allow_html=True)

    col_header_1, col_header_2, col_header_3 = st.columns([7, 2, 1], vertical_alignment="center")

    with col_header_1:
        st.markdown('<div class="app-title">🚚 Contrôle des factures transport</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="app-subtitle">Analyse automatique • Détection d\'écarts • Comparaison transporteurs</div>',
            unsafe_allow_html=True,
        )

    with col_header_2:
        st.markdown(
            f'<div class="user-badge">Connecté : {st.session_state.get("user", "-")}</div>',
            unsafe_allow_html=True,
        )

    with col_header_3:
        if st.button("Déconnexion", key="logout_btn"):
            st.session_state.clear()
            st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)

    st.divider()

    init_db()

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "➕ Nouveau contrôle",
        "📊 Historique",
        "📈 Dashboard",
        "📦 Convertisseur",
        "🚀 Comparateur transporteurs"
    ])

    with tab1:
        st.header("Nouveau contrôle")

        segment = st.radio(
            "Segment",
            options=["colis", "palettes"],
            horizontal=True,
            key="tab1_segment_radio",
            format_func=lambda x: "Messagerie colis" if x == "colis" else "Messagerie palettes",
        )

        uploaded_file = st.file_uploader("Téléverser un fichier Excel", type=["xlsx", "xls"], key="tab1_uploader")

        tolerance = st.number_input(
            "Tolérance acceptée sur l’écart (€)",
            min_value=0.0,
            max_value=50.0,
            value=float(DEFAULT_TOLERANCE_EUR),
            step=0.01,
            key="tab1_tolerance",
        )

        if uploaded_file is not None:
            carrier_detected = detect_carrier(uploaded_file)
            if carrier_detected:
                raw_segment = CARRIERS[carrier_detected]["segment"]
                if segment == raw_segment:
                    if raw_segment == "palettes":
                        st.error("Ceci est une facture brute palettes. Va dans l’onglet Convertisseur (Palettes).")
                    else:
                        st.error("Ceci est une facture brute colis. Va dans l’onglet Convertisseur (Colis).")
                    st.stop()

        if uploaded_file is not None and st.button("Lancer le contrôle", key="tab1_run_btn"):
            try:
                if segment == "colis":
                    df_res = controler_colis(uploaded_file, uploaded_file.name, tolerance)
                else:
                    df_res = controler_palettes(uploaded_file, uploaded_file.name, tolerance)
            except Exception as e:
                st.error(str(e))
                st.stop()

            run_info = build_run_info(df_res, uploaded_file.name, segment)
            run_id = save_run_and_lines(run_info, df_res)

            st.success(f"Contrôle terminé ✅ (run #{run_id})")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Lignes", run_info["nb_lignes"])
            c2.metric("OK", run_info["nb_ok"])
            c3.metric("KO", run_info["nb_ko"])
            c4.metric("INCOMPLET", run_info["nb_incomplet"])

            c5, c6, c7 = st.columns(3)
            c5.metric("Facturé", f"{run_info['montant_facture_total']:.2f} €")
            c6.metric("Calculé", f"{run_info['montant_calcule_total']:.2f} €")
            c7.metric("Écart total", f"{run_info['ecart_total']:.2f} €")

            c8, c9 = st.columns(2)
            c8.metric("Écarts + (à réclamer)", f"{run_info['ecart_total_pos']:.2f} €")
            c9.metric("Écarts - (informatif)", f"{run_info['ecart_total_neg']:.2f} €")

            st.metric("Taux de conformité", f"{run_info['taux_conformite']:.2f} %")

            df_show = df_res.copy()

            f1, f2, f3 = st.columns(3)
            statuts = f1.multiselect("Statut", ["OK", "KO", "INCOMPLET"], default=["KO", "INCOMPLET"], key="tab1_statut")
            transporteurs = sorted(df_show["transporteur"].dropna().astype(str).unique().tolist())
            tr_sel = f2.multiselect("Transporteur", transporteurs, default=transporteurs, key="tab1_transporteur")
            factures = sorted(df_show["numero_facture"].dropna().astype(str).unique().tolist())
            fac_sel = f3.multiselect("Numéro facture", factures, default=factures, key="tab1_facture")

            df_show = df_show[df_show["statut"].isin(statuts)]
            df_show = df_show[df_show["transporteur"].astype(str).isin([str(x) for x in tr_sel])]
            df_show = df_show[df_show["numero_facture"].astype(str).isin([str(x) for x in fac_sel])]

            st.dataframe(df_show, use_container_width=True)

            excel_bytes = build_excel_report(df_res)
            st.download_button(
                "📥 Télécharger le rapport Excel",
                data=excel_bytes,
                file_name=f"rapport_controle_{segment}_{run_id}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="tab1_download",
            )

            st.altair_chart(chart_statuts(df_res), use_container_width=True)

    with tab2:
        st.header("Historique")
        df_runs = get_runs()
        if df_runs.empty:
            st.info("Aucun contrôle enregistré.")
        else:
            st.dataframe(df_runs, use_container_width=True)
            run_id_sel = st.selectbox("Choisir un run", df_runs["id"].tolist(), key="tab2_run_select")
            df_lines = get_run_lines(int(run_id_sel))
            st.subheader(f"Lignes du run #{run_id_sel}")
            st.dataframe(df_lines, use_container_width=True)

    with tab3:
        st.header("📊 Tableau de bord")

        df_runs = get_runs()

        if df_runs.empty:
            st.info("Aucun contrôle disponible.")
        else:
            mode_dashboard = st.radio(
                "Mode d'analyse",
                ["Contrôle sélectionné", "Tous les contrôles"],
                horizontal=True,
                key="tab3_mode_dashboard",
            )

            if mode_dashboard == "Contrôle sélectionné":
                run_ids = df_runs["id"].tolist()
                run_sel = st.selectbox("Choisir un contrôle", run_ids, key="tab3_run_select")

                run_row = df_runs[df_runs["id"] == run_sel].iloc[0]
                st.caption(
                    f"Fichier : {run_row['filename']} | "
                    f"Date : {run_row['created_at']} | "
                    f"Segment : {run_row['segment']}"
                )

                df = get_run_lines(run_sel).copy()

            else:
                st.caption("Vue consolidée sur l’ensemble des contrôles enregistrés.")
                df = get_all_lines().copy()

            if df.empty:
                st.info("Aucune donnée disponible pour cette vue.")
            else:
                # =========================
                # FILTRES
                # =========================
                st.markdown("### 🎛️ Filtres")

                filter_cols = st.columns(4)

                segments = sorted(df["segment"].dropna().astype(str).unique().tolist()) if "segment" in df.columns else []
                if segments:
                    segments_sel = filter_cols[0].multiselect(
                        "Segment",
                        segments,
                        default=segments,
                        key="tab3_segments_filter",
                    )
                    df = df[df["segment"].astype(str).isin([str(x) for x in segments_sel])].copy()

                transporteurs = sorted(df["transporteur"].dropna().astype(str).unique().tolist())
                transporteurs_sel = filter_cols[1].multiselect(
                    "Transporteur",
                    transporteurs,
                    default=transporteurs,
                    key="tab3_transporteurs_filter",
                )

                statuts = ["OK", "KO", "INCOMPLET"]
                statuts_sel = filter_cols[2].multiselect(
                    "Statut",
                    statuts,
                    default=statuts,
                    key="tab3_statuts_filter",
                )

                if "date_facture" in df.columns:
                    df["date_facture_dt"] = pd.to_datetime(df["date_facture"], errors="coerce")
                    df_dates = df[df["date_facture_dt"].notna()].copy()

                    if not df_dates.empty:
                        min_date = df_dates["date_facture_dt"].min().date()
                        max_date = df_dates["date_facture_dt"].max().date()
                        date_range = filter_cols[3].date_input(
                            "Période",
                            value=(min_date, max_date),
                            key="tab3_date_range",
                        )

                        if isinstance(date_range, tuple) and len(date_range) == 2:
                            date_start, date_end = date_range
                            df = df[
                                (df["date_facture_dt"].isna()) |
                                (
                                    (df["date_facture_dt"].dt.date >= date_start) &
                                    (df["date_facture_dt"].dt.date <= date_end)
                                )
                            ].copy()

                df = df[
                    df["transporteur"].astype(str).isin([str(x) for x in transporteurs_sel]) &
                    df["statut"].astype(str).isin([str(x) for x in statuts_sel])
                ].copy()

                if df.empty:
                    st.warning("Aucune donnée après application des filtres.")
                else:
                    # =========================
                    # SÉCURISATION DES COLONNES
                    # =========================
                    for c in [
                        "ecart_pos", "ecart_neg", "ecart_ht",
                        "montant_facture_ht", "montant_calcule_ht",
                        "surcharge_relabeling", "surcharge_pick_return"
                    ]:
                        if c not in df.columns:
                            df[c] = 0.0
                        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

                    # =========================
                    # KPI
                    # =========================
                    total = len(df)
                    nb_ok = int((df["statut"] == "OK").sum())
                    nb_ko = int((df["statut"] == "KO").sum())
                    nb_inc = int((df["statut"] == "INCOMPLET").sum())

                    mt_fact = float(df["montant_facture_ht"].sum())
                    mt_calc = float(df["montant_calcule_ht"].sum())
                    mt_ecart = float(df["ecart_ht"].sum())
                    mt_pos = float(df["ecart_pos"].sum())
                    mt_neg = float(df["ecart_neg"].sum())

                    taux = (nb_ok / total * 100) if total else 0.0

                    # =========================
                    # KPI DIRECTION
                    # =========================
                    st.markdown("### 🧭 Vue direction")

                    synth_dir = (
                        df.groupby("transporteur", dropna=False)
                        .agg(
                            ecart_pos=("ecart_pos", "sum"),
                            nb_ko=("statut", lambda s: (s == "KO").sum()),
                        )
                        .reset_index()
                    )

                    if not synth_dir.empty:
                        worst_row = synth_dir.sort_values("ecart_pos", ascending=False).iloc[0]
                        worst_transporteur = str(worst_row["transporteur"])
                        worst_ecart = float(worst_row["ecart_pos"])
                    else:
                        worst_transporteur = "-"
                        worst_ecart = 0.0

                    d1, d2, d3, d4 = st.columns(4)
                    d1.metric("Montant à réclamer", f"{mt_pos:.2f} €")
                    d2.metric("Taux de conformité global", f"{taux:.2f} %")
                    d3.metric("Transporteur le plus en écart", worst_transporteur)
                    d4.metric("Nb lignes KO", nb_ko)

                    st.caption(
                        f"Transporteur le plus en écart : {worst_transporteur} "
                        f"avec {worst_ecart:.2f} € d'écarts positifs."
                    )

                    st.divider()

                    # =========================
                    # TOP 5 TRANSPORTEURS
                    # =========================
                    st.markdown("### 🏆 Top 5 transporteurs")

                    top5 = (
                        df.groupby("transporteur", dropna=False)
                        .agg(
                            nb_lignes=("numero_facture", "count"),
                            nb_ok=("statut", lambda s: (s == "OK").sum()),
                            nb_ko=("statut", lambda s: (s == "KO").sum()),
                            nb_incomplet=("statut", lambda s: (s == "INCOMPLET").sum()),
                            montant_facture=("montant_facture_ht", "sum"),
                            ecart_pos=("ecart_pos", "sum"),
                            ecart_neg=("ecart_neg", "sum"),
                        )
                        .reset_index()
                    )

                    top5["taux_conformite"] = np.where(
                        top5["nb_lignes"] > 0,
                        top5["nb_ok"] / top5["nb_lignes"] * 100,
                        0.0,
                    )

                    top5 = top5.sort_values(
                        ["ecart_pos", "taux_conformite"],
                        ascending=[False, True]
                    ).head(5).copy()

                    if not top5.empty:
                        top5["rang"] = range(1, len(top5) + 1)

                        top5 = top5[
                            [
                                "rang",
                                "transporteur",
                                "nb_lignes",
                                "nb_ko",
                                "nb_incomplet",
                                "montant_facture",
                                "ecart_pos",
                                "ecart_neg",
                                "taux_conformite",
                            ]
                        ]

                        st.dataframe(top5, use_container_width=True)
                    else:
                        st.info("Aucun transporteur à afficher.")

                    nb_relabel = int((df["surcharge_relabeling"] > 0).sum()) if "surcharge_relabeling" in df.columns else 0
                    mt_relabel = float(df["surcharge_relabeling"].sum()) if "surcharge_relabeling" in df.columns else 0.0

                    nb_pick = int((df["surcharge_pick_return"] > 0).sum()) if "surcharge_pick_return" in df.columns else 0
                    mt_pick = float(df["surcharge_pick_return"].sum()) if "surcharge_pick_return" in df.columns else 0.0

                    st.markdown("### 📌 Indicateurs clés")

                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Lignes contrôlées", total)
                    c2.metric("OK", nb_ok)
                    c3.metric("KO", nb_ko)
                    c4.metric("INCOMPLET", nb_inc)

                    c5, c6, c7, c8 = st.columns(4)
                    c5.metric("Facturé", f"{mt_fact:.2f} €")
                    c6.metric("Calculé", f"{mt_calc:.2f} €")
                    c7.metric("Écart total", f"{mt_ecart:.2f} €")
                    c8.metric("Conformité", f"{taux:.2f} %")

                    c9, c10, c11, c12 = st.columns(4)
                    c9.metric("À réclamer", f"{mt_pos:.2f} €")
                    c10.metric("Écarts négatifs", f"{mt_neg:.2f} €")
                    c11.metric("Nb relabel", nb_relabel)
                    c12.metric("Nb pick & return", nb_pick)

                    c13, c14 = st.columns(2)
                    c13.metric("Coût relabel", f"{mt_relabel:.2f} €")
                    c14.metric("Coût pick & return", f"{mt_pick:.2f} €")

                    st.divider()

                    # =========================
                    # EXPORT
                    # =========================
                    st.markdown("### 📤 Export des anomalies")

                    df_reclam = df[
                        (df["statut"].astype(str) == "KO") &
                        (pd.to_numeric(df["ecart_pos"], errors="coerce").fillna(0.0) > 0)
                    ].copy()

                    if df_reclam.empty:
                        st.info("Aucune anomalie à réclamer sur la sélection actuelle.")
                    else:
                        total_reclam = float(
                            pd.to_numeric(df_reclam["ecart_pos"], errors="coerce").fillna(0.0).sum()
                        )
                        st.write(
                            f"**{len(df_reclam)}** ligne(s) à réclamer pour un total de **{total_reclam:.2f} €**"
                        )

                        reclam_bytes = build_excel_reclamation(df)

                        reclam_name = (
                            f"reclamation_transport_run_{run_sel}.xlsx"
                            if mode_dashboard == "Contrôle sélectionné"
                            else "reclamation_transport_tous_les_controles.xlsx"
                        )

                        st.download_button(
                            "📥 Télécharger le fichier de réclamation",
                            data=reclam_bytes,
                            file_name=reclam_name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="tab3_download_reclamation",
                        )

                    st.divider()

                    # =========================
                    # GRAPHIQUES
                    # =========================
                    st.markdown("### 📈 Vue visuelle")

                    g1, g2 = st.columns(2)
                    with g1:
                        st.altair_chart(chart_conformite_transporteur(df), use_container_width=True)
                    with g2:
                        st.altair_chart(chart_ecarts_transporteur(df), use_container_width=True)

                    g3, g4 = st.columns(2)
                    with g3:
                        st.altair_chart(chart_surcharges_transporteur(df), use_container_width=True)
                    with g4:
                        st.altair_chart(chart_top_anomalies(df), use_container_width=True)

                    g5, g6 = st.columns(2)
                    with g5:
                        st.markdown("#### 📈 Taux de conformité mensuel")
                        st.altair_chart(chart_evolution_mensuelle_conformite(df), use_container_width=True)
                    with g6:
                        st.markdown("#### 💰 Écarts mensuels")
                        st.altair_chart(chart_evolution_mensuelle_ecarts(df), use_container_width=True)

                    st.divider()

                    # =========================
                    # SYNTHÈSE
                    # =========================
                    synth = (
                        df.groupby("transporteur", dropna=False)
                        .agg(
                            nb_lignes=("numero_facture", "count"),
                            nb_ok=("statut", lambda s: (s == "OK").sum()),
                            nb_ko=("statut", lambda s: (s == "KO").sum()),
                            nb_incomplet=("statut", lambda s: (s == "INCOMPLET").sum()),
                            montant_facture=("montant_facture_ht", "sum"),
                            montant_calcule=("montant_calcule_ht", "sum"),
                            ecart_total=("ecart_ht", "sum"),
                            ecart_pos=("ecart_pos", "sum"),
                            ecart_neg=("ecart_neg", "sum"),
                            relabel=("surcharge_relabeling", "sum"),
                            pick_return=("surcharge_pick_return", "sum"),
                        )
                        .reset_index()
                        .sort_values("ecart_pos", ascending=False)
                    )

                    st.markdown("### 🧾 Synthèse par transporteur")
                    st.dataframe(synth, use_container_width=True)

                    st.divider()

                    # =========================
                    # SURCHARGES
                    # =========================
                    st.markdown("### ⚠️ Lignes avec surcharges")

                    df_surch = df[
                        (df.get("surcharge_relabeling", 0) > 0) |
                        (df.get("surcharge_pick_return", 0) > 0)
                    ].copy()

                    if df_surch.empty:
                        st.info("Aucune surcharge détectée.")
                    else:
                        cols_show = [
                            c for c in [
                                "date_facture", "transporteur", "numero_facture",
                                "reference_expedition", "reference_client",
                                "service_code", "pays_orig", "pays_dest",
                                "poids_kg", "montant_facture_ht",
                                "surcharge_relabeling", "surcharge_pick_return", "raison"
                            ] if c in df_surch.columns
                        ]

                        st.dataframe(df_surch[cols_show], use_container_width=True)
    with tab4:
        st.header("📦 Convertisseur de factures brutes")

        subtab_conv1, subtab_conv2 = st.tabs(["🧱 Palettes", "📦 Colis"])

        # =========================
        # PALETTES - CONVERTISSEUR
        # =========================
        with subtab_conv1:
            st.subheader("Convertisseur palettes")
            st.caption(
                "Téléverse une facture brute palettes (TFM, GEODIS, ...) pour la convertir "
                "et/ou la contrôler avec le tarif master palettes."
            )

            if Path(TARIFS_PALETTE_MASTER_PATH).exists():
                st.success(f"Tarif master détecté : {TARIFS_PALETTE_MASTER_PATH}")
            else:
                st.warning(f"Tarif master manquant : {TARIFS_PALETTE_MASTER_PATH}")

            raw_file = st.file_uploader(
                "Facture brute palettes (.xlsx)",
                type=["xlsx"],
                key="tab4_pal_uploader",
            )

            colA, colB = st.columns([1, 1])

            tol_palette = st.number_input(
                "Tolérance (€) pour ce contrôle palettes",
                min_value=0.0,
                max_value=50.0,
                value=float(DEFAULT_TOLERANCE_EUR),
                step=0.01,
                key="tab4_pal_tol",
            )

            if raw_file is not None:
                carrier = detect_carrier(raw_file)
                if carrier and CARRIERS[carrier]["segment"] == "palettes":
                    st.info(f"Transporteur détecté : **{carrier}**")
                elif carrier:
                    st.warning(
                        f"Le fichier détecté appartient au segment "
                        f"**{CARRIERS[carrier]['segment']}**, pas palettes."
                    )
                else:
                    st.warning("Transporteur non détecté automatiquement.")

                if colA.button("Convertir", key="tab4_pal_convert_btn"):
                    try:
                        df_std, df_source, carrier2, segment2 = convert_raw_invoice(raw_file)
                        if segment2 != "palettes":
                            raise ValueError("Cette facture brute n'appartient pas au segment palettes.")

                        out_bytes = build_excel_from_df(df_std, df_source, f"source_{carrier2}", segment2)
                        st.success("Conversion terminée ✅")
                        st.dataframe(df_std.head(200), use_container_width=True)

                        st.download_button(
                            "📥 Télécharger la facture standardisée palettes",
                            data=out_bytes,
                            file_name=f"{carrier2}_{Path(raw_file.name).stem}_standardisee_palettes.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="tab4_pal_download_std",
                        )
                    except Exception as e:
                        st.error(str(e))

                if colB.button("Convertir + Lancer contrôle", key="tab4_pal_convert_and_control"):
                    try:
                        t_master = load_tarifs_palette_master()
                        df_std, _, carrier2, segment2 = convert_raw_invoice(raw_file)

                        if segment2 != "palettes":
                            raise ValueError("Cette facture brute n'appartient pas au segment palettes.")

                        xls_out = BytesIO()
                        with pd.ExcelWriter(xls_out, engine="openpyxl") as w:
                            t_master.to_excel(w, sheet_name="tarifs_palette", index=False)
                            df_std.to_excel(w, sheet_name="facture_palette_brut", index=False)
                        xls_out.seek(0)

                        df_res = controler_palettes(xls_out, f"CTRL_{raw_file.name}", tol_palette)
                        run_info = build_run_info(df_res, f"CTRL_{raw_file.name}", "palettes")
                        run_id = save_run_and_lines(run_info, df_res)

                        st.success(f"Contrôle palettes terminé ✅ (run #{run_id})")
                        st.subheader("Lignes KO / INCOMPLET")
                        st.dataframe(
                            df_res[df_res["statut"].isin(["KO", "INCOMPLET"])],
                            use_container_width=True,
                        )

                        report_bytes = build_excel_report(df_res)
                        st.download_button(
                            "📥 Télécharger le rapport de contrôle palettes",
                            data=report_bytes,
                            file_name=f"rapport_controle_palettes_{carrier2}_{run_id}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="tab4_pal_download_report",
                        )
                    except Exception as e:
                        st.error(str(e))

        # =========================
        # COLIS - CONVERTISSEUR
        # =========================
        with subtab_conv2:
            st.subheader("Convertisseur colis")
            st.caption(
                "Téléverse une facture brute colis (DPD, GLS, ...) pour la convertir "
                "et/ou la contrôler avec le tarif master colis."
            )

            if Path(TARIFS_COLIS_MASTER_PATH).exists():
                st.success(f"Tarif master détecté : {TARIFS_COLIS_MASTER_PATH}")
            else:
                st.warning(f"Tarif master manquant : {TARIFS_COLIS_MASTER_PATH}")

            raw_file = st.file_uploader(
                "Facture brute colis (.xlsx)",
                type=["xlsx"],
                key="tab4_col_uploader",
            )

            colA, colB = st.columns([1, 1])

            tol_colis = st.number_input(
                "Tolérance (€) pour ce contrôle colis",
                min_value=0.0,
                max_value=50.0,
                value=float(DEFAULT_TOLERANCE_EUR),
                step=0.01,
                key="tab4_col_tol",
            )

            if raw_file is not None:
                carrier = detect_carrier(raw_file)
                if carrier and CARRIERS[carrier]["segment"] == "colis":
                    st.info(f"Transporteur détecté : **{carrier}**")
                elif carrier:
                    st.warning(
                        f"Le fichier détecté appartient au segment "
                        f"**{CARRIERS[carrier]['segment']}**, pas colis."
                    )
                else:
                    st.warning("Transporteur non détecté automatiquement.")

                if colA.button("Convertir", key="tab4_col_convert_btn"):
                    try:
                        df_std, df_source, carrier2, segment2 = convert_raw_invoice(raw_file)
                        if segment2 != "colis":
                            raise ValueError("Cette facture brute n'appartient pas au segment colis.")

                        out_bytes = build_excel_from_df(df_std, df_source, f"source_{carrier2}", segment2)
                        st.success("Conversion terminée ✅")
                        st.dataframe(df_std.head(200), use_container_width=True)

                        st.download_button(
                            "📥 Télécharger la facture standardisée colis",
                            data=out_bytes,
                            file_name=f"{carrier2}_{Path(raw_file.name).stem}_standardisee_colis.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="tab4_col_download_std",
                        )
                    except Exception as e:
                        st.error(str(e))

                if colB.button("Convertir + Lancer contrôle", key="tab4_col_convert_and_control"):
                    try:
                        tarifs = load_tarifs_colis_master()
                        df_std, _, carrier2, segment2 = convert_raw_invoice(raw_file)

                        if segment2 != "colis":
                            raise ValueError("Cette facture brute n'appartient pas au segment colis.")

                        xls_out = BytesIO()
                        with pd.ExcelWriter(xls_out, engine="openpyxl") as w:
                            tarifs.to_excel(w, sheet_name="tarifs", index=False)
                            df_std.to_excel(w, sheet_name="facture_lignes", index=False)
                        xls_out.seek(0)

                        df_res = controler_colis(xls_out, f"CTRL_{raw_file.name}", tol_colis)
                        run_info = build_run_info(df_res, f"CTRL_{raw_file.name}", "colis")
                        run_id = save_run_and_lines(run_info, df_res)

                        st.success(f"Contrôle colis terminé ✅ (run #{run_id})")
                        st.subheader("Lignes KO / INCOMPLET")
                        st.dataframe(
                            df_res[df_res["statut"].isin(["KO", "INCOMPLET"])],
                            use_container_width=True,
                        )

                        report_bytes = build_excel_report(df_res)
                        st.download_button(
                            "📥 Télécharger le rapport de contrôle colis",
                            data=report_bytes,
                            file_name=f"rapport_controle_colis_{carrier2}_{run_id}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="tab4_col_download_report",
                        )
                    except Exception as e:
                        st.error(str(e))
    with tab5:
        st.header("🚀 Comparateur transporteurs")

        subtab1, subtab2 = st.tabs(["📦 Colis", "🧱 Palettes"])

        # =========================
        # COLIS - COMPARATEUR
        # =========================
        with subtab1:
            st.subheader("Comparateur colis")

            col1, col2, col3, col4, col5 = st.columns(5)

            poids = col1.number_input(
                "Poids (kg)",
                min_value=0.0,
                step=0.1,
                key="cmp_colis_poids",
            )
            pays_orig = col2.text_input(
                "Pays origine",
                value="BE",
                key="cmp_colis_pays_orig",
            )
            pays_dest = col3.text_input(
                "Pays destination",
                value="BE",
                key="cmp_colis_pays_dest",
            )
            cp_dest = col4.text_input(
                "Code postal destination",
                key="cmp_colis_cp_dest",
            )
            date_ref = col5.date_input(
                "Date d'expédition",
                key="cmp_colis_date",
            )

            if st.button("🔍 Comparer colis", key="compare_colis_btn"):
                try:
                    tarifs = load_tarifs_colis_master()

                    df_res = simuler_tarifs_colis(
                        tarifs=tarifs,
                        poids=poids,
                        pays_orig=pays_orig,
                        pays_dest=pays_dest,
                        cp_dest=cp_dest,
                        date_ref=pd.to_datetime(date_ref),
                    )

                    if df_res.empty:
                        st.warning("Aucun transporteur trouvé.")
                    else:
                        df_res["prix_estime"] = pd.to_numeric(df_res["prix_estime"], errors="coerce").fillna(999999.0)
                        df_res["score"] = df_res["prix_estime"]
                        df_res = df_res.sort_values(["score", "transporteur", "service_code"]).reset_index(drop=True)
                        df_res["rang"] = df_res.index + 1

                        st.success("Comparaison réalisée")
                        st.dataframe(
                            df_res[["rang", "transporteur", "service_code", "prix_estime", "score"]],
                            use_container_width=True,
                        )

                        best = df_res.iloc[0]
                        st.success(
                            f"🏆 Meilleur choix : {best['transporteur']} "
                            f"({best['service_code']}) → {best['prix_estime']:.2f} €"
                        )

                except Exception as e:
                    st.error(f"Erreur : {e}")

        # =========================
        # PALETTES - COMPARATEUR
        # =========================
        with subtab2:
            st.subheader("Comparateur palettes")

            c1, c2, c3, c4 = st.columns(4)

            nb_palettes = c1.number_input(
                "Nb palettes",
                min_value=0.0,
                step=0.5,
                key="cmp_pal_nb_palettes",
            )
            poids_total_kg = c2.number_input(
                "Poids total (kg)",
                min_value=0.0,
                step=1.0,
                key="cmp_pal_poids_total",
            )
            pays_orig_pal = c3.text_input(
                "Pays origine",
                value="BE",
                key="cmp_pal_pays_orig",
            )
            cp_orig = c4.text_input(
                "CP origine",
                key="cmp_pal_cp_orig",
            )

            c5, c6, c7 = st.columns(3)

            pays_dest_pal = c5.text_input(
                "Pays destination",
                value="FR",
                key="cmp_pal_pays_dest",
            )
            cp_dest_pal = c6.text_input(
                "CP destination",
                key="cmp_pal_cp_dest",
            )
            date_ref_pal = c7.date_input(
                "Date d'expédition",
                key="cmp_pal_date",
            )

            if st.button("🔍 Comparer palettes", key="compare_pal_btn"):
                try:
                    tarifs = load_tarifs_palette_master()

                    df_res = simuler_tarifs_palettes(
                        tarifs=tarifs,
                        nb_palettes=nb_palettes,
                        poids_total_kg=poids_total_kg,
                        pays_orig=pays_orig_pal,
                        cp_orig=cp_orig,
                        pays_dest=pays_dest_pal,
                        cp_dest=cp_dest_pal,
                        date_ref=pd.to_datetime(date_ref_pal),
                    )

                    if df_res.empty:
                        st.warning("Aucun transporteur palette trouvé.")
                    else:
                        st.success("Comparaison palettes réalisée")
                        st.dataframe(
                            df_res[
                                [
                                    "rang",
                                    "transporteur",
                                    "service_code",
                                    "mode_calcul",
                                    "prix_estime",
                                    "prix_base_ht",
                                    "taxe_km",
                                    "taxe_gazoil",
                                    "taxe_rdv",
                                    "taxe_gestion",
                                    "taxe_securite",
                                    "taxe_energie",
                                ]
                            ],
                            use_container_width=True,
                        )

                        best = df_res.iloc[0]
                        st.success(
                            f"🏆 Meilleur choix : {best['transporteur']} "
                            f"({best['service_code']}) → {best['prix_estime']:.2f} €"
                        )

                except Exception as e:
                    st.error(f"Erreur : {e}")


if __name__ == "__main__":
    main()
