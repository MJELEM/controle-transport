import pandas as pd
from datetime import datetime, date
from pathlib import Path

# Nom du fichier Excel d'entrée (à adapter si besoin)
INPUT_XLSX = "modele_tarifs_palette.xlsx"
OUTPUT_XLSX = "rapport_controle_palettes.xlsx"

TOLERANCE_EUR = 0.45  # tolérance sur l'écart, comme pour les colis


def parse_date(val):
    if pd.isna(val) or val == "":
        return None
    if isinstance(val, (datetime, date)):
        return val
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(str(val), fmt)
        except ValueError:
            continue
    return None


def charger_donnees(input_path: str):
    xls = pd.ExcelFile(input_path)
    tarifs = pd.read_excel(xls, "tarifs_palette")
    facture_brut = pd.read_excel(xls, "facture_palette_brut")
    return tarifs, facture_brut


def agreger_facture_brut(facture_brut: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme facture_palette_brut (plusieurs lignes par expédition)
    en facture_palette_lignes (1 ligne par expédition).
    """
    df = facture_brut.copy()

    # normalisation type_ligne
    df["type_ligne_norm"] = (
        df["type_ligne"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # clés d'agrégation (tu peux adapter si besoin)
    group_keys = [
        "numero_facture",
        "reference_expedition",
        "date_facture",
        "transporteur",
        "service_code",
        "pays_dest",
        "cp_dest",
    ]

    def agg_group(g: pd.DataFrame) -> pd.Series:
        base = g.loc[g["type_ligne_norm"] == "BASE", "montant_ht"].sum()
        gazoil = g.loc[g["type_ligne_norm"] == "GAZOIL", "montant_ht"].sum()
        autres = g.loc[g["type_ligne_norm"] == "AUTRES", "montant_ht"].sum()
        total = g["montant_ht"].sum()

        nb_pal = g["nb_palettes"].max()
        dist = g["distance_km"].max()

        return pd.Series(
            {
                "nb_palettes": nb_pal,
                "distance_km": dist,
                "montant_base_ht": base,
                "montant_gazoil_ht": gazoil,
                "autres_frais_ht": autres,
                "montant_ligne_ht": total,
            }
        )

    agg = df.groupby(group_keys, dropna=False).apply(agg_group).reset_index()

    # parse date_facture
    agg["date_facture"] = agg["date_facture"].apply(parse_date)

    return agg


def preparer_tarifs(tarifs: pd.DataFrame) -> pd.DataFrame:
    t = tarifs.copy()

    # normalisation de base
    t["transporteur"] = t["transporteur"].astype(str).str.strip()
    t["service_code"] = t["service_code"].astype(str).str.strip()
    t["pays_dest"] = t["pays_dest"].astype(str).str.strip()

    # numériques de base
    for col in ["cp_debut", "cp_fin", "nb_pal_min", "nb_pal_max", "dist_min_km", "dist_max_km"]:
        if col in t.columns:
            t[col] = pd.to_numeric(t[col], errors="coerce")

    # dates
    if "date_debut" in t.columns:
        t["date_debut"] = t["date_debut"].apply(
            lambda v: parse_date(v).date() if parse_date(v) else None
        )
    else:
        t["date_debut"] = None

    if "date_fin" in t.columns:
        t["date_fin"] = t["date_fin"].apply(
            lambda v: parse_date(v).date() if parse_date(v) else None
        )
    else:
        t["date_fin"] = None

    # taxes numériques
    for col in [
        "prix_base_ht",
        "taxe_km_ht_par_km",   # montant fixe "taxe km"
        "taxe_gazoil_pct",     # montant fixe de gazoil (malgré le nom)
        "taxe_gestion PAL",
        "taxe_sécurité",
        "taxe_énergie",
    ]:
        if col in t.columns:
            t[col] = pd.to_numeric(t[col], errors="coerce").fillna(0.0)
        else:
            t[col] = 0.0

    return t


def tarif_actif(row, date_facture):
    d_deb = row.get("date_debut")
    d_fin = row.get("date_fin")
    if date_facture is None:
        return True
    d_fact = date_facture.date() if isinstance(date_facture, datetime) else date_facture
    if d_deb and d_fact < d_deb:
        return False
    if d_fin and d_fact > d_fin:
        return False
    return True


def trouver_tarif_palette(
    tarifs: pd.DataFrame,
    transporteur: str,
    service_code: str,
    pays_dest: str,
    cp_dest: str,
    nb_palettes: float,
    date_facture,
):
    t = tarifs.copy()

    t = t[
        (t["transporteur"].str.lower() == str(transporteur).lower())
        & (t["service_code"].str.lower() == str(service_code).lower())
    ]
    if t.empty:
        return None

    # filtre dates
    t = t[t.apply(lambda r: tarif_actif(r, date_facture), axis=1)]
    if t.empty:
        return None

    # filtre pays
    t_pays_exact = t[t["pays_dest"].str.lower() == str(pays_dest).lower()]
    t_sans_pays = t[t["pays_dest"] == ""]

    candidats_pays = pd.concat([t_pays_exact, t_sans_pays]).drop_duplicates()

    if candidats_pays.empty:
        return None

    # CP en entier si possible
    cp_val = None
    try:
        cp_val = int(float(cp_dest))
    except Exception:
        pass

    def match_cp(r):
        deb = r.get("cp_debut")
        fin = r.get("cp_fin")
        if pd.isna(deb) and pd.isna(fin):
            return True  # pas de restriction CP
        if cp_val is None:
            return False
        try:
            return deb <= cp_val <= fin
        except Exception:
            return False

    candidats_cp = candidats_pays[candidats_pays.apply(match_cp, axis=1)]
    if candidats_cp.empty:
        return None

    # nb palettes
    def match_pal(r):
        mn = r.get("nb_pal_min")
        mx = r.get("nb_pal_max")
        if pd.isna(mn) and pd.isna(mx):
            return True
        try:
            return mn <= nb_palettes <= mx
        except Exception:
            return False

    candidats_pal = candidats_cp[candidats_cp.apply(match_pal, axis=1)]
    if candidats_pal.empty:
        return None

    # pour l'instant, on ignore dist_min_km / dist_max_km
    return candidats_pal.iloc[0]


def controler_palettes(tarifs: pd.DataFrame, fact_lignes: pd.DataFrame, tolerance_eur: float):
    """
    Contrôle les palettes :
    - taxe_km_ht_par_km est utilisée comme TAXE KM FIXE par expédition.
    - taxe_gazoil_pct est utilisée comme MONTANT FIXE de taxe gazoil (malgré le nom).
    - Règle d'écart :
        * écart < 0  -> OK (facturé moins que le tarif)
        * 0 <= écart <= tolérance -> OK
        * écart > tolérance -> KO
    """
    lignes_res = []

    for idx, row in fact_lignes.iterrows():
        numero_facture = row.get("numero_facture", "")
        ref_exp = row.get("reference_expedition", "")
        transporteur = row.get("transporteur", "")
        service_code = row.get("service_code", "")
        pays_dest = row.get("pays_dest", "")
        cp_dest = row.get("cp_dest", "")
        nb_pal = row.get("nb_palettes", 0) or 0
        date_facture = row.get("date_facture", None)
        distance_km = row.get("distance_km", 0) or 0  # conservé pour analyse

        montant_ligne_ht = float(row.get("montant_ligne_ht", 0) or 0)

        trow = trouver_tarif_palette(
            tarifs,
            transporteur,
            service_code,
            pays_dest,
            cp_dest,
            float(nb_pal),
            date_facture,
        )

        if trow is None:
            lignes_res.append(
                {
                    "numero_facture": numero_facture,
                    "reference_expedition": ref_exp,
                    "transporteur": transporteur,
                    "service_code": service_code,
                    "pays_dest": pays_dest,
                    "cp_dest": cp_dest,
                    "nb_palettes": nb_pal,
                    "montant_facture_ht": montant_ligne_ht,
                    "montant_calcule_ht": None,
                    "ecart_ht": None,
                    "statut": "INCOMPLET",
                    "raison": "Aucun tarif trouvé (transporteur/service/pays/CP/nb palettes)",
                }
            )
            continue

        # === LOGIQUE DE CALCUL AVEC TAXES FIXES ===
        base = float(trow.get("prix_base_ht", 0.0))

        # taxe km : montant fixe par expédition
        taxe_km_fixe = float(trow.get("taxe_km_ht_par_km", 0.0))

        # taxe gazoil : montant fixe par expédition (pas un %)
        taxe_gazoil_fixe = float(trow.get("taxe_gazoil_pct", 0.0))

        # autres taxes fixes
        gestion = float(trow.get("taxe_gestion PAL", 0.0))
        securite = float(trow.get("taxe_sécurité", 0.0))
        energie = float(trow.get("taxe_énergie", 0.0))

        montant_calcule = base + taxe_km_fixe + taxe_gazoil_fixe + gestion + securite + energie
        montant_calcule = round(montant_calcule, 2)

        ecart = round(montant_ligne_ht - montant_calcule, 2)

        # Nouvelle logique : écart négatif = toujours OK
        if ecart < 0:
            statut = "OK"
            raison = "Facturé moins que le tarif"
        else:
            if ecart <= tolerance_eur:
                statut = "OK"
                raison = ""
            else:
                statut = "KO"
                raison = f"Écart {ecart:.2f}€ > tolérance {tolerance_eur:.2f}€"

        lignes_res.append(
            {
                "numero_facture": numero_facture,
                "reference_expedition": ref_exp,
                "transporteur": transporteur,
                "service_code": service_code,
                "pays_dest": pays_dest,
                "cp_dest": cp_dest,
                "nb_palettes": nb_pal,
                "distance_km": distance_km,
                "montant_facture_ht": montant_ligne_ht,
                "montant_calcule_ht": montant_calcule,
                "ecart_ht": ecart,
                "statut": statut,
                "raison": raison,
            }
        )

    df_res = pd.DataFrame(lignes_res)
    return df_res


def main():
    input_path = Path(INPUT_XLSX)
    if not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {input_path}")

    print(f"Chargement des données depuis {input_path}...")
    tarifs, facture_brut = charger_donnees(str(input_path))

    print("Préparation des tarifs palettes...")
    tarifs_prep = preparer_tarifs(tarifs)

    print("Agrégation des factures brutes palettes...")
    fact_lignes = agreger_facture_brut(facture_brut)

    print("Contrôle des palettes...")
    df_res = controler_palettes(tarifs_prep, fact_lignes, TOLERANCE_EUR)

    # Sauvegarde dans un nouveau fichier Excel
    output_path = Path(OUTPUT_XLSX)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        tarifs.to_excel(writer, sheet_name="tarifs_palette", index=False)
        facture_brut.to_excel(writer, sheet_name="facture_palette_brut", index=False)
        fact_lignes.to_excel(writer, sheet_name="facture_palette_lignes", index=False)
        df_res.to_excel(writer, sheet_name="rapport_controle_palette", index=False)

    print(f"Contrôle terminé. Résultat enregistré dans : {output_path}")


if __name__ == "__main__":
    main()
