import pandas as pd
from datetime import datetime
from pathlib import Path

INPUT = Path("equipement_reseau.csv")          
OUTDIR = Path("outputs")             

def parse_date(s):
    return pd.to_datetime(s, errors="coerce")

def main():
    OUTDIR.mkdir(exist_ok=True)

    # Lire CSV 
    df = pd.read_csv(INPUT, sep=";", dtype=str).fillna("")
    df.columns = df.columns.str.strip()

    # Dates
    df["Date_Planifiee_dt"] = parse_date(df.get("Date_Planifiee", ""))
    df["Date_Realisation_dt"] = parse_date(df.get("Date_Realisation", ""))

    # Retard = pas réalisé ET date planifiée avant aujourd’hui
    today = pd.Timestamp.today().normalize()
    df["EN_RETARD"] = df["Date_Realisation_dt"].isna() & df["Date_Planifiee_dt"].notna() & (df["Date_Planifiee_dt"] < today)

    # Qualité
    df["FAIT_SANS_PREUVE"] = (df["Statut"].str.upper() == "FAIT") & (df.get("Preuve_Execution", "").str.strip() == "")
    df["FAIT_NON_VALIDE"] = (df["Statut"].str.upper() == "FAIT") & (df.get("Validation_Superviseur", "").str.upper().str.strip() != "OUI")

    # Exports
    retards = df[df["EN_RETARD"]].copy()
    qualite = df[df["FAIT_SANS_PREUVE"] | df["FAIT_NON_VALIDE"]].copy()

    # Si technicien vide -> NON_ASSIGNE
    tech_col = "Technicien_Assigne"
    if tech_col not in df.columns:
        df[tech_col] = "NON_ASSIGNE"
    df[tech_col] = df[tech_col].replace("", "NON_ASSIGNE")

    # Stats techniciens
    tech = df.groupby(tech_col).agg(
        Tickets_Total=("Ticket_ID", "count"),
        Retards=("EN_RETARD", "sum"),
        Fait_Sans_Preuve=("FAIT_SANS_PREUVE", "sum"),
        Fait_Non_Valide=("FAIT_NON_VALIDE", "sum"),
    ).reset_index()

    # Sauvegardes
    df.to_csv(OUTDIR / "data_enrichie.csv", index=False, sep=";", encoding="utf-8")
    retards.to_csv(OUTDIR / "tickets_en_retard.csv", index=False, sep=";", encoding="utf-8")
    qualite.to_csv(OUTDIR / "controle_qualite.csv", index=False, sep=";", encoding="utf-8")
    tech.to_csv(OUTDIR / "suivi_techniciens.csv", index=False, sep=";", encoding="utf-8")

    # Affichage résumé console
    print("✅ Terminé")
    print(f"- Retards: {len(retards)}")
    print(f"- Qualité: {len(qualite)}")
    print(f"- Techniciens: {len(tech)}")
    print(f"📁 Fichiers dans: {OUTDIR.resolve()}")

if __name__ == "__main__":
    main()
