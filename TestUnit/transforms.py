import pandas as pd

# Règle métier : mapping "source" -> "channel"
CHANNEL_MAP = {
    "google_ads": "search",
    "google": "search",
    "facebook": "social",
    "meta": "social",
    "newsletter": "email",
    "email": "email",
}

REQUIRED_COLS = {"campaign_id", "source", "spend", "clicks", "impressions"}


def transform_campaign_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme un DataFrame de campagnes en un format prêt pour l'analyse.
    """
    # 1) Fail fast : on refuse de travailler si le schéma n'est pas bon
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Copie défensive : évite de modifier df en place (important pour tests)
    out = df.copy(deep=True)

    # 2) Cast métriques
    for col in ["spend", "clicks", "impressions"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

    # 3) Normalisation source -> channel
    def normalize_source(x) -> str:
        key = str(x).strip().lower()
        return CHANNEL_MAP.get(key, "other")

    out["channel"] = out["source"].apply(normalize_source)

    # 4) KPIs
    # CTR : si impressions = 0, on met 0 (choix métier)
    out["ctr"] = out["clicks"] / out["impressions"].replace({0: pd.NA})
    out["ctr"] = out["ctr"].fillna(0)

    # CPC : si clicks = 0, on met NaN (car CPC "non défini")
    out["cpc"] = out["spend"] / out["clicks"].replace({0: pd.NA})

    return out
