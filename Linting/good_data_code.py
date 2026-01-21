# src/good_data_code.py
"""
Version plus sûre et plus robuste :
- Pas de secret en dur (utiliser variables d'environnement)
- SQL paramétré (pas de concat)
- Transformations robustes (copie défensive, to_numeric, division par zéro)
"""

from __future__ import annotations

import pandas as pd


def build_query() -> str:
    # Exemple : requête paramétrée (le param dépendra de votre driver DB)
    # Ici on retourne juste la forme, pour illustrer la pratique.
    return "SELECT campaign_id, event_time, spend, clicks FROM events WHERE campaign_id = ?"


def transform(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy(deep=True)

    # Conversion robuste : strings, nulls, valeurs invalides => NaN puis fill
    out["spend"] = pd.to_numeric(out["spend"], errors="coerce").fillna(0.0)
    out["clicks"] = pd.to_numeric(out["clicks"], errors="coerce").fillna(0)

    # CPC : si clicks=0 => NaN (non défini), évite division par 0
    out["cpc"] = out["spend"] / out["clicks"].replace({0: pd.NA})

    return out
