# src/bad_data_code.py
"""
Fichier volontairement "imparfait" pour montrer ce que le linting / l'analyse statique détecte.

- import inutile
- variable non utilisée
- SELECT * (souvent déconseillé en data pipelines)
- concat SQL par string (risque d'injection + bugs)
- traitement fragile des NaN / divisions par zéro
- secret en dur (exemple pédagogique)
"""

import pandas as pd  # noqa: F401  (on le laisse pour voir comment le linter réagit)

API_KEY = "SUPER_SECRET_KEY"  # <-- à détecter par scan secrets


def build_query(user_input: str) -> str:
    # SQL construit par concat -> dangereux (SQL injection)
    query = "SELECT * FROM events WHERE campaign_id = '" + user_input + "'"
    return query


def compute_cpc(spend, clicks):
    # division par zéro possible
    return spend / clicks


def transform(df):
    temp = 123  # variable non utilisée
    # risque : mutation + NaN pas gérés
    df["spend"] = df["spend"].astype(float)
    df["clicks"] = df["clicks"].astype(int)
    df["cpc"] = df["spend"] / df["clicks"]
    return df
