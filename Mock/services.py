# services.py
"""
isoler la logique métier en "injectant" les dépendances,
ce qui rend le mocking simple.

1) API externe (HTTP) : récupère des stats de campagne
2) Base de données : stocke des événements
3) Stockage cloud : charge un fichier CSV

- On ne "mock" pas la logique métier.
- On mock les I/O (réseau, DB, cloud) pour des tests rapides et reproductibles.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Protocol, List
import pandas as pd


# -------------------------
# 1) API externe : interface
# -------------------------

class HttpClient(Protocol):
    """Interface minimale (Protocol) d'un client HTTP."""
    def get_json(self, url: str) -> Dict: ...


def fetch_campaign_stats(http: HttpClient, campaign_id: str) -> Dict:
    """
    Récupère les statistiques d'une campagne depuis une API externe.

    - On injecte http (dépendance) => facile à mocker en test.
    - On centralise la validation de la réponse (robustesse).
    """
    url = f"https://ads.example.com/campaigns/{campaign_id}/stats"
    payload = http.get_json(url)

    # Validation minimale : fail fast si API cassée ou format inattendu
    for k in ("spend", "clicks", "impressions"):
        if k not in payload:
            raise ValueError(f"API response missing key: {k}")

    return payload


# -------------------------
# 2) Base de données : interface
# -------------------------

class Database(Protocol):
    """Interface DB minimale."""
    def insert_events(self, events: List[Dict]) -> int: ...
    def count_events(self, campaign_id: str) -> int: ...


def store_events(db: Database, campaign_id: str, events: List[Dict]) -> int:
    """
    Stocke une liste d'événements en base.

    Exemples d'événements :
      {"campaign_id": "C1", "event": "click", "ts": "..."}
    """
    # Petite règle métier : impose le campaign_id sur tous les events
    normalized = []
    for e in events:
        item = dict(e)
        item["campaign_id"] = campaign_id
        normalized.append(item)

    inserted = db.insert_events(normalized)
    return inserted


# -------------------------
# 3) Stockage cloud : interface
# -------------------------

class CloudStorage(Protocol):
    """Interface d'un stockage objet (S3-like)."""
    def download_text(self, path: str) -> str: ...


def load_campaign_csv(storage: CloudStorage, path: str) -> pd.DataFrame:
    """
    Charge un CSV depuis un stockage cloud.

    En production, path pourrait être : "s3://bucket/file.csv"
    En test, on mock download_text pour retourner du contenu CSV.
    """
    csv_text = storage.download_text(path)

    # pandas peut lire un CSV depuis une string via StringIO
    from io import StringIO
    return pd.read_csv(StringIO(csv_text))
