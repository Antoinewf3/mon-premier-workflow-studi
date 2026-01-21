# test_services.py
"""
Tests isolés = pas de réseau, pas de vraie base, pas de cloud.

On montre 3 techniques :
- Stub simple (classe fake)
- Mock (unittest.mock) avec assertions sur appels
- Simulation d'erreurs (timeouts / réponses invalides)
"""

import pytest
import pandas as pd
from unittest.mock import Mock

from services import (
    fetch_campaign_stats,
    store_events,
    load_campaign_csv,
)


# -------------------------
# A) Mocking API HTTP
# -------------------------

class FakeHttpOk:
    """Stub (fake) : retourne toujours une réponse valide."""
    def get_json(self, url: str):
        return {"spend": 10.0, "clicks": 5, "impressions": 100}


class FakeHttpBad:
    """Stub : réponse invalide (clé manquante) pour tester le fail fast."""
    def get_json(self, url: str):
        return {"spend": 10.0, "clicks": 5}  # impressions manquant


def test_fetch_campaign_stats_ok_with_stub():
    http = FakeHttpOk()

    stats = fetch_campaign_stats(http, "C1")

    assert stats["spend"] == 10.0
    assert stats["clicks"] == 5
    assert stats["impressions"] == 100


def test_fetch_campaign_stats_fail_fast_on_invalid_api_payload():
    http = FakeHttpBad()

    with pytest.raises(ValueError) as err:
        fetch_campaign_stats(http, "C1")

    assert "missing key" in str(err.value).lower()


def test_fetch_campaign_stats_with_mock_and_call_assertion():
    # Mock permet de vérifier que l'appel a bien été fait avec la bonne URL
    http = Mock()
    http.get_json.return_value = {"spend": 1.0, "clicks": 1, "impressions": 10}

    stats = fetch_campaign_stats(http, "C99")

    http.get_json.assert_called_once_with("https://ads.example.com/campaigns/C99/stats")
    assert stats["spend"] == 1.0


# -------------------------
# B) Mocking base de données
# -------------------------

def test_store_events_inserts_normalized_events():
    db = Mock()
    db.insert_events.return_value = 3

    events = [
        {"event": "click", "ts": "2026-01-06T10:00:00Z"},
        {"event": "view", "ts": "2026-01-06T10:01:00Z"},
        {"event": "conversion", "ts": "2026-01-06T10:05:00Z"},
    ]

    inserted = store_events(db, "C1", events)

    # On a bien inséré 3 lignes
    assert inserted == 3

    # On vérifie que store_events a injecté campaign_id sur chaque event
    args, kwargs = db.insert_events.call_args
    normalized_events = args[0]
    assert all(e["campaign_id"] == "C1" for e in normalized_events)


def test_store_events_does_not_touch_input_list():
    db = Mock()
    db.insert_events.return_value = 1

    events = [{"event": "click"}]
    store_events(db, "C1", events)

    # L'entrée ne doit pas être modifiée (bonne pratique)
    assert "campaign_id" not in events[0]


# -------------------------
# C) Mocking stockage cloud (CSV)
# -------------------------

def test_load_campaign_csv_from_cloud_storage():
    storage = Mock()

    # On simule un CSV "téléchargé" depuis le cloud
    storage.download_text.return_value = (
        "campaign_id,spend,clicks\n"
        "C1,10,5\n"
        "C2,0,0\n"
    )

    df = load_campaign_csv(storage, "s3://bucket/campaigns.csv")

    storage.download_text.assert_called_once_with("s3://bucket/campaigns.csv")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["campaign_id", "spend", "clicks"]
    assert len(df) == 2
    assert df.loc[0, "campaign_id"] == "C1"
