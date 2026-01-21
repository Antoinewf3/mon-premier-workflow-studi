# test_db_integration.py
"""
Tests d'intégration DB (SQLite embarqué).

- migrations OK (table existante)
- insert + contraintes (doublons)
- transactions (commit implicite ici)
- requêtes analytiques (KPIs)
- isolation : chaque test a sa base en mémoire
"""

import pytest
import sqlite3

from db_app import connect, run_migrations, insert_events, count_events, get_kpis_by_campaign


# ---- Fixture : une base SQLite en mémoire par test ----
@pytest.fixture
def conn():
    c = connect(":memory:")
    run_migrations(c)
    yield c
    c.close()


def test_migrations_create_table(conn):
    # On vérifie que la table 'events' existe réellement
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'")
    assert cur.fetchone() is not None


def test_insert_events_and_count(conn):
    events = [
        {"campaign_id": "C1", "event_type": "impression", "event_time": "2026-01-06T10:00:00Z", "cost": 0, "user_id": "U1"},
        {"campaign_id": "C1", "event_type": "click",      "event_time": "2026-01-06T10:01:00Z", "cost": 0.2, "user_id": "U1"},
        {"campaign_id": "C1", "event_type": "click",      "event_time": "2026-01-06T10:02:00Z", "cost": 0.2, "user_id": "U2"},
    ]

    inserted = insert_events(conn, events)
    assert inserted == 3
    assert count_events(conn, "C1") == 3


def test_unique_constraint_prevents_duplicates(conn):
    # Même event => doit être ignoré grâce à UNIQUE + INSERT OR IGNORE
    e = {"campaign_id": "C1", "event_type": "click", "event_time": "2026-01-06T10:01:00Z", "cost": 0.2, "user_id": "U1"}

    inserted1 = insert_events(conn, [e])
    inserted2 = insert_events(conn, [e])  # doublon

    assert inserted1 == 1
    assert inserted2 == 0
    assert count_events(conn, "C1") == 1


def test_kpi_query_returns_expected_aggregates(conn):
    events = [
        {"campaign_id": "C1", "event_type": "impression", "event_time": "2026-01-06T10:00:00Z", "cost": 0.0, "user_id": "U1"},
        {"campaign_id": "C1", "event_type": "impression", "event_time": "2026-01-06T10:00:10Z", "cost": 0.0, "user_id": "U2"},
        {"campaign_id": "C1", "event_type": "click",      "event_time": "2026-01-06T10:01:00Z", "cost": 0.3, "user_id": "U1"},
        {"campaign_id": "C2", "event_type": "click",      "event_time": "2026-01-06T11:00:00Z", "cost": 0.5, "user_id": "U9"},
    ]

    insert_events(conn, events)

    kpis = get_kpis_by_campaign(conn)

    # Format : (campaign_id, impressions, clicks, spend)
    assert kpis == [
        ("C1", 2, 1, 0.3),
        ("C2", 0, 1, 0.5),
    ]
