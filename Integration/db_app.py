# db_app.py

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, List, Tuple, Dict, Any, Optional


# -------------------------
# Migration de schéma (très simple)
# -------------------------

MIGRATIONS = [
    # v1 : table d'événements marketing
    """
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id TEXT NOT NULL,
        event_type TEXT NOT NULL,           -- e.g. click, impression, conversion
        event_time TEXT NOT NULL,           -- ISO string pour simplifier l'exemple
        cost REAL NOT NULL DEFAULT 0,
        user_id TEXT,
        -- Contrainte d'unicité : empêche de réinsérer deux fois le même event (ex)
        UNIQUE(campaign_id, event_type, event_time, user_id)
    );
    """,
    # v2 : index pour accélérer requêtes analytiques
    """
    CREATE INDEX IF NOT EXISTS idx_events_campaign_time
    ON events(campaign_id, event_time);
    """,
]


def connect(db_url: str) -> sqlite3.Connection:
    """
    Crée une connexion SQLite.
    - db_url = ':memory:' => base en RAM (ultra rapide)
    - db_url = 'file:...' => base fichier (persistante)
    """
    return sqlite3.connect(db_url)


def run_migrations(conn: sqlite3.Connection) -> None:
    """
    Applique les migrations de schéma.
    En réel, vous utiliseriez Alembic / Flyway / Liquibase, etc.
    """
    cur = conn.cursor()
    for sql in MIGRATIONS:
        cur.execute(sql)
    conn.commit()


# -------------------------
# Fonctions "repository" testables
# -------------------------

def insert_events(conn: sqlite3.Connection, events: List[Dict[str, Any]]) -> int:
    """
    Insère des événements.
    Retourne le nombre de lignes effectivement insérées.
    - Utilise INSERT OR IGNORE pour éviter l'échec sur doublons (via UNIQUE)
    """
    cur = conn.cursor()

    sql = """
    INSERT OR IGNORE INTO events (campaign_id, event_type, event_time, cost, user_id)
    VALUES (?, ?, ?, ?, ?)
    """

    rows = 0
    for e in events:
        cur.execute(
            sql,
            (
                e["campaign_id"],
                e["event_type"],
                e["event_time"],
                float(e.get("cost", 0)),
                e.get("user_id"),
            ),
        )
        # rowcount = 1 si insert, 0 si ignore (doublon)
        rows += cur.rowcount

    conn.commit()
    return rows


def count_events(conn: sqlite3.Connection, campaign_id: str) -> int:
    """Compte le nombre d'événements pour une campagne."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM events WHERE campaign_id = ?", (campaign_id,))
    return int(cur.fetchone()[0])


def get_kpis_by_campaign(conn: sqlite3.Connection) -> List[Tuple[str, int, int, float]]:
    """
    Requête analytique simple :
    - impressions = nombre d'events 'impression'
    - clicks = nombre d'events 'click'
    - spend = somme des coûts

    Retour : liste (campaign_id, impressions, clicks, spend)
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          campaign_id,
          SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END) AS impressions,
          SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END) AS clicks,
          SUM(cost) AS spend
        FROM events
        GROUP BY campaign_id
        ORDER BY campaign_id
        """
    )
    return [(r[0], int(r[1]), int(r[2]), float(r[3] or 0.0)) for r in cur.fetchall()]
