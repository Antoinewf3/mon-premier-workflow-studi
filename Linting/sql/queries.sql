-- sql/queries.sql
-- Exemples de requêtes "pas optimales" pour la démo

SELECT * FROM events;  -- souvent mauvais : scan complet, colonnes inutiles

SELECT
  *
FROM events
WHERE event_time >= '2026-01-01'; -- * + filtre partiel

SELECT
  e.*
FROM events e
JOIN campaigns c ON e.campaign_id = c.id; -- * sur join => coûts + ambiguïtés
