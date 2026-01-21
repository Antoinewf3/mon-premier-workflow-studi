import pandas as pd
import pytest

from transforms import transform_campaign_data


# ---- Fixture : dataset petit mais réaliste ----
@pytest.fixture
def campaign_df():
    return pd.DataFrame(
        [
            # Cas "normal"
            {"campaign_id": "C1", "source": "Google_Ads", "spend": "10.0", "clicks": "5", "impressions": "100"},
            # Cas avec espaces + valeur manquante spend
            {"campaign_id": "C2", "source": " Facebook ", "spend": None, "clicks": "0", "impressions": "200"},
            # Cas impressions = 0 (division par zéro CTR)
            {"campaign_id": "C3", "source": "Unknown", "spend": "7.5", "clicks": "3", "impressions": 0},
            # Cas clicks non numérique (doit devenir 0)
            {"campaign_id": "C4", "source": "newsletter", "spend": "2", "clicks": "not_a_number", "impressions": "50"},
        ]
    )


def test_fail_fast_if_missing_columns(campaign_df):
    # On supprime une colonne obligatoire pour provoquer l'erreur
    bad = campaign_df.drop(columns=["spend"])

    with pytest.raises(ValueError) as err:
        transform_campaign_data(bad)

    assert "Missing required columns" in str(err.value)


def test_adds_expected_columns(campaign_df):
    out = transform_campaign_data(campaign_df)

    # La transformation doit ajouter les colonnes calculées
    assert "channel" in out.columns
    assert "ctr" in out.columns
    assert "cpc" in out.columns


def test_casts_metrics_to_numeric_and_fills_na(campaign_df):
    out = transform_campaign_data(campaign_df)

    # spend/clicks/impressions doivent être numériques après transformation
    assert pd.api.types.is_numeric_dtype(out["spend"])
    assert pd.api.types.is_numeric_dtype(out["clicks"])
    assert pd.api.types.is_numeric_dtype(out["impressions"])

    # Ligne C2 : spend None => 0
    spend_c2 = out.loc[out["campaign_id"] == "C2", "spend"].iloc[0]
    assert spend_c2 == 0

    # Ligne C4 : clicks "not_a_number" => 0
    clicks_c4 = out.loc[out["campaign_id"] == "C4", "clicks"].iloc[0]
    assert clicks_c4 == 0


def test_channel_mapping_rules(campaign_df):
    out = transform_campaign_data(campaign_df)

    # Google_Ads => search
    ch_c1 = out.loc[out["campaign_id"] == "C1", "channel"].iloc[0]
    assert ch_c1 == "search"

    # " Facebook " (espaces) => social
    ch_c2 = out.loc[out["campaign_id"] == "C2", "channel"].iloc[0]
    assert ch_c2 == "social"

    # Unknown => other
    ch_c3 = out.loc[out["campaign_id"] == "C3", "channel"].iloc[0]
    assert ch_c3 == "other"


def test_kpi_calculations_are_robust(campaign_df):
    out = transform_campaign_data(campaign_df)

    # C1 : CTR = 5 / 100 = 0.05 ; CPC = 10 / 5 = 2
    row_c1 = out[out["campaign_id"] == "C1"].iloc[0]
    assert row_c1["ctr"] == 0.05
    assert row_c1["cpc"] == 2

    # C2 : clicks=0 => CPC = NaN (non défini), CTR = 0/200 = 0
    row_c2 = out[out["campaign_id"] == "C2"].iloc[0]
    assert row_c2["ctr"] == 0
    assert pd.isna(row_c2["cpc"])

    # C3 : impressions=0 => CTR = 0 (choix métier), CPC = 7.5/3 = 2.5
    row_c3 = out[out["campaign_id"] == "C3"].iloc[0]
    assert row_c3["ctr"] == 0
    assert row_c3["cpc"] == 2.5
