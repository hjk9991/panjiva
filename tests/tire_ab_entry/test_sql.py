import re
import sqlite3

import pytest

import scripts.tire_ab_entry.config as config_module
import scripts.tire_ab_entry.sql as sql_module
from scripts.tire_ab_entry.config import APPROVED_DATABASE, APPROVED_SCHEMA
from scripts.tire_ab_entry.schema_probe import assert_read_only_query
from scripts.tire_ab_entry.sql import (
    DescriptionIdentity,
    build_finished_sql,
    build_raw_sql,
    build_validation_sql,
    reference_review_pending_technical_eligibility,
    reference_validation_identity,
)


PARENT_IDS = {
    "MICHELIN": 101,
    "GOODYEAR": 202,
    "HANKOOK": 303,
}


def normalized(sql):
    return re.sub(r"\s+", " ", sql).strip().lower()


def test_raw_sql_preserves_supplier_and_hs():
    sql = build_raw_sql(PARENT_IDS, "2024-01-01", "2024-04-01")
    lowered = sql.lower()
    assert "shipper_up" in lowered
    assert "hs6" in lowered
    assert "shpmtorigin" in lowered
    assert "group by" in lowered
    assert "insert " not in lowered
    assert "create " not in lowered


def test_finished_sql_filters_pvlt_and_keeps_routes():
    sql = build_finished_sql(
        PARENT_IDS,
        "2024-01-01",
        "2024-04-01",
        description_column=None,
    )
    lowered = sql.lower()
    assert "401110" in lowered
    assert "401120" in lowered
    assert "manufacturer_direct" in lowered
    assert "distributor_intermediated" in lowered
    assert "null as shipment_description" in lowered


@pytest.mark.parametrize(
    "parent_ids",
    [
        {},
        {"MICHELIN": 101},
        {"MICHELIN": 101, "GOODYEAR": 202},
        {"MICHELIN": 101, "GOODYEAR": 202, "HANKOOK": 303, "OTHER": 404},
        {"MICHELIN": 101, "GOODYEAR": 101, "HANKOOK": 303},
        {"MICHELIN": 0, "GOODYEAR": 202, "HANKOOK": 303},
        {"MICHELIN": -101, "GOODYEAR": 202, "HANKOOK": 303},
        {"MICHELIN": True, "GOODYEAR": 202, "HANKOOK": 303},
        {"MICHELIN": 101.0, "GOODYEAR": 202, "HANKOOK": 303},
        {"MICHELIN": "101", "GOODYEAR": 202, "HANKOOK": 303},
        {"michelin": 101, "GOODYEAR": 202, "HANKOOK": 303},
    ],
)
def test_builders_require_exactly_three_unique_positive_integral_parent_ids(parent_ids):
    for builder in (build_raw_sql, build_finished_sql):
        with pytest.raises(ValueError, match="keyed parent mapping"):
            builder(parent_ids, "2024-01-01", "2024-04-01")


def test_parent_ids_reject_positional_sequences():
    with pytest.raises(ValueError, match="keyed parent mapping"):
        build_finished_sql([101, 202, 303], "2024-01-01", "2024-04-01")


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2024-01-01' or 1=1", "2024-04-01"),
        ("20240101", "2024-04-01"),
        ("2024-02-30", "2024-04-01"),
        ("2024-04-01", "2024-01-01"),
        ("2024-01-01", "2024-01-01"),
    ],
)
def test_builders_reject_non_iso_invalid_or_nonincreasing_dates(start, end):
    for builder in (build_raw_sql, build_finished_sql):
        with pytest.raises(ValueError, match="date"):
            builder(PARENT_IDS, start, end)


def test_generated_queries_pass_shared_read_only_guard_and_use_approved_namespace():
    prefix = f"{APPROVED_DATABASE}.{APPROVED_SCHEMA}.".lower()
    for sql in (
        build_raw_sql(PARENT_IDS, "2024-01-01", "2024-04-01"),
        build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01"),
    ):
        assert_read_only_query(sql)
        lowered = sql.lower()
        assert lowered.startswith("with ")
        assert f"from {prefix}panjivausimport i" in lowered
        assert f"from {prefix}panjivausimphscode h" in lowered
        for table in (
            "panjivacompanycrossref",
            "ciqcompanyultimateparentpit",
            "ciqcompanyultimateparent",
        ):
            assert prefix + table in lowered
        assert ";" not in sql
        assert "--" not in sql
        assert "/*" not in sql


def test_sql_is_deterministic_and_parent_keys_control_brand_mapping():
    first = build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01")
    second = build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01")
    reversed_mapping = build_finished_sql(
        dict(reversed(tuple(PARENT_IDS.items()))), "2024-01-01", "2024-04-01"
    )
    assert first == second
    assert first == reversed_mapping
    assert "(0, 101" in first
    assert "(1, 202" in first
    assert "(2, 303" in first


def test_raw_sql_rejects_frob_uses_half_open_dates_and_allocates_every_additive_measure():
    sql = normalized(build_raw_sql(PARENT_IDS, "2024-01-01", "2024-04-01"))
    assert "i.arrivaldate >= '2024-01-01'" in sql
    assert "i.arrivaldate < '2024-04-01'" in sql
    assert "i.frob is null or i.frob <> 1" in sql
    assert "1.0::double / n_hs6 as allocation_factor" in sql
    assert "sum(value_usd * allocation_factor) as value_usd" in sql
    assert "sum(weight_kg * allocation_factor) as weight_kg" in sql
    assert "sum(teu * allocation_factor) as teu" in sql
    assert "sum(container_count * allocation_factor) as container_count" in sql
    assert "sum(allocation_factor) as shipment_equivalent" in sql
    assert "count(distinct panjivarecordid) as shipment_count" in sql


@pytest.mark.parametrize("game", ["raw", "finished"])
def test_validation_sql_counts_record_level_unique_shipments_and_output_rows(game):
    sql = normalized(
        build_validation_sql(game, PARENT_IDS, "2024-01-01", "2024-04-01")
    )
    assert "count(distinct panjivarecordid) as unique_shipment_count" in sql
    assert "sum(allocation_factor) as shipment_equivalent" in sql
    assert "as output_row_count" in sql
    assert "from finalized" in sql
    assert "count(*) as unique_shipment_count" not in sql


def test_multi_hs_one_record_allocation_reconciles_to_one_unique_shipment():
    metrics = reference_validation_identity(
        [("SYN-1", 0.5), ("SYN-1", 0.5)]
    )
    assert metrics == {
        "unique_shipment_count": 1,
        "shipment_equivalent": 1.0,
        "allocation_identity_reconciled": 1,
    }


@pytest.mark.parametrize(
    "override",
    [
        {"hs_eligible": 0},
        {"manufacturer_conflict": 1},
        {"description_ambiguous": 1},
        {"importer_xref_ambiguous": 1},
        {"shipper_xref_ambiguous": 1},
        {"importer_pit_ambiguous": 1},
        {"shipper_pit_ambiguous": 1},
        {"importer_historical_backcast": 1},
        {"shipper_historical_backcast": 1},
        {"import_route": "unattributed"},
    ],
)
def test_pending_review_technical_predicate_never_releases_hard_failures(override):
    flags = {
        "hs_eligible": 1,
        "manufacturer_conflict": 0,
        "description_ambiguous": 0,
        "importer_xref_ambiguous": 0,
        "shipper_xref_ambiguous": 0,
        "importer_pit_ambiguous": 0,
        "shipper_pit_ambiguous": 0,
        "importer_historical_backcast": 0,
        "shipper_historical_backcast": 0,
        "import_route": "distributor_intermediated",
    }
    flags.update(override)
    assert reference_review_pending_technical_eligibility(**flags) == 0


def test_description_candidate_can_remain_technically_eligible_pending_review():
    assert reference_review_pending_technical_eligibility(
        hs_eligible=1,
        manufacturer_conflict=0,
        description_ambiguous=0,
        importer_xref_ambiguous=0,
        shipper_xref_ambiguous=0,
        importer_pit_ambiguous=0,
        shipper_pit_ambiguous=0,
        importer_historical_backcast=0,
        shipper_historical_backcast=0,
        import_route="distributor_intermediated",
    ) == 1
    sql = normalized(build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01"))
    finalized = sql.split("finalized as (", 1)[1].split(") select", 1)[0]
    predicate = finalized.split("as review_pending_technically_eligible", 1)[0]
    assert "description_candidate = 0" not in predicate.rsplit("iff(", 1)[1]
    assert "importer_historical_backcast = 0" in predicate


def test_hs_parsing_scope_and_review_flags_are_explicit():
    raw = normalized(build_raw_sql(PARENT_IDS, "2024-01-01", "2024-04-01"))
    classified = raw.index("classified:")
    parsed = raw.index("parsed:")
    manual = raw.index("manual:")
    assert classified < parsed < manual
    assert "count(*) over (partition by panjivarecordid) as n_hs6" in raw
    assert "listagg(distinct hs_full_code, '|')" in raw
    for prefix in ("4001", "4002", "4005", "280300", "5902", "7312", "7217", "7228"):
        assert prefix in raw
    assert "requires_review" in raw
    assert "as input_group" in raw
    assert "as hs_review_status" in raw
    assert "as hs_eligible" in raw

    finished = normalized(
        build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01")
    )
    for code in ("4011101000", "4011105000", "4011201005", "4011205010"):
        assert code in finished
    assert "broad_unreviewed" in finished
    assert "as finished_market" in finished
    assert "as hs_review_status" in finished
    assert "as hs_eligible" in finished


def test_supplier_preserving_group_contract_and_diagnostics_are_selected():
    required = (
        "manufacturer_parent_id",
        "importer_companyid",
        "importer_up",
        "shipper_panjiva_id",
        "shipper_companyid",
        "shipper_up",
        "origin_country",
        "hs6",
        "year_quarter",
        "relationship",
        "import_route",
    )
    diagnostics = (
        "manufacturer_conflict_shipment_count_nonadditive",
        "manufacturer_conflict_value_usd",
        "importer_xref_ambiguous_shipment_count_nonadditive",
        "shipper_xref_ambiguous_shipment_count_nonadditive",
        "importer_xref_unmatched_shipment_count_nonadditive",
        "importer_xref_unmatched_value_usd",
        "shipper_xref_unmatched_shipment_count_nonadditive",
        "shipper_xref_unmatched_value_usd",
        "importer_pit_ambiguous_shipment_count_nonadditive",
        "shipper_pit_ambiguous_shipment_count_nonadditive",
        "importer_current_parent_fallback_shipment_count_nonadditive",
        "shipper_current_parent_fallback_shipment_count_nonadditive",
        "description_candidate_shipment_count_nonadditive",
        "description_candidate_value_usd",
    )
    for builder in (build_raw_sql, build_finished_sql):
        sql = normalized(builder(PARENT_IDS, "2024-01-01", "2024-04-01"))
        final = sql.rsplit("select ", 1)[1]
        for column in required + diagnostics:
            assert column in final
        assert "group by manufacturer_parent_id" in final


def test_raw_attribution_uses_importer_parent_only():
    sql = normalized(build_raw_sql(PARENT_IDS, "2024-01-01", "2024-04-01"))
    attribution = sql.split("attributed as (", 1)[1].split("), routed as", 1)[0]
    assert "when importer_up in (101, 202, 303) then importer_up" in attribution
    assert "shipper_up in (101, 202, 303)" not in attribution
    assert "where importer_up in (101, 202, 303)" in attribution


def test_finished_attribution_precedence_routes_and_exclusions_are_explicit():
    sql = normalized(
        build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01")
    )
    attribution = sql.split("attributed as (", 1)[1].split("), routed as", 1)[0]
    conflict = attribution.index("importer_up in (101, 202, 303) and shipper_up in")
    importer = attribution.index("when importer_up in (101, 202, 303) then importer_up")
    shipper = attribution.index("when shipper_up in (101, 202, 303) then shipper_up")
    assert conflict < importer < shipper
    assert "manufacturer_conflict" in attribution
    assert "description_candidate" in attribution
    assert "lower(" not in sql
    assert " like " not in sql
    assert "relationship in ('self', 'parent_sub', 'sibling')" in sql
    assert "relationship = 'arms_length'" in sql
    assert re.search(
        r"upper\(trim\(shipper_country\)\) not in "
        r"\( ?'united states', 'us', 'usa' ?\)",
        sql,
    )
    assert "origin_country <> 'united states'" not in sql
    assert "as estimation_eligible" in sql


def _distributor_route_condition(sql):
    compact = re.sub(r"\s+", " ", sql).strip()
    routed = compact.split("routed as (", 1)[1].split("), finalized as", 1)[0]
    match = re.search(
        r"when (attribution_source = 'shipper_parent'.*?) "
        r"then 'distributor_intermediated'",
        routed,
    )
    assert match is not None
    return match.group(1)


@pytest.mark.parametrize("country_literal", ["null", "''", "'   '"])
def test_distributor_route_rejects_null_blank_and_whitespace_shipper_country(
    country_literal,
):
    condition = _distributor_route_condition(
        build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01")
    )
    assert "nullif(trim(shipper_country), '') is not null" in condition
    executable = (
        condition.replace("attribution_source", "'shipper_parent'")
        .replace("relationship", "'arms_length'")
        .replace("shipper_country", country_literal)
    )
    result = sqlite3.connect(":memory:").execute(
        f"select case when {executable} then 1 else 0 end"
    ).fetchone()[0]
    assert result == 0


@pytest.mark.parametrize(
    ("country_literal", "expected"),
    [("'United States'", 0), ("' Mexico '", 1)],
)
def test_distributor_route_requires_known_non_us_shipper_country(
    country_literal, expected
):
    condition = _distributor_route_condition(
        build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01")
    )
    executable = (
        condition.replace("attribution_source", "'shipper_parent'")
        .replace("relationship", "'arms_length'")
        .replace("shipper_country", country_literal)
    )
    result = sqlite3.connect(":memory:").execute(
        f"select case when {executable} then 1 else 0 end"
    ).fetchone()[0]
    assert result == expected


def test_description_identity_must_be_structured_and_match_probe_contract():
    approved = DescriptionIdentity(
        APPROVED_DATABASE,
        APPROVED_SCHEMA,
        "PANJIVAUSIMPORT",
        "GOODSDESCRIPTION",
    )
    sql = normalized(
        build_finished_sql(
            PARENT_IDS,
            "2024-01-01",
            "2024-04-01",
            description_column=approved,
        )
    )
    assert "i.goodsdescription as shipment_description" in sql
    assert "lower(o.shipment_description) like" in sql
    assert "coalesce(o.importer_up, -1) not in (101, 202, 303)" in sql
    assert "coalesce(o.shipper_up, -1) not in (101, 202, 303)" in sql
    assert "as description_candidate" in sql
    assert re.search(
        r"iff\( ?count\(distinct o\.manufacturer_parent_id\) = 1, "
        r"min\(o\.manufacturer_parent_id\), null ?\) "
        r"as description_candidate_parent_id",
        sql,
    )
    assert "dm.description_match_count > 1 or dm.description_alias_count > 1" in sql

    invalid = (
        "GOODSDESCRIPTION",
        (APPROVED_DATABASE, APPROVED_SCHEMA, "PANJIVAUSIMPORT", "GOODSDESCRIPTION"),
        DescriptionIdentity(
            APPROVED_DATABASE, APPROVED_SCHEMA, "PANJIVAUSIMPORT", "X; DROP TABLE Y"
        ),
        DescriptionIdentity(
            "OTHER_DATABASE", APPROVED_SCHEMA, "PANJIVAUSIMPORT", "GOODSDESCRIPTION"
        ),
        DescriptionIdentity(
            APPROVED_DATABASE, "OTHER_SCHEMA", "PANJIVAUSIMPORT", "GOODSDESCRIPTION"
        ),
        DescriptionIdentity(
            APPROVED_DATABASE, APPROVED_SCHEMA, "PANJIVAUSIMPOTHER", "GOODSDESCRIPTION"
        ),
        DescriptionIdentity(
            APPROVED_DATABASE, APPROVED_SCHEMA, "PANJIVAUSIMPORT", "UNKNOWNCOLUMN"
        ),
    )
    for identity in invalid:
        with pytest.raises(ValueError, match="description"):
            build_finished_sql(
                PARENT_IDS,
                "2024-01-01",
                "2024-04-01",
                description_column=identity,
            )


def test_description_unavailable_never_text_matches_and_emits_null():
    sql = normalized(
        build_finished_sql(
            PARENT_IDS,
            "2024-01-01",
            "2024-04-01",
            description_column=None,
        )
    )
    assert "null as shipment_description" in sql
    assert "description_matches as" not in sql
    assert "lower(" not in sql
    assert " like " not in sql
    assert "null as description_candidate_parent_id" in sql
    assert "0 as description_match_count" in sql
    assert "0 as description_ambiguous" in sql


def test_description_diagnostics_are_preserved_as_final_group_keys():
    for description in (
        None,
        DescriptionIdentity(
            APPROVED_DATABASE,
            APPROVED_SCHEMA,
            "PANJIVAUSIMPORT",
            "GOODSDESCRIPTION",
        ),
    ):
        sql = normalized(
            build_finished_sql(
                PARENT_IDS,
                "2024-01-01",
                "2024-04-01",
                description_column=description,
            )
        )
        final = sql.rsplit("select ", 1)[1]
        group_by = final.split("group by ", 1)[1]
        for column in (
            "description_candidate_parent_id",
            "description_match_count",
            "description_ambiguous",
        ):
            assert column in final
            assert column in group_by


def test_reviewed_description_aliases_are_keyed_and_used_in_sql():
    aliases = config_module.MANUFACTURER_DESCRIPTION_ALIASES
    assert tuple(aliases) == ("MICHELIN", "GOODYEAR", "HANKOOK")
    assert "hankook" in aliases["HANKOOK"]
    assert all(alias == alias.strip().lower() for group in aliases.values() for alias in group)
    sql = normalized(
        build_finished_sql(
            PARENT_IDS,
            "2024-01-01",
            "2024-04-01",
            description_column=DescriptionIdentity(
                APPROVED_DATABASE,
                APPROVED_SCHEMA,
                "PANJIVAUSIMPORT",
                "GOODSDESCRIPTION",
            ),
        )
    )
    assert "description_alias" in sql
    assert "description_matched_alias" in sql
    assert "description_alias_count" in sql


def test_distinct_candidate_reference_resolution_fails_closed():
    assert sql_module.resolve_distinct_candidate([901, 901]) == {
        "candidate": 901,
        "distinct_candidate_count": 1,
        "ambiguous": 0,
    }


def test_importer_and_shipper_pit_candidates_resolve_independently():
    result = sql_module.resolve_ownership_sides(
        importer_parent_candidates=[701, 701],
        shipper_parent_candidates=[801, 802, 801],
    )
    assert result["importer"] == {
        "candidate": 701,
        "distinct_candidate_count": 1,
        "ambiguous": 0,
    }
    assert result["shipper"] == {
        "candidate": None,
        "distinct_candidate_count": 2,
        "ambiguous": 1,
    }


def test_pit_interval_reference_distinguishes_duplicates_from_same_parent_overlap():
    exact_duplicate = sql_module.resolve_pit_intervals(
        [(701, "2020-01-01", "2020-12-31"), (701, "2020-01-01", "2020-12-31")]
    )
    assert exact_duplicate == {
        "candidate": 701,
        "distinct_interval_match_count": 1,
        "distinct_parent_candidate_count": 1,
        "ambiguous": 0,
        "same_parent_overlap": 0,
    }

    same_parent_overlap = sql_module.resolve_pit_intervals(
        [(701, "2020-01-01", "2020-12-31"), (701, "2020-06-01", "2021-05-31")]
    )
    assert same_parent_overlap == {
        "candidate": 701,
        "distinct_interval_match_count": 2,
        "distinct_parent_candidate_count": 1,
        "ambiguous": 0,
        "same_parent_overlap": 1,
    }

    distinct_parent_conflict = sql_module.resolve_pit_intervals(
        [(701, "2020-01-01", "2020-12-31"), (702, "2020-06-01", "2021-05-31")]
    )
    assert distinct_parent_conflict["candidate"] is None
    assert distinct_parent_conflict["ambiguous"] == 1
    assert distinct_parent_conflict["same_parent_overlap"] == 0
    assert sql_module.resolve_distinct_candidate([901, 902, 901]) == {
        "candidate": None,
        "distinct_candidate_count": 2,
        "ambiguous": 1,
    }
    assert sql_module.resolve_distinct_candidate([]) == {
        "candidate": None,
        "distinct_candidate_count": 0,
        "ambiguous": 0,
    }


def test_reference_hs6_allocation_deduplicates_and_flags_mixed_codes():
    rows = sql_module.reference_hs6_allocation(
        ["4011101000", "4011101000", "4011109999", "4011201005"],
        value_usd=120.0,
    )
    assert rows == {
        "401110": {
            "distinct_full_code_count": 2,
            "reviewed_full_code_count": 1,
            "unreviewed_full_code_count": 1,
            "mixed_review": 1,
            "allocation_factor": 0.5,
            "allocated_value_usd": 60.0,
            "hs_eligible": 0,
        },
        "401120": {
            "distinct_full_code_count": 1,
            "reviewed_full_code_count": 1,
            "unreviewed_full_code_count": 0,
            "mixed_review": 0,
            "allocation_factor": 0.5,
            "allocated_value_usd": 60.0,
            "hs_eligible": 1,
        },
    }
    assert sum(row["allocation_factor"] for row in rows.values()) == 1.0
    assert sum(row["allocated_value_usd"] for row in rows.values()) == 120.0


def test_raw_external_supplier_is_direct_and_eligible_reference_contract():
    result = sql_module.reference_raw_route(
        importer_is_reviewed=True,
        importer_up=101,
        shipper_up=909,
        relationship="arms_length",
        identity_ambiguous=False,
        historical_backcast=False,
        hs_eligible=True,
    )
    assert result == {
        "import_route": "manufacturer_direct",
        "supplier_relationship": "external_supplier",
        "estimation_eligible": 1,
        "sensitivity_eligible": 1,
    }


def test_current_parent_fallback_is_sensitivity_only_reference_contract():
    result = sql_module.reference_raw_route(
        importer_is_reviewed=True,
        importer_up=101,
        shipper_up=909,
        relationship="arms_length",
        identity_ambiguous=False,
        historical_backcast=True,
        hs_eligible=True,
    )
    assert result["import_route"] == "manufacturer_direct"
    assert result["estimation_eligible"] == 0
    assert result["sensitivity_eligible"] == 1


def test_raw_sql_keeps_external_suppliers_in_main_estimation():
    sql = normalized(build_raw_sql(PARENT_IDS, "2024-01-01", "2024-04-01"))
    attribution = sql.split("attributed as (", 1)[1].split("), routed as", 1)[0]
    raw_route = sql.split("routed as (", 1)[1].split("), finalized as", 1)[0]
    assert "then 'manufacturer_direct'" in raw_route
    assert "intragroup_supplier" in raw_route
    assert "external_supplier" in raw_route
    assert "unknown_supplier" in raw_route
    assert "or importer_xref_ambiguous = 1" in attribution
    assert "or importer_pit_ambiguous = 1" in attribution
    assert "supplier_relationship" in sql.rsplit("select ", 1)[1]


def test_xref_and_pit_resolution_use_distinct_candidates_without_arbitrary_choice():
    sql = normalized(build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01"))
    assert "xref_distinct as" in sql
    assert "select distinct identifiervalue, companyid" in sql
    assert "xref_resolved as" in sql
    assert "count(*) as distinct_company_candidate_count" in sql
    assert "importer_pit_interval_matches as" in sql
    assert "shipper_pit_interval_matches as" in sql
    assert "importer_pit_resolved as" in sql
    assert "shipper_pit_resolved as" in sql
    assert "distinct_parent_candidate_count" in sql
    assert "ownership_join_rows" not in sql
    assert "row_number() over ( partition by identifiervalue" not in sql


def test_pit_same_parent_interval_overlap_is_preserved_without_exclusion():
    sql = normalized(build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01"))
    assert "importer_pit_interval_matches as" in sql
    assert "shipper_pit_interval_matches as" in sql
    assert "select distinct i.panjivarecordid, p.ultimateparentcompanyid, p.startdate, p.enddate" in sql
    for side in ("importer", "shipper"):
        assert f"{side}_pit_distinct_interval_match_count" in sql
        assert f"{side}_pit_same_parent_overlap" in sql
        assert f"{side}_pit_same_parent_overlap_shipment_equivalent" in sql.rsplit(
            "select ", 1
        )[1]
    finalized = sql.split("finalized as (", 1)[1].split(") select", 1)[0]
    assert "pit_same_parent_overlap = 0" not in finalized


def test_identity_ambiguities_and_backcasts_are_explicit_and_fail_main_eligibility():
    sql = normalized(build_finished_sql(PARENT_IDS, "2014-01-01", "2014-04-01"))
    for column in (
        "importer_xref_ambiguous",
        "shipper_xref_ambiguous",
        "importer_pit_ambiguous",
        "shipper_pit_ambiguous",
        "importer_ownership_source",
        "shipper_ownership_source",
        "importer_historical_backcast",
        "shipper_historical_backcast",
        "sensitivity_eligible",
    ):
        assert column in sql
    finalized = sql.split("finalized as (", 1)[1].split(") select", 1)[0]
    for exclusion in (
        "importer_xref_ambiguous = 0",
        "shipper_xref_ambiguous = 0",
        "importer_pit_ambiguous = 0",
        "shipper_pit_ambiguous = 0",
        "importer_historical_backcast = 0",
        "shipper_historical_backcast = 0",
    ):
        assert exclusion in finalized


def test_hs_full_code_diagnostics_and_all_reviewed_rule_are_in_sql():
    sql = normalized(build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01"))
    for column in (
        "distinct_full_code_count",
        "reviewed_full_code_count",
        "unreviewed_full_code_count",
        "mixed_review",
    ):
        assert column in sql.rsplit("select ", 1)[1]
    assert "reviewed_full_code_count = distinct_full_code_count" in sql
    assert "reviewed_full_code_count > 0 and unreviewed_full_code_count > 0" in sql
    assert "mixed_review" in sql


def test_grouped_counts_are_labeled_nonadditive_and_flags_have_equivalents():
    sql = normalized(build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01"))
    final = sql.rsplit("select ", 1)[1]
    assert "shipment_count_nonadditive" in final
    assert "shipment_count_compatibility_nonadditive" in final
    for diagnostic in (
        "manufacturer_conflict",
        "importer_xref_unmatched",
        "shipper_xref_unmatched",
        "importer_xref_ambiguous",
        "shipper_xref_ambiguous",
        "importer_pit_ambiguous",
        "shipper_pit_ambiguous",
        "importer_historical_backcast",
        "shipper_historical_backcast",
        "description_candidate",
        "description_ambiguous",
        "unattributed",
        "hs_review",
        "main_ineligible",
    ):
        assert f"{diagnostic}_shipment_equivalent" in final
