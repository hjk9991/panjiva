import re
import sqlite3

import pytest

from scripts.tire_ab_entry.config import APPROVED_DATABASE, APPROVED_SCHEMA
from scripts.tire_ab_entry.schema_probe import assert_read_only_query
from scripts.tire_ab_entry.sql import (
    DescriptionIdentity,
    build_finished_sql,
    build_raw_sql,
)


PARENT_IDS = [101, 202, 303]


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
        [],
        [101],
        [101, 202],
        [101, 202, 303, 404],
        [101, 101, 303],
        [0, 202, 303],
        [-101, 202, 303],
        [True, 202, 303],
        [101.0, 202, 303],
        ["101", 202, 303],
    ],
)
def test_builders_require_exactly_three_unique_positive_integral_parent_ids(parent_ids):
    for builder in (build_raw_sql, build_finished_sql):
        with pytest.raises(ValueError, match="three unique positive integral"):
            builder(parent_ids, "2024-01-01", "2024-04-01")


def test_parent_ids_reject_unordered_iterables_for_deterministic_brand_mapping():
    with pytest.raises(ValueError, match="ordered"):
        build_finished_sql({101, 202, 303}, "2024-01-01", "2024-04-01")


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


def test_sql_is_deterministic_and_parent_order_is_preserved_for_review_mapping():
    first = build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01")
    second = build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01")
    reversed_ids = build_finished_sql(
        list(reversed(PARENT_IDS)), "2024-01-01", "2024-04-01"
    )
    assert first == second
    assert first != reversed_ids
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
        "manufacturer_conflict_shipment_count",
        "manufacturer_conflict_value_usd",
        "importer_xref_conflict_shipment_count",
        "shipper_xref_conflict_shipment_count",
        "importer_xref_unmatched_shipment_count",
        "importer_xref_unmatched_value_usd",
        "shipper_xref_unmatched_shipment_count",
        "shipper_xref_unmatched_value_usd",
        "pit_overlap_shipment_count",
        "importer_current_parent_fallback_shipment_count",
        "shipper_current_parent_fallback_shipment_count",
        "description_review_shipment_count",
        "description_review_value_usd",
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
        r"iff\( ?count\(distinct rm\.manufacturer_parent_id\) = 1, "
        r"min\(rm\.manufacturer_parent_id\), null ?\) "
        r"as description_candidate_parent_id",
        sql,
    )
    assert "iff(dm.description_match_count > 1, 1, 0) as description_ambiguous" in sql

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
