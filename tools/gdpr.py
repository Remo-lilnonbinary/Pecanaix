"""
GDPR compliance helpers for agents: anonymise data before LLM calls, reattach identity after.
"""

from __future__ import annotations

from typing import Any

from tools.database import log_agent

_LLM_SAFE_KEYS = (
    "id",
    "graduation_year",
    "degree",
    "department",
    "location_city",
    "industry",
    "interests",
    "engagement_score",
    "past_events",
    "job_title",
    "company",
)


def anonymise_for_llm(alumni_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return a new list of dicts with name and email removed; only non-identifying fields kept."""
    return [{k: a.get(k) for k in _LLM_SAFE_KEYS} for a in alumni_list]


def reattach_identity(
    anonymised_results: list[dict[str, Any]],
    alumni_lookup: dict[Any, dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Merge each result with the full alumni record from alumni_lookup keyed by id.
    Lookup values win for identity fields; result dict overlays for LLM-specific keys.
    """
    out: list[dict[str, Any]] = []
    for r in anonymised_results:
        aid = r.get("id")
        if aid is None or aid not in alumni_lookup:
            out.append(dict(r))
            continue
        merged = {**alumni_lookup[aid], **r}
        out.append(merged)
    return out


def filter_consented(
    alumni_list: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, int]:
    """
    Keep only alumni with gdpr_consent == 1 and email_valid == 1.
    Returns (eligible_list, excluded_gdpr_count, excluded_email_count).
    A record failing both increments both exclusion counts.
    """
    eligible: list[dict[str, Any]] = []
    excluded_gdpr = 0
    excluded_email = 0
    for a in alumni_list:
        if a.get("gdpr_consent") != 1:
            excluded_gdpr += 1
        if a.get("email_valid") != 1:
            excluded_email += 1
        if a.get("gdpr_consent") == 1 and a.get("email_valid") == 1:
            eligible.append(a)
    return eligible, excluded_gdpr, excluded_email


def log_gdpr_action(campaign_id: int, action: str, detail: str) -> None:
    log_agent(
        campaign_id,
        "GDPR Compliance",
        action,
        detail,
        "Automated GDPR compliance check",
    )
