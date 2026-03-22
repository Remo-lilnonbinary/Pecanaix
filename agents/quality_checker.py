"""
Pre-outreach Quality Checker: deterministic rules plus a Kimi K2 summary via Groq.
"""

from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from typing import Any

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage

from tools.database import get_agent_log, get_connection, log_agent
from tools.gdpr import anonymise_for_llm
from tools.llm_router import get_llm

load_dotenv()

_AGENT = "Quality Checker"
# Must match `agent_name` values logged by outreach and personalisation agents.
_OUTREACH_LOG_AGENT_NAMES = (
    "Outreach Agent (Loop 2)",
    "Personalisation Agent (Loop 2)",
)


def _extract_alumni_ids_from_text(text: str | None) -> list[int]:
    if not text:
        return []
    out: list[int] = []
    for pattern in (
        r'["\']?alumni_id["\']?\s*[:=]\s*(\d+)',
        r'["\']?id["\']?\s*:\s*(\d+)',
        r"\bID:\s*(\d+)\b",
    ):
        for m in re.finditer(pattern, text, flags=re.IGNORECASE):
            try:
                out.append(int(m.group(1)))
            except (TypeError, ValueError):
                continue
    return out


def _recent_outreach_alumni_ids_from_agent_log() -> set[int]:
    """IDs mentioned in outreach-related agent_log rows from the last 30 days (all campaigns)."""
    ids: set[int] = set()
    conn = get_connection()
    try:
        in_placeholders = ",".join("?" * len(_OUTREACH_LOG_AGENT_NAMES))
        rows = conn.execute(
            f"""
            SELECT decision, reasoning, agent_name, action_type
            FROM agent_log
            WHERE datetime(timestamp) >= datetime('now', '-30 days')
            AND (
                agent_name IN ({in_placeholders})
                OR UPPER(IFNULL(action_type, '')) IN ('SEND', 'DRAFT', 'SEND_WAVE')
            )
            """,
            _OUTREACH_LOG_AGENT_NAMES,
        ).fetchall()
    finally:
        conn.close()

    for row in rows:
        blob = f"{row['decision'] or ''}\n{row['reasoning'] or ''}"
        for aid in _extract_alumni_ids_from_text(blob):
            ids.add(aid)
    return ids


def _log_flags(campaign_id: int, flags: list[dict[str, Any]]) -> None:
    for f in flags:
        try:
            log_agent(
                campaign_id,
                _AGENT,
                str(f.get("type", "unknown")),
                str(f.get("severity", "warning")),
                json.dumps(
                    {
                        "detail": f.get("detail", ""),
                        "alumni_ids": f.get("alumni_ids", []),
                    },
                    ensure_ascii=False,
                ),
            )
        except Exception:
            pass


def run_quality_check(
    campaign_id: int,
    selected_alumni: list[dict[str, Any]],
) -> dict[str, Any]:
    flags: list[dict[str, Any]] = []

    # --- Duplicate detection (email) ---
    email_to_ids: dict[str, list[int]] = defaultdict(list)
    for a in selected_alumni:
        aid = a.get("id")
        if aid is None:
            continue
        em = (a.get("email") or "").strip().lower()
        if em:
            email_to_ids[em].append(int(aid))
    for em, ids in email_to_ids.items():
        uniq = sorted(set(ids))
        duplicate_rows = len(ids) != len(set(ids))
        multiple_people = len(uniq) > 1
        if duplicate_rows or multiple_people:
            flags.append(
                {
                    "type": "duplicate_email",
                    "severity": "critical",
                    "detail": (
                        f"Duplicate selection for email {em!r}: "
                        f"{'repeated rows' if duplicate_rows else ''}"
                        f"{'; ' if duplicate_rows and multiple_people else ''}"
                        f"{'multiple distinct alumni IDs' if multiple_people else ''}."
                    ),
                    "alumni_ids": uniq,
                }
            )

    # --- Over-representation by company ---
    company_counts: dict[str, list[int]] = defaultdict(list)
    for a in selected_alumni:
        aid = a.get("id")
        if aid is None:
            continue
        co = (a.get("company") or "").strip() or "Unknown"
        company_counts[co].append(int(aid))
    for company, ids in company_counts.items():
        if len(ids) > 5:
            flags.append(
                {
                    "type": "company_over_represented",
                    "severity": "warning",
                    "detail": f"Company {company!r} has {len(ids)} alumni selected (>5).",
                    "alumni_ids": sorted(set(ids)),
                }
            )

    # --- Low-confidence matches ---
    low_ids: list[int] = []
    for a in selected_alumni:
        aid = a.get("id")
        if aid is None:
            continue
        raw = a.get("match_score")
        if raw is None:
            continue
        try:
            score = float(raw)
        except (TypeError, ValueError):
            continue
        if score < 40:
            low_ids.append(int(aid))
    if low_ids:
        flags.append(
            {
                "type": "low_confidence_match",
                "severity": "warning",
                "detail": f"{len(low_ids)} alumni have match_score below 40.",
                "alumni_ids": sorted(set(low_ids)),
            }
        )

    # --- GDPR re-verify (database source of truth) ---
    id_list = [int(a["id"]) for a in selected_alumni if a.get("id") is not None]
    gdpr_bad: list[int] = []
    if id_list:
        conn = get_connection()
        try:
            ph = ",".join("?" * len(id_list))
            rows = conn.execute(
                f"SELECT id, gdpr_consent FROM alumni WHERE id IN ({ph})",
                id_list,
            ).fetchall()
            have = {int(r["id"]) for r in rows}
            for r in rows:
                if r["gdpr_consent"] != 1:
                    gdpr_bad.append(int(r["id"]))
            for aid in id_list:
                if aid not in have:
                    gdpr_bad.append(aid)
        finally:
            conn.close()
    if gdpr_bad:
        flags.append(
            {
                "type": "gdpr_consent_revoked_or_missing",
                "severity": "critical",
                "detail": f"{len(set(gdpr_bad))} selected alumni do not have gdpr_consent=1 in the database.",
                "alumni_ids": sorted(set(gdpr_bad)),
            }
        )

    # --- Recent contact (agent_log, outreach-related, last 30 days) ---
    recent_ids = _recent_outreach_alumni_ids_from_agent_log()
    selected_ids = {int(a["id"]) for a in selected_alumni if a.get("id") is not None}
    overlap = sorted(selected_ids & recent_ids)
    if overlap:
        flags.append(
            {
                "type": "recent_outreach_contact",
                "severity": "warning",
                "detail": (
                    f"{len(overlap)} selected alumni appear in outreach-related agent_log "
                    f"entries from the last 30 days (outreach fatigue risk)."
                ),
                "alumni_ids": overlap,
            }
        )

    critical_count = sum(1 for f in flags if f.get("severity") == "critical")
    warning_count = sum(1 for f in flags if f.get("severity") == "warning")
    passed = critical_count == 0

    _log_flags(campaign_id, flags)

    flags_for_llm = [
        {k: v for k, v in f.items() if k in ("type", "severity", "detail", "alumni_ids")}
        for f in flags
    ]
    anonymised = anonymise_for_llm(selected_alumni)
    try:
        campaign_log = get_agent_log(campaign_id)
    except Exception:
        campaign_log = []
    summary_payload = {
        "campaign_id": campaign_id,
        "pool_size": len(selected_alumni),
        "passed": passed,
        "critical_count": critical_count,
        "warning_count": warning_count,
        "flags": flags_for_llm,
        "anonymised_pool": anonymised,
        "campaign_agent_log_entry_count": len(campaign_log),
    }

    assessment = "LLM assessment unavailable"
    try:
        llm = get_llm()
        if os.getenv("GROQ_API_KEY"):
            prompt = (
                "You are reviewing a pre-outreach alumni selection for a university campaign.\n\n"
                "Here is a JSON summary of automated quality checks (flags may be empty):\n"
                f"{json.dumps(summary_payload, indent=2, ensure_ascii=False)}\n\n"
                "Write one paragraph: a concise quality assessment for the team based only on these flags "
                "and counts. Do not invent alumni details beyond the anonymised fields."
            )
            resp = llm.invoke([HumanMessage(content=prompt)])
            assessment = (
                resp.content.strip()
                if hasattr(resp, "content") and resp.content
                else str(resp)
            )
    except Exception:
        assessment = "LLM assessment unavailable"

    return {
        "passed": passed,
        "flags": flags,
        "critical_count": critical_count,
        "warning_count": warning_count,
        "assessment": assessment,
    }
