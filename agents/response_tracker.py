"""
Loop 2 — Response Tracker: simulate opens/accepts, update DB, optional LLM diagnosis for next wave.
"""

from __future__ import annotations

import json
import os
import random
import sys
from datetime import datetime
from typing import Any, Optional

from dotenv import load_dotenv

_parent = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _parent not in sys.path:
    sys.path.append(_parent)

from tools.database import get_connection, log_agent, save_warm_lead  # noqa: E402
from tools.llm_router import get_llm  # noqa: E402

load_dotenv()

_AGENT = "Response Tracker (Loop 2)"


def _cycle_rate(cycle_number: int, cycle_acceptance_rates: list[float]) -> float:
    idx = cycle_number - 1
    if idx < 0:
        return cycle_acceptance_rates[0] if cycle_acceptance_rates else 0.2
    if idx >= len(cycle_acceptance_rates):
        return cycle_acceptance_rates[-1]
    return float(cycle_acceptance_rates[idx])


def _count_acceptances(conn, campaign_id: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM outreach_messages
        WHERE campaign_id = ? AND status = 'ACCEPTED'
        """,
        (campaign_id,),
    ).fetchone()
    return int(dict(row)["c"]) if row else 0


def run_response_tracking(
    campaign_id: int,
    cycle_number: int,
    target_acceptances: float,
    cycle_acceptance_rates: list[float],
) -> dict[str, Any]:
    """
    Simulate responses for SENT messages in this cycle; update statuses and warm_leads.
    Compare total ACCEPTED across all cycles to target_acceptances.
    """
    target = int(round(float(target_acceptances)))
    conn = get_connection()
    now = datetime.now().isoformat()
    total_acceptances = 0
    try:
        rows = conn.execute(
            """
            SELECT o.id, o.alumni_id, a.engagement_score
            FROM outreach_messages o
            JOIN alumni a ON o.alumni_id = a.id
            WHERE o.campaign_id = ?
              AND o.wave = ?
              AND o.status = 'SENT'
            ORDER BY o.id
            """,
            (campaign_id, cycle_number),
        ).fetchall()
    except Exception as exc:
        conn.close()
        log_agent(
            campaign_id,
            _AGENT,
            "ERROR",
            "Failed to load SENT messages",
            str(exc),
        )
        raise

    messages = [dict(r) for r in rows]
    cycle_rate = _cycle_rate(cycle_number, cycle_acceptance_rates)

    accepted_this = 0
    opened_this = 0
    no_response = 0
    warm_lead_warns: list[str] = []

    try:
        for m in messages:
            mid = m["id"]
            aid = m["alumni_id"]
            eng = float(m.get("engagement_score") or 50.0)
            eng_factor = max(0.0, min(1.0, eng / 100.0))
            p_accept = cycle_rate * eng_factor * 1.5
            cap = cycle_rate * 2.0
            p_accept = min(cap, max(0.0, p_accept))

            r1 = random.random()
            if r1 < p_accept:
                conn.execute(
                    """
                UPDATE outreach_messages
                SET status = 'ACCEPTED', replied_at = ?, opened_at = COALESCE(opened_at, ?)
                WHERE id = ?
                """,
                    (now, now, mid),
                )
                accepted_this += 1
                continue

            p_open_if_not = 0.2 + 0.25 * eng_factor
            r2 = random.random()
            if r2 < p_open_if_not:
                conn.execute(
                    """
                UPDATE outreach_messages
                SET status = 'OPENED', opened_at = ?
                WHERE id = ?
                """,
                    (now, mid),
                )
                try:
                    save_warm_lead(campaign_id, aid, cycle_number, "OPENED", conn=conn)
                except Exception as exc:
                    warm_lead_warns.append(f"alumni_id={aid}: {exc}")
                opened_this += 1
            else:
                conn.execute(
                    """
                UPDATE outreach_messages
                SET status = 'NO_RESPONSE'
                WHERE id = ?
                """,
                    (mid,),
                )
                no_response += 1

        conn.commit()

        total_acceptances = _count_acceptances(conn, campaign_id)
    finally:
        conn.close()

    if warm_lead_warns:
        log_agent(
            campaign_id,
            _AGENT,
            "WARN",
            "save_warm_lead failures",
            "; ".join(warm_lead_warns)[:2000],
        )

    log_agent(
        campaign_id,
        _AGENT,
        "SIMULATE",
        (
            f"Cycle {cycle_number}: processed SENT={len(messages)}, "
            f"ACCEPTED={accepted_this}, OPENED={opened_this}, NO_RESPONSE={no_response}; "
            f"total_acceptances(all waves)={total_acceptances}"
        ),
        json.dumps({"cycle_rate": cycle_rate}),
    )

    if total_acceptances >= target:
        return {
            "goal_met": True,
            "total_acceptances": total_acceptances,
            "continue_cycling": False,
        }

    if cycle_number >= 4:
        log_agent(
            campaign_id,
            _AGENT,
            "STOP",
            "Max cycles reached without goal",
            f"total_acceptances={total_acceptances}, target={target}",
        )
        return {
            "goal_met": False,
            "continue_cycling": False,
            "reason": "max cycles reached",
        }

    shortfall = max(0, target - total_acceptances)
    scenario: str = "A" if total_acceptances <= 0.5 * target else "B"
    if scenario == "A":
        next_batch_size = max(shortfall * 2, int(max(target * 0.25, 10)))
    else:
        next_batch_size = max(int(shortfall * 1.25), int(max(target * 0.15, 8)))

    n_sim = len(messages)
    denom = max(1, n_sim)
    open_pct = round((accepted_this + opened_this) / denom * 100, 2)
    accept_pct = round(accepted_this / denom * 100, 2)

    diagnosis: Optional[str] = None
    try:
        llm = get_llm()
        diag_prompt = (
            f"Open rate is {open_pct}%, acceptance rate is {accept_pct}% "
            f"(this wave). Scenario {scenario}: we are short by {shortfall} acceptances "
            f"vs target {target} (total so far: {total_acceptances}). "
            "What should we change for the next wave? Give 3–5 concise bullet points."
        )
        resp = llm.invoke(diag_prompt)
        diagnosis = (resp.content if hasattr(resp, "content") else str(resp)).strip()
        log_agent(
            campaign_id,
            _AGENT,
            "DIAGNOSIS",
            f"Scenario {scenario}, shortfall {shortfall}",
            diagnosis[:2000],
        )
    except Exception as exc:
        diagnosis = (
            "LLM diagnosis unavailable; consider stronger subject lines, clearer CTA, "
            "and segmenting by engagement."
        )
        log_agent(
            campaign_id,
            _AGENT,
            "ERROR",
            "Diagnosis LLM failed",
            str(exc),
        )

    return {
        "goal_met": False,
        "continue_cycling": True,
        "diagnosis": diagnosis,
        "scenario": scenario,
        "shortfall": shortfall,
        "next_batch_size": int(next_batch_size),
        "total_acceptances": total_acceptances,
    }
