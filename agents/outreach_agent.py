"""
Loop 2 — Outreach Agent: simulate send; mark DRAFTED messages as SENT or BOUNCED (no LLM).
"""

from __future__ import annotations

import os
import random
import sys
from datetime import datetime
from typing import Any

from dotenv import load_dotenv

_parent = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _parent not in sys.path:
    sys.path.append(_parent)

from tools.database import get_connection, log_agent  # noqa: E402

load_dotenv()

_AGENT = "Outreach Agent (Loop 2)"


def run_outreach(
    campaign_id: int,
    cycle_number: int,
    actual_bounce_rate: float = 0.10,
) -> dict[str, Any]:
    """
    Load DRAFTED outreach for this campaign and wave (cycle). Assign BOUNCED or SENT
    with overall bounce rate ~actual_bounce_rate, weighted so lower engagement bounces more often.
    """
    conn = get_connection()
    try:
        try:
            rows = conn.execute(
                """
            SELECT o.id, o.alumni_id, a.engagement_score
            FROM outreach_messages o
            JOIN alumni a ON o.alumni_id = a.id
            WHERE o.campaign_id = ?
              AND o.wave = ?
              AND o.status = 'DRAFTED'
            ORDER BY o.id
            """,
                (campaign_id, cycle_number),
            ).fetchall()
        except Exception as exc:
            log_agent(
                campaign_id,
                _AGENT,
                "ERROR",
                "Failed to load DRAFTED messages",
                str(exc),
            )
            raise

        messages = [dict(r) for r in rows]
        total_in_batch = len(messages)
        if total_in_batch == 0:
            log_agent(
                campaign_id,
                _AGENT,
                "SUMMARY",
                f"Wave {cycle_number}: no DRAFTED messages",
                "",
            )
            return {
                "total_in_batch": 0,
                "sent_count": 0,
                "bounced_count": 0,
                "observed_bounce_rate": 0.0,
            }

        weights = []
        for m in messages:
            eng = int(m.get("engagement_score") or 50)
            w = max(1.0, 101.0 - float(eng))
            weights.append(w)
        mean_w = sum(weights) / len(weights) if weights else 1.0

        bounced_count = 0
        sent_count = 0
        now = datetime.now().isoformat()

        for m, w in zip(messages, weights, strict=True):
            mid = m["id"]
            p_bounce = actual_bounce_rate * (w / mean_w)
            p_bounce = min(0.95, max(0.0, p_bounce))
            try:
                if random.random() < p_bounce:
                    conn.execute(
                        """
                    UPDATE outreach_messages
                    SET status = 'BOUNCED', sent_at = ?
                    WHERE id = ?
                    """,
                        (now, mid),
                    )
                    bounced_count += 1
                else:
                    conn.execute(
                        """
                    UPDATE outreach_messages
                    SET status = 'SENT', sent_at = ?
                    WHERE id = ?
                    """,
                        (now, mid),
                    )
                    sent_count += 1
            except Exception as exc:
                log_agent(
                    campaign_id,
                    _AGENT,
                    "ERROR",
                    f"Update failed for message id={mid}",
                    str(exc),
                )
                raise

        conn.commit()
    finally:
        conn.close()

    observed_bounce_rate = (bounced_count / total_in_batch) if total_in_batch else 0.0
    log_agent(
        campaign_id,
        _AGENT,
        "SUMMARY",
        (
            f"Wave {cycle_number}: total={total_in_batch}, sent={sent_count}, "
            f"bounced={bounced_count}, observed_bounce_rate={observed_bounce_rate:.4f}"
        ),
        f"Target bounce rate input={actual_bounce_rate}; engagement-weighted per recipient.",
    )

    return {
        "total_in_batch": total_in_batch,
        "sent_count": sent_count,
        "bounced_count": bounced_count,
        "observed_bounce_rate": round(observed_bounce_rate, 6),
    }
