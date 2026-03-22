"""
Campaign Reporter: analyses a completed campaign, persists insights and benchmarks.
Uses Kimi K2 via Groq (OpenAI-compatible API).
"""

from __future__ import annotations

import json
import os
import traceback
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

from tools.database import (
    get_agent_log,
    get_all_cycles,
    get_campaign,
    get_checkins,
    get_matches,
    get_memories,
    get_outreach,
    log_agent,
    save_memory,
    update_campaign,
)
from tools.llm_router import get_llm

load_dotenv()

_AGENT = "Campaign Reporter"


def _safe_parse_json(text: str, fallback: Any = None) -> Any:
    if not text or not str(text).strip():
        return fallback
    cleaned = str(text).strip().strip("```json").strip("```").strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    for start_char, end_char in [("{", "}"), ("[", "]")]:
        start = cleaned.find(start_char)
        end = cleaned.rfind(end_char)
        if start != -1 and end > start:
            try:
                return json.loads(cleaned[start : end + 1])
            except json.JSONDecodeError:
                continue
    return fallback


def _parsed_brief_dict(campaign: Dict[str, Any]) -> Dict[str, Any]:
    raw = campaign.get("parsed_brief")
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


def _is_sent(status: Optional[str]) -> bool:
    return status not in (None, "", "DRAFTED")


def _is_opened(status: Optional[str]) -> bool:
    return status in ("OPENED", "REPLIED", "ACCEPTED")


def _is_accepted(status: Optional[str]) -> bool:
    return status in ("REPLIED", "ACCEPTED")


def _is_bounced(status: Optional[str]) -> bool:
    return status == "BOUNCED"


def _is_personalised_row(o: Dict[str, Any]) -> bool:
    body = (o.get("body") or "").strip()
    note = (o.get("personalisation_note") or "").strip()
    return bool(body or note)


def _outreach_for_wave(outreach: List[Dict[str, Any]], wave: int) -> List[Dict[str, Any]]:
    return [o for o in outreach if int(o.get("wave") or 1) == int(wave)]


def _per_cycle_funnels(
    cycles: List[Dict[str, Any]], outreach: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    cycle_numbers = [int(c["cycle_number"]) for c in cycles] if cycles else []
    if not cycle_numbers and outreach:
        cycle_numbers = sorted({int(o.get("wave") or 1) for o in outreach})
    if not cycle_numbers:
        cycle_numbers = [1]

    cycle_by_num = {int(c["cycle_number"]): c for c in cycles}

    for cn in cycle_numbers:
        rows = _outreach_for_wave(outreach, cn)
        c_row = cycle_by_num.get(cn, {})
        personalised = sum(1 for o in rows if _is_personalised_row(o))
        sent = sum(1 for o in rows if _is_sent(o.get("status")))
        opened = sum(1 for o in rows if _is_opened(o.get("status")))
        accepted = sum(1 for o in rows if _is_accepted(o.get("status")))
        bounced = sum(1 for o in rows if _is_bounced(o.get("status")))

        if not rows and c_row:
            personalised = int(c_row.get("personalised_count") or 0)
            sent = int(c_row.get("reached_count") or 0)
            bounced = int(c_row.get("bounce_count") or 0)
            accepted = int(c_row.get("acceptance_count") or 0)
            opened = 0

        exp = c_row.get("expected_acceptance_rate")
        if exp is not None:
            try:
                exp_f = float(exp)
                if exp_f > 1.0:
                    exp_f = exp_f / 100.0
            except (TypeError, ValueError):
                exp_f = None
        else:
            exp_f = None

        actual_rate = (accepted / sent) if sent else 0.0
        expected_rate_display = exp_f if exp_f is not None else None

        results.append(
            {
                "cycle_number": cn,
                "personalised": personalised,
                "sent": sent,
                "opened": opened,
                "accepted": accepted,
                "bounced": bounced,
                "expected_acceptance_rate": expected_rate_display,
                "actual_acceptance_rate": round(actual_rate, 4),
                "vs_expected": (
                    round(actual_rate - exp_f, 4)
                    if exp_f is not None and sent
                    else None
                ),
            }
        )
    return results


def _grad_bucket(gy: Optional[int]) -> str:
    if gy is None:
        return "unknown"
    g = int(gy)
    decade = (g // 5) * 5
    return f"{decade}-{decade + 4}"


def _segment_performance(
    outreach: List[Dict[str, Any]], alumni_by_id: Dict[int, Dict[str, Any]]
) -> Dict[str, List[Dict[str, Any]]]:
    """Per segment key: acceptance rate and counts (min n for stability)."""
    by_dept: Dict[str, List[int]] = defaultdict(lambda: [0, 0])
    by_year: Dict[str, List[int]] = defaultdict(lambda: [0, 0])
    by_industry: Dict[str, List[int]] = defaultdict(lambda: [0, 0])
    by_loc: Dict[str, List[int]] = defaultdict(lambda: [0, 0])

    for o in outreach:
        if not _is_sent(o.get("status")):
            continue
        aid = o.get("alumni_id")
        if aid is None:
            continue
        a = alumni_by_id.get(int(aid), {})
        acc = 1 if _is_accepted(o.get("status")) else 0

        dept = (a.get("department") or "unknown").strip() or "unknown"
        by_dept[dept][0] += 1
        by_dept[dept][1] += acc

        gy = a.get("graduation_year")
        yk = _grad_bucket(int(gy) if gy is not None else None)
        by_year[yk][0] += 1
        by_year[yk][1] += acc

        ind = (a.get("industry") or "unknown").strip() or "unknown"
        by_industry[ind][0] += 1
        by_industry[ind][1] += acc

        city = (a.get("location_city") or "").strip()
        country = (a.get("location_country") or "").strip()
        loc = ", ".join(x for x in (city, country) if x) or "unknown"
        by_loc[loc][0] += 1
        by_loc[loc][1] += acc

    def top_rates(
        buckets: Dict[str, List[int]], min_n: int = 2
    ) -> List[Dict[str, Any]]:
        rows_out: List[Dict[str, Any]] = []
        for key, (n, acc) in buckets.items():
            if n < min_n:
                continue
            rows_out.append(
                {
                    "segment": key,
                    "sent": n,
                    "accepted": acc,
                    "acceptance_rate": round(acc / n, 4),
                }
            )
        rows_out.sort(key=lambda x: (-x["acceptance_rate"], -x["sent"]))
        return rows_out[:15]

    return {
        "by_department": top_rates(by_dept),
        "by_graduation_year_range": top_rates(by_year),
        "by_industry": top_rates(by_industry),
        "by_location": top_rates(by_loc),
    }


def _overall_funnel(
    campaign: Dict[str, Any],
    matches: List[Dict[str, Any]],
    outreach: List[Dict[str, Any]],
    checkins: List[Dict[str, Any]],
) -> Dict[str, Any]:
    pool = campaign.get("total_pool_size")
    if pool is None:
        pool = len(matches)
    else:
        try:
            pool = int(pool)
        except (TypeError, ValueError):
            pool = len(matches)

    personalised = sum(1 for o in outreach if _is_personalised_row(o))
    sent = sum(1 for o in outreach if _is_sent(o.get("status")))
    opened = sum(1 for o in outreach if _is_opened(o.get("status")))
    accepted = sum(1 for o in outreach if _is_accepted(o.get("status")))
    bounced = sum(1 for o in outreach if _is_bounced(o.get("status")))

    checked_in = sum(1 for c in checkins if int(c.get("checked_in") or 0) == 1)
    has_checkins = len(checkins) > 0

    return {
        "total_pool": pool,
        "total_personalised": personalised,
        "total_sent": sent,
        "total_opened": opened,
        "total_accepted": accepted,
        "total_bounced": bounced,
        "total_checked_in": checked_in,
        "has_checkin_data": has_checkins,
    }


def _basic_report_from_metrics(
    metrics: Dict[str, Any],
    segment_summary: str,
) -> Dict[str, Any]:
    funnel = metrics.get("overall_funnel", {})
    per_cycle = metrics.get("per_cycle_funnels", [])
    benchmarks: Dict[str, Any] = {}
    for pc in per_cycle:
        cn = pc.get("cycle_number")
        benchmarks[str(cn)] = pc.get("actual_acceptance_rate", 0.0)

    ts = max(int(funnel.get("total_sent") or 0), 1)
    acc_pct = round((funnel.get("total_accepted") or 0) / ts * 100, 1)
    return {
        "summary": (
            f"Campaign reached {funnel.get('total_sent', 0)} sends with "
            f"{funnel.get('total_accepted', 0)} acceptances ({acc_pct}% acceptance). "
            f"Opens: {funnel.get('total_opened', 0)}; bounces: {funnel.get('total_bounced', 0)}."
        ),
        "key_insight": (
            "Compare actual acceptance rates per cycle to expected rates to calibrate "
            "targets for the next run."
        ),
        "best_segment": segment_summary or "Insufficient segment data; widen targeting or increase volume per segment.",
        "recommendation": (
            "Replicate messaging and channel tactics from the highest-yield cycle and segment "
            "in the next campaign."
        ),
        "updated_benchmarks": benchmarks,
    }


def _best_segment_sentence(segments: Dict[str, List[Dict[str, Any]]]) -> str:
    best: Optional[Tuple[str, str, float]] = None
    for dim, rows in segments.items():
        if not rows:
            continue
        top = rows[0]
        ar = float(top.get("acceptance_rate", 0))
        label = f"{dim}: {top.get('segment')}"
        if best is None or ar > best[2]:
            best = (dim, label, ar)
    if not best:
        return ""
    return f"{best[1]} (acceptance rate {best[2]:.1%})"


def run_report(campaign_id: int) -> Dict[str, Any]:
    log_agent(
        campaign_id,
        _AGENT,
        "START",
        "run_report",
        "Loading campaign data for reporting.",
    )

    campaign = get_campaign(campaign_id)
    if not campaign:
        empty = {"error": "campaign_not_found", "campaign_id": campaign_id}
        log_agent(
            campaign_id,
            _AGENT,
            "ERROR",
            "abort",
            "Campaign not found.",
        )
        return empty

    try:
        cycles = get_all_cycles(campaign_id)
        matches = get_matches(campaign_id, selected_only=True)
        outreach = get_outreach(campaign_id)
        checkins = get_checkins(campaign_id)
        agent_log = get_agent_log(campaign_id)
        memories = get_memories()

        alumni_by_id: Dict[int, Dict[str, Any]] = {}
        for m in matches:
            aid = m.get("alumni_id")
            if aid is not None:
                alumni_by_id[int(aid)] = m

        per_cycle_funnels = _per_cycle_funnels(cycles, outreach)
        overall = _overall_funnel(campaign, matches, outreach, checkins)
        segments = _segment_performance(outreach, alumni_by_id)
        segment_sentence = _best_segment_sentence(segments)

        memory_ctx = ""
        if memories:
            top3 = memories[:3]
            memory_ctx = "\n".join(
                f"- {m.get('event_type', '')}: {m.get('key_insight', '')}" for m in top3
            )

        parsed = _parsed_brief_dict(campaign)

        metrics_payload = {
            "campaign_id": campaign_id,
            "campaign_snapshot": {
                "status": campaign.get("status"),
                "total_invited": campaign.get("total_invited"),
                "total_opened": campaign.get("total_opened"),
                "total_bounced": campaign.get("total_bounced"),
                "total_replied": campaign.get("total_replied"),
                "open_rate": campaign.get("open_rate"),
                "checkin_rate": campaign.get("checkin_rate"),
                "target_acceptances": campaign.get("target_acceptances"),
            },
            "parsed_brief_excerpt": {
                k: parsed.get(k)
                for k in (
                    "event_type",
                    "topic",
                    "location_city",
                    "date",
                )
                if parsed.get(k) is not None
            },
            "per_cycle_funnels": per_cycle_funnels,
            "overall_funnel": overall,
            "segment_breakdown": segments,
            "agent_log_entry_count": len(agent_log),
        }

        prompt = f"""You are the Campaign Reporter for Pecan. Analyse these campaign results and provide: 1) A 3-5 sentence summary of the campaign, 2) The single most important insight for future campaigns of this type, 3) Which audience segment responded best and why, 4) One specific recommendation for next time, 5) Updated acceptance rate benchmarks per cycle based on what you observed. Return as JSON with keys: summary, key_insight, best_segment, recommendation, updated_benchmarks.

CAMPAIGN METRICS (JSON):
{json.dumps(metrics_payload, indent=2)}

PRIOR CAMPAIGN MEMORY (top insights):
{memory_ctx or "(none)"}
"""

        report: Dict[str, Any]
        llm_ok = False
        try:
            llm = get_llm()
            if not os.getenv("GROQ_API_KEY"):
                raise ValueError("GROQ_API_KEY not set")
            response = llm.invoke(prompt)
            content = getattr(response, "content", None) or str(response)
            if isinstance(content, list):
                content = "".join(
                    getattr(block, "text", str(block)) for block in content
                )
            parsed_llm = _safe_parse_json(content, None)
            if isinstance(parsed_llm, dict) and parsed_llm.get("summary"):
                report = {
                    "summary": str(parsed_llm.get("summary", "")).strip(),
                    "key_insight": str(parsed_llm.get("key_insight", "")).strip(),
                    "best_segment": str(parsed_llm.get("best_segment", "")).strip(),
                    "recommendation": str(parsed_llm.get("recommendation", "")).strip(),
                    "updated_benchmarks": parsed_llm.get("updated_benchmarks")
                    if isinstance(parsed_llm.get("updated_benchmarks"), (dict, list))
                    else {},
                }
                llm_ok = True
            else:
                raise ValueError("LLM returned invalid JSON structure")
        except Exception as e:
            err_txt = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            log_agent(
                campaign_id,
                _AGENT,
                "LLM_FALLBACK",
                "using_numeric_report",
                err_txt[:4000],
            )
            report = _basic_report_from_metrics(
                {
                    "overall_funnel": overall,
                    "per_cycle_funnels": per_cycle_funnels,
                },
                segment_sentence,
            )
            llm_ok = False

        report["metrics"] = metrics_payload
        report["generated_at"] = datetime.now().isoformat()
        report["llm_analysis"] = llm_ok

        summary_json = json.dumps(report, indent=2)
        update_campaign(campaign_id, agent_summary=summary_json)

        open_rate = float(campaign.get("open_rate") or 0.0)
        checkin_rate = float(campaign.get("checkin_rate") or 0.0)
        if overall.get("has_checkin_data") and overall.get("total_pool"):
            checkin_rate = round(
                100.0 * overall["total_checked_in"] / max(overall["total_pool"], 1),
                2,
            )

        try:
            save_memory(
                campaign_id,
                str(
                    parsed.get("event_type")
                    or campaign.get("campaign_phase")
                    or "unknown"
                ),
                (report.get("best_segment") or segment_sentence or "")[:500],
                report.get("key_insight") or "",
                open_rate,
                checkin_rate,
            )
        except Exception as mem_e:
            log_agent(
                campaign_id,
                _AGENT,
                "WARN",
                "save_memory_skipped",
                str(mem_e)[:2000],
            )

        update_campaign(
            campaign_id,
            status="COMPLETE",
            completed_at=datetime.now().isoformat(),
        )

        log_agent(
            campaign_id,
            _AGENT,
            "COMPLETE",
            "report_saved",
            f"Summary length={len(report.get('summary', ''))}; llm={llm_ok}",
        )

        return report

    except Exception as e:
        err_txt = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        log_agent(
            campaign_id,
            _AGENT,
            "ERROR",
            "run_report_failed",
            err_txt[:4000],
        )
        minimal = {
            "error": str(e),
            "campaign_id": campaign_id,
            "summary": "Reporting failed; see agent_log.",
            "key_insight": "",
            "best_segment": "",
            "recommendation": "",
            "updated_benchmarks": {},
        }
        return minimal
