"""
Matching Agent: vector search, algorithmic scoring, and LLM reasoning for alumni–event fit.
Uses Kimi K2 via Groq (OpenAI-compatible API).
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime
from typing import Any

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

_parent = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _parent not in sys.path:
    sys.path.append(_parent)

from tools.database import get_all_alumni, log_agent, save_match
from tools.gdpr import anonymise_for_llm, log_gdpr_action, reattach_identity
from tools.llm_router import get_llm
from tools.vector_store import search_alumni

load_dotenv()

_AGENT = "Matching Agent"
_STOPWORDS = frozenset(
    "the a an and or for of in on at to from with by as is are was were be been being "
    "this that these those it its we you they he she them our your their event alumni "
    "panel networking career careers session workshop".split()
)


def _safe_parse_json(text: str, fallback: Any = None) -> Any:
    cleaned = text.strip().strip("```json").strip("```").strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    for start_char, end_char in [("[", "]"), ("{", "}")]:
        start = cleaned.find(start_char)
        end = cleaned.rfind(end_char)
        if start != -1 and end > start:
            try:
                return json.loads(cleaned[start : end + 1])
            except json.JSONDecodeError:
                continue
    return fallback


def _as_brief_dict(parsed_brief: Any) -> dict[str, Any]:
    if isinstance(parsed_brief, dict):
        return parsed_brief
    if isinstance(parsed_brief, str):
        try:
            return json.loads(parsed_brief)
        except json.JSONDecodeError:
            return {}
    return {}


def _event_description(parsed: dict[str, Any]) -> str:
    parts = [
        str(parsed.get("event_type") or ""),
        str(parsed.get("topic") or ""),
        str(parsed.get("location_city") or ""),
        str(parsed.get("audience_constraints") or ""),
    ]
    return " ".join(p for p in parts if p).strip() or "Alumni event"


def _topic_keywords(text: str) -> set[str]:
    words = re.findall(r"\w+", (text or "").lower())
    return {w for w in words if len(w) >= 3 and w not in _STOPWORDS}


def _score_topic_alignment(
    topic_blob: str, industry: str | None, interests: str | None
) -> float:
    keys = _topic_keywords(topic_blob)
    if not keys:
        return 20.0
    ind = (industry or "").lower()
    intr = (interests or "").lower()
    hits = sum(1 for k in keys if k in ind or k in intr)
    ratio = hits / max(len(keys), 1)
    return min(40.0, 40.0 * ratio)


def _score_location(event_city: str | None, alumni_city: str | None) -> float:
    ec = (event_city or "").strip().lower()
    ac = (alumni_city or "").strip().lower()
    if not ec or not ac:
        return 10.0
    return 20.0 if ec == ac else 0.0


def _score_graduation(constraints: str | None, graduation_year: int | None) -> float:
    cy = datetime.now().year
    gy = graduation_year
    if gy is None:
        return 7.5
    ac = (constraints or "").lower()
    targets_recency = any(
        x in ac for x in ("recent graduate", "recent graduates", "new grad", "early career", "junior")
    )
    age = cy - int(gy)
    if not targets_recency:
        return 15.0
    if age <= 3:
        return 15.0
    if age <= 7:
        return 10.0
    if age <= 12:
        return 5.0
    return 0.0


def _score_engagement(engagement_score: int | None) -> float:
    es = float(engagement_score if engagement_score is not None else 50)
    es = max(0.0, min(100.0, es))
    return es / 100.0 * 15.0


def _truncate_field_for_llm(val: Any, max_len: int = 80) -> Any:
    if val is None:
        return None
    s = val if isinstance(val, str) else str(val)
    return s if len(s) <= max_len else s[:max_len]


def _truncate_alumni_for_llm_payload(alumni_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Shallow copies with interests and past_events capped before JSON payload to the LLM."""
    out: list[dict[str, Any]] = []
    for a in alumni_list:
        row = dict(a)
        for key in ("interests", "past_events"):
            if key in row:
                row[key] = _truncate_field_for_llm(row.get(key), 80)
        out.append(row)
    return out


def _normalize_vector_fit(distances: list[float | None]) -> dict[int, float]:
    valid = [d for d in distances if d is not None and isinstance(d, (int, float))]
    if not valid:
        return {i: 0.0 for i in range(len(distances))}
    d_min = min(valid)
    d_max = max(valid)
    out: dict[int, float] = {}
    for i, d in enumerate(distances):
        if d is None or not isinstance(d, (int, float)):
            out[i] = 0.0
        elif d_max <= d_min + 1e-9:
            out[i] = 10.0
        else:
            inv = (d_max - float(d)) / (d_max - d_min + 1e-9)
            out[i] = max(0.0, min(10.0, 10.0 * inv))
    return out


def _llm_reasoning_batches(
    campaign_id: int,
    event_desc: str,
    candidates: list[dict[str, Any]],
    llm: ChatOpenAI,
) -> dict[int, str]:
    reasoning_by_id: dict[int, str] = {}
    batch_size = 8
    for start in range(0, len(candidates), batch_size):
        batch = _truncate_alumni_for_llm_payload(candidates[start : start + batch_size])
        try:
            anon = anonymise_for_llm(batch)
            log_gdpr_action(
                campaign_id,
                "anonymise_for_matching",
                f"Batch {start // batch_size + 1}: {len(batch)} alumni for LLM review",
            )
            payload = json.dumps(anon, ensure_ascii=False)
            prompt = (
                f"You are the Matching Agent for Pecan. Review these alumni candidates for a {event_desc}. "
                "For each, write a one-sentence reasoning explaining why they are or aren't a good match. "
                'Return JSON array: [{"id": alumni_id, "reasoning": "..."}]. Valid JSON only.\n\n'
                f"Candidates (JSON):\n{payload}"
            )
            resp = llm.invoke([HumanMessage(content=prompt)])
            text = resp.content if hasattr(resp, "content") else str(resp)
            parsed = _safe_parse_json(text, None)
            if not isinstance(parsed, list):
                log_agent(
                    campaign_id,
                    _AGENT,
                    "llm_batch_parse",
                    "skipped",
                    f"Batch {start // batch_size + 1}: expected JSON array, got {type(parsed).__name__}",
                )
                continue
            lookup = {a["id"]: a for a in batch}
            llm_items: list[dict[str, Any]] = []
            for x in parsed:
                if not isinstance(x, dict):
                    continue
                rid = x.get("id") if x.get("id") is not None else x.get("alumni_id")
                if rid is None:
                    continue
                try:
                    llm_items.append({"id": int(rid), "reasoning": str(x.get("reasoning", "") or "")})
                except (TypeError, ValueError):
                    continue
            merged = reattach_identity(llm_items, lookup)
            for row in merged:
                aid = row.get("id")
                if aid is not None:
                    try:
                        reasoning_by_id[int(aid)] = str(row.get("reasoning") or "")
                    except (TypeError, ValueError):
                        pass
            log_agent(
                campaign_id,
                _AGENT,
                "llm_batch",
                "ok",
                f"Batch {start // batch_size + 1}: parsed {len(parsed)} items",
            )
        except Exception as ex:
            log_agent(
                campaign_id,
                _AGENT,
                "llm_batch",
                "error",
                f"Batch {start // batch_size + 1}: {type(ex).__name__}: {ex}",
            )
            continue
    return reasoning_by_id


def run_matching(
    campaign_id: int,
    parsed_brief: Any,
    target_attendance: int,
) -> dict[str, Any]:
    try:
        ta = max(1, int(target_attendance))
    except (TypeError, ValueError):
        ta = 1

    parsed = _as_brief_dict(parsed_brief)
    event_desc = _event_description(parsed)
    topic_blob = " ".join(
        [
            str(parsed.get("event_type") or ""),
            str(parsed.get("topic") or ""),
            str(parsed.get("audience_constraints") or ""),
        ]
    )
    event_city = parsed.get("location_city")
    audience_constraints = parsed.get("audience_constraints")

    log_agent(
        campaign_id,
        _AGENT,
        "start",
        "run_matching",
        f"target_attendance={ta}, event_desc_len={len(event_desc)}",
    )

    all_alumni = get_all_alumni()
    total_count = len(all_alumni)
    alumni_by_id = {a["id"]: a for a in all_alumni}

    if total_count == 0:
        log_agent(campaign_id, _AGENT, "vector_search", "0 candidates", "No alumni in database")
        return {
            "pool": [],
            "total_scored": 0,
            "total_selected": 0,
            "insufficient_matches": True,
            "suggestion": "No eligible alumni with consent and valid email; add alumni records before matching.",
        }

    n_results = min(5 * ta + 50, total_count)
    vector_hits: list[dict[str, Any]] = []
    if n_results > 0:
        try:
            vector_hits = search_alumni(event_desc, n_results=n_results)
        except Exception as ex:
            log_agent(
                campaign_id,
                _AGENT,
                "vector_search",
                "error",
                f"{type(ex).__name__}: {ex}",
            )
    log_agent(
        campaign_id,
        _AGENT,
        "vector_search",
        f"{len(vector_hits)} candidates",
        f"Vector search returned {len(vector_hits)} candidates.",
    )

    distances = [h.get("similarity_distance") for h in vector_hits]
    vec_fits = _normalize_vector_fit(distances)

    scored_rows: list[dict[str, Any]] = []
    for idx, hit in enumerate(vector_hits):
        aid = hit.get("alumni_id")
        if aid is None:
            continue
        aid = int(aid)
        alum = alumni_by_id.get(aid)
        if not alum:
            continue
        t = _score_topic_alignment(topic_blob, alum.get("industry"), alum.get("interests"))
        l = _score_location(event_city, alum.get("location_city"))
        g = _score_graduation(
            str(audience_constraints) if audience_constraints else None,
            alum.get("graduation_year"),
        )
        e = _score_engagement(alum.get("engagement_score"))
        v = vec_fits.get(idx, 0.0)
        raw = t + l + g + e + v
        score = max(0, min(100, int(round(raw))))
        scored_rows.append(
            {
                "alumni": alum,
                "match_score": score,
                "vector_distance": hit.get("similarity_distance"),
                "_raw": raw,
            }
        )

    scored_rows.sort(key=lambda x: (-x["match_score"], x["alumni"]["id"]))
    total_scored = len(scored_rows)

    need = 5 * ta
    llm_n = min(20, ta, total_scored)
    top_for_llm = [r["alumni"] for r in scored_rows[:llm_n]]

    reasoning_by_id: dict[int, str] = {}
    if top_for_llm and os.getenv("GROQ_API_KEY"):
        try:
            llm = get_llm()
            reasoning_by_id = _llm_reasoning_batches(campaign_id, event_desc, top_for_llm, llm)
        except Exception as ex:
            log_agent(
                campaign_id,
                _AGENT,
                "llm_init",
                "failed",
                f"{type(ex).__name__}: {ex}",
            )
    elif top_for_llm:
        log_agent(
            campaign_id,
            _AGENT,
            "llm_skip",
            "no GROQ_API_KEY",
            "Skipping LLM reasoning; set GROQ_API_KEY for match explanations.",
        )

    select_n = min(need, total_scored)
    selected_ids: set[int] = set()
    for i, row in enumerate(scored_rows):
        alum = row["alumni"]
        aid = alum["id"]
        is_sel = i < select_n
        if is_sel:
            selected_ids.add(aid)
        reasoning = reasoning_by_id.get(aid, "")
        try:
            save_match(
                campaign_id,
                aid,
                row["match_score"],
                reasoning,
                selected=is_sel,
                wave=1,
            )
        except Exception as ex:
            log_agent(
                campaign_id,
                _AGENT,
                "save_match",
                "error",
                f"alumni_id={aid}: {type(ex).__name__}: {ex}",
            )

    log_agent(
        campaign_id,
        _AGENT,
        "save_complete",
        f"{total_scored} rows",
        f"Persisted matches; selected={select_n}",
    )

    pool: list[dict[str, Any]] = []
    for row in scored_rows[:select_n]:
        a = dict(row["alumni"])
        a["match_score"] = row["match_score"]
        a["match_reasoning"] = reasoning_by_id.get(row["alumni"]["id"], "")
        pool.append(a)

    strong_count = sum(1 for r in scored_rows if r["match_score"] > 30)
    insufficient_matches = strong_count < need
    suggestion = ""
    if insufficient_matches:
        suggestion = (
            f"Only {strong_count} alumni in the scored pool have a match score above 30, "
            f"but the campaign targets {need} high-potential invites. "
            "Consider broadening the topic or location, relaxing audience constraints, or embedding more diverse alumni profiles."
        )

    return {
        "pool": pool,
        "total_scored": total_scored,
        "total_selected": select_n,
        "insufficient_matches": insufficient_matches,
        "suggestion": suggestion if insufficient_matches else "",
    }
