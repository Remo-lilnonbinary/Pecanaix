"""
Loop 2 — Personalisation Agent: draft personalised and follow-up outreach via Kimi K2 (Groq).
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Optional

from dotenv import load_dotenv

_parent = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _parent not in sys.path:
    sys.path.append(_parent)

from tools.database import get_connection, log_agent, save_outreach  # noqa: E402
from tools.gdpr import anonymise_for_llm  # noqa: E402
from tools.llm_router import get_llm  # noqa: E402

load_dotenv()

_AGENT = "Personalisation Agent (Loop 2)"


def _safe_parse_json(text: str, fallback: Any = None) -> Any:
    cleaned = text.strip().strip("```json").strip("```").strip()
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


def _truncate_field_for_prompt(val: Any, max_len: int = 80) -> Any:
    if val is None:
        return None
    s = val if isinstance(val, str) else str(val)
    return s if len(s) <= max_len else s[:max_len]


def _truncate_profiles_for_llm_prompt(batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for a in batch:
        row = dict(a)
        for key in ("interests", "past_events"):
            if key in row:
                row[key] = _truncate_field_for_prompt(row.get(key), 80)
        out.append(row)
    return out


def _first_name(name: Optional[str]) -> str:
    if not name or not str(name).strip():
        return "there"
    return str(name).strip().split()[0]


def _fetch_alumni_by_ids(ids: list[int]) -> dict[int, dict[str, Any]]:
    if not ids:
        return {}
    conn = get_connection()
    try:
        placeholders = ",".join("?" * len(ids))
        rows = conn.execute(
            f"SELECT * FROM alumni WHERE id IN ({placeholders})",
            tuple(ids),
        ).fetchall()
        return {dict(r)["id"]: dict(r) for r in rows}
    finally:
        conn.close()


def run_personalisation(
    campaign_id: int,
    cycle_number: int,
    alumni_batch: list[dict[str, Any]],
    parsed_brief: dict[str, Any],
    warm_leads_to_followup: Optional[list[dict[str, Any]]] = None,
    diagnosis: Optional[str] = None,
) -> dict[str, int]:
    """
    Draft fresh emails for alumni_batch and optional shorter follow-ups for warm leads.
    Persists rows with wave=cycle_number. Returns counts.
    """
    fresh_count = 0
    followup_count = 0
    llm = get_llm()

    diagnosis_block = ""
    if diagnosis:
        diagnosis_block = (
            f"\n\nADJUSTMENT FROM PRIOR CYCLE DIAGNOSIS (apply as appropriate):\n{diagnosis}\n"
            "If the diagnosis mentions tactics (e.g. rewrite subject lines, add urgency), reflect that."
        )

    def _draft_fresh_batch(batch: list[dict[str, Any]]) -> None:
        nonlocal fresh_count
        if not batch:
            return
        anon = anonymise_for_llm(_truncate_profiles_for_llm_prompt(batch))
        brief_json = json.dumps(parsed_brief, ensure_ascii=False)
        anon_json = json.dumps(anon, indent=1, ensure_ascii=False)
        prompt = f"""You are the Personalisation Agent for Pecan (alumni outreach). Draft one email per profile.

EVENT CONTEXT (JSON): {brief_json}

ALUMNI PROFILES (anonymised, no names/emails — use id as alumni_id): {anon_json}
{diagnosis_block}

RULES FOR EACH EMAIL:
- subject_line: personalised, not generic.
- body: under 120 words; reference AT LEAST TWO concrete profile fields (e.g. degree, department, industry, interests, past_events, job_title).
- Do NOT invent facts. Use only fields present in the profile.
- personalisation_note: briefly list which profile details you used.
- Address the reader as "Dear alumni" or similar — no real names (names are withheld).
- Tone: warm, professional.

Return ONLY a JSON array:
[{{"alumni_id": <int>, "subject_line": "...", "body": "...", "personalisation_note": "..."}}]"""

        try:
            response = llm.invoke(prompt)
            text = response.content if hasattr(response, "content") else str(response)
            parsed = _safe_parse_json(text, None)
        except Exception as exc:
            log_agent(
                campaign_id,
                _AGENT,
                "ERROR",
                "LLM call failed for fresh batch",
                str(exc),
            )
            parsed = None

        lookup = {a["id"]: a for a in batch}
        if not isinstance(parsed, list):
            for a in batch:
                subj = f"Invitation: {parsed_brief.get('topic', 'our event')} — {parsed_brief.get('location_city', '')}"
                body = (
                    f"Dear {_first_name(a.get('name'))},\n\n"
                    f"We would love to invite you to connect at our upcoming event aligned with your "
                    f"background in {a.get('department', 'your field')}. "
                    f"Details match your interests in {a.get('interests', 'our community')}. "
                    f"Reply if you would like to join.\n"
                )
                save_outreach(
                    campaign_id,
                    a["id"],
                    subj,
                    body,
                    "Fallback after parse/call failure",
                    wave=cycle_number,
                )
                fresh_count += 1
            log_agent(
                campaign_id,
                _AGENT,
                "ERROR_RECOVERY",
                f"Fallback emails for {len(batch)} alumni",
                "Unparseable LLM output or exception.",
            )
            return

        for item in parsed:
            try:
                aid = int(item.get("alumni_id"))
            except (TypeError, ValueError):
                continue
            alum = lookup.get(aid)
            if not alum:
                continue
            subj = str(item.get("subject_line", "You're invited")).strip()
            body_raw = str(item.get("body", "")).strip()
            note = str(item.get("personalisation_note", "")).strip()
            greeting = f"Dear {_first_name(alum.get('name'))},"
            if body_raw.lower().startswith("dear "):
                body = body_raw
            else:
                body = f"{greeting}\n\n{body_raw}"
            save_outreach(campaign_id, aid, subj, body, note, wave=cycle_number)
            fresh_count += 1

        log_agent(
            campaign_id,
            _AGENT,
            "DRAFT_FRESH",
            f"Batch saved {len(parsed)} rows (wave {cycle_number})",
            json.dumps({"alumni_ids": [x.get("alumni_id") for x in parsed if isinstance(x, dict)]}),
        )

    def _draft_followup_batch(batch: list[dict[str, Any]]) -> None:
        nonlocal followup_count
        if not batch:
            return
        anon = anonymise_for_llm(_truncate_profiles_for_llm_prompt(batch))
        brief_json = json.dumps(parsed_brief, ensure_ascii=False)
        anon_json = json.dumps(anon, indent=1, ensure_ascii=False)
        prompt = f"""You are the Personalisation Agent for Pecan. These alumni showed interest (e.g. opened a prior email) but have not registered. Write SHORT, direct follow-up emails — different angle from a first invite.

EVENT CONTEXT: {brief_json}

PROFILES (anonymised): {anon_json}
{diagnosis_block}

RULES:
- Shorter than a first email (under 80 words).
- One clear CTA (e.g. save a spot, quick yes/no).
- subject_line: direct, can reference they engaged before.
- body: mention you noticed prior engagement in generic terms (do not fabricate specifics).
- personalisation_note: what you leveraged from the profile.

Return ONLY JSON array:
[{{"alumni_id": <int>, "subject_line": "...", "body": "...", "personalisation_note": "..."}}]"""

        try:
            response = llm.invoke(prompt)
            text = response.content if hasattr(response, "content") else str(response)
            parsed = _safe_parse_json(text, None)
        except Exception as exc:
            log_agent(
                campaign_id,
                _AGENT,
                "ERROR",
                "LLM call failed for follow-up batch",
                str(exc),
            )
            parsed = None

        lookup = {a["id"]: a for a in batch}
        if not isinstance(parsed, list):
            for a in batch:
                subj = f"Quick follow-up: {parsed_brief.get('topic', 'our event')}"
                body = (
                    f"Dear {_first_name(a.get('name'))},\n\n"
                    f"I noticed you had looked at our recent note about the event — "
                    f"can I save you a spot? Reply yes and we will confirm.\n"
                )
                save_outreach(
                    campaign_id,
                    a["id"],
                    subj,
                    body,
                    "Fallback follow-up",
                    wave=cycle_number,
                )
                followup_count += 1
            log_agent(
                campaign_id,
                _AGENT,
                "ERROR_RECOVERY",
                f"Fallback follow-ups for {len(batch)} alumni",
                "Unparseable LLM output or exception.",
            )
            return

        for item in parsed:
            try:
                aid = int(item.get("alumni_id"))
            except (TypeError, ValueError):
                continue
            alum = lookup.get(aid)
            if not alum:
                continue
            subj = str(item.get("subject_line", "Following up")).strip()
            body_raw = str(item.get("body", "")).strip()
            note = str(item.get("personalisation_note", "")).strip()
            greeting = f"Dear {_first_name(alum.get('name'))},"
            if body_raw.lower().startswith("dear "):
                body = body_raw
            else:
                body = f"{greeting}\n\n{body_raw}"
            save_outreach(campaign_id, aid, subj, body, note, wave=cycle_number)
            followup_count += 1

        log_agent(
            campaign_id,
            _AGENT,
            "DRAFT_FOLLOWUP",
            f"Follow-up batch saved {len(parsed)} rows (wave {cycle_number})",
            json.dumps({"alumni_ids": [x.get("alumni_id") for x in parsed if isinstance(x, dict)]}),
        )

    try:
        for i in range(0, len(alumni_batch), 5):
            chunk = alumni_batch[i : i + 5]
            _draft_fresh_batch(chunk)

        warm = warm_leads_to_followup or []
        if warm:
            ids: list[int] = []
            for w in warm:
                try:
                    ids.append(int(w.get("alumni_id")))
                except (TypeError, ValueError):
                    continue
            id_to_row = _fetch_alumni_by_ids(list(dict.fromkeys(ids)))
            follow_batch: list[dict[str, Any]] = []
            for wid in ids:
                row = id_to_row.get(wid)
                if row:
                    follow_batch.append(row)
            for i in range(0, len(follow_batch), 5):
                _draft_followup_batch(follow_batch[i : i + 5])

        log_agent(
            campaign_id,
            _AGENT,
            "COMPLETE",
            f"Personalisation wave {cycle_number}: fresh={fresh_count}, followup={followup_count}",
            "",
        )
    except Exception as exc:
        log_agent(
            campaign_id,
            _AGENT,
            "ERROR",
            "run_personalisation failed",
            str(exc),
        )
        raise

    return {
        "fresh_count": fresh_count,
        "followup_count": followup_count,
        "total_drafted": fresh_count + followup_count,
    }
