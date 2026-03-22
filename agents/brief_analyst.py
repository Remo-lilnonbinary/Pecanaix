"""
Conversational Brief Analyst: multi-turn chat to collect event brief fields.
Uses Kimi K2 via Groq (OpenAI-compatible API).
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from tools.llm_router import get_llm

load_dotenv()

_BRIEF_FIELDS = (
    "event_type",
    "topic",
    "date",
    "location_city",
    "location_country",
    "target_attendance",
    "audience_constraints",
    "event_platform",
    "exclusions",
    "goal_beyond_attendance",
)


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


def _non_none_fields(session: Dict[str, Any]) -> Dict[str, Any]:
    return {k: session[k] for k in _BRIEF_FIELDS if session.get(k) is not None}


def _missing_fields(session: Dict[str, Any]) -> List[str]:
    return [k for k in _BRIEF_FIELDS if session.get(k) is None]


def _history_to_messages(system_prompt: str, history: List[Dict[str, str]]) -> List:
    msgs: List = [SystemMessage(content=system_prompt)]
    for turn in history:
        role = turn.get("role")
        content = turn.get("content", "")
        if role == "user":
            msgs.append(HumanMessage(content=content))
        elif role == "assistant":
            msgs.append(AIMessage(content=content))
    return msgs


def _format_conversation_for_extraction(history: List[Dict[str, str]]) -> str:
    lines: List[str] = []
    for turn in history:
        label = "User" if turn.get("role") == "user" else "Assistant"
        lines.append(f"{label}: {turn.get('content', '')}")
    return "\n".join(lines)


def create_brief_session() -> Dict[str, Any]:
    session: Dict[str, Any] = {
        "event_type": None,
        "topic": None,
        "date": None,
        "location_city": None,
        "location_country": None,
        "target_attendance": None,
        "audience_constraints": None,
        "event_platform": None,
        "exclusions": None,
        "goal_beyond_attendance": None,
        "conversation_history": [],
        "is_complete": False,
    }
    return session


def process_user_message(
    session: Dict[str, Any], user_message: str
) -> Tuple[str, Dict[str, Any]]:
    llm = get_llm()
    history: List[Dict[str, str]] = session["conversation_history"]

    history.append({"role": "user", "content": user_message})

    collected = _non_none_fields(session)
    still_needed = _missing_fields(session)
    system_prompt = """You are the Brief Analyst for Pecan, an AI-powered alumni engagement platform. You are having a conversation with a university alumni team member to understand an event they want to run. Your job is to collect: event type, topic, date, location, target attendance number, audience constraints (who should be invited — graduation years, departments, industries, interests), which event platform they use, any exclusions, and their goal beyond attendance. Ask ONE follow-up question at a time. Be conversational, warm, and specific. When you have enough information for all critical fields (at minimum: topic, date, location, target attendance, and at least one audience constraint), summarise everything back to the user and ask them to confirm. If they confirm, respond with exactly the word CONFIRMED on its own line at the end of your message."""

    system_prompt += (
        f'\n\nCurrent collected fields: {json.dumps(collected)}. '
        f"Still needed: {json.dumps(still_needed)}."
    )

    messages = _history_to_messages(system_prompt, history)
    response = llm.invoke(messages)
    assistant_text = response.content if hasattr(response, "content") else str(response)

    history.append({"role": "assistant", "content": assistant_text})

    conv_text = _format_conversation_for_extraction(history)
    extract_prompt = f"""Given this conversation, extract any event details mentioned. Return JSON with only the fields that have been clearly stated: event_type, topic, date, location_city, location_country, target_attendance, audience_constraints, event_platform, exclusions, goal_beyond_attendance. Only include fields where the user has given a clear answer. Return valid JSON only.

Conversation:
{conv_text}"""

    extract_resp = llm.invoke(extract_prompt)
    extract_text = (
        extract_resp.content if hasattr(extract_resp, "content") else str(extract_resp)
    )
    extracted = _safe_parse_json(extract_text, {})
    if isinstance(extracted, dict):
        for key in _BRIEF_FIELDS:
            if key in extracted and extracted[key] is not None:
                val = extracted[key]
                if key == "target_attendance":
                    try:
                        session[key] = int(val)
                    except (TypeError, ValueError):
                        pass
                else:
                    session[key] = val

    if "CONFIRMED" in assistant_text:
        session["is_complete"] = True

    return assistant_text, session


def get_parsed_brief(session: Dict[str, Any]) -> Dict[str, Any]:
    """Return collected fields for the pipeline; target_attendance as int; default country UK."""
    def _int_or_none(v: Any) -> Optional[int]:
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    country = session.get("location_country")
    if country is None:
        country = "UK"

    return {
        "event_type": session.get("event_type"),
        "topic": session.get("topic"),
        "date": session.get("date"),
        "location_city": session.get("location_city"),
        "location_country": country,
        "target_attendance": _int_or_none(session.get("target_attendance")),
        "audience_constraints": session.get("audience_constraints"),
        "event_platform": session.get("event_platform"),
        "exclusions": session.get("exclusions"),
        "goal_beyond_attendance": session.get("goal_beyond_attendance"),
    }


if __name__ == "__main__":
    greeting = (
        "Let's get started! I am here to help you. "
        "Tell me a bit about what you have in mind — what kind of event are you thinking of?"
    )
    print(greeting)
    sess = create_brief_session()
    while not sess["is_complete"]:
        try:
            user_input = input("\nYou: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not user_input:
            continue
        reply, sess = process_user_message(sess, user_input)
        print(f"\nBrief Analyst: {reply}")
    if sess["is_complete"]:
        brief = get_parsed_brief(sess)
        print("\n--- Parsed brief ---")
        print(json.dumps(brief, indent=2, ensure_ascii=False))
