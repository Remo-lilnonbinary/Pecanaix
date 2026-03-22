"""
Pecan API v2: dashboard, Brief Analyst, campaigns, integrations, webhooks, monitoring.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import uuid
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict

load_dotenv()

logger = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# --- Optional imports (missing modules must not crash startup) ---
create_brief_session = None
process_user_message = None
get_parsed_brief = None
try:
    from agents.brief_analyst import create_brief_session as _cbs
    from agents.brief_analyst import get_parsed_brief as _gpb
    from agents.brief_analyst import process_user_message as _pum

    create_brief_session = _cbs
    get_parsed_brief = _gpb
    process_user_message = _pum
except ImportError:
    pass

run_campaign = None
try:
    from agents.pipeline import run_campaign as _rc

    run_campaign = _rc
except ImportError:
    pass

run_data_integration = None
try:
    from agents.data_integrator import run_data_integration as _rdi

    run_data_integration = _rdi
except ImportError:
    pass

get_campaign = None
get_matches = None
get_outreach = None
get_agent_log = None
get_checkins = None
get_memories = None
get_all_alumni_unfiltered = None
get_all_cycles = None
save_checkin = None
log_agent = None
get_connection = None
try:
    from tools.database import get_agent_log as _gal
    from tools.database import get_all_alumni_unfiltered as _gaau
    from tools.database import get_all_cycles as _gac
    from tools.database import get_campaign as _gc
    from tools.database import get_checkins as _gch
    from tools.database import get_connection as _gconn
    from tools.database import get_matches as _gm
    from tools.database import get_memories as _gmem
    from tools.database import get_outreach as _go
    from tools.database import log_agent as _la
    from tools.database import save_checkin as _sc

    get_campaign = _gc
    get_matches = _gm
    get_outreach = _go
    get_agent_log = _gal
    get_checkins = _gch
    get_memories = _gmem
    get_all_alumni_unfiltered = _gaau
    get_all_cycles = _gac
    save_checkin = _sc
    log_agent = _la
    get_connection = _gconn
except ImportError:
    pass

_sessions: Dict[str, Dict[str, Any]] = {}

_BRIEF_FIELD_KEYS = (
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


def _collected_fields(session: Dict[str, Any]) -> Dict[str, Any]:
    return {k: session[k] for k in _BRIEF_FIELD_KEYS if session.get(k) is not None}


app = FastAPI(title="Pecan API", version="2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Pydantic models ---


class BriefMessageBody(BaseModel):
    message: str


class CampaignLaunchBody(BaseModel):
    session_id: Optional[str] = None
    parsed_brief: Optional[Dict[str, Any]] = None


class EventbriteWebhookBody(BaseModel):
    model_config = ConfigDict(extra="ignore")

    email: Optional[str] = None
    attendee_email: Optional[str] = None
    checked_in: bool = True
    campaign_id: Optional[int] = None


# --- Root ---


@app.get("/")
def root():
    try:
        return {"name": "Pecan API", "version": "2.0", "status": "running"}
    except Exception as exc:
        return {"name": "Pecan API", "version": "2.0", "status": "error", "detail": str(exc)}


# --- Brief Analyst ---


@app.post("/api/brief/start")
def brief_start():
    try:
        if create_brief_session is None:
            raise HTTPException(status_code=503, detail="Brief Analyst module unavailable")
        sid = str(uuid.uuid4())
        _sessions[sid] = create_brief_session()
        return {
            "session_id": sid,
            "greeting": (
                "Let's get started! I am here to help you. "
                "Tell me a bit about what you have in mind — what kind of event are you thinking of?"
            ),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/brief/{session_id}/message")
def brief_message(session_id: str, body: BriefMessageBody):
    try:
        if process_user_message is None:
            raise HTTPException(status_code=503, detail="Brief Analyst module unavailable")
        if session_id not in _sessions:
            raise HTTPException(status_code=404, detail="Session not found")
        session = _sessions[session_id]
        agent_text, session = process_user_message(session, body.message)
        _sessions[session_id] = session
        return {
            "response": agent_text,
            "is_complete": bool(session.get("is_complete")),
            "collected_fields": _collected_fields(session),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/brief/{session_id}")
def brief_get(session_id: str):
    try:
        if session_id not in _sessions:
            raise HTTPException(status_code=404, detail="Session not found")
        session = _sessions[session_id]
        return {
            "session_id": session_id,
            "is_complete": bool(session.get("is_complete")),
            "collected_fields": _collected_fields(session),
            "fields": {k: session.get(k) for k in _BRIEF_FIELD_KEYS},
            "conversation_history": session.get("conversation_history", []),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# --- Campaign ---


@app.post("/api/campaign/launch")
def campaign_launch(body: CampaignLaunchBody):
    try:
        if run_campaign is None:
            raise HTTPException(status_code=503, detail="Campaign pipeline unavailable")

        has_session = body.session_id is not None
        has_brief = body.parsed_brief is not None
        if has_session == has_brief:
            raise HTTPException(
                status_code=400,
                detail="Provide exactly one of session_id or parsed_brief",
            )

        if body.session_id is not None:
            if get_parsed_brief is None:
                raise HTTPException(status_code=503, detail="Brief Analyst module unavailable")
            if body.session_id not in _sessions:
                raise HTTPException(status_code=404, detail="Session not found")
            session = _sessions[body.session_id]
            if not session.get("is_complete"):
                raise HTTPException(
                    status_code=400,
                    detail="Brief session is not complete; finish the conversation first",
                )
            parsed_brief = get_parsed_brief(session)
        else:
            parsed_brief = body.parsed_brief or {}

        event = threading.Event()
        state: Dict[str, Any] = {}

        def on_created(cid: int) -> None:
            state["campaign_id"] = cid
            event.set()

        def worker() -> None:
            try:
                run_campaign(parsed_brief, on_campaign_created=on_created)
            except Exception as exc:
                state["error"] = str(exc)
            finally:
                if not event.is_set():
                    event.set()

        t = threading.Thread(target=worker, daemon=True)
        t.start()

        if not event.wait(timeout=120.0):
            raise HTTPException(
                status_code=504,
                detail="Timed out waiting for campaign record to be created",
            )
        if state.get("error") and "campaign_id" not in state:
            raise HTTPException(status_code=500, detail=state["error"])
        if "campaign_id" not in state:
            raise HTTPException(status_code=500, detail="Campaign was not created")

        return {"campaign_id": state["campaign_id"], "status": "launched"}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/campaign/{campaign_id}")
def campaign_get(campaign_id: int):
    try:
        if any(
            f is None
            for f in (
                get_campaign,
                get_all_cycles,
                get_matches,
                get_outreach,
                get_checkins,
                get_agent_log,
                get_memories,
            )
        ):
            raise HTTPException(status_code=503, detail="Database helpers unavailable")
        c = get_campaign(campaign_id)
        if c is None:
            raise HTTPException(status_code=404, detail="Campaign not found")
        return {
            "campaign": c,
            "cycles": get_all_cycles(campaign_id),
            "matches": get_matches(campaign_id),
            "outreach": get_outreach(campaign_id),
            "checkins": get_checkins(campaign_id),
            "agent_log": get_agent_log(campaign_id),
            "memories": get_memories(),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/campaign/{campaign_id}/status")
def campaign_status(campaign_id: int):
    try:
        if get_campaign is None or get_outreach is None:
            raise HTTPException(status_code=503, detail="Database helpers unavailable")
        campaign = get_campaign(campaign_id)
        if campaign is None:
            raise HTTPException(status_code=404, detail="Campaign not found")
        outreach = get_outreach(campaign_id)
        total_acceptances = sum(
            1 for row in outreach if (row.get("status") or "").upper() == "ACCEPTED"
        )
        return {
            "status": campaign.get("status"),
            "current_cycle": campaign.get("current_cycle"),
            "campaign_phase": campaign.get("campaign_phase"),
            "total_acceptances": total_acceptances,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# --- Data ---


@app.get("/api/alumni")
def alumni_all():
    try:
        if get_all_alumni_unfiltered is None:
            raise HTTPException(status_code=503, detail="Database helpers unavailable")
        return get_all_alumni_unfiltered()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/alumni/count")
def alumni_count():
    try:
        if get_connection is None:
            raise HTTPException(status_code=503, detail="Database helpers unavailable")
        conn = get_connection()
        try:
            total = conn.execute("SELECT COUNT(*) FROM alumni").fetchone()[0]
            eligible = conn.execute(
                "SELECT COUNT(*) FROM alumni WHERE gdpr_consent = 1 AND email_valid = 1"
            ).fetchone()[0]
        finally:
            conn.close()
        return {"total": total, "eligible": eligible}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# --- Integrations ---


@app.post("/api/integrations/sync")
def integrations_sync():
    try:
        if run_data_integration is None:
            raise HTTPException(status_code=503, detail="Data integrator unavailable")

        def worker() -> None:
            try:
                run_data_integration(0)
            except Exception:
                logger.exception("Background data integration failed")

        threading.Thread(target=worker, daemon=True).start()
        return {"status": "syncing"}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# --- Webhooks ---


@app.post("/api/webhook/eventbrite")
def webhook_eventbrite(body: EventbriteWebhookBody):
    try:
        if any(f is None for f in (get_connection, save_checkin, log_agent)):
            raise HTTPException(status_code=503, detail="Database helpers unavailable")

        email = (body.email or body.attendee_email or "").strip()
        if not email:
            raise HTTPException(status_code=400, detail="Missing attendee email")

        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT id, name FROM alumni WHERE LOWER(email) = LOWER(?)",
                (email,),
            ).fetchone()
        finally:
            conn.close()

        if not row:
            return {"success": False, "detail": "Alumni not found", "email": email}

        alumni_dict = dict(row)
        alumni_id = alumni_dict["id"]
        name = alumni_dict.get("name") or email

        campaign_id = body.campaign_id
        if campaign_id is None:
            conn = get_connection()
            try:
                r2 = conn.execute(
                    "SELECT id FROM campaigns ORDER BY id DESC LIMIT 1"
                ).fetchone()
            finally:
                conn.close()
            if not r2:
                raise HTTPException(
                    status_code=400,
                    detail="No campaign_id provided and no campaigns exist in the database",
                )
            campaign_id = r2[0]

        save_checkin(campaign_id, alumni_id, checked_in=body.checked_in, source="Eventbrite")
        log_agent(
            campaign_id,
            "Check-in Tracker",
            "WEBHOOK",
            f"Eventbrite check-in: {name} ({email}) — "
            f"{'checked in' if body.checked_in else 'not checked in'}",
            "Received from Eventbrite webhook.",
        )
        return {
            "success": True,
            "alumni_name": name,
            "checked_in": body.checked_in,
            "campaign_id": campaign_id,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# --- Monitoring ---


@app.get("/api/agent-log/{campaign_id}")
def agent_log_get(campaign_id: int):
    try:
        if get_agent_log is None:
            raise HTTPException(status_code=503, detail="Database helpers unavailable")
        return get_agent_log(campaign_id)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/memories")
def memories_get():
    try:
        if get_memories is None:
            raise HTTPException(status_code=503, detail="Database helpers unavailable")
        return get_memories()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/cycles/{campaign_id}")
def cycles_get(campaign_id: int):
    try:
        if get_all_cycles is None:
            raise HTTPException(status_code=503, detail="Database helpers unavailable")
        return get_all_cycles(campaign_id)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
