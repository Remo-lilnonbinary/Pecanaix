"""
Two-loop LangGraph campaign pipeline (Loop 1: data → match → quality; Loop 2: batch → personalise → outreach → track).

Brief Analyst is out of band; this graph expects a completed `parsed_brief` dict.
"""

from __future__ import annotations

import gc
import json
import logging
import operator
import sys
import time
from datetime import datetime
from typing import Annotated, Any, Callable, Optional, TypedDict

from dotenv import load_dotenv

load_dotenv()

_ROOT = __file__.rsplit("agents", 1)[0]
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# --- Optional imports (missing modules must not crash import of this file) ---
run_data_integration = None
run_matching = None
run_quality_check = None
run_personalisation = None
run_outreach = None
run_response_tracking = None
run_report = None

create_campaign = None
update_campaign = None
log_agent = None
get_all_alumni = None
get_matches = None
get_unfollowed_warm_leads = None
create_cycle = None
update_cycle = None
get_all_cycles = None
get_connection = None

StateGraph: Any = None
END: Any = None

try:
    from langgraph.graph import END as _END
    from langgraph.graph import StateGraph as _StateGraph

    END = _END
    StateGraph = _StateGraph
except ImportError as e:
    logger.error("langgraph import failed: %s", e)

try:
    from agents.campaign_reporter import run_report as _rr
except ImportError as e:
    logger.error("agents.campaign_reporter import failed: %s", e)
else:
    run_report = _rr

try:
    from agents.data_integrator import run_data_integration as _rdi
except ImportError as e:
    logger.error("agents.data_integrator import failed: %s", e)
else:
    run_data_integration = _rdi

try:
    from agents.matching_agent import run_matching as _rm
except ImportError as e:
    logger.error("agents.matching_agent import failed: %s", e)
else:
    run_matching = _rm

try:
    from agents.outreach_agent import run_outreach as _ro
except ImportError as e:
    logger.error("agents.outreach_agent import failed: %s", e)
else:
    run_outreach = _ro

try:
    from agents.personalisation_agent import run_personalisation as _rp
except ImportError as e:
    logger.error("agents.personalisation_agent import failed: %s", e)
else:
    run_personalisation = _rp

try:
    from agents.quality_checker import run_quality_check as _rqc
except ImportError as e:
    logger.error("agents.quality_checker import failed: %s", e)
else:
    run_quality_check = _rqc

try:
    from agents.response_tracker import run_response_tracking as _rrt
except ImportError as e:
    logger.error("agents.response_tracker import failed: %s", e)
else:
    run_response_tracking = _rrt

try:
    from tools.database import create_campaign as _cc
    from tools.database import create_cycle as _cyc
    from tools.database import get_all_alumni as _gaa
    from tools.database import get_all_cycles as _gacyc
    from tools.database import get_connection as _gconn
    from tools.database import get_matches as _gm
    from tools.database import get_unfollowed_warm_leads as _guwl
    from tools.database import log_agent as _la
    from tools.database import update_campaign as _uc
    from tools.database import update_cycle as _ucyc
except ImportError as e:
    logger.error("tools.database import failed: %s", e)
else:
    create_campaign = _cc
    update_campaign = _uc
    log_agent = _la
    get_all_alumni = _gaa
    get_matches = _gm
    get_unfollowed_warm_leads = _guwl
    create_cycle = _cyc
    update_cycle = _ucyc
    get_all_cycles = _gacyc
    get_connection = _gconn


class CampaignState(TypedDict):
    """Graph state; `continue_cycling` supports response_tracker → batch routing."""

    campaign_id: int
    parsed_brief: dict[str, Any]
    target_attendance: int
    target_acceptances: float
    total_pool_size: int
    alumni_pool: list[dict[str, Any]]
    selected_batch: list[dict[str, Any]]
    warm_leads: list[dict[str, Any]]
    total_acceptances: int
    actual_bounce_rate: float
    current_cycle: int
    max_cycles: int
    cycle_acceptance_rates: list[float]
    planning_date: str
    event_date: str
    campaign_phase: str
    diagnosis: Optional[str]
    scenario: Optional[str]
    goal_met: bool
    continue_cycling: bool
    errors: Annotated[list[str], operator.add]


def _log_pipeline(campaign_id: int, action: str, decision: str, reasoning: str) -> None:
    if log_agent is None:
        logger.info("[campaign %s] %s %s — %s", campaign_id, action, decision, reasoning)
        return
    try:
        log_agent(campaign_id, "Campaign Pipeline", action, decision, reasoning)
    except Exception as exc:
        logger.error("log_agent failed: %s", exc)


def _append_errors(
    state: CampaignState, messages: list[str]
) -> dict[str, list[str]]:
    return {"errors": list(messages)}


def _contacted_alumni_ids(campaign_id: int) -> set[int]:
    if get_connection is None:
        return set()
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT DISTINCT alumni_id FROM outreach_messages WHERE campaign_id = ?",
            (campaign_id,),
        ).fetchall()
    finally:
        conn.close()
    out: set[int] = set()
    for r in rows:
        try:
            out.add(int(dict(r)["alumni_id"]))
        except (TypeError, ValueError, KeyError):
            continue
    return out


def _alumni_rows_by_ids(ids: list[int]) -> list[dict[str, Any]]:
    if not ids:
        return []
    if get_connection is not None:
        conn = get_connection()
        try:
            ph = ",".join("?" * len(ids))
            rows = conn.execute(
                f"SELECT * FROM alumni WHERE id IN ({ph})",
                tuple(ids),
            ).fetchall()
        finally:
            conn.close()
        return [dict(r) for r in rows]
    if get_all_alumni is not None:
        try:
            by_id = {int(a["id"]): a for a in get_all_alumni() if a.get("id") is not None}
        except Exception as exc:
            logger.error("get_all_alumni fallback failed: %s", exc)
            return []
        return [by_id[i] for i in ids if i in by_id]
    return []


def _enrich_warm_leads_for_personalisation(campaign_id: int) -> list[dict[str, Any]]:
    if get_unfollowed_warm_leads is None:
        return []
    try:
        wl = get_unfollowed_warm_leads(campaign_id) or []
    except Exception as exc:
        logger.error("get_unfollowed_warm_leads failed: %s", exc)
        return []
    ids: list[int] = []
    for r in wl:
        aid = r.get("alumni_id")
        if aid is not None:
            try:
                ids.append(int(aid))
            except (TypeError, ValueError):
                continue
    alumni = _alumni_rows_by_ids(ids)
    by_id = {int(a["id"]): a for a in alumni if a.get("id") is not None}
    ordered: list[dict[str, Any]] = []
    for i in ids:
        if i in by_id:
            ordered.append(dict(by_id[i]))
    return ordered


def data_integrator_node(state: CampaignState) -> dict[str, Any]:
    gc.collect()
    time.sleep(1)
    cid = state["campaign_id"]
    updates: dict[str, Any] = {"campaign_phase": "LOOP_1"}
    if update_campaign:
        try:
            update_campaign(cid, campaign_phase="LOOP_1")
        except Exception as exc:
            logger.error("update_campaign LOOP_1: %s", exc)
            _log_pipeline(cid, "ERROR", "update_campaign", str(exc))
    if run_data_integration is None:
        msg = "run_data_integration unavailable (import failed)"
        logger.error(msg)
        _log_pipeline(cid, "ERROR", "data_integrator", msg)
        return {**updates, **_append_errors(state, [msg])}
    try:
        run_data_integration(cid)
    except Exception as exc:
        err = f"data_integration: {exc}"
        logger.error(err)
        _log_pipeline(cid, "ERROR", "run_data_integration", str(exc))
        return {**updates, **_append_errors(state, [err])}
    _log_pipeline(cid, "OK", "data_integrator", "Data integration finished")
    return updates


def matching_node(state: CampaignState) -> dict[str, Any]:
    gc.collect()
    time.sleep(1)
    cid = state["campaign_id"]
    pb = state.get("parsed_brief") or {}
    ta = int(state.get("target_attendance") or 1)
    if run_matching is None:
        msg = "run_matching unavailable (import failed)"
        logger.error(msg)
        return {"alumni_pool": [], **_append_errors(state, [msg])}
    try:
        result = run_matching(cid, pb, ta)
    except Exception as exc:
        err = f"matching: {exc}"
        logger.error(err)
        _log_pipeline(cid, "ERROR", "run_matching", str(exc))
        return {"alumni_pool": [], **_append_errors(state, [err])}

    pool = list(result.get("pool") or [])
    insufficient = bool(result.get("insufficient_matches"))
    upd: dict[str, Any] = {"alumni_pool": pool}
    errs: list[str] = []
    if insufficient:
        sug = (result.get("suggestion") or "").strip()
        errs.append(
            "insufficient_matches: pool may be too weak for target."
            + (f" {sug}" if sug else "")
        )
        _log_pipeline(cid, "WARN", "matching", errs[-1])
    if update_campaign:
        try:
            update_campaign(
                cid,
                target_attendance=ta,
                target_acceptances=float(state.get("target_acceptances") or 1.2 * ta),
                total_pool_size=len(pool),
            )
        except Exception as exc:
            logger.error("update_campaign after matching: %s", exc)
    if get_matches is not None:
        try:
            _log_pipeline(
                cid,
                "INFO",
                "matches_in_db",
                str(len(get_matches(cid, selected_only=True))),
            )
        except Exception as exc:
            logger.error("get_matches log: %s", exc)
    return {**upd, **_append_errors(state, errs)}


def quality_check_node(state: CampaignState) -> dict[str, Any]:
    gc.collect()
    time.sleep(1)
    cid = state["campaign_id"]
    pool = state.get("alumni_pool") or []
    if run_quality_check is None:
        msg = "run_quality_check unavailable (import failed)"
        logger.error(msg)
        return {**_append_errors(state, [msg])}
    try:
        qc = run_quality_check(cid, pool)
    except Exception as exc:
        err = f"quality_check: {exc}"
        logger.error(err)
        _log_pipeline(cid, "ERROR", "run_quality_check", str(exc))
        return {**_append_errors(state, [err])}

    passed = bool(qc.get("passed"))
    if not passed:
        detail = f"Quality check failed (critical flags: {qc.get('critical_count', 0)})"
        _log_pipeline(cid, "BLOCK", "quality_check", detail)
        if update_campaign:
            try:
                update_campaign(cid, campaign_phase="LOOP_1_BLOCKED")
            except Exception as exc:
                logger.error("update_campaign LOOP_1_BLOCKED: %s", exc)
        return {**_append_errors(state, [detail])}
    _log_pipeline(cid, "OK", "quality_check", "Passed")
    if update_campaign:
        try:
            update_campaign(cid, campaign_phase="LOOP_1_OK")
        except Exception as exc:
            logger.error("update_campaign LOOP_1_OK: %s", exc)
    return {}


def _eligible_pool(
    alumni_pool: list[dict[str, Any]], contacted: set[int]
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for a in alumni_pool:
        aid = a.get("id")
        if aid is None:
            continue
        try:
            i = int(aid)
        except (TypeError, ValueError):
            continue
        if i not in contacted:
            out.append(a)
    return out


def calculate_batch_node(state: CampaignState) -> dict[str, Any]:
    gc.collect()
    time.sleep(1)
    cid = state["campaign_id"]
    ta = max(1, int(state.get("target_attendance") or 1))
    X = ta
    rates = state.get("cycle_acceptance_rates") or [0.35, 0.25, 0.18, 0.12]
    max_c = int(state.get("max_cycles") or 4)

    next_cycle = int(state.get("current_cycle") or 0) + 1
    if next_cycle > max_c:
        _log_pipeline(
            cid,
            "SKIP",
            "calculate_batch",
            f"Would exceed max_cycles={max_c}; routing should end loop.",
        )
        return {
            "current_cycle": max_c,
            "selected_batch": [],
            "warm_leads": [],
            "continue_cycling": False,
        }

    scenario_prior = state.get("scenario")
    alumni_pool = state.get("alumni_pool") or []
    contacted = _contacted_alumni_ids(cid)
    eligible = _eligible_pool(alumni_pool, contacted)
    remaining = len(eligible)

    if next_cycle == 1:
        batch_cap = 2 * X
    else:
        if scenario_prior == "B":
            batch_cap = min(int(1.5 * X), remaining)
        else:
            batch_cap = min(2 * X, remaining)

    batch = eligible[: max(0, batch_cap)]
    warm = _enrich_warm_leads_for_personalisation(cid)

    exp_rate = rates[next_cycle - 1] if next_cycle - 1 < len(rates) else (rates[-1] if rates else 0.2)
    if create_cycle:
        try:
            create_cycle(cid, next_cycle, exp_rate)
        except Exception as exc:
            logger.error("create_cycle: %s", exc)
            _log_pipeline(cid, "ERROR", "create_cycle", str(exc))

    upd: dict[str, Any] = {
        "current_cycle": next_cycle,
        "selected_batch": batch,
        "warm_leads": warm,
        "campaign_phase": "LOOP_2",
        "continue_cycling": True,
    }
    if update_campaign:
        try:
            update_campaign(cid, current_cycle=next_cycle, campaign_phase="LOOP_2")
        except Exception as exc:
            logger.error("update_campaign calculate_batch: %s", exc)
    _log_pipeline(
        cid,
        "BATCH",
        f"cycle {next_cycle}",
        f"batch_size={len(batch)}, warm_leads={len(warm)}, scenario_prior={scenario_prior!r}",
    )
    return upd


def personalisation_node(state: CampaignState) -> dict[str, Any]:
    gc.collect()
    time.sleep(1)
    cid = state["campaign_id"]
    cyc = int(state.get("current_cycle") or 0)
    batch = state.get("selected_batch") or []
    warm = state.get("warm_leads") or []
    pb = state.get("parsed_brief") or {}
    diag = state.get("diagnosis")

    if run_personalisation is None:
        msg = "run_personalisation unavailable (import failed)"
        logger.error(msg)
        return {**_append_errors(state, [msg])}

    try:
        run_personalisation(
            cid,
            cyc,
            batch,
            pb,
            warm_leads_to_followup=warm or None,
            diagnosis=diag,
        )
    except Exception as exc:
        err = f"personalisation: {exc}"
        logger.error(err)
        _log_pipeline(cid, "ERROR", "run_personalisation", str(exc))
        return {**_append_errors(state, [err])}

    if update_campaign:
        try:
            update_campaign(cid, campaign_phase="LOOP_2_PERSONALISED")
        except Exception as exc:
            logger.error("update_campaign personalisation: %s", exc)
    _log_pipeline(cid, "OK", "personalisation", f"cycle {cyc}")
    return {}


def outreach_node(state: CampaignState) -> dict[str, Any]:
    gc.collect()
    time.sleep(1)
    cid = state["campaign_id"]
    cyc = int(state.get("current_cycle") or 0)
    bounce = float(state.get("actual_bounce_rate") or 0.10)

    if run_outreach is None:
        msg = "run_outreach unavailable (import failed)"
        logger.error(msg)
        return {**_append_errors(state, [msg])}

    try:
        out = run_outreach(cid, cyc, bounce)
    except Exception as exc:
        err = f"outreach: {exc}"
        logger.error(err)
        _log_pipeline(cid, "ERROR", "run_outreach", str(exc))
        return {**_append_errors(state, [err])}

    obs = float(out.get("observed_bounce_rate") or 0.0)
    upd = {"actual_bounce_rate": obs}
    if update_campaign:
        try:
            update_campaign(cid, campaign_phase="LOOP_2_OUTREACH")
        except Exception as exc:
            logger.error("update_campaign outreach: %s", exc)
    _log_pipeline(
        cid,
        "OK",
        "outreach",
        f"cycle {cyc} observed_bounce_rate={obs}",
    )
    return upd


def response_tracker_node(state: CampaignState) -> dict[str, Any]:
    gc.collect()
    time.sleep(1)
    time.sleep(3)
    cid = state["campaign_id"]
    cyc = int(state.get("current_cycle") or 0)
    target_acc = float(state.get("target_acceptances") or 0.0)
    rates = state.get("cycle_acceptance_rates") or [0.35, 0.25, 0.18, 0.12]
    max_c = int(state.get("max_cycles") or 4)

    if run_response_tracking is None:
        msg = "run_response_tracking unavailable (import failed)"
        logger.error(msg)
        return {
            "goal_met": False,
            "continue_cycling": False,
            **_append_errors(state, [msg]),
        }

    try:
        rt = run_response_tracking(cid, cyc, target_acc, rates)
    except Exception as exc:
        err = f"response_tracking: {exc}"
        logger.error(err)
        _log_pipeline(cid, "ERROR", "run_response_tracking", str(exc))
        return {
            "goal_met": False,
            "continue_cycling": False,
            **_append_errors(state, [err]),
        }

    goal_met = bool(rt.get("goal_met"))
    cont = bool(rt.get("continue_cycling"))
    if cyc >= max_c:
        cont = False
    total_acc = int(rt.get("total_acceptances", state.get("total_acceptances") or 0))

    diagnosis = rt.get("diagnosis")
    if isinstance(diagnosis, str):
        pass
    else:
        diagnosis = state.get("diagnosis")

    scenario = rt.get("scenario")
    if scenario is not None and not isinstance(scenario, str):
        scenario = str(scenario)

    upd: dict[str, Any] = {
        "goal_met": goal_met,
        "continue_cycling": cont and not goal_met,
        "total_acceptances": total_acc,
        "diagnosis": diagnosis,
        "scenario": scenario if scenario is not None else state.get("scenario"),
    }

    if update_campaign:
        try:
            update_campaign(
                cid,
                total_replied=total_acc,
                campaign_phase="LOOP_2_RESPONSE",
            )
        except Exception as exc:
            logger.error("update_campaign response_tracker: %s", exc)

    # Sync cycle row if possible
    if get_all_cycles and update_cycle:
        try:
            cycles = get_all_cycles(cid) or []
            row = next(
                (c for c in cycles if int(c.get("cycle_number") or -1) == cyc),
                None,
            )
            if row and row.get("id") is not None:
                update_cycle(
                    int(row["id"]),
                    status="COMPLETED",
                    completed_at=datetime.now().isoformat(),
                )
        except Exception as exc:
            logger.error("update_cycle after response: %s", exc)

    _log_pipeline(
        cid,
        "OK",
        "response_tracker",
        f"cycle {cyc} goal_met={goal_met} continue={upd['continue_cycling']}",
    )
    return upd


def reporter_node(state: CampaignState) -> dict[str, Any]:
    gc.collect()
    time.sleep(1)
    time.sleep(3)
    cid = state["campaign_id"]
    if run_report is None:
        msg = "run_report unavailable (import failed)"
        logger.error(msg)
        if update_campaign:
            try:
                update_campaign(cid, campaign_phase="COMPLETE_ERROR")
            except Exception:
                pass
        return {"campaign_phase": "COMPLETE_ERROR", **_append_errors(state, [msg])}

    try:
        run_report(cid)
    except Exception as exc:
        err = f"run_report: {exc}"
        logger.error(err)
        _log_pipeline(cid, "ERROR", "run_report", str(exc))
        if update_campaign:
            try:
                update_campaign(cid, campaign_phase="COMPLETE_ERROR")
            except Exception:
                pass
        return {"campaign_phase": "COMPLETE_ERROR", **_append_errors(state, [err])}

    if update_campaign:
        try:
            update_campaign(
                cid,
                campaign_phase="COMPLETE",
                completed_at=datetime.now().isoformat(),
            )
        except Exception as exc:
            logger.error("update_campaign complete: %s", exc)

    _log_pipeline(cid, "OK", "reporter", "Campaign report finished")
    return {"campaign_phase": "COMPLETE", "goal_met": bool(state.get("goal_met"))}


def route_after_quality(state: CampaignState) -> str:
    errs = state.get("errors") or []
    if any(
        "Quality check failed" in e
        or e.startswith("quality_check:")
        or "run_quality_check unavailable" in e
        for e in errs
    ):
        return "end"
    return "calculate_batch_node"


def route_after_response(state: CampaignState) -> str:
    if state.get("goal_met"):
        return "reporter_node"
    max_c = int(state.get("max_cycles") or 4)
    cur = int(state.get("current_cycle") or 0)
    if not state.get("continue_cycling") or cur >= max_c:
        return "reporter_node"
    cid = state["campaign_id"]
    pool = state.get("alumni_pool") or []
    contacted = _contacted_alumni_ids(cid)
    elig = _eligible_pool(pool, contacted)
    wn = 0
    if get_unfollowed_warm_leads is not None:
        try:
            wn = len(get_unfollowed_warm_leads(cid) or [])
        except Exception as exc:
            logger.error("route_after_response warm leads: %s", exc)
    if not elig and wn == 0:
        return "reporter_node"
    return "calculate_batch_node"


def build_graph():
    if StateGraph is None or END is None:
        raise RuntimeError("langgraph is not available; install langgraph")

    g = StateGraph(CampaignState)
    g.add_node("data_integrator_node", data_integrator_node)
    g.add_node("matching_node", matching_node)
    g.add_node("quality_check_node", quality_check_node)
    g.add_node("calculate_batch_node", calculate_batch_node)
    g.add_node("personalisation_node", personalisation_node)
    g.add_node("outreach_node", outreach_node)
    g.add_node("response_tracker_node", response_tracker_node)
    g.add_node("reporter_node", reporter_node)

    g.set_entry_point("data_integrator_node")
    g.add_edge("data_integrator_node", "matching_node")
    g.add_edge("matching_node", "quality_check_node")
    g.add_conditional_edges(
        "quality_check_node",
        route_after_quality,
        {"calculate_batch_node": "calculate_batch_node", "end": END},
    )
    g.add_edge("calculate_batch_node", "personalisation_node")
    g.add_edge("personalisation_node", "outreach_node")
    g.add_edge("outreach_node", "response_tracker_node")
    g.add_conditional_edges(
        "response_tracker_node",
        route_after_response,
        {
            "reporter_node": "reporter_node",
            "calculate_batch_node": "calculate_batch_node",
        },
    )
    g.add_edge("reporter_node", END)
    return g.compile()


def _initial_state(
    campaign_id: int,
    parsed_brief: dict[str, Any],
    planning_date: Optional[str],
    event_date: Optional[str],
) -> CampaignState:
    ta = int(parsed_brief.get("target_attendance") or 50)
    ta = max(1, ta)
    target_acc = 1.2 * float(ta)
    pool_size = 5 * ta
    return CampaignState(
        campaign_id=campaign_id,
        parsed_brief=dict(parsed_brief),
        target_attendance=ta,
        target_acceptances=target_acc,
        total_pool_size=pool_size,
        alumni_pool=[],
        selected_batch=[],
        warm_leads=[],
        total_acceptances=0,
        actual_bounce_rate=0.10,
        current_cycle=0,
        max_cycles=4,
        cycle_acceptance_rates=[0.35, 0.25, 0.18, 0.12],
        planning_date=planning_date or "",
        event_date=event_date or "",
        campaign_phase="INIT",
        diagnosis=None,
        scenario=None,
        goal_met=False,
        continue_cycling=True,
        errors=[],
    )


def run_campaign(
    parsed_brief: dict[str, Any],
    planning_date: Optional[str] = None,
    event_date: Optional[str] = None,
    on_campaign_created: Optional[Callable[[int], None]] = None,
) -> int:
    if create_campaign is None or update_campaign is None:
        logger.error("Database helpers unavailable; cannot run campaign")
        raise RuntimeError("tools.database not available")

    raw = json.dumps(parsed_brief, ensure_ascii=False)
    try:
        campaign_id = int(create_campaign(raw))
    except Exception as exc:
        logger.error("create_campaign failed: %s", exc)
        raise

    st = _initial_state(campaign_id, parsed_brief, planning_date, event_date)
    try:
        update_campaign(
            campaign_id,
            parsed_brief=raw,
            target_attendance=st["target_attendance"],
            target_acceptances=st["target_acceptances"],
            total_pool_size=st["total_pool_size"],
            planning_date=st.get("planning_date") or None,
            event_date=st.get("event_date") or None,
            max_cycles=st["max_cycles"],
            current_cycle=0,
            campaign_phase="INIT",
        )
    except Exception as exc:
        logger.error("update_campaign initial sync: %s", exc)

    if on_campaign_created is not None:
        try:
            on_campaign_created(campaign_id)
        except Exception as exc:
            logger.error("on_campaign_created callback failed: %s", exc)

    try:
        graph = build_graph()
    except Exception as exc:
        logger.error("build_graph failed: %s", exc)
        raise

    try:
        graph.invoke(st)
    except Exception as exc:
        logger.error("graph.invoke failed: %s", exc)
        try:
            update_campaign(campaign_id, campaign_phase="FAILED")
        except Exception:
            pass
        raise

    return campaign_id


if __name__ == "__main__":
    sample_brief: dict[str, Any] = {
        "event_type": "Careers panel",
        "topic": "AI in finance",
        "location_city": "London",
        "target_attendance": 10,
        "audience_constraints": "Graduates from the last 10 years with quantitative backgrounds",
    }
    cid = run_campaign(sample_brief, planning_date="2025-06-01", event_date="2025-07-15")
    print(json.dumps({"campaign_id": cid, "status": "finished"}, indent=2))
    try:
        from tools.database import get_campaign

        camp = get_campaign(cid)
        print(json.dumps(camp, indent=2, default=str))
    except Exception as exc:
        print("Could not load campaign row:", exc)
