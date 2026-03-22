"""
Data Integrator agent: pull from connected sources, dedupe, GDPR filter, SQLite + ChromaDB.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import warnings
from typing import Any, Callable, Dict, List, Optional, Tuple

# Project root on path for `tools.*` imports
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

logger = logging.getLogger(__name__)

get_all_alumni: Optional[Callable[..., List[Dict[str, Any]]]] = None
get_all_alumni_unfiltered: Optional[Callable[..., List[Dict[str, Any]]]] = None
log_agent: Optional[Callable[..., None]] = None
get_connection: Optional[Callable[..., Any]] = None

connect_salesforce: Optional[Callable[..., Any]] = None
pull_alumni_from_salesforce: Optional[Callable[..., List[Dict[str, Any]]]] = None

get_eventbrite_headers: Optional[Callable[..., Any]] = None
pull_events: Optional[Callable[..., List[Dict[str, Any]]]] = None
pull_attendees: Optional[Callable[..., List[Dict[str, Any]]]] = None

ingest_all_files: Optional[Callable[..., Dict[str, Any]]] = None

embed_alumni: Optional[Callable[..., int]] = None
get_client: Optional[Callable[..., Any]] = None

filter_consented: Optional[
    Callable[..., Tuple[List[Dict[str, Any]], int, int]]
] = None
log_gdpr_action: Optional[Callable[..., None]] = None

try:
    from tools.database import (
        get_all_alumni as _ga,
        get_all_alumni_unfiltered as _gau,
        get_connection as _gc,
        log_agent as _la,
    )

    get_all_alumni = _ga
    get_all_alumni_unfiltered = _gau
    get_connection = _gc
    log_agent = _la
except ImportError as e:
    warnings.warn(f"tools.database import failed: {e}", stacklevel=2)

try:
    from tools.salesforce_connector import (
        connect_salesforce as _csf,
        pull_alumni_from_salesforce as _psf,
    )

    connect_salesforce = _csf
    pull_alumni_from_salesforce = _psf
except ImportError as e:
    warnings.warn(f"tools.salesforce_connector import failed: {e}", stacklevel=2)

try:
    from tools.eventbrite_connector import (
        get_eventbrite_headers as _geb,
        pull_attendees as _pa,
        pull_events as _pe,
    )

    get_eventbrite_headers = _geb
    pull_events = _pe
    pull_attendees = _pa
except ImportError as e:
    warnings.warn(f"tools.eventbrite_connector import failed: {e}", stacklevel=2)

try:
    from tools.file_ingestor import ingest_all_files as _iaf

    ingest_all_files = _iaf
except ImportError as e:
    warnings.warn(f"tools.file_ingestor import failed: {e}", stacklevel=2)

try:
    from tools.vector_store import embed_alumni as _ea, get_client as _gcl

    embed_alumni = _ea
    get_client = _gcl
except ImportError as e:
    warnings.warn(f"tools.vector_store import failed: {e}", stacklevel=2)

try:
    from tools.gdpr import filter_consented as _fc, log_gdpr_action as _lgdpr

    filter_consented = _fc
    log_gdpr_action = _lgdpr
except ImportError as e:
    warnings.warn(f"tools.gdpr import failed: {e}", stacklevel=2)

_ALUMNI_COLS = (
    "name",
    "email",
    "graduation_year",
    "degree",
    "department",
    "location_city",
    "location_country",
    "job_title",
    "company",
    "industry",
    "interests",
    "engagement_score",
    "email_valid",
    "gdpr_consent",
    "past_events",
    "data_source",
)


def _safe_log(
    campaign_id: Optional[int],
    action_type: str,
    decision: str,
    reasoning: str,
) -> None:
    if log_agent is None or campaign_id is None:
        logger.info("[Data Integrator] %s: %s — %s", action_type, decision, reasoning)
        return
    try:
        log_agent(
            campaign_id,
            "Data Integrator",
            action_type,
            decision,
            reasoning,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("log_agent failed: %s", exc)


def _fallback_filter_consented(
    alumni_list: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], int, int]:
    eligible: List[Dict[str, Any]] = []
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


def _email_looks_valid(email: Optional[str]) -> bool:
    if not email or not str(email).strip():
        return False
    return bool(re.search(r"^[^@\s]+@[^@\s]+\.[^@\s]+", str(email).strip()))


def _field_score(rec: Dict[str, Any]) -> int:
    skip = {"id", "created_at"}
    s = 0
    for k, v in rec.items():
        if k in skip:
            continue
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        s += 1
    return s


def _merge_two(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    if _field_score(b) > _field_score(a):
        a, b = b, a
    merged = dict(a)
    for k, v in b.items():
        if k == "data_source":
            continue
        if v is None:
            continue
        if isinstance(v, str) and not str(v).strip():
            continue
        cur = merged.get(k)
        if cur is None or (isinstance(cur, str) and not str(cur).strip()):
            merged[k] = v
    sources = []
    for src in (a.get("data_source"), b.get("data_source")):
        if not src:
            continue
        for part in str(src).split(";"):
            p = part.strip()
            if p and p not in sources:
                sources.append(p)
    merged["data_source"] = "; ".join(sources)
    return merged


def _normalize_import_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(rec)
    if out.get("name") is None:
        out["name"] = ""
    out["name"] = str(out.get("name", "")).strip() or "Unknown"
    em = out.get("email")
    out["email"] = str(em).strip() if em is not None and str(em).strip() else None
    gy = out.get("graduation_year")
    if gy is not None and gy != "":
        try:
            out["graduation_year"] = int(gy)
        except (TypeError, ValueError):
            out["graduation_year"] = None
    else:
        out["graduation_year"] = None
    for key in (
        "degree",
        "department",
        "location_city",
        "job_title",
        "company",
        "industry",
        "interests",
        "past_events",
    ):
        v = out.get(key)
        out[key] = str(v).strip() if v is not None else ""
    lc = out.get("location_country")
    out["location_country"] = (
        str(lc).strip() if lc not in (None, "") else "UK"
    )
    try:
        out["engagement_score"] = int(out.get("engagement_score", 50))
    except (TypeError, ValueError):
        out["engagement_score"] = 50
    out["email_valid"] = 1 if _email_looks_valid(out.get("email")) else 0
    if out.get("gdpr_consent") is None:
        out["gdpr_consent"] = 1
    else:
        try:
            out["gdpr_consent"] = int(out.get("gdpr_consent"))
        except (TypeError, ValueError):
            out["gdpr_consent"] = 1
    ds = out.get("data_source")
    out["data_source"] = str(ds).strip() if ds else "import"
    return out


def _eventbrite_attendee_to_alumni(att: Dict[str, Any]) -> Dict[str, Any]:
    past = att.get("event_name") or ""
    email = att.get("email")
    if email is not None:
        email = str(email).strip() or None
    name = att.get("name")
    if name is not None:
        name = str(name).strip() or "Unknown"
    else:
        name = "Unknown"
    return _normalize_import_record(
        {
            "name": name,
            "email": email,
            "graduation_year": None,
            "degree": "",
            "department": "",
            "location_city": "",
            "location_country": "UK",
            "job_title": "",
            "company": "",
            "industry": "",
            "interests": "",
            "engagement_score": 50,
            "email_valid": 1 if _email_looks_valid(email) else 0,
            "gdpr_consent": 1,
            "past_events": str(past).strip() if past else "",
            "data_source": "Eventbrite",
        }
    )


def dedupe_by_email(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    with_email: List[Dict[str, Any]] = []
    without_email: List[Dict[str, Any]] = []
    for r in records:
        em = r.get("email")
        if em and str(em).strip():
            with_email.append(r)
        else:
            without_email.append(r)

    from collections import defaultdict

    by_email: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in with_email:
        key = str(r["email"]).strip().lower()
        by_email[key].append(r)

    merged_list: List[Dict[str, Any]] = []
    for _k, group in by_email.items():
        m = group[0]
        for r in group[1:]:
            m = _merge_two(m, r)
        merged_list.append(m)
    return merged_list + without_email


def _row_from_db_tuple(row: Any) -> Dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def _merge_existing_db(
    existing: Dict[str, Any], incoming: Dict[str, Any]
) -> Dict[str, Any]:
    """Prefer incoming non-empty fields; merge data_source from both."""
    merged = dict(existing)
    for k in _ALUMNI_COLS:
        if k == "data_source":
            continue
        inc = incoming.get(k)
        if inc is None:
            continue
        if isinstance(inc, str) and not inc.strip():
            continue
        cur = merged.get(k)
        if cur is None or (isinstance(cur, str) and not str(cur).strip()):
            merged[k] = inc
    sources = []
    for src in (existing.get("data_source"), incoming.get("data_source")):
        if not src:
            continue
        for part in str(src).split(";"):
            p = part.strip()
            if p and p not in sources:
                sources.append(p)
    merged["data_source"] = "; ".join(sources)
    return merged


def upsert_alumni_rows(
    conn: Any, records: List[Dict[str, Any]]
) -> Tuple[int, int]:
    """Insert or update alumni. Returns (inserted_count, updated_count)."""
    inserted = 0
    updated = 0
    for rec in records:
        try:
            email = rec.get("email")
            row = None
            if email and str(email).strip():
                row = conn.execute(
                    "SELECT * FROM alumni WHERE lower(trim(email)) = lower(trim(?))",
                    (str(email).strip(),),
                ).fetchone()
            if row is None:
                vals = [rec.get(c) for c in _ALUMNI_COLS]
                conn.execute(
                    f"""INSERT INTO alumni ({", ".join(_ALUMNI_COLS)})
                        VALUES ({", ".join("?" * len(_ALUMNI_COLS))})""",
                    vals,
                )
                inserted += 1
            else:
                ex = _row_from_db_tuple(row)
                merged = _merge_existing_db(ex, rec)
                sets = ", ".join(f"{c} = ?" for c in _ALUMNI_COLS)
                conn.execute(
                    f"UPDATE alumni SET {sets} WHERE id = ?",
                    [merged[c] for c in _ALUMNI_COLS] + [ex["id"]],
                )
                updated += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping alumni upsert due to error: %s", exc)
    return inserted, updated


def embed_unstructured_documents(unstructured: List[Dict[str, Any]]) -> int:
    """Store unstructured document text in ChromaDB (separate collection)."""
    if not unstructured or get_client is None:
        return 0
    try:
        client = get_client()
        coll = client.get_or_create_collection(
            name="ingested_documents",
            metadata={"description": "Unstructured source documents for context"},
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not open Chroma collection for documents: %s", exc)
        return 0

    count = 0
    for i, doc in enumerate(unstructured):
        try:
            text = (doc.get("text_content") or "").strip()
            if not text:
                continue
            fname = doc.get("filename", f"doc_{i}")
            safe = re.sub(r"[^a-zA-Z0-9._-]", "_", str(fname))[:180]
            doc_id = f"src_{safe}_{i}"
            chunk = text[:80000]
            coll.upsert(
                ids=[doc_id],
                documents=[chunk],
                metadatas=[{"filename": str(fname), "source": "file_ingestor"}],
            )
            count += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to embed document %s: %s", doc.get("filename"), exc)
    return count


def run_data_integration(campaign_id: int) -> Dict[str, Any]:
    counts: Dict[str, Any] = {
        "total_raw": 0,
        "salesforce_count": 0,
        "eventbrite_count": 0,
        "file_structured_count": 0,
        "file_unstructured_count": 0,
        "after_dedup": 0,
        "eligible": 0,
        "excluded_gdpr": 0,
        "excluded_email": 0,
        "documents_embedded": 0,
    }

    if get_connection is None:
        _safe_log(
            campaign_id,
            "ERROR",
            "Database tools unavailable",
            "tools.database did not import; aborting data integration.",
        )
        return counts

    sf_rows: List[Dict[str, Any]] = []
    if connect_salesforce and pull_alumni_from_salesforce:
        try:
            sf = connect_salesforce()
            if sf is not None:
                sf_rows = pull_alumni_from_salesforce(sf) or []
                counts["salesforce_count"] = len(sf_rows)
                _safe_log(
                    campaign_id,
                    "INGEST",
                    f"Salesforce: pulled {len(sf_rows)} contact(s)",
                    "Salesforce connection OK.",
                )
            else:
                _safe_log(
                    campaign_id,
                    "SKIP",
                    "Salesforce not configured, skipping",
                    "No Salesforce client (missing credentials or connection failed).",
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Salesforce integration failed: %s", exc)
            _safe_log(
                campaign_id,
                "SKIP",
                "Salesforce not configured, skipping",
                str(exc),
            )
    else:
        _safe_log(
            campaign_id,
            "SKIP",
            "Salesforce connector unavailable",
            "Import failed or connector not loaded.",
        )

    eb_rows: List[Dict[str, Any]] = []
    if (
        pull_events
        and pull_attendees
        and get_eventbrite_headers
        and os.getenv("EVENTBRITE_OAUTH_TOKEN")
    ):
        try:
            hdrs = get_eventbrite_headers()
            if hdrs:
                events = pull_events() or []
                _safe_log(
                    campaign_id,
                    "INGEST",
                    f"Eventbrite: fetched {len(events)} event(s)",
                    "Listing events for attendee pull.",
                )
                for ev in events:
                    if not isinstance(ev, dict):
                        continue
                    eid = ev.get("id")
                    if not eid:
                        continue
                    try:
                        atts = pull_attendees(str(eid)) or []
                        for att in atts:
                            eb_rows.append(_eventbrite_attendee_to_alumni(att))
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "Eventbrite attendees failed for event %s: %s", eid, exc
                        )
                counts["eventbrite_count"] = len(eb_rows)
                _safe_log(
                    campaign_id,
                    "INGEST",
                    f"Eventbrite: normalised {len(eb_rows)} attendee record(s)",
                    "Past attendees merged into alumni-shaped rows.",
                )
            else:
                _safe_log(
                    campaign_id,
                    "SKIP",
                    "Eventbrite not configured, skipping",
                    "EVENTBRITE_OAUTH_TOKEN set but headers returned None.",
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Eventbrite integration failed: %s", exc)
            _safe_log(
                campaign_id,
                "SKIP",
                "Eventbrite ingest failed",
                str(exc),
            )
    else:
        _safe_log(
            campaign_id,
            "SKIP",
            "Eventbrite not configured, skipping",
            "EVENTBRITE_OAUTH_TOKEN not set or connector unavailable.",
        )

    structured: List[Dict[str, Any]] = []
    unstructured: List[Dict[str, Any]] = []
    if ingest_all_files:
        try:
            ingest_result = ingest_all_files("data/sources/") or {}
            structured = list(ingest_result.get("structured") or [])
            unstructured = list(ingest_result.get("unstructured") or [])
            summary = ingest_result.get("summary", "")
            counts["file_structured_count"] = len(structured)
            counts["file_unstructured_count"] = len(unstructured)
            _safe_log(
                campaign_id,
                "INGEST",
                f"Files: {summary}",
                "ingest_all_files(data/sources/) completed.",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("File ingestion failed: %s", exc)
            _safe_log(
                campaign_id,
                "WARN",
                "File ingestion failed",
                str(exc),
            )
    else:
        _safe_log(
            campaign_id,
            "SKIP",
            "file_ingestor unavailable",
            "ingest_all_files not loaded.",
        )

    raw_combined: List[Dict[str, Any]] = []
    for row in sf_rows:
        raw_combined.append(_normalize_import_record(dict(row)))
    raw_combined.extend(eb_rows)
    for row in structured:
        raw_combined.append(_normalize_import_record(dict(row)))

    counts["total_raw"] = len(raw_combined)
    deduped = dedupe_by_email(raw_combined)
    counts["after_dedup"] = len(deduped)

    _safe_log(
        campaign_id,
        "DEDUP",
        f"Deduplicated to {len(deduped)} record(s) (by email; no-email rows kept)",
        f"Raw combined count was {counts['total_raw']}.",
    )

    fc = filter_consented if filter_consented else _fallback_filter_consented
    eligible, ex_gdpr, ex_email = fc(deduped)
    counts["eligible"] = len(eligible)
    counts["excluded_gdpr"] = ex_gdpr
    counts["excluded_email"] = ex_email

    if log_gdpr_action:
        try:
            log_gdpr_action(
                campaign_id,
                "FILTER",
                f"Eligible={len(eligible)}, excluded_gdpr={ex_gdpr}, excluded_email={ex_email}",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("log_gdpr_action failed: %s", exc)
    _safe_log(
        campaign_id,
        "GDPR",
        f"Filter: {len(eligible)} eligible, GDPR exclusions {ex_gdpr}, email exclusions {ex_email}",
        "filter_consented applied to merged batch.",
    )

    try:
        conn = get_connection()
        try:
            ins, upd = upsert_alumni_rows(conn, deduped)
            conn.commit()
        finally:
            conn.close()
        _safe_log(
            campaign_id,
            "STORE",
            f"SQLite: inserted {ins}, updated {upd}",
            "Upsert into alumni by email.",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("SQLite upsert failed: %s", exc)
        _safe_log(campaign_id, "ERROR", "SQLite upsert failed", str(exc))

    if embed_alumni and get_all_alumni:
        try:
            to_embed = get_all_alumni() or []
            n_emb = embed_alumni(to_embed)
            _safe_log(
                campaign_id,
                "EMBED",
                f"ChromaDB alumni_profiles: embedded {n_emb} profile(s)",
                "embed_alumni on eligible alumni from DB.",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("embed_alumni failed: %s", exc)
            _safe_log(campaign_id, "WARN", "Chroma alumni embed failed", str(exc))
    else:
        _safe_log(
            campaign_id,
            "SKIP",
            "Vector store embed unavailable",
            "embed_alumni or get_all_alumni not loaded.",
        )

    try:
        doc_n = embed_unstructured_documents(unstructured)
        counts["documents_embedded"] = doc_n
        _safe_log(
            campaign_id,
            "EMBED",
            f"ChromaDB ingested_documents: embedded {doc_n} document(s)",
            "Unstructured file text stored for retrieval context.",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Unstructured embed failed: %s", exc)
        _safe_log(campaign_id, "WARN", "Document embedding failed", str(exc))

    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cid = 0
    out = run_data_integration(cid)
    print(out)
