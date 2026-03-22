"""
Eventbrite API v3 connector (events, attendees, webhooks).

Requires: pip install requests python-dotenv
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv

BASE_URL = "https://www.eventbriteapi.com/v3/"


def get_eventbrite_headers() -> dict[str, str] | None:
    """Returns Authorization headers or None if EVENTBRITE_OAUTH_TOKEN is missing."""
    load_dotenv()
    token = os.getenv("EVENTBRITE_OAUTH_TOKEN")
    if not token:
        print("Warning: EVENTBRITE_OAUTH_TOKEN is not set (add it to .env or your environment).")
        return None
    return {"Authorization": f"Bearer {token}"}


def _safe_get_json(url: str, headers: dict[str, str], params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    try:
        resp = requests.get(url, headers=headers, params=params or {}, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as exc:
        body = ""
        if exc.response is not None:
            try:
                body = exc.response.text[:500]
            except Exception:  # noqa: BLE001
                body = ""
        print(f"Eventbrite HTTP error ({exc.response.status_code if exc.response else '?'}): {exc}. {body}")
        return None
    except requests.exceptions.RequestException as exc:
        print(f"Eventbrite request failed: {exc}")
        return None
    except ValueError as exc:
        print(f"Eventbrite response was not valid JSON: {exc}")
        return None


def _paginate_list(
    path: str,
    headers: dict[str, str],
    list_key: str,
    extra_params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Fetch all pages using continuation token, with page-number fallback."""
    url = urljoin(BASE_URL, path.lstrip("/"))
    items: list[dict[str, Any]] = []
    params: dict[str, Any] = dict(extra_params or {})
    continuation: str | None = None
    page_fallback: int | None = None

    while True:
        req_params = dict(params)
        if continuation:
            req_params["continuation"] = continuation
        elif page_fallback is not None:
            req_params["page"] = page_fallback

        data = _safe_get_json(url, headers=headers, params=req_params)
        if not data:
            break

        batch = data.get(list_key)
        if not isinstance(batch, list):
            batch = []
        items.extend(batch)

        pagination = data.get("pagination") or {}
        if not pagination.get("has_more_items"):
            break

        next_cont = pagination.get("continuation")
        if next_cont:
            continuation = str(next_cont)
            page_fallback = None
            continue

        continuation = None
        current_page = int(pagination.get("page_number") or 1)
        page_fallback = current_page + 1
        if page_fallback > 1000:
            print("Warning: Eventbrite pagination stopped after 1000 pages to avoid infinite loop.")
            break

    return items


def _format_venue_location(venue: dict[str, Any] | None) -> str | None:
    if not venue:
        return None
    addr = venue.get("address")
    if isinstance(addr, dict):
        display = addr.get("localized_address_display")
        if display:
            return str(display).strip() or None
        parts = []
        for key in (
            "address_1",
            "address_2",
            "city",
            "region",
            "postal_code",
            "country",
        ):
            val = addr.get(key)
            if val:
                parts.append(str(val).strip())
        if parts:
            return ", ".join(parts)
    name = venue.get("name")
    if name:
        return str(name).strip()
    return None


def _fetch_venue_location(headers: dict[str, str], venue_id: str | None) -> str | None:
    if not venue_id:
        return None
    data = _safe_get_json(urljoin(BASE_URL, f"venues/{venue_id}/"), headers=headers)
    if not data:
        return None
    return _format_venue_location(data)


def _event_title(event: dict[str, Any]) -> str | None:
    name = event.get("name")
    if isinstance(name, dict):
        t = name.get("text") or name.get("html")
        return str(t).strip() if t else None
    if name is not None:
        return str(name).strip() or None
    return None


def _event_type_label(event: dict[str, Any]) -> str:
    fmt = event.get("format")
    if isinstance(fmt, dict):
        n = fmt.get("name")
        if n:
            return str(n).strip()
    fid = event.get("format_id")
    if fid is not None:
        return str(fid)
    return ""


def _normalize_event(event: dict[str, Any], headers: dict[str, str] | None) -> dict[str, Any]:
    title = _event_title(event)
    start = event.get("start") or {}
    date_str = start.get("local")
    if date_str is not None:
        date_str = str(date_str)

    location: str | None = None
    venue = event.get("venue")
    if isinstance(venue, dict):
        location = _format_venue_location(venue)
    elif headers:
        vid = event.get("venue_id")
        if vid:
            location = _fetch_venue_location(headers, str(vid))

    capacity = event.get("capacity")
    if capacity is None:
        capacity = event.get("capacity_total")

    return {
        "title": title,
        "event_type": _event_type_label(event),
        "date": date_str,
        "location": location,
        "capacity": capacity,
        # keep raw id for downstream use
        "id": event.get("id"),
    }


def pull_events(organization_id: str | None = None) -> list[dict[str, Any]]:
    """
    List events for an organization or for the authenticated user.

    - With organization_id: GET /organizations/{id}/events/
    - Without: tries GET /users/me/events/ first, then GET /users/me/owned_events/
      if the first URL is not available (Eventbrite often returns 404 for /users/me/events/).
    """
    headers = get_eventbrite_headers()
    if not headers:
        return []

    if organization_id:
        path = f"organizations/{organization_id}/events/"
        paths_to_try = (path,)
    else:
        paths_to_try = ("users/me/events/", "users/me/owned_events/")

    raw_events: list[dict[str, Any]] = []
    for path in paths_to_try:
        try:
            raw_events = _paginate_list(path, headers, list_key="events")
        except Exception as exc:  # noqa: BLE001
            print(f"Unexpected error while listing Eventbrite events: {exc}")
            return []
        if raw_events:
            break
        if organization_id:
            break
        # First path may 404 (see docstring); try owned_events next.
        continue

    out: list[dict[str, Any]] = []
    for ev in raw_events:
        if not isinstance(ev, dict):
            continue
        try:
            out.append(_normalize_event(ev, headers))
        except Exception as exc:  # noqa: BLE001
            print(f"Skipping malformed event record: {exc}")
    return out


def _fetch_event_title(headers: dict[str, str], event_id: str) -> str | None:
    data = _safe_get_json(urljoin(BASE_URL, f"events/{event_id}/"), headers=headers)
    if not data:
        return None
    return _event_title(data)


def _normalize_attendee(att: dict[str, Any], event_name: str | None) -> dict[str, Any]:
    profile = att.get("profile") or {}
    if not isinstance(profile, dict):
        profile = {}

    first = (profile.get("first_name") or "") or ""
    last = (profile.get("last_name") or "") or ""
    name = f"{first} {last}".strip() or None

    email = profile.get("email")
    if email is not None:
        email = str(email).strip() or None

    status = att.get("status")
    if status is not None:
        status = str(status).strip()

    return {
        "name": name,
        "email": email,
        "event_name": event_name,
        "rsvp_status": status,
    }


def pull_attendees(event_id: str) -> list[dict[str, Any]]:
    """GET /events/{event_id}/attendees/ — paginated, normalised attendee dicts."""
    headers = get_eventbrite_headers()
    if not headers:
        return []

    event_name: str | None = None
    try:
        event_name = _fetch_event_title(headers, str(event_id))
    except Exception as exc:  # noqa: BLE001
        print(f"Could not fetch event title for {event_id}: {exc}")

    path = f"events/{event_id}/attendees/"
    try:
        raw = _paginate_list(path, headers, list_key="attendees")
    except Exception as exc:  # noqa: BLE001
        print(f"Unexpected error while listing attendees for event {event_id}: {exc}")
        return []

    out: list[dict[str, Any]] = []
    for att in raw:
        if not isinstance(att, dict):
            continue
        try:
            out.append(_normalize_attendee(att, event_name))
        except Exception as exc:  # noqa: BLE001
            print(f"Skipping malformed attendee record: {exc}")
    return out


def setup_checkin_webhook(event_id: str, endpoint_url: str) -> str | None:
    """
    POST /webhooks/ — registers attendee check-in notifications to endpoint_url.
    Returns webhook id string or None on failure.
    """
    headers = get_eventbrite_headers()
    if not headers:
        return None

    url = urljoin(BASE_URL, "webhooks/")
    payload = {
        "endpoint_url": endpoint_url,
        "actions": "attendee.checked_in",
        "event_id": str(event_id),
    }
    req_headers = {**headers, "Content-Type": "application/json"}

    try:
        resp = requests.post(url, headers=req_headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.HTTPError as exc:
        body = ""
        if exc.response is not None:
            try:
                body = exc.response.text[:800]
            except Exception:  # noqa: BLE001
                body = ""
        print(f"Eventbrite webhook HTTP error ({exc.response.status_code if exc.response else '?'}): {exc}. {body}")
        return None
    except requests.exceptions.RequestException as exc:
        print(f"Eventbrite webhook request failed: {exc}")
        return None
    except ValueError as exc:
        print(f"Eventbrite webhook response was not valid JSON: {exc}")
        return None

    wid = data.get("id") if isinstance(data, dict) else None
    if wid is not None:
        return str(wid)
    print("Eventbrite webhook created but response had no id field.")
    return None


if __name__ == "__main__":
    h = get_eventbrite_headers()
    if h is None:
        print("Cannot load Eventbrite events: missing EVENTBRITE_OAUTH_TOKEN.")
    else:
        events = pull_events()
        print(f"Events fetched: {len(events)}")
        for i, ev in enumerate(events):
            print(f"  [{i + 1}] {ev}")
