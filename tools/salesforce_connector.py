"""
Salesforce connector for pulling Contact (alumni) and Campaign (events) data.

Requires: pip install simple-salesforce python-dotenv
"""

from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv

try:
    from simple_salesforce import Salesforce
except ImportError:  # pragma: no cover
    Salesforce = None  # type: ignore[misc, assignment]


def _sf_record_to_plain(record: dict[str, Any]) -> dict[str, Any]:
    """Strip Salesforce metadata and return a plain dict."""
    out = {k: v for k, v in record.items() if k != "attributes"}
    return out


def connect_salesforce():
    """
    Reads credentials from environment (via .env) and returns a Salesforce client.
    Uses: SALESFORCE_CONSUMER_KEY, SALESFORCE_CONSUMER_SECRET, SALESFORCE_USERNAME,
    SALESFORCE_PASSWORD, SALESFORCE_SECURITY_TOKEN.
    """
    load_dotenv()

    if Salesforce is None:
        print(
            "Error: simple_salesforce is not installed. Run: pip install simple-salesforce"
        )
        return None

    required = (
        "SALESFORCE_CONSUMER_KEY",
        "SALESFORCE_CONSUMER_SECRET",
        "SALESFORCE_USERNAME",
        "SALESFORCE_PASSWORD",
        "SALESFORCE_SECURITY_TOKEN",
    )
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print(
            "Warning: Missing Salesforce credentials (set in .env or environment): "
            + ", ".join(missing)
        )
        return None

    username = os.getenv("SALESFORCE_USERNAME")
    password = os.getenv("SALESFORCE_PASSWORD")
    security_token = os.getenv("SALESFORCE_SECURITY_TOKEN")
    consumer_key = os.getenv("SALESFORCE_CONSUMER_KEY")
    consumer_secret = os.getenv("SALESFORCE_CONSUMER_SECRET")

    try:
        sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
        )
        return sf
    except Exception as exc:  # noqa: BLE001 - surface any login/API error clearly
        print(f"Failed to connect to Salesforce: {exc}")
        return None


def _account_name(record: dict[str, Any]) -> str | None:
    acct = record.get("Account")
    if isinstance(acct, dict):
        name = acct.get("Name")
        return str(name).strip() if name is not None else None
    return None


def pull_alumni_from_salesforce(sf_connection) -> list[dict[str, Any]]:
    """
    Queries Contact via SOQL and returns dicts aligned with the alumni schema.
    Fields: FirstName, LastName, Email, MailingCity, MailingCountry, Department, Title, Account.Name
    """
    if not sf_connection:
        return []

    soql = """
        SELECT FirstName, LastName, Email, MailingCity, MailingCountry,
               Department, Title, Account.Name
        FROM Contact
    """

    try:
        result = sf_connection.query_all(soql)
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to query Salesforce contacts: {exc}")
        return []

    records = result.get("records") or []
    alumni: list[dict[str, Any]] = []

    for raw in records:
        rec = _sf_record_to_plain(raw)
        first = (rec.get("FirstName") or "") or ""
        last = (rec.get("LastName") or "") or ""
        name = f"{first} {last}".strip() or None

        email = rec.get("Email")
        if email is not None:
            email = str(email).strip() or None

        mailing_city = rec.get("MailingCity")
        if mailing_city is not None:
            mailing_city = str(mailing_city).strip() or None

        mailing_country = rec.get("MailingCountry")
        if mailing_country is not None:
            mailing_country = str(mailing_country).strip() or None

        department = rec.get("Department")
        if department is not None:
            department = str(department).strip() or None

        title = rec.get("Title")
        if title is not None:
            title = str(title).strip() or None

        company = _account_name(rec) or ""

        alumni.append(
            {
                "name": name,
                "email": email,
                "location_city": mailing_city,
                "location_country": mailing_country if mailing_country else "UK",
                "department": department,
                "job_title": title,
                "company": company,
                "industry": "",
                "interests": "",
                "engagement_score": 50,
                "email_valid": 1 if email else 0,
                "gdpr_consent": 1,
                "past_events": "",
                "data_source": "Salesforce",
            }
        )

    return alumni


def pull_events_from_salesforce(sf_connection) -> list[dict[str, Any]]:
    """
    Queries Campaign for Name, Type, StartDate, Status.
    Returns normalised dicts suitable for the events table / app use.
    """
    if not sf_connection:
        return []

    soql = """
        SELECT Name, Type, StartDate, Status
        FROM Campaign
    """

    try:
        result = sf_connection.query_all(soql)
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to query Salesforce campaigns: {exc}")
        return []

    records = result.get("records") or []
    events: list[dict[str, Any]] = []

    for raw in records:
        rec = _sf_record_to_plain(raw)
        name = rec.get("Name")
        if name is not None:
            name = str(name).strip() or None

        evt_type = rec.get("Type")
        if evt_type is not None:
            evt_type = str(evt_type).strip() or None

        start = rec.get("StartDate")
        if start is not None:
            start = str(start)

        status = rec.get("Status")
        if status is not None:
            status = str(status).strip() or None

        events.append(
            {
                "title": name,
                "event_type": evt_type,
                "event_date": start,
                "status": status,
            }
        )

    return events


if __name__ == "__main__":
    sf = connect_salesforce()
    if sf is None:
        print("Could not establish Salesforce connection.")
    else:
        contacts = pull_alumni_from_salesforce(sf)
        print(f"Contacts pulled: {len(contacts)}")
        for i, row in enumerate(contacts[:3]):
            print(f"  [{i + 1}] {row}")
