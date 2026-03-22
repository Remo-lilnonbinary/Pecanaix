import os
import sqlite3
from datetime import datetime


DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "pecan.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    return conn


def _column_names(conn, table):
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def _ensure_campaign_columns(conn):
    """Add new campaign columns on existing databases (CREATE IF NOT EXISTS does not alter)."""
    cols = _column_names(conn, "campaigns")
    additions = [
        ("target_attendance", "INTEGER"),
        ("target_acceptances", "REAL"),
        ("total_pool_size", "INTEGER"),
        ("current_cycle", "INTEGER DEFAULT 0"),
        ("max_cycles", "INTEGER DEFAULT 4"),
        ("planning_date", "TEXT"),
        ("event_date", "TEXT"),
        ("campaign_phase", "TEXT DEFAULT 'LOOP_1'"),
    ]
    for name, definition in additions:
        if name not in cols:
            conn.execute(f"ALTER TABLE campaigns ADD COLUMN {name} {definition}")


def init_database():
    """Creates all tables. Run this once."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute(
            """CREATE TABLE IF NOT EXISTS alumni (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT,
        graduation_year INTEGER,
        degree TEXT,
        department TEXT,
        location_city TEXT,
        location_country TEXT DEFAULT 'UK',
        job_title TEXT,
        company TEXT,
        industry TEXT,
        interests TEXT,
        engagement_score INTEGER DEFAULT 50,
        email_valid INTEGER DEFAULT 1,
        gdpr_consent INTEGER DEFAULT 1,
        past_events TEXT,
        data_source TEXT DEFAULT 'CRM',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )"""
        )

        c.execute(
            """CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        event_type TEXT,
        topic_tags TEXT,
        event_date TEXT,
        location_city TEXT,
        capacity INTEGER,
        audience_description TEXT,
        status TEXT DEFAULT 'Active',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )"""
        )

        c.execute(
            """CREATE TABLE IF NOT EXISTS campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id INTEGER,
        status TEXT DEFAULT 'NEW',
        raw_brief TEXT,
        parsed_brief TEXT,
        total_invited INTEGER DEFAULT 0,
        total_opened INTEGER DEFAULT 0,
        total_bounced INTEGER DEFAULT 0,
        total_replied INTEGER DEFAULT 0,
        open_rate REAL DEFAULT 0,
        checkin_rate REAL DEFAULT 0,
        agent_summary TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        completed_at TEXT,
        target_attendance INTEGER,
        target_acceptances REAL,
        total_pool_size INTEGER,
        current_cycle INTEGER DEFAULT 0,
        max_cycles INTEGER DEFAULT 4,
        planning_date TEXT,
        event_date TEXT,
        campaign_phase TEXT DEFAULT 'LOOP_1',
        FOREIGN KEY (event_id) REFERENCES events(id)
    )"""
        )

        _ensure_campaign_columns(conn)

        c.execute(
            """CREATE TABLE IF NOT EXISTS matches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER,
        alumni_id INTEGER,
        score INTEGER,
        reasoning TEXT,
        selected INTEGER DEFAULT 0,
        wave INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
        FOREIGN KEY (alumni_id) REFERENCES alumni(id)
    )"""
        )

        c.execute(
            """CREATE TABLE IF NOT EXISTS outreach_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER,
        alumni_id INTEGER,
        wave INTEGER DEFAULT 1,
        subject_line TEXT,
        body TEXT,
        personalisation_note TEXT,
        status TEXT DEFAULT 'DRAFTED',
        sent_at TEXT,
        opened_at TEXT,
        replied_at TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
        FOREIGN KEY (alumni_id) REFERENCES alumni(id)
    )"""
        )

        c.execute(
            """CREATE TABLE IF NOT EXISTS checkins (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER,
        alumni_id INTEGER,
        rsvp_status TEXT DEFAULT 'INVITED',
        checked_in INTEGER DEFAULT 0,
        checkin_time TEXT,
        source TEXT DEFAULT 'Manual',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
        FOREIGN KEY (alumni_id) REFERENCES alumni(id)
    )"""
        )

        c.execute(
            """CREATE TABLE IF NOT EXISTS agent_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER,
        timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
        agent_name TEXT,
        action_type TEXT,
        decision TEXT,
        reasoning TEXT,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
    )"""
        )

        c.execute(
            """CREATE TABLE IF NOT EXISTS campaign_memory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER,
        event_type TEXT,
        audience_segment TEXT,
        key_insight TEXT,
        open_rate_achieved REAL,
        checkin_rate_achieved REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
    )"""
        )

        c.execute(
            """CREATE TABLE IF NOT EXISTS cycles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER,
        cycle_number INTEGER,
        scenario TEXT,
        batch_size INTEGER,
        personalised_count INTEGER DEFAULT 0,
        reached_count INTEGER DEFAULT 0,
        bounce_count INTEGER DEFAULT 0,
        acceptance_count INTEGER DEFAULT 0,
        warm_leads_count INTEGER DEFAULT 0,
        expected_acceptance_rate REAL,
        actual_acceptance_rate REAL DEFAULT 0,
        status TEXT DEFAULT 'PENDING',
        started_at TEXT,
        completed_at TEXT,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
    )"""
        )

        c.execute(
            """CREATE TABLE IF NOT EXISTS warm_leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER,
        alumni_id INTEGER,
        cycle_number INTEGER,
        signal_type TEXT,
        followed_up INTEGER DEFAULT 0,
        converted INTEGER DEFAULT 0,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
        FOREIGN KEY (alumni_id) REFERENCES alumni(id)
    )"""
        )

        conn.commit()
    finally:
        conn.close()
    print("Database initialised successfully.")


def get_all_alumni():
    """Returns all alumni with GDPR consent and valid email."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM alumni WHERE gdpr_consent = 1 AND email_valid = 1"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_all_alumni_unfiltered():
    """Returns all alumni including those without consent."""
    conn = get_connection()
    try:
        rows = conn.execute("SELECT * FROM alumni").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def create_campaign(raw_brief):
    """Creates a new campaign and returns its ID."""
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute("INSERT INTO campaigns (raw_brief, status) VALUES (?, 'NEW')", (raw_brief,))
        campaign_id = c.lastrowid
        conn.commit()
        return campaign_id
    finally:
        conn.close()


def update_campaign(campaign_id, **kwargs):
    """Updates any fields on a campaign by keyword arguments."""
    conn = get_connection()
    try:
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [campaign_id]
        conn.execute(f"UPDATE campaigns SET {sets} WHERE id = ?", values)
        conn.commit()
    finally:
        conn.close()


def save_match(campaign_id, alumni_id, score, reasoning, selected=False, wave=1):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO matches (campaign_id, alumni_id, score, reasoning, selected, wave) VALUES (?,?,?,?,?,?)",
            (campaign_id, alumni_id, score, reasoning, int(selected), wave),
        )
        conn.commit()
    finally:
        conn.close()


def save_outreach(campaign_id, alumni_id, subject_line, body, personalisation_note, wave=1):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO outreach_messages (campaign_id, alumni_id, subject_line, body, personalisation_note, wave) VALUES (?,?,?,?,?,?)",
            (campaign_id, alumni_id, subject_line, body, personalisation_note, wave),
        )
        conn.commit()
    finally:
        conn.close()


def save_checkin(campaign_id, alumni_id, checked_in, source="Eventbrite"):
    conn = get_connection()
    try:
        checkin_time = datetime.now().isoformat() if checked_in else None
        conn.execute(
            "INSERT INTO checkins (campaign_id, alumni_id, checked_in, checkin_time, source) VALUES (?,?,?,?,?)",
            (campaign_id, alumni_id, int(checked_in), checkin_time, source),
        )
        conn.commit()
    finally:
        conn.close()


def log_agent(campaign_id, agent_name, action_type, decision, reasoning):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO agent_log (campaign_id, agent_name, action_type, decision, reasoning) VALUES (?,?,?,?,?)",
            (campaign_id, agent_name, action_type, decision, reasoning),
        )
        conn.commit()
    finally:
        conn.close()


def save_memory(campaign_id, event_type, audience_segment, key_insight, open_rate, checkin_rate):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO campaign_memory (campaign_id, event_type, audience_segment, key_insight, open_rate_achieved, checkin_rate_achieved) VALUES (?,?,?,?,?,?)",
            (campaign_id, event_type, audience_segment, key_insight, open_rate, checkin_rate),
        )
        conn.commit()
    finally:
        conn.close()


def get_agent_log(campaign_id):
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM agent_log WHERE campaign_id = ? ORDER BY timestamp",
            (campaign_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_matches(campaign_id, selected_only=True):
    conn = get_connection()
    try:
        q = """SELECT m.*, a.name, a.email, a.degree, a.department, a.graduation_year,
                  a.location_city, a.location_country, a.industry, a.interests
           FROM matches m JOIN alumni a ON m.alumni_id = a.id
           WHERE m.campaign_id = ?"""
        if selected_only:
            q += " AND m.selected = 1"
        q += " ORDER BY m.score DESC"
        rows = conn.execute(q, (campaign_id,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_outreach(campaign_id):
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT o.*, a.name, a.email
           FROM outreach_messages o JOIN alumni a ON o.alumni_id = a.id
           WHERE o.campaign_id = ? ORDER BY o.wave, o.id""",
            (campaign_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_campaign(campaign_id):
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_checkins(campaign_id):
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT c.*, a.name, a.email
           FROM checkins c JOIN alumni a ON c.alumni_id = a.id
           WHERE c.campaign_id = ?""",
            (campaign_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_memories():
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM campaign_memory ORDER BY created_at DESC LIMIT 10"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def create_cycle(campaign_id, cycle_number, expected_rate):
    """Inserts a new cycle row, returns cycle id"""
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute(
            """INSERT INTO cycles (campaign_id, cycle_number, expected_acceptance_rate, status)
           VALUES (?, ?, ?, 'PENDING')""",
            (campaign_id, cycle_number, expected_rate),
        )
        cycle_id = c.lastrowid
        conn.commit()
        return cycle_id
    finally:
        conn.close()


def update_cycle(cycle_id, **kwargs):
    """Updates any fields on a cycle by keyword arguments"""
    conn = get_connection()
    try:
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [cycle_id]
        conn.execute(f"UPDATE cycles SET {sets} WHERE id = ?", values)
        conn.commit()
    finally:
        conn.close()


def get_cycle(cycle_id):
    """Returns one cycle as a dict"""
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM cycles WHERE id = ?", (cycle_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_all_cycles(campaign_id):
    """Returns all cycles for a campaign ordered by cycle_number"""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM cycles WHERE campaign_id = ? ORDER BY cycle_number",
            (campaign_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def save_warm_lead(campaign_id, alumni_id, cycle_number, signal_type, conn=None):
    """Inserts a warm lead record. Pass ``conn`` to join the caller's transaction (avoids nested SQLite locks)."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO warm_leads (campaign_id, alumni_id, cycle_number, signal_type)
           VALUES (?, ?, ?, ?)""",
            (campaign_id, alumni_id, cycle_number, signal_type),
        )
        if own:
            conn.commit()
    finally:
        if own:
            conn.close()


def get_warm_leads(campaign_id, cycle_number):
    """Returns warm leads for a specific cycle"""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM warm_leads WHERE campaign_id = ? AND cycle_number = ? ORDER BY id",
            (campaign_id, cycle_number),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_unfollowed_warm_leads(campaign_id):
    """Returns all warm leads where followed_up = 0"""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM warm_leads WHERE campaign_id = ? AND followed_up = 0 ORDER BY id",
            (campaign_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


if __name__ == "__main__":
    init_database()
