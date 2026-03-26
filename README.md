# Pecan — Autonomous Alumni Engagement Operator

> **One brief. Zero hand-holding. Full campaign.**

Pecan is an autonomous multi-agent system that takes a single event brief from a university alumni team and runs an entire outreach campaign — from data integration to personalised emails to real-time response tracking — without human intervention.

Built for the AI Agents hackathon track, Pecan demonstrates deep autonomy, real-world usefulness, and sophisticated multi-agent orchestration.

---

## The Problem

96% of university alumni disengage within 3 years of graduation. Alumni teams face fragmented data across CRMs, spreadsheets, and event platforms. Outreach is manual, generic, and impossible to track.

## The Solution

Pecan replaces the entire manual workflow with a two-loop autonomous agent architecture:

**Loop 1 (Understand → Match):** A conversational Brief Analyst collects event details through natural dialogue. A Data Integrator pulls alumni records from Salesforce, Eventbrite, uploaded files, and normalises everything into a unified profile store. A Matching Agent uses vector search + algorithmic scoring + LLM reasoning to find the best 5X candidates. A Quality Checker flags duplicates, over-representation, and GDPR issues.

**Loop 2 (Outreach → Track → Adapt):** A Personalisation Agent writes unique emails referencing each alumnus's profile. An Outreach Agent manages delivery. A Response Tracker monitors acceptances (only full RSVPs count — not opens). If the goal isn't met, the system automatically diagnoses what went wrong, adjusts its approach, and runs another cycle. Up to 4 cycles, fully autonomous.

**Phase 3 (Report → Learn):** A Campaign Reporter analyses the full funnel, identifies top-performing segments, and stores insights for future campaigns. Pecan gets smarter with every run.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    BRIEF ANALYST                         │
│            (Conversational, multi-turn)                  │
│         Collects event details via natural chat          │
└──────────────────────┬──────────────────────────────────┘
                       │ Confirmed parsed brief
                       ▼
┌─────────────────── LOOP 1 ──────────────────────────────┐
│                                                          │
│  Data Integrator → Matching Agent → Quality Checker      │
│  (Salesforce,       (Vector search +   (Duplicates,      │
│   Eventbrite,        Algorithm +        GDPR,            │
│   CSV/Excel/PDF)     LLM reasoning)     Diversity)       │
│                                                          │
└──────────────────────┬──────────────────────────────────┘
                       │ 5X alumni pool, quality-checked
                       ▼
┌─────────────────── LOOP 2 (up to 4 cycles) ─────────────┐
│                                                          │
│  Calculate Batch → Personalise → Outreach → Track        │
│  (Scenario A/B)    (LLM emails)  (Send +    (Accept/     │
│                                   bounce)    open/none)  │
│                                                          │
│  ◄──── If goal not met: diagnose + adjust + retry ────►  │
│                                                          │
└──────────────────────┬──────────────────────────────────┘
                       │ Goal met OR max cycles
                       ▼
┌─────────────────── PHASE 3 ─────────────────────────────┐
│                                                          │
│  Campaign Reporter → Segment Analysis → Campaign Memory  │
│  (LLM analysis)      (Dept, year,       (Cross-campaign  │
│                       industry, city)    learning)        │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| **LLM** | Kimi K2 via Groq | Open-source, 1T params, 32B active, purpose-built for agentic tool-calling, ~200 tokens/sec on Groq's LPU hardware |
| **Agent Orchestration** | LangGraph (Python) | Conditional routing for two-loop architecture with state management |
| **LLM Routing** | Custom SmartLLMRouter | Rate-limited request management with automatic pacing to stay under Groq's TPM limits |
| **Structured DB** | SQLite (WAL mode) | Local-first, no data leaves the machine — strong GDPR story |
| **Vector DB** | ChromaDB | Semantic search over alumni profiles using built-in embeddings |
| **CRM Integration** | Salesforce REST API | OAuth-based connection to university's existing Salesforce instance |
| **Event Platform** | Eventbrite API v3 | Pull past attendance data + real-time check-in webhooks |
| **File Ingestion** | Python (csv, openpyxl, PyPDF2, python-docx) | Auto-normalise CSV, Excel, PDF, and Word uploads |
| **API Backend** | FastAPI | REST endpoints for dashboard, Brief Analyst conversation, and webhooks |
| **GDPR** | Custom anonymisation layer | PII stripped before any data reaches the LLM; re-attached after |

---

## Key Features

**Conversational Brief Collection** — The Brief Analyst asks one question at a time, collecting event type, topic, date, location, target attendance, audience constraints, platform, exclusions, and goals. Confirms everything before launching.

**Multi-Source Data Integration** — Pulls from Salesforce CRM, Eventbrite past events, and uploaded CSV/Excel/PDF/Word files. Deduplicates by email. Normalises all records to a common schema. Handles missing sources gracefully.

**Three-Stage Matching** — (1) ChromaDB vector search for semantic relevance, (2) algorithmic scoring across 5 dimensions (topic 40%, location 20%, graduation 15%, engagement 15%, vector fit 10%), (3) LLM reasoning on anonymised top candidates.

**GDPR-First Design** — Alumni names and emails are stripped before any LLM call. The LLM only sees anonymised profiles. Identities are re-attached after. All GDPR actions are logged. Consent is re-verified at every stage.

**Autonomous Multi-Cycle Outreach** — Each cycle generates fresh personalised emails. If acceptance targets aren't met, the Response Tracker diagnoses what went wrong (subject lines too generic? wrong segment?) and adjusts the next cycle. Declining acceptance rates per cycle (35% → 25% → 18% → 12%) reflect real-world behaviour.

**Intelligent Campaign Reporting** — Full funnel analysis (pool → personalised → sent → opened → accepted → checked-in). Segment breakdown by department, graduation year range, industry, and location. LLM-generated insights stored as cross-campaign memory.

**Smart Rate Limiting** — Custom LLM router tracks API call timestamps and automatically paces requests to stay within provider limits. No crashed pipelines from rate limit errors.

---

## Project Structure

```
pecanaix/
├── .env.example                  # Template for API keys
├── api.py                        # FastAPI backend (endpoints for dashboard + Brief Analyst)
├── agents/
│   ├── pipeline.py               # LangGraph two-loop orchestration
│   ├── brief_analyst.py          # Conversational event brief collection
│   ├── data_integrator.py        # Multi-source data ingestion
│   ├── matching_agent.py         # Vector + algorithm + LLM matching
│   ├── quality_checker.py        # Pre-outreach QA
│   ├── personalisation_agent.py  # Per-alumnus email generation
│   ├── outreach_agent.py         # Simulated send with bounce modelling
│   ├── response_tracker.py       # Acceptance tracking + cycle diagnosis
│   └── campaign_reporter.py      # Final analysis + memory storage
├── tools/
│   ├── database.py               # SQLite schema + all CRUD helpers
│   ├── seed_data.py              # 250 mock alumni generator
│   ├── vector_store.py           # ChromaDB embedding + search
│   ├── llm_router.py             # Smart rate-limited LLM routing
│   ├── salesforce_connector.py   # Salesforce OAuth + SOQL queries
│   ├── eventbrite_connector.py   # Eventbrite API v3 integration
│   ├── file_ingestor.py          # CSV/Excel/PDF/Word auto-processing
│   └── gdpr.py                   # Anonymisation + consent utilities
└── data/
    ├── pecan.db                  # Auto-generated SQLite database
    ├── chroma_db/                # Auto-generated vector store
    └── sources/                  # Upload folder for file ingestion
```

---

## Quick Start

### Prerequisites

- Python 3.10+
- A Groq API key (free at [console.groq.com](https://console.groq.com))

### Setup

```bash
git clone https://github.com/YOUR_USERNAME/pecanaix.git
cd pecanaix
python3 -m venv venv
source venv/bin/activate
pip install langgraph langchain langchain-openai fastapi uvicorn python-dotenv pydantic chromadb simple-salesforce openpyxl PyPDF2 python-docx requests
```

Copy `.env.example` to `.env` and add your API keys:

```bash
cp .env.example .env
# Edit .env with your keys
```

### Initialise

```bash
python3 tools/database.py      # Create tables
python3 tools/seed_data.py     # Generate 250 mock alumni
python3 tools/vector_store.py  # Embed profiles into ChromaDB
```

### Run the Pipeline

```bash
python3 agents/pipeline.py
```

This runs a complete campaign with a sample brief (AI in finance careers panel, London, target attendance 10). Takes about 2-3 minutes. Outputs full campaign results with segment analysis.

### Run the Brief Analyst (Interactive)

```bash
python3 agents/brief_analyst.py
```

Have a natural conversation to define your event. The agent collects all details and outputs a parsed brief.

### Start the API

```bash
python3 api.py
```

API runs at `http://localhost:8000`. Endpoints include:

- `POST /api/brief/start` — Start a brief conversation
- `POST /api/brief/{id}/message` — Send a message
- `POST /api/campaign/launch` — Launch a campaign from a completed brief
- `GET /api/campaign/{id}` — Full campaign data
- `GET /api/campaign/{id}/status` — Campaign progress
- `GET /api/agent-log/{id}` — Agent decision trail
- `GET /api/alumni` — All alumni records
- `GET /api/memories` — Cross-campaign insights

---

## Optional Integrations

**Salesforce:** Add `SALESFORCE_CONSUMER_KEY`, `SALESFORCE_CONSUMER_SECRET`, `SALESFORCE_USERNAME`, `SALESFORCE_PASSWORD`, `SALESFORCE_SECURITY_TOKEN` to `.env`. The Data Integrator will automatically pull contacts.

**Eventbrite:** Add `EVENTBRITE_OAUTH_TOKEN` to `.env`. The Data Integrator will pull past events and attendee data. Check-in webhooks can be configured via `POST /api/webhook/eventbrite`.

**File Upload:** Drop CSV, Excel, PDF, or Word files into `data/sources/`. The Data Integrator auto-detects file types, normalises column names, and loads structured data into the alumni database.

All integrations are optional. The pipeline works with local seed data alone.

---

## Database Schema

**Core tables:** `alumni`, `events`, `campaigns`, `matches`, `outreach_messages`, `checkins`

**Cycle tracking:** `cycles` (per-cycle metrics), `warm_leads` (opened but not accepted)

**Intelligence:** `agent_log` (every agent decision), `campaign_memory` (cross-campaign insights)

---

## How Matching Works

1. **Vector search** — ChromaDB finds semantically similar alumni based on event description
2. **Algorithmic scoring** (0–100) — Topic alignment (40%), location match (20%), graduation recency (15%), engagement score (15%), vector similarity (10%)
3. **LLM reasoning** — Kimi K2 reviews anonymised top candidates in batches, writes one-sentence reasoning per match
4. **Quality check** — Flags duplicates, company over-representation, low-confidence matches, GDPR issues, and outreach fatigue

---

## Outreach Algorithm

- **Target acceptances:** 1.2× target attendance (buffer for no-shows)
- **Total pool:** 5× target attendance
- **Cycle 1:** Top 2X from pool, expected 35% acceptance
- **Cycle 2:** Scenario A (≤50% of target met → send 2X more) or B (>50% → send 1.5X)
- **Cycle 3-4:** Progressively smaller batches with declining expected rates (18%, 12%)
- **Acceptance:** Only full RSVPs or explicit "yes" replies count. Opens and clicks are warm leads for follow-up, not acceptances
- **Minimum 3 days between cycles**

---

## Why Kimi K2

- Open-source (Modified MIT) — data sovereignty, self-hostable
- 1T total parameters, 32B active (MoE architecture)
- Purpose-built for agentic tool-calling (200-300 sequential tool calls)
- Groq inference: ~200 tokens/sec on LPU hardware
- OpenAI-compatible API — one line change to switch providers

---

## Team

Built for the AI Agents hackathon by the Pecan team.

---

## License

MIT
