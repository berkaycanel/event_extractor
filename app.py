import streamlit as st
import json
import re
import requests
from datetime import datetime
from urllib.parse import urlparse, urljoin

import google.generativeai as genai
from firecrawl import FirecrawlApp


# ── CONFIG ────────────────────────────────────────────────────
FIRECRAWL_API_KEY = st.secrets["FIRECRAWL_API_KEY"]
GEMINI_API_KEY = st.secrets["GEMINI_API_KEY"]
AIRTABLE_API_KEY = st.secrets["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID = st.secrets["AIRTABLE_BASE_ID"]
AIRTABLE_TABLE_ID = st.secrets["AIRTABLE_TABLE_ID"]

AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
DEFAULT_URL = "https://saastrlondon.com/"


# ── INIT GEMINI ───────────────────────────────────────────────
genai.configure(api_key=GEMINI_API_KEY)

model = genai.GenerativeModel(
    model_name="gemini-2.5-flash",
    generation_config={
        "temperature": 0,
        "response_mime_type": "application/json",
    },
)


# ── PROMPT ────────────────────────────────────────────────────
SYSTEM_PROMPT = """
Du bist ein hochpräziser Daten-Extraktor für Event-Websites.

WICHTIG:
- Gib ausschließlich valides JSON zurück
- KEIN zusätzlicher Text
- KEINE Halluzinationen

event_type MUSS einer dieser Werte sein:
["Konferenz/Summit", "Workshop/Hackathon", "Meetup/Networking", "Webinar", "Pitch", "Expo/Messe", "Award", "Collaboration"]

WICHTIG:
- Sponsoren müssen möglichst vollständig extrahiert werden
- Auch Inhalte aus /partners oder /sponsors berücksichtigen

Schema:
{
  "url": "",
  "title": "",
  "start_datetime": "",
  "end_datetime": "",
  "location_text": "",
  "city": "",
  "country": "",
  "event_type": "",
  "description": "",
  "tags": [],
  "event_source": "Event Own Website",
  "speakers": [],
  "sponsors": []
}
"""


# ── HELPERS ───────────────────────────────────────────────────
def is_valid_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in ("http", "https") and parsed.netloc


def build_candidate_urls(base_url: str):
    base = base_url.rstrip("/")
    paths = [
        "",
        "/partners",
        "/sponsors",
        "/agenda",
        "/speakers",
        "/speaker",
        "/vortragende",
        "/referenten",
        "/faculty"
    ]

    return list(dict.fromkeys([urljoin(base + "/", p.lstrip("/")) for p in paths]))


# ── FIRECRAWL ─────────────────────────────────────────────────
def fetch_with_firecrawl(url: str) -> str:
    app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)

    if hasattr(app, "scrape"):
        result = app.scrape(url, formats=["markdown"])
    else:
        result = app.scrape_url(url, formats=["markdown"])

    markdown = getattr(result, "markdown", "")
    if not markdown and isinstance(result, dict):
        markdown = result.get("markdown", "")

    return markdown


def fetch_multiple_pages(url: str):
    pages = {}
    for u in build_candidate_urls(url):
        try:
            md = fetch_with_firecrawl(u)
            if md and len(md) > 200:
                pages[u] = md
        except:
            pass
    return pages


# ── GEMINI EXTRACTION ─────────────────────────────────────────
def extract_with_gemini(pages: dict, url: str):
    combined = ""
    for u, md in pages.items():
        combined += f"\n\n=== PAGE: {u} ===\n{md}"

    prompt = f"""
Website URL: {url}

Extrahiere Event-Daten aus ALLEN folgenden Seiten:

{combined}
"""

    response = model.generate_content([SYSTEM_PROMPT, prompt])
    raw = response.text.strip()

    try:
        data = json.loads(raw)

        if isinstance(data, list):
            data = data[0] if data else {"error": "Empty list"}

        if not isinstance(data, dict):
            return {"error": "Invalid format", "raw": raw}

        data["url"] = url
        data.setdefault("event_source", "Event Own Website")
        data.setdefault("tags", [])
        data.setdefault("speakers", [])
        data.setdefault("sponsors", [])

        return data

    except:
        return {"error": "JSON parse failed", "raw": raw}


# ── NEW: SPEAKER EXTRACTION (RULE-BASED) ───────────────────────
def extract_speakers_from_pages(pages: dict):
    speakers = set()

    for url, md in pages.items():
        if any(x in url.lower() for x in ["speaker", "vortrag", "referent", "faculty"]):
            lines = md.split("\n")

            for line in lines:
                line = line.strip()

                if 3 < len(line) < 80:
                    if not any(x in line.lower() for x in ["http", "cookie", "login"]):
                        speakers.add(line)

    return [{"name": s} for s in list(speakers)[:100]]


# ── POST-PROCESSING ───────────────────────────────────────────
def enrich_location_fields(data):
    loc = data.get("location_text", "")

    if not data.get("city") and loc:
        parts = re.split(r",|\|", loc)
        if parts:
            data["city"] = parts[0].strip()

    return data


def normalize_event_type(data):
    VALID = [
        "Konferenz/Summit", "Workshop/Hackathon", "Meetup/Networking",
        "Webinar", "Pitch", "Expo/Messe", "Award", "Collaboration"
    ]
    if data.get("event_type") not in VALID:
        data["event_type"] = ""
    return data


def clean_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).strftime("%Y-%m-%d")
    except:
        return None


def get_current_timestamp():
    return datetime.utcnow().isoformat()


# ── AIRTABLE ──────────────────────────────────────────────────
def send_to_airtable(data):
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }

    record = {
        "fields": {
            "url": data.get("url"),
            "title": data.get("title"),
            "start_datetime": clean_date(data.get("start_datetime")),
            "end_datetime": clean_date(data.get("end_datetime")),
            "location_text": data.get("location_text"),
            "city": data.get("city"),
            "country": data.get("country"),
            "event_type": data.get("event_type"),
            "description": data.get("description"),
            "tags": ", ".join(data.get("tags", [])),
            "Event Source": data.get("event_source"),
            "Speakers": json.dumps(data.get("speakers", [])),
            "Sponsors": json.dumps(data.get("sponsors", [])),
            "Zeitpunkt der Erstellung": get_current_timestamp(),
        }
    }

    return requests.post(AIRTABLE_URL, headers=headers, json=record)


# ── UI ────────────────────────────────────────────────────────
st.set_page_config(page_title="Event Extractor", page_icon="🎤")

st.title("Event Extractor → Airtable")

url = st.text_input("Event URL", value=DEFAULT_URL)

if st.button("Execute"):

    if not is_valid_url(url):
        st.error("Invalid URL")
    else:
        try:
            progress = st.progress(0)
            status = st.empty()

            status.write("Fetching pages...")
            progress.progress(20)

            pages = fetch_multiple_pages(url)

            status.write(f"Fetched {len(pages)} pages")
            progress.progress(40)

            status.write("Extracting data...")
            data = extract_with_gemini(pages, url)

            progress.progress(70)

            if not isinstance(data, dict) or data.get("error"):
                progress.progress(100)
                status.write("❌ Extraction failed")

                st.error("Extraction failed")

                if isinstance(data, dict):
                    st.code(data.get("raw", ""), language="json")
                else:
                    st.write(data)

            else:
                # 🔥 ADDITION: merge speakers
                extra_speakers = extract_speakers_from_pages(pages)

                existing = set([s.get("name") for s in data.get("speakers", []) if isinstance(s, dict)])

                for sp in extra_speakers:
                    if sp["name"] not in existing:
                        data["speakers"].append(sp)

                data = enrich_location_fields(data)
                data = normalize_event_type(data)

                status.write("Sending to Airtable...")
                progress.progress(90)

                res = send_to_airtable(data)

                progress.progress(100)
                status.write("Done")

                if res.status_code in [200, 201]:
                    st.success("Saved to Airtable")
                else:
                    st.error(res.text)

                st.json(data)

        except Exception as e:
            progress.progress(100)
            st.error(str(e))
