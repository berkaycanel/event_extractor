import streamlit as st
import json
import re
import requests
from datetime import datetime
from urllib.parse import urlparse

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
- ISO Datetime Format: YYYY-MM-DDTHH:MM:SS
- Wenn unsicher → leer ""

event_type MUSS einer dieser Werte sein:
["Konferenz/Summit", "Workshop/Hackathon", "Meetup/Networking", "Webinar", "Pitch", "Expo/Messe", "Award", "Collaboration"]

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
  "speakers": [
    {
      "name": "",
      "title": "",
      "company": "",
      "topic": "",
      "linkedin": ""
    }
  ],
  "sponsors": [
    {
      "name": "",
      "tier": "",
      "website": ""
    }
  ]
}
"""


# ── HELPERS ───────────────────────────────────────────────────
def is_valid_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in ("http", "https") and parsed.netloc


def fetch_with_firecrawl(url: str) -> str:
    if not is_valid_url(url):
        raise ValueError(f"Invalid URL: {url}")

    app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
    result = app.scrape(url, formats=["markdown"])

    markdown = getattr(result, "markdown", "")

    if not markdown:
        raise ValueError("No markdown returned from Firecrawl")

    return markdown


def extract_with_gemini(markdown: str, url: str) -> dict:
    prompt = f"""
Website URL: {url}

Extrahiere die Event-Daten aus folgendem Inhalt:

---

{markdown}
"""

    response = model.generate_content([
        SYSTEM_PROMPT,
        prompt
    ])

    raw = response.text.strip()

    try:
        data = json.loads(raw)
        data["url"] = url
        data.setdefault("event_source", "Event Own Website")
        data.setdefault("tags", [])
        data.setdefault("speakers", [])
        data.setdefault("sponsors", [])
        return data
    except json.JSONDecodeError:
        return {"error": "JSON parse failed", "raw": raw}


def enrich_location_fields(data: dict):
    loc = data.get("location_text", "")

    if not data.get("city") and loc:
        parts = re.split(r",|\|", loc)
        if parts:
            data["city"] = parts[0].strip()

    if not data.get("country") and loc:
        loc_lower = loc.lower()

        if "switzerland" in loc_lower:
            data["country"] = "Switzerland"
        elif "germany" in loc_lower:
            data["country"] = "Germany"
        elif "austria" in loc_lower:
            data["country"] = "Austria"

    return data


VALID_TYPES = [
    "Konferenz/Summit",
    "Workshop/Hackathon",
    "Meetup/Networking",
    "Webinar",
    "Pitch",
    "Expo/Messe",
    "Award",
    "Collaboration"
]


def normalize_event_type(data: dict):
    if data.get("event_type") not in VALID_TYPES:
        data["event_type"] = ""
    return data


def clean_date(value):
    if not value or str(value).strip() == "":
        return None

    try:
        dt = datetime.fromisoformat(value)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    try:
        dt = datetime.strptime(value, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    return None


def send_to_airtable(data: dict):
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
            "Speakers": json.dumps(data.get("speakers", []), ensure_ascii=False),
            "Sponsors": json.dumps(data.get("sponsors", []), ensure_ascii=False),
        }
    }

    response = requests.post(AIRTABLE_URL, headers=headers, json=record, timeout=60)
    return response


# ── UI ────────────────────────────────────────────────────────
st.set_page_config(page_title="Saga Event Extractor", page_icon="🎤", layout="centered")

st.title("Saga Event Extractor → Airtable")
st.caption("Paste an event website URL and send the extracted event to Airtable.")

with st.form("event_form"):
    url = st.text_input("Event URL", value=DEFAULT_URL, placeholder="https://example.com/event")
    submitted = st.form_submit_button("Execute", use_container_width=True)

if submitted:
    if not is_valid_url(url):
        st.error("Invalid URL.")
    else:
        try:
            with st.spinner("Fetching website..."):
                markdown = fetch_with_firecrawl(url)

            with st.spinner("Extracting event data..."):
                data = extract_with_gemini(markdown, url)

            if "error" in data:
                st.error("JSON parse failed.")
                st.code(data.get("raw", ""), language="json")
            else:
                data = enrich_location_fields(data)
                data = normalize_event_type(data)

                with st.spinner("Sending to Airtable..."):
                    response = send_to_airtable(data)

                if response.status_code in [200, 201]:
                    st.success("Successfully saved to Airtable.")
                else:
                    st.error(f"{response.status_code} {response.text}")

                with st.expander("Preview extracted JSON"):
                    st.json(data)

        except Exception as e:
            st.exception(e)