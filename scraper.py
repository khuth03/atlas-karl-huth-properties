"""
Atlas Reonomy Scraper — Wade Asset Management
Uses direct Reonomy internal API calls (no browser automation credits used).

API endpoints:
  - POST /v2/search/count                 → total matching count
  - POST /v2/search/pins?offset=N&limit=N → property IDs
  - POST /v2/property/summaries           → batch property data
  - GET  /v2/property/{id}/ownership      → contacts (phones/emails)
  - GET  /v2/property/{id}/reported-owners → mailing address

Auth: Auth0 JWT obtained via headless co/authenticate + PKCE flow (no browser needed).
Token is cached in-memory and auto-refreshes on expiry — zero manual intervention.
"""
import logging
import time
import json
import csv
import io
import re
import os
import threading
import hashlib
import secrets
import base64
import requests
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger("atlas-scraper")

REONOMY_BASE = "https://api.reonomy.com/v2"
REONOMY_LOGIN_URL = "https://app.reonomy.com"
AUTH0_KEY = "@@auth0spajs@@::UTqjIZf5jqE0RoRCJPD216aT9CWZrq2C::https://app.reonomy.com/v2/::openid profile email"

# Industrial land use codes
INDUSTRIAL_CODES = [
    "806","0514","208","303","308","6018","311","6023","218","304","342","5005",
    "6020","830","300","338","5000","5001","5008","5013","5018","5019","5020",
    "6008","6016","320","6000","6015","321","5010","322","5012","323","324",
    "5002","5006","5015","328","6007","326","6010","331","6019","349","6003",
    "6014","334","6009","310","5016","5017","344","6006","258","207","6004",
    "354","6011","358","5004","6024","361","6021","309","800","877","6001",
    "6500","6505","6512","364","5003","6002","883","6013","886","899"
]

OFFICE_CODES = [
    "400","401","402","403","404","405","406","407","408","409","410",
    "411","412","413","414","415","416","417","418","419","420"
]

RETAIL_CODES = [
    "500","501","502","503","504","505","506","507","508","509","510",
    "511","512","513","514","515","516","517","518","519","520"
]

MULTIFAMILY_CODES = [
    "100","101","102","103","104","105","106","107","108","109","110",
    "111","112","113","114","115","116","117","118","119","120"
]

PROPERTY_TYPE_CODES = {
    "Industrial": INDUSTRIAL_CODES,
    "Office": OFFICE_CODES,
    "Retail": RETAIL_CODES,
    "Multifamily": MULTIFAMILY_CODES,
}

# CSV output columns
CSV_COLUMNS = [
    "reonomy_id", "apn", "link",
    "address_full", "address_city", "address_state", "address_zip", "county",
    "property_type", "property_subtype", "building_area_sf", "lot_size_sf",
    "lot_size_acres", "year_built", "total_units", "floors",
    "last_sale_price", "last_sale_date",
    "tax_amount", "tax_year", "total_assessed_value",
    "mortgage_amount", "mortgage_lender", "mortgage_date",
    "reported_owner_name", "owner_type",
    "mailing_address", "mailing_city", "mailing_state", "mailing_zip",
    "owner_instate",
    "contact_1_name", "contact_1_title", "contact_1_company",
    "contact_1_phone_1", "contact_1_phone_2", "contact_1_phone_3", "contact_1_phone_4", "contact_1_phone_5",
    "contact_1_email_1", "contact_1_email_2", "contact_1_email_3",
    "contact_2_name", "contact_2_title", "contact_2_company",
    "contact_2_phone_1", "contact_2_phone_2", "contact_2_phone_3", "contact_2_phone_4", "contact_2_phone_5",
    "contact_2_email_1", "contact_2_email_2", "contact_2_email_3",
    "contact_3_name", "contact_3_title", "contact_3_company",
    "contact_3_phone_1", "contact_3_phone_2", "contact_3_phone_3", "contact_3_phone_4", "contact_3_phone_5",
    "contact_3_email_1", "contact_3_email_2", "contact_3_email_3",
    "contact_4_name", "contact_4_title", "contact_4_company",
    "contact_4_phone_1", "contact_4_phone_2", "contact_4_phone_3", "contact_4_phone_4", "contact_4_phone_5",
    "contact_4_email_1", "contact_4_email_2", "contact_4_email_3",
    "contact_5_name", "contact_5_title", "contact_5_company",
    "contact_5_phone_1", "contact_5_phone_2", "contact_5_phone_3", "contact_5_phone_4", "contact_5_phone_5",
    "contact_5_email_1", "contact_5_email_2", "contact_5_email_3",
]

# ── Token Management ──────────────────────────────────────────────────────────
_token_lock = threading.Lock()
_cached_token = None
_token_expires_at = 0


def get_reonomy_token(email: str, password: str) -> str:
    global _cached_token, _token_expires_at
    with _token_lock:
        now = time.time()
        if _cached_token and _token_expires_at > now + 300:
            logger.info("Using cached token (expires in %.0f min)", (_token_expires_at - now) / 60)
            return _cached_token
        env_token = os.environ.get("REONOMY_TOKEN", "").strip()
        env_expires = float(os.environ.get("REONOMY_TOKEN_EXPIRES_AT", "0").strip() or "0")
        if env_token and env_expires > now + 300:
            logger.info("Using env token (expires in %.0f min)", (env_expires - now) / 60)
            _cached_token = env_token
            _token_expires_at = env_expires
            return env_token
        logger.info("Refreshing Reonomy token via Auth0 headless flow...")
        token, expires_at = _login_and_get_token(email, password)
        _cached_token = token
        _token_expires_at = expires_at
        logger.info("Got fresh token (expires in %.0f min)", (expires_at - now) / 60)
        return token


def _login_and_get_token(email: str, password: str) -> tuple:
    """Get a fresh Reonomy token via Auth0 co/authenticate + PKCE flow.
    No browser or Playwright needed — works from any server IP.
    """
    AUTH0_DOMAIN = "auth.reonomy.com"
    CLIENT_ID = "UTqjIZf5jqE0RoRCJPD216aT9CWZrq2C"
    REDIRECT_URI = "https://app.reonomy.com/callback"
    REALM = "Username-Password-Authentication"
    hdrs = {
        "Content-Type": "application/json",
        "Origin": "https://app.reonomy.com",
        "Referer": "https://app.reonomy.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }
    sess = requests.Session()
    sess.headers.update(hdrs)

    # Step 1: co/authenticate
    r1 = sess.post(
        f"https://{AUTH0_DOMAIN}/co/authenticate",
        json={
            "client_id": CLIENT_ID,
            "username": email,
            "password": password,
            "realm": REALM,
            "credential_type": "http://auth0.com/oauth/grant-type/password-realm"
        },
        timeout=20
    )
    r1.raise_for_status()
    d1 = r1.json()
    login_ticket = d1["login_ticket"]
    co_verifier = d1["co_verifier"]

    # Step 2: PKCE
    code_verifier = secrets.token_urlsafe(64)
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode()).digest()
    ).rstrip(b"=").decode()
    state = secrets.token_urlsafe(16)
    nonce = secrets.token_urlsafe(16)

    # Step 3: /authorize — returns 302 directly to callback with code
    r2 = sess.get(
        f"https://{AUTH0_DOMAIN}/authorize",
        params={
            "client_id": CLIENT_ID,
            "response_type": "code",
            "redirect_uri": REDIRECT_URI,
            "scope": "openid profile email",
            "audience": "https://app.reonomy.com/v2/",
            "state": state,
            "nonce": nonce,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
            "login_ticket": login_ticket,
            "co_verifier": co_verifier,
            "realm": REALM,
        },
        allow_redirects=False,
        timeout=20
    )
    location = r2.headers.get("location", "")
    if "code=" not in location:
        raise ValueError(f"No code in /authorize redirect: status={r2.status_code} location={location[:200]}")

    parsed = urlparse(location)
    code = parse_qs(parsed.query).get("code", [None])[0]
    if not code:
        raise ValueError(f"Could not extract code from: {location[:200]}")

    # Step 4: Exchange code for tokens
    r3 = sess.post(
        f"https://{AUTH0_DOMAIN}/oauth/token",
        json={
            "grant_type": "authorization_code",
            "client_id": CLIENT_ID,
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "code_verifier": code_verifier,
        },
        timeout=20
    )
    r3.raise_for_status()
    token_data = r3.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise ValueError(f"No access_token in response: {token_data}")
    expires_in = token_data.get("expires_in", 86400)
    expires_at = time.time() + expires_in
    return access_token, expires_at


# ── API Helpers ───────────────────────────────────────────────────────────────
def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token.strip()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": "https://app.reonomy.com",
        "Referer": "https://app.reonomy.com/!/search",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }


def get_total_count(token: str, settings: dict) -> int:
    payload = {"settings": settings, "bounding_box": {}}
    logger.info("search/count payload: %s", json.dumps(payload)[:500])
    r = requests.post(
        f"{REONOMY_BASE}/search/count",
        headers=_headers(token),
        json=payload,
        timeout=30
    )
    if not r.ok:
        logger.error("search/count error %d: %s", r.status_code, r.text[:500])
    r.raise_for_status()
    return r.json().get("count", 0)


def get_property_ids(token: str, settings: dict, offset: int = 0, limit: int = 500) -> list:
    r = requests.post(
        f"{REONOMY_BASE}/search/pins?offset={offset}&limit={limit}",
        headers=_headers(token),
        json={"settings": settings, "bounding_box": {}},
        timeout=30
    )
    r.raise_for_status()
    return [item["id"] for item in r.json().get("items", [])]


def get_property_summaries(token: str, property_ids: list) -> list:
    if not property_ids:
        return []
    r = requests.post(
        f"{REONOMY_BASE}/property/summaries",
        headers=_headers(token),
        json={"property_ids": property_ids},
        timeout=30
    )
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("items", [])


def get_property_ownership(token: str, property_id: str) -> list:
    for attempt in range(3):
        r = requests.get(
            f"{REONOMY_BASE}/property/{property_id}/ownership",
            headers=_headers(token),
            timeout=30
        )
        if r.status_code == 404:
            return []
        if r.status_code == 429:
            time.sleep(5 * (attempt + 1))
            continue
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            contacts = []
            for obj in data:
                if isinstance(obj, dict):
                    contacts.extend(obj.get("contacts", []))
            return contacts
        return []
    return []


def get_reported_owners(token: str, property_id: str) -> dict:
    for attempt in range(3):
        r = requests.get(
            f"{REONOMY_BASE}/property/{property_id}/reported-owners",
            headers=_headers(token),
            timeout=30
        )
        if r.status_code in (404, 403):
            return {}
        if r.status_code == 429:
            time.sleep(5 * (attempt + 1))
            continue
        r.raise_for_status()
        return r.json()
    return {}


# ── Buy-Box Settings Builder ──────────────────────────────────────────────────
def build_search_settings(params: dict) -> dict:
    settings = {}

    property_type = params.get("property_type", "Industrial")
    if property_type in PROPERTY_TYPE_CODES:
        settings["land_use_code"] = PROPERTY_TYPE_CODES[property_type]
    else:
        settings["land_use_code"] = INDUSTRIAL_CODES

    states = params.get("states", [])
    if isinstance(states, str):
        states = [s.strip() for s in states.split(",") if s.strip()]
    if states:
        settings["state"] = states

    counties = params.get("counties", [])
    if isinstance(counties, str):
        counties = [c.strip() for c in counties.split(",") if c.strip()]
    if counties:
        settings["county"] = counties

    building_area = {}
    if params.get("building_sf_min"):
        try:
            building_area["min"] = int(params["building_sf_min"])
        except (ValueError, TypeError):
            pass
    if params.get("building_sf_max"):
        try:
            building_area["max"] = int(params["building_sf_max"])
        except (ValueError, TypeError):
            pass
    if building_area:
        settings["building_area"] = building_area

    year_built = {}
    if params.get("year_built_min"):
        try:
            year_built["min"] = int(params["year_built_min"])
        except (ValueError, TypeError):
            pass
    if params.get("year_built_max"):
        try:
            year_built["max"] = int(params["year_built_max"])
        except (ValueError, TypeError):
            pass
    if year_built:
        settings["year_built"] = year_built

    lot_size = {}
    if params.get("lot_size_min"):
        try:
            lot_size["min"] = int(params["lot_size_min"])
        except (ValueError, TypeError):
            pass
    if params.get("lot_size_max"):
        try:
            lot_size["max"] = int(params["lot_size_max"])
        except (ValueError, TypeError):
            pass
    if lot_size:
        settings["lot_size"] = lot_size

    if params.get("owner_occupied") in (True, "true", "True", "1", 1):
        settings["owner_occupied"] = True

    if params.get("last_sale_after"):
        settings["last_sale_date"] = {"min": params["last_sale_after"]}

    assessed_value = {}
    if params.get("assessed_value_min"):
        try:
            assessed_value["min"] = int(params["assessed_value_min"])
        except (ValueError, TypeError):
            pass
    if params.get("assessed_value_max"):
        try:
            assessed_value["max"] = int(params["assessed_value_max"])
        except (ValueError, TypeError):
            pass
    if assessed_value:
        settings["assessed_value"] = assessed_value

    return settings


# ── Contact Extraction ────────────────────────────────────────────────────────
def format_phone(number: str) -> str:
    if not number:
        return ""
    digits = re.sub(r'\D', '', str(number))
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == '1':
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return str(number)


def extract_contacts(ownership_data: list) -> list:
    contacts = []
    seen = set()

    for obj in ownership_data:
        ctype = obj.get("contact_type", "")

        if ctype in ("person", "individual"):
            persons_list = obj.get("persons", [])
            if persons_list:
                for person in persons_list:
                    name = person.get("display", "")
                    if name and name not in seen:
                        seen.add(name)
                        jobs = person.get("jobs", [])
                        contacts.append({
                            "name": name,
                            "title": jobs[0].get("title", "") if jobs else "",
                            "company": jobs[0].get("organization", "") if jobs else "",
                            "phones": [format_phone(p["number"]) for p in person.get("phones", []) if p.get("number")][:5],
                            "emails": [e["address"] for e in person.get("emails", []) if e.get("address")][:3],
                        })
            else:
                name = obj.get("display", "")
                if name and name not in seen:
                    seen.add(name)
                    jobs = obj.get("jobs", [])
                    contacts.append({
                        "name": name,
                        "title": jobs[0].get("title", "") if jobs else "",
                        "company": jobs[0].get("organization", "") if jobs else "",
                        "phones": [format_phone(p["number"]) for p in obj.get("phones", []) if p.get("number")][:5],
                        "emails": [e["address"] for e in obj.get("emails", []) if e.get("address")][:3],
                    })

        elif ctype == "company":
            company = obj.get("company", {})
            cname = company.get("name", "")
            for person in obj.get("persons", []):
                name = person.get("display", "")
                if name and name not in seen:
                    seen.add(name)
                    jobs = person.get("jobs", [])
                    contacts.append({
                        "name": name,
                        "title": jobs[0].get("title", "") if jobs else "",
                        "company": cname,
                        "phones": [format_phone(p["number"]) for p in person.get("phones", []) if p.get("number")][:5],
                        "emails": [e["address"] for e in person.get("emails", []) if e.get("address")][:3],
                    })
            if not obj.get("persons") and cname and cname not in seen:
                seen.add(cname)
                contacts.append({
                    "name": cname,
                    "title": "Company",
                    "company": cname,
                    "phones": [format_phone(p["number"]) for p in company.get("phones", []) if p.get("number")][:5],
                    "emails": [e["address"] for e in company.get("emails", []) if e.get("address")][:3],
                })

    return contacts


# ── Row Builder ───────────────────────────────────────────────────────────────
def build_row(summary: dict, ownership_data: list, reported_owners: dict) -> dict:
    prop_id = summary.get("id", "")

    parts = [p for p in [
        summary.get("house_nbr", ""),
        summary.get("direction_left", ""),
        summary.get("street", ""),
        summary.get("mode", "")
    ] if p]
    street_addr = " ".join(parts)
    city = summary.get("city", "")
    state = summary.get("state", "")
    zip5 = summary.get("zip5", "")
    address_full = f"{street_addr}, {city}, {state} {zip5}".strip(", ") if city else street_addr

    reported_owner_name = ""
    owner_type = ""
    mailing_address = mailing_city = mailing_state = mailing_zip = owner_instate = ""

    if reported_owners:
        owners_list = reported_owners.get("reported_owners", [])
        if owners_list:
            reported_owner_name = owners_list[0].get("name", "")
            owner_type = owners_list[0].get("entity_type", "")
        mailing_address = reported_owners.get("address_line1", "")
        mailing_city = reported_owners.get("city", "")
        mailing_state = reported_owners.get("state", "")
        mailing_zip = reported_owners.get("zip_code", "")
        owner_instate = "Yes" if (mailing_state and mailing_state == state) else ("No" if mailing_state else "")

    if not reported_owner_name:
        reported_owner_name = summary.get("first_owner_name", "")

    contacts = extract_contacts(ownership_data)

    def gc(idx):
        if idx < len(contacts):
            return contacts[idx]
        return {"name": "", "title": "", "company": "", "phones": [], "emails": []}

    c1, c2, c3, c4, c5 = gc(0), gc(1), gc(2), gc(3), gc(4)

    def ph(c, i): return c["phones"][i] if len(c["phones"]) > i else ""
    def em(c, i): return c["emails"][i] if len(c["emails"]) > i else ""

    return {
        "reonomy_id": prop_id,
        "apn": summary.get("formatted_apn", ""),
        "link": f"https://app.reonomy.com/!/property/{prop_id}",
        "address_full": address_full,
        "address_city": city,
        "address_state": state,
        "address_zip": zip5,
        "county": summary.get("fips_county", ""),
        "property_type": summary.get("asset_category", ""),
        "property_subtype": summary.get("asset_type", ""),
        "building_area_sf": summary.get("building_area", ""),
        "lot_size_sf": summary.get("lot_size_sqft", ""),
        "lot_size_acres": summary.get("lot_size_acres", ""),
        "year_built": summary.get("year_built", ""),
        "total_units": summary.get("total_units", ""),
        "floors": summary.get("floors", ""),
        "last_sale_price": summary.get("sales_price", ""),
        "last_sale_date": summary.get("sales_date", ""),
        "tax_amount": summary.get("tax_amount", ""),
        "tax_year": summary.get("tax_year", ""),
        "total_assessed_value": summary.get("total_assessed_value", ""),
        "mortgage_amount": summary.get("mortgage_amount", ""),
        "mortgage_lender": summary.get("mortgage_standardized_name", "") or summary.get("mortgage_recorded_name", ""),
        "mortgage_date": summary.get("mortgage_recording_date", ""),
        "reported_owner_name": reported_owner_name,
        "owner_type": owner_type,
        "mailing_address": mailing_address,
        "mailing_city": mailing_city,
        "mailing_state": mailing_state,
        "mailing_zip": mailing_zip,
        "owner_instate": owner_instate,
        "contact_1_name": c1["name"], "contact_1_title": c1["title"], "contact_1_company": c1["company"],
        "contact_1_phone_1": ph(c1,0), "contact_1_phone_2": ph(c1,1), "contact_1_phone_3": ph(c1,2), "contact_1_phone_4": ph(c1,3), "contact_1_phone_5": ph(c1,4),
        "contact_1_email_1": em(c1,0), "contact_1_email_2": em(c1,1), "contact_1_email_3": em(c1,2),
        "contact_2_name": c2["name"], "contact_2_title": c2["title"], "contact_2_company": c2["company"],
        "contact_2_phone_1": ph(c2,0), "contact_2_phone_2": ph(c2,1), "contact_2_phone_3": ph(c2,2), "contact_2_phone_4": ph(c2,3), "contact_2_phone_5": ph(c2,4),
        "contact_2_email_1": em(c2,0), "contact_2_email_2": em(c2,1), "contact_2_email_3": em(c2,2),
        "contact_3_name": c3["name"], "contact_3_title": c3["title"], "contact_3_company": c3["company"],
        "contact_3_phone_1": ph(c3,0), "contact_3_phone_2": ph(c3,1), "contact_3_phone_3": ph(c3,2), "contact_3_phone_4": ph(c3,3), "contact_3_phone_5": ph(c3,4),
        "contact_3_email_1": em(c3,0), "contact_3_email_2": em(c3,1), "contact_3_email_3": em(c3,2),
        "contact_4_name": c4["name"], "contact_4_title": c4["title"], "contact_4_company": c4["company"],
        "contact_4_phone_1": ph(c4,0), "contact_4_phone_2": ph(c4,1), "contact_4_phone_3": ph(c4,2), "contact_4_phone_4": ph(c4,3), "contact_4_phone_5": ph(c4,4),
        "contact_4_email_1": em(c4,0), "contact_4_email_2": em(c4,1), "contact_4_email_3": em(c4,2),
        "contact_5_name": c5["name"], "contact_5_title": c5["title"], "contact_5_company": c5["company"],
        "contact_5_phone_1": ph(c5,0), "contact_5_phone_2": ph(c5,1), "contact_5_phone_3": ph(c5,2), "contact_5_phone_4": ph(c5,3), "contact_5_phone_5": ph(c5,4),
        "contact_5_email_1": em(c5,0), "contact_5_email_2": em(c5,1), "contact_5_email_3": em(c5,2),
    }


# ── Main Scrape Function ──────────────────────────────────────────────────────
def run_scrape_job(job_params: dict, progress_callback=None) -> tuple:
    """
    Main scrape function. Returns (records_list, csv_string).
    job_params keys:
      reonomy_email, reonomy_password,
      property_type, states, counties,
      building_sf_min, building_sf_max,
      year_built_min, year_built_max,
      lot_size_min, lot_size_max,
      owner_occupied, last_sale_after,
      assessed_value_min, assessed_value_max,
      include_contacts, max_records
    """
    email = job_params.get("reonomy_email", "")
    password = job_params.get("reonomy_password", "")
    include_contacts = job_params.get("include_contacts", True)
    max_records = int(job_params.get("max_records", 100))

    token = get_reonomy_token(email, password)
    settings = build_search_settings(job_params)
    logger.info("Search settings: %s", settings)

    total_count = get_total_count(token, settings)
    logger.info("Total matching: %d (scraping up to %d)", total_count, max_records)

    if progress_callback:
        progress_callback("total", min(total_count, max_records))
        progress_callback("count", total_count)

    all_ids = []
    offset = 0
    while len(all_ids) < max_records:
        remaining = max_records - len(all_ids)
        fetch_size = min(500, remaining)
        ids = get_property_ids(token, settings, offset=offset, limit=fetch_size)
        if not ids:
            break
        all_ids.extend(ids)
        offset += len(ids)
        if len(ids) < fetch_size:
            break

    all_ids = all_ids[:max_records]
    logger.info("Collected %d property IDs", len(all_ids))

    records = []
    processed = 0
    for i in range(0, len(all_ids), 25):
        batch_ids = all_ids[i:i + 25]
        try:
            summaries = get_property_summaries(token, batch_ids)
        except Exception as e:
            logger.error("Summaries failed: %s", e)
            continue

        summary_map = {s.get("id"): s for s in summaries}

        for prop_id in batch_ids:
            summary = summary_map.get(prop_id, {"id": prop_id})
            ownership_data = []
            reported_owners = {}

            if include_contacts:
                try:
                    ownership_data = get_property_ownership(token, prop_id)
                except Exception as e:
                    logger.warning("Ownership failed for %s: %s", prop_id, e)
                try:
                    reported_owners = get_reported_owners(token, prop_id)
                except Exception as e:
                    logger.warning("Reported owners failed for %s: %s", prop_id, e)

            row = build_row(summary, ownership_data, reported_owners)
            records.append(row)
            processed += 1

            if progress_callback:
                progress_callback("progress", processed)

            time.sleep(0.3)

        time.sleep(1.0)

    logger.info("Done: %d records", len(records))
    return records, build_csv(records)


def build_csv(records: list) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for row in records:
        writer.writerow(row)
    return output.getvalue()
