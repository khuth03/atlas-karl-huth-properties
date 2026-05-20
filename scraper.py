"""
Atlas Reonomy Scraper — Karl Huth Properties
Uses direct Reonomy internal API calls (no browser automation, no credits used).

API endpoints discovered via reverse engineering:
  - POST /v2/search/pins?offset=N&limit=N  → property IDs
  - POST /v2/property/summaries            → batch property data
  - GET  /v2/property/{id}/ownership       → contacts (phones/emails)
  - GET  /v2/property/{id}/reported-owners → mailing address

Filter fields confirmed working:
  - land_use_code: list of asset type codes
  - state: ["FL"]
  - building_area: {"max": 100000}
  - owner_occupied: True
"""
import logging
import time
import json
import csv
import io
import re
import os
import threading
import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

logger = logging.getLogger("atlas-scraper")
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

REONOMY_BASE = "https://api.reonomy.com/v2"
REONOMY_LOGIN_URL = "https://app.reonomy.com"
AUTH0_KEY = "@@auth0spajs@@::UTqjIZf5jqE0RoRCJPD216aT9CWZrq2C::https://app.reonomy.com/v2/::openid profile email"

# Asset type land use codes by category
ASSET_TYPE_CODES = {
    "industrial": [
        "806","0514","208","303","308","6018","311","6023","218","304","342","5005",
        "6020","830","300","338","5000","5001","5008","5013","5018","5019","5020",
        "6008","6016","320","6000","6015","321","5010","322","5012","323","324",
        "5002","5006","5015","328","6007","326","6010","331","6019","349","6003",
        "6014","334","6009","310","5016","5017","344","6006","258","207","6004",
        "354","6011","358","5004","6024","361","6021","309","800","877","6001",
        "6500","6505","6512","364","5003","6002","883","6013","886","899"
    ],
    "multifamily": [
        "100","101","102","103","104","105","106","107","108","109",
        "110","111","112","113","114","115","116","117","118","119",
        "120","121","122","123","124","125","126","127","128","129",
        "130","131","132","133","134","135","136","137","138","139",
        "140","141","142","143","144","145","146","147","148","149",
        "150","151","152","153","154","155","156","157","158","159",
        "160","161","162","163","164","165","166","167","168","169",
        "170","171","172","173","174","175","176","177","178","179",
        "180","181","182","183","184","185","186","187","188","189",
        "190","191","192","193","194","195","196","197","198","199"
    ],
    "retail": [
        "400","401","402","403","404","405","406","407","408","409",
        "410","411","412","413","414","415","416","417","418","419",
        "420","421","422","423","424","425","426","427","428","429",
        "430","431","432","433","434","435","436","437","438","439",
        "440","441","442","443","444","445","446","447","448","449",
        "450","451","452","453","454","455","456","457","458","459"
    ],
    "office": [
        "200","201","202","203","204","205","206","207","208","209",
        "210","211","212","213","214","215","216","217","218","219",
        "220","221","222","223","224","225","226","227","228","229",
        "230","231","232","233","234","235","236","237","238","239",
        "240","241","242","243","244","245","246","247","248","249"
    ],
    "mixed_use": [
        "700","701","702","703","704","705","706","707","708","709",
        "710","711","712","713","714","715","716","717","718","719",
        "720","721","722","723","724","725","726","727","728","729"
    ],
    "hospitality": [
        "500","501","502","503","504","505","506","507","508","509",
        "510","511","512","513","514","515","516","517","518","519",
        "520","521","522","523","524","525","526","527","528","529"
    ],
}

# All commercial codes combined
ALL_COMMERCIAL_CODES = [code for codes in ASSET_TYPE_CODES.values() for code in codes]

# Legacy alias
INDUSTRIAL_CODES = ASSET_TYPE_CODES["industrial"]

# Output columns
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
    "contact_1_phone_1", "contact_1_phone_2", "contact_1_phone_3",
    "contact_1_email_1", "contact_1_email_2",
    "contact_2_name", "contact_2_title", "contact_2_company",
    "contact_2_phone_1", "contact_2_phone_2",
    "contact_2_email_1",
    "contact_3_name", "contact_3_title", "contact_3_company",
    "contact_3_phone_1",
    "contact_3_email_1",
]

# Token cache
_token_lock = threading.Lock()
_cached_token = None
_token_expires_at = 0


def get_reonomy_token(email: str, password: str) -> str:
    global _cached_token, _token_expires_at
    with _token_lock:
        now = time.time()
        # 1. Use in-memory cache if still valid
        if _cached_token and _token_expires_at > now + 300:
            logger.info("Using cached token (expires in %.0f min)", (_token_expires_at - now) / 60)
            return _cached_token
        # 2. Check environment variable (set by Railway env vars)
        env_token = os.environ.get("REONOMY_TOKEN", "").strip()
        env_expires = float(os.environ.get("REONOMY_TOKEN_EXPIRES_AT", "0").strip())
        if env_token and env_expires > now + 300:
            logger.info("Using token from env var (expires in %.0f min)", (env_expires - now) / 60)
            _cached_token = env_token
            _token_expires_at = env_expires
            return env_token
        # 3. Fall back to browser login
        logger.info("Authenticating with Reonomy via browser...")
        token, expires_at = _login_and_get_token(email, password)
        _cached_token = token
        _token_expires_at = expires_at
        logger.info("Got fresh token (expires in %.0f min)", (expires_at - now) / 60)
        return token


def _login_and_get_token(email: str, password: str) -> tuple:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = browser.new_context()
        page = ctx.new_page()
        try:
            logger.info("Navigating to Reonomy...")
            page.goto(REONOMY_LOGIN_URL, wait_until="networkidle", timeout=60000)
            
            # Check if already logged in
            token_data = page.evaluate(
                f"JSON.parse(localStorage.getItem({json.dumps(AUTH0_KEY)}) || 'null')"
            )
            if token_data:
                body = token_data.get("body", {})
                expires_at = token_data.get("expiresAt", 0)
                if expires_at > time.time() + 300:
                    logger.info("Found valid token in localStorage")
                    return body["access_token"], expires_at
            
            # Log in
            logger.info("Logging in to Reonomy...")
            # Wait for login form
            try:
                page.wait_for_selector("#username, input[type='email']", timeout=15000)
                page.fill("#username, input[type='email']", email)
                page.fill("#password, input[type='password']", password)
                page.click("button[type='submit']")
                page.wait_for_load_state("networkidle", timeout=60000)
            except Exception as e:
                logger.warning("Login attempt 1 failed: %s", e)
                # Try clicking login button first
                try:
                    page.click("text=Log in", timeout=5000)
                    page.wait_for_selector("#username", timeout=10000)
                    page.fill("#username", email)
                    page.fill("#password", password)
                    page.click("button[type='submit']")
                    page.wait_for_load_state("networkidle", timeout=60000)
                except Exception as e2:
                    logger.error("Login failed: %s", e2)
                    raise
            
            token_data = page.evaluate(
                f"JSON.parse(localStorage.getItem({json.dumps(AUTH0_KEY)}) || 'null')"
            )
            if not token_data:
                raise ValueError("No auth token found after login")
            
            body = token_data.get("body", {})
            expires_at = token_data.get("expiresAt", 0)
            return body["access_token"], expires_at
        finally:
            browser.close()


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token.strip()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": "https://app.reonomy.com",
        "Referer": "https://app.reonomy.com/!/search",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    }


def get_total_count(token: str, settings: dict) -> int:
    r = requests.post(
        f"{REONOMY_BASE}/search/count",
        headers=_headers(token),
        json={"settings": settings, "bounding_box": {}},
        timeout=30
    )
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
            wait = 5 * (attempt + 1)
            logger.debug("Rate limited on ownership, waiting %ds", wait)
            time.sleep(wait)
            continue
        r.raise_for_status()
        break
    else:
        return []
    data = r.json()
    # Response is a list of ownership objects, each with a "contacts" array
    # e.g. [{"contacts": [{"contact_type": ..., "persons": [...]}], "property_id": ...}]
    if isinstance(data, list):
        contacts = []
        for ownership_obj in data:
            if isinstance(ownership_obj, dict):
                contacts.extend(ownership_obj.get("contacts", []))
        return contacts
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
            wait = 5 * (attempt + 1)
            logger.debug("Rate limited on reported-owners, waiting %ds", wait)
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    return {}


def format_phone(number: str) -> str:
    if not number:
        return ""
    digits = re.sub(r'\D', '', str(number))
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == '1':
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return number


def extract_contacts(ownership_data: list) -> list:
    """
    Extract person contacts from the ownership contacts list.
    contact_type can be: 'person', 'company', 'individual'
    'individual' type has persons[] but no company.
    """
    contacts = []
    seen = set()
    
    for obj in ownership_data:
        ctype = obj.get("contact_type")
        
        if ctype in ("person", "individual"):
            # 'individual' type wraps persons in a persons[] array
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
                            "phones": [format_phone(p["number"]) for p in person.get("phones", []) if p.get("number")][:3],
                            "emails": [e["address"] for e in person.get("emails", []) if e.get("address")][:2],
                        })
            else:
                # Direct person object
                name = obj.get("display", "")
                if name and name not in seen:
                    seen.add(name)
                    jobs = obj.get("jobs", [])
                    contacts.append({
                        "name": name,
                        "title": jobs[0].get("title", "") if jobs else "",
                        "company": jobs[0].get("organization", "") if jobs else "",
                        "phones": [format_phone(p["number"]) for p in obj.get("phones", []) if p.get("number")][:3],
                        "emails": [e["address"] for e in obj.get("emails", []) if e.get("address")][:2],
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
                        "phones": [format_phone(p["number"]) for p in person.get("phones", []) if p.get("number")][:3],
                        "emails": [e["address"] for e in person.get("emails", []) if e.get("address")][:2],
                    })
            
            if not obj.get("persons") and cname and cname not in seen:
                seen.add(cname)
                contacts.append({
                    "name": cname,
                    "title": "Company",
                    "company": cname,
                    "phones": [format_phone(p["number"]) for p in company.get("phones", []) if p.get("number")][:3],
                    "emails": [e["address"] for e in company.get("emails", []) if e.get("address")][:2],
                })
    
    return contacts


def build_row(summary: dict, ownership_data: list, reported_owners: dict) -> dict:
    prop_id = summary.get("id", "")
    
    # Build address
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
    
    # Reported owner / mailing
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
        owner_instate = str(reported_owners.get("owner_instate", ""))
    
    if not reported_owner_name:
        reported_owner_name = summary.get("first_owner_name", "")
    
    contacts = extract_contacts(ownership_data)
    
    def gc(idx):
        if idx < len(contacts):
            return contacts[idx]
        return {"name": "", "title": "", "company": "", "phones": [], "emails": []}
    
    c1, c2, c3 = gc(0), gc(1), gc(2)
    
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
        "contact_1_name": c1["name"],
        "contact_1_title": c1["title"],
        "contact_1_company": c1["company"],
        "contact_1_phone_1": c1["phones"][0] if len(c1["phones"]) > 0 else "",
        "contact_1_phone_2": c1["phones"][1] if len(c1["phones"]) > 1 else "",
        "contact_1_phone_3": c1["phones"][2] if len(c1["phones"]) > 2 else "",
        "contact_1_email_1": c1["emails"][0] if len(c1["emails"]) > 0 else "",
        "contact_1_email_2": c1["emails"][1] if len(c1["emails"]) > 1 else "",
        "contact_2_name": c2["name"],
        "contact_2_title": c2["title"],
        "contact_2_company": c2["company"],
        "contact_2_phone_1": c2["phones"][0] if len(c2["phones"]) > 0 else "",
        "contact_2_phone_2": c2["phones"][1] if len(c2["phones"]) > 1 else "",
        "contact_2_email_1": c2["emails"][0] if len(c2["emails"]) > 0 else "",
        "contact_3_name": c3["name"],
        "contact_3_title": c3["title"],
        "contact_3_company": c3["company"],
        "contact_3_phone_1": c3["phones"][0] if len(c3["phones"]) > 0 else "",
        "contact_3_email_1": c3["emails"][0] if len(c3["emails"]) > 0 else "",
    }


def run_scrape_job(job_params: dict, progress_callback=None) -> tuple:
    """
    Main scrape function. Returns (records_list, csv_string).
    """
    email = job_params.get("reonomy_email", "")
    password = job_params.get("reonomy_password", "")
    state = job_params.get("state", "WI")
    sf_max = job_params.get("building_sf_max", 100000)
    sf_min = job_params.get("building_sf_min", "")
    owner_occupied = job_params.get("owner_occupied", True)
    include_contacts = job_params.get("include_contacts", True)
    max_records = int(job_params.get("max_records", 100))
    
    logger.info("Starting scrape: state=%s, sf_max=%s, owner_occupied=%s, max=%d",
               state, sf_max, owner_occupied, max_records)
    
    token = get_reonomy_token(email, password)
    
    # Determine asset type codes
    asset_type = job_params.get("asset_type", "all").lower().replace("-", "_").replace(" ", "_")
    if asset_type in ASSET_TYPE_CODES:
        land_use_codes = ASSET_TYPE_CODES[asset_type]
    else:
        land_use_codes = ALL_COMMERCIAL_CODES

    # Build search settings
    settings = {
        "land_use_code": land_use_codes,
        "state": [state],
    }
    
    if sf_max and str(sf_max).strip():
        try:
            building_area = {}
            if sf_max:
                building_area["max"] = int(sf_max)
            if sf_min and str(sf_min).strip():
                building_area["min"] = int(sf_min)
            if building_area:
                settings["building_area"] = building_area
        except (ValueError, TypeError):
            pass
    
    if owner_occupied:
        settings["owner_occupied"] = True
    
    # Get total count
    total_count = get_total_count(token, settings)
    logger.info("Total matching: %d (scraping up to %d)", total_count, max_records)
    
    if progress_callback:
        progress_callback("total", min(total_count, max_records))
    
    # Collect property IDs
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
    
    # Process in batches of 25
    records = []
    processed = 0
    
    for i in range(0, len(all_ids), 25):
        batch_ids = all_ids[i:i + 25]
        logger.info("Batch %d/%d", i // 25 + 1, (len(all_ids) + 24) // 25)
        
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
            
            contact_count = sum(1 for k in ["contact_1_name", "contact_2_name", "contact_3_name"] if row.get(k))
            logger.info("[%d/%d] %s | %s sqft | Owner: %s | Contacts: %d",
                       processed, len(all_ids),
                       row.get("address_full", "N/A"),
                       row.get("building_area_sf", ""),
                       row.get("reported_owner_name", "N/A"),
                       contact_count)
            
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
