"""
Atlas — Wade Asset Management
Full-stack Flask app:
  - Login gate (single user)
  - Buy-box configuration UI (all Reonomy filter fields)
  - Background scrape jobs with SSE progress streaming
  - Persistent SQLite database (Railway volume at /data/atlas.db)
  - Records dashboard with search, filter, sort, pagination
  - CSV export (all records or filtered)
  - Job history
"""
import os
import json
import uuid
import sqlite3
import threading
import time
import csv
import io
import logging
from pathlib import Path
from datetime import datetime
from flask import (Flask, request, session, redirect, url_for,
                   jsonify, send_file, Response, stream_with_context)

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("atlas-app")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "wade-atlas-secret-2024")

# ── Config ────────────────────────────────────────────────────────────────────
APP_USERNAME = os.environ.get("APP_USERNAME", "karlhuth@hotmail.com")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "Karl1074$")
REONOMY_EMAIL = os.environ.get("REONOMY_EMAIL", "kaleb@multifamilycartel.com")
REONOMY_PASSWORD = os.environ.get("REONOMY_PASSWORD", "97Transam!")

DB_PATH = Path(os.environ.get("DB_PATH", "/data/atlas.db" if os.path.exists("/data") else "/tmp/atlas.db"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# In-memory job store
jobs = {}
jobs_lock = threading.Lock()

# ── Database ──────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS properties (
                reonomy_id TEXT PRIMARY KEY,
                apn TEXT,
                link TEXT,
                address_full TEXT,
                address_city TEXT,
                address_state TEXT,
                address_zip TEXT,
                county TEXT,
                property_type TEXT,
                property_subtype TEXT,
                building_area_sf TEXT,
                lot_size_sf TEXT,
                lot_size_acres TEXT,
                year_built TEXT,
                total_units TEXT,
                floors TEXT,
                last_sale_price TEXT,
                last_sale_date TEXT,
                tax_amount TEXT,
                tax_year TEXT,
                total_assessed_value TEXT,
                mortgage_amount TEXT,
                mortgage_lender TEXT,
                mortgage_date TEXT,
                reported_owner_name TEXT,
                owner_type TEXT,
                mailing_address TEXT,
                mailing_city TEXT,
                mailing_state TEXT,
                mailing_zip TEXT,
                owner_instate TEXT,
                contact_1_name TEXT, contact_1_title TEXT, contact_1_company TEXT,
                contact_1_phone_1 TEXT, contact_1_phone_2 TEXT, contact_1_phone_3 TEXT, contact_1_phone_4 TEXT, contact_1_phone_5 TEXT,
                contact_1_email_1 TEXT, contact_1_email_2 TEXT, contact_1_email_3 TEXT,
                contact_2_name TEXT, contact_2_title TEXT, contact_2_company TEXT,
                contact_2_phone_1 TEXT, contact_2_phone_2 TEXT, contact_2_phone_3 TEXT, contact_2_phone_4 TEXT, contact_2_phone_5 TEXT,
                contact_2_email_1 TEXT, contact_2_email_2 TEXT, contact_2_email_3 TEXT,
                contact_3_name TEXT, contact_3_title TEXT, contact_3_company TEXT,
                contact_3_phone_1 TEXT, contact_3_phone_2 TEXT, contact_3_phone_3 TEXT, contact_3_phone_4 TEXT, contact_3_phone_5 TEXT,
                contact_3_email_1 TEXT, contact_3_email_2 TEXT, contact_3_email_3 TEXT,
                contact_4_name TEXT, contact_4_title TEXT, contact_4_company TEXT,
                contact_4_phone_1 TEXT, contact_4_phone_2 TEXT, contact_4_phone_3 TEXT, contact_4_phone_4 TEXT, contact_4_phone_5 TEXT,
                contact_4_email_1 TEXT, contact_4_email_2 TEXT, contact_4_email_3 TEXT,
                contact_5_name TEXT, contact_5_title TEXT, contact_5_company TEXT,
                contact_5_phone_1 TEXT, contact_5_phone_2 TEXT, contact_5_phone_3 TEXT, contact_5_phone_4 TEXT, contact_5_phone_5 TEXT,
                contact_5_email_1 TEXT, contact_5_email_2 TEXT, contact_5_email_3 TEXT,
                scraped_at TEXT DEFAULT (datetime('now')),
                job_id TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                status TEXT,
                params TEXT,
                total_matching INTEGER,
                records_scraped INTEGER,
                created_at TEXT,
                completed_at TEXT,
                error TEXT
            )
        """)
        conn.commit()


init_db()


def migrate_db():
    """Add new contact columns to existing databases that have the old schema."""
    new_cols = [
        "contact_1_phone_4", "contact_1_phone_5", "contact_1_email_3",
        "contact_2_phone_3", "contact_2_phone_4", "contact_2_phone_5", "contact_2_email_2", "contact_2_email_3",
        "contact_3_phone_2", "contact_3_phone_3", "contact_3_phone_4", "contact_3_phone_5", "contact_3_email_2", "contact_3_email_3",
        "contact_4_name", "contact_4_title", "contact_4_company",
        "contact_4_phone_1", "contact_4_phone_2", "contact_4_phone_3", "contact_4_phone_4", "contact_4_phone_5",
        "contact_4_email_1", "contact_4_email_2", "contact_4_email_3",
        "contact_5_name", "contact_5_title", "contact_5_company",
        "contact_5_phone_1", "contact_5_phone_2", "contact_5_phone_3", "contact_5_phone_4", "contact_5_phone_5",
        "contact_5_email_1", "contact_5_email_2", "contact_5_email_3",
    ]
    with get_db() as conn:
        existing = {row[1] for row in conn.execute("PRAGMA table_info(properties)").fetchall()}
        for col in new_cols:
            if col not in existing:
                conn.execute(f"ALTER TABLE properties ADD COLUMN {col} TEXT DEFAULT ''")
                logger.info("Migrated: added column %s", col)
        conn.commit()


migrate_db()

# ── Auth ──────────────────────────────────────────────────────────────────────
def login_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated

# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/login", methods=["GET", "POST"])
def login():
    error = ""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if username == APP_USERNAME and password == APP_PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("dashboard"))
        error = "Invalid credentials. Please try again."
    return LOGIN_HTML.replace("{{ERROR}}", f'<div class="error">{error}</div>' if error else "")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def dashboard():
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
        recent_jobs = conn.execute(
            "SELECT * FROM jobs ORDER BY created_at DESC LIMIT 5"
        ).fetchall()
    return DASHBOARD_HTML.replace("{{TOTAL_RECORDS}}", str(total)).replace(
        "{{RECENT_JOBS}}", build_jobs_html(recent_jobs)
    )


@app.route("/scrape", methods=["GET", "POST"])
@login_required
def scrape():
    if request.method == "GET":
        return SCRAPE_HTML
    # POST: start a new scrape job
    params = {
        "reonomy_email": REONOMY_EMAIL,
        "reonomy_password": REONOMY_PASSWORD,
        "property_type": request.form.get("property_type", "Industrial"),
        "states": [s.strip().upper() for s in (request.form.getlist("states") or request.form.get("states_text", "").split(",")) if s.strip()],
        "counties": request.form.get("counties", ""),
        "building_sf_min": request.form.get("building_sf_min", ""),
        "building_sf_max": request.form.get("building_sf_max", ""),
        "year_built_min": request.form.get("year_built_min", ""),
        "year_built_max": request.form.get("year_built_max", ""),
        "lot_size_min": request.form.get("lot_size_min", ""),
        "lot_size_max": request.form.get("lot_size_max", ""),
        "owner_occupied": request.form.get("owner_occupied", ""),
        "last_sale_after": request.form.get("last_sale_after", ""),
        "assessed_value_min": request.form.get("assessed_value_min", ""),
        "assessed_value_max": request.form.get("assessed_value_max", ""),
        "include_contacts": request.form.get("include_contacts", "true") == "true",
        "max_records": request.form.get("max_records", "100"),
    }
    job_id = str(uuid.uuid4())
    with jobs_lock:
        jobs[job_id] = {
            "id": job_id,
            "status": "queued",
            "params": params,
            "progress": 0,
            "total": 0,
            "total_matching": 0,
            "log": [],
        }
    with get_db() as conn:
        conn.execute(
            "INSERT INTO jobs (id, status, params, total_matching, records_scraped, created_at) VALUES (?,?,?,?,?,?)",
            (job_id, "queued", json.dumps(params), 0, 0, datetime.utcnow().isoformat())
        )
        conn.commit()

    thread = threading.Thread(target=_run_job, args=(job_id,), daemon=True)
    thread.start()
    return redirect(url_for("job_status", job_id=job_id))


def _run_job(job_id: str):
    from scraper import run_scrape_job
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return
        job["status"] = "running"
        params = job["params"]

    def progress_cb(key, value):
        with jobs_lock:
            j = jobs.get(job_id)
            if j:
                if key == "progress":
                    j["progress"] = value
                elif key == "total":
                    j["total"] = value
                elif key == "count":
                    j["total_matching"] = value
                j["log"].append(f"{key}:{value}")

    try:
        records, csv_str = run_scrape_job(params, progress_callback=progress_cb)

        # Save to DB (upsert)
        with get_db() as conn:
            for row in records:
                row["job_id"] = job_id
                row["scraped_at"] = datetime.utcnow().isoformat()
                cols = list(row.keys())
                placeholders = ",".join(["?" for _ in cols])
                updates = ",".join([f"{c}=excluded.{c}" for c in cols if c != "reonomy_id"])
                conn.execute(
                    f"INSERT INTO properties ({','.join(cols)}) VALUES ({placeholders}) "
                    f"ON CONFLICT(reonomy_id) DO UPDATE SET {updates}",
                    [row.get(c, "") for c in cols]
                )
            conn.execute(
                "UPDATE jobs SET status=?, records_scraped=?, total_matching=?, completed_at=? WHERE id=?",
                ("completed", len(records), jobs[job_id].get("total_matching", 0),
                 datetime.utcnow().isoformat(), job_id)
            )
            conn.commit()

        with jobs_lock:
            j = jobs.get(job_id)
            if j:
                j["status"] = "completed"
                j["records"] = len(records)

    except Exception as e:
        logger.exception("Job %s failed: %s", job_id, e)
        with get_db() as conn:
            conn.execute(
                "UPDATE jobs SET status=?, error=?, completed_at=? WHERE id=?",
                ("failed", str(e), datetime.utcnow().isoformat(), job_id)
            )
            conn.commit()
        with jobs_lock:
            j = jobs.get(job_id)
            if j:
                j["status"] = "failed"
                j["error"] = str(e)


@app.route("/job/<job_id>")
@login_required
def job_status(job_id):
    with get_db() as conn:
        job_row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    job_mem = {}
    with jobs_lock:
        job_mem = dict(jobs.get(job_id, {}))
    return JOB_STATUS_HTML.replace("{{JOB_ID}}", job_id).replace(
        "{{JOB_DATA}}", json.dumps({
            "id": job_id,
            "status": job_mem.get("status", job_row["status"] if job_row else "unknown"),
            "progress": job_mem.get("progress", 0),
            "total": job_mem.get("total", 0),
            "total_matching": job_mem.get("total_matching", job_row["total_matching"] if job_row else 0),
            "records": job_mem.get("records", job_row["records_scraped"] if job_row else 0),
        })
    )


@app.route("/api/job/<job_id>/status")
@login_required
def api_job_status(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
    if job:
        return jsonify({
            "status": job["status"],
            "progress": job.get("progress", 0),
            "total": job.get("total", 0),
            "total_matching": job.get("total_matching", 0),
            "records": job.get("records", 0),
            "error": job.get("error", ""),
        })
    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if row:
        return jsonify({
            "status": row["status"],
            "progress": row["records_scraped"] or 0,
            "total": row["records_scraped"] or 0,
            "total_matching": row["total_matching"] or 0,
            "records": row["records_scraped"] or 0,
            "error": row["error"] or "",
        })
    return jsonify({"status": "not_found"}), 404


@app.route("/records")
@login_required
def records():
    page = int(request.args.get("page", 1))
    per_page = 50
    search = request.args.get("q", "").strip()
    state_filter = request.args.get("state", "").strip()
    type_filter = request.args.get("type", "").strip()

    where_clauses = []
    params = []
    if search:
        where_clauses.append(
            "(address_full LIKE ? OR reported_owner_name LIKE ? OR contact_1_name LIKE ? OR county LIKE ?)"
        )
        like = f"%{search}%"
        params.extend([like, like, like, like])
    if state_filter:
        where_clauses.append("address_state = ?")
        params.append(state_filter)
    if type_filter:
        where_clauses.append("property_type = ?")
        params.append(type_filter)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    with get_db() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM properties {where_sql}", params).fetchone()[0]
        rows = conn.execute(
            f"SELECT * FROM properties {where_sql} ORDER BY scraped_at DESC LIMIT ? OFFSET ?",
            params + [per_page, (page - 1) * per_page]
        ).fetchall()
        states = [r[0] for r in conn.execute(
            "SELECT DISTINCT address_state FROM properties WHERE address_state != '' ORDER BY address_state"
        ).fetchall()]
        types = [r[0] for r in conn.execute(
            "SELECT DISTINCT property_type FROM properties WHERE property_type != '' ORDER BY property_type"
        ).fetchall()]

    return build_records_page(rows, total, page, per_page, search, state_filter, type_filter, states, types)


@app.route("/export")
@login_required
def export_csv():
    search = request.args.get("q", "").strip()
    state_filter = request.args.get("state", "").strip()
    type_filter = request.args.get("type", "").strip()
    job_id = request.args.get("job_id", "").strip()

    where_clauses = []
    params = []
    if search:
        where_clauses.append(
            "(address_full LIKE ? OR reported_owner_name LIKE ? OR contact_1_name LIKE ?)"
        )
        like = f"%{search}%"
        params.extend([like, like, like])
    if state_filter:
        where_clauses.append("address_state = ?")
        params.append(state_filter)
    if type_filter:
        where_clauses.append("property_type = ?")
        params.append(type_filter)
    if job_id:
        where_clauses.append("job_id = ?")
        params.append(job_id)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    from scraper import CSV_COLUMNS
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT {','.join(CSV_COLUMNS)} FROM properties {where_sql} ORDER BY scraped_at DESC",
            params
        ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(CSV_COLUMNS)
    for row in rows:
        writer.writerow(list(row))

    output.seek(0)
    filename = f"atlas_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.route("/jobs")
@login_required
def job_history():
    with get_db() as conn:
        all_jobs = conn.execute(
            "SELECT * FROM jobs ORDER BY created_at DESC LIMIT 50"
        ).fetchall()
    return JOB_HISTORY_HTML.replace("{{JOBS_TABLE}}", build_jobs_table(all_jobs))


# ── HTML Helpers ──────────────────────────────────────────────────────────────
def build_jobs_html(jobs_list):
    if not jobs_list:
        return '<p style="color:#666;font-size:13px;">No scrape jobs yet.</p>'
    html = ""
    for j in jobs_list:
        status = j["status"]
        color = {"completed": "#22c55e", "failed": "#ef4444", "running": "#3b82f6", "queued": "#f59e0b"}.get(status, "#888")
        params = json.loads(j["params"] or "{}")
        label = f"{params.get('property_type','?')} | {','.join(params.get('states',[]) or ['?'])}"
        html += f"""
        <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid #1e1e2e;">
          <div>
            <div style="font-size:13px;color:#e8e8e8;">{label}</div>
            <div style="font-size:11px;color:#666;">{j['created_at'][:16] if j['created_at'] else ''}</div>
          </div>
          <div style="display:flex;align-items:center;gap:12px;">
            <span style="font-size:12px;color:{color};font-weight:600;">{status.upper()}</span>
            <span style="font-size:13px;color:#aaa;">{j['records_scraped'] or 0} records</span>
            <a href="/job/{j['id']}" style="font-size:12px;color:#3b82f6;">View</a>
          </div>
        </div>"""
    return html


def build_jobs_table(jobs_list):
    if not jobs_list:
        return '<tr><td colspan="6" style="text-align:center;color:#666;padding:20px;">No jobs yet</td></tr>'
    html = ""
    for j in jobs_list:
        status = j["status"]
        color = {"completed": "#22c55e", "failed": "#ef4444", "running": "#3b82f6", "queued": "#f59e0b"}.get(status, "#888")
        params = json.loads(j["params"] or "{}")
        states_str = ",".join(params.get("states", []) or [])
        html += f"""<tr>
          <td>{j['created_at'][:16] if j['created_at'] else ''}</td>
          <td>{params.get('property_type','')}</td>
          <td>{states_str}</td>
          <td><span style="color:{color};font-weight:600;">{status.upper()}</span></td>
          <td>{j['records_scraped'] or 0}</td>
          <td>
            <a href="/job/{j['id']}" style="color:#3b82f6;margin-right:8px;">View</a>
            <a href="/export?job_id={j['id']}" style="color:#22c55e;">Export</a>
          </td>
        </tr>"""
    return html


def build_records_page(rows, total, page, per_page, search, state_filter, type_filter, states, types):
    total_pages = max(1, (total + per_page - 1) // per_page)

    state_options = "".join(
        f'<option value="{s}" {"selected" if s == state_filter else ""}>{s}</option>'
        for s in states
    )
    type_options = "".join(
        f'<option value="{t}" {"selected" if t == type_filter else ""}>{t}</option>'
        for t in types
    )

    rows_html = ""
    for r in rows:
        has_contact = bool(r["contact_1_name"] or r["contact_1_phone_1"])
        contact_badge = f'<span style="background:#1a3a1a;color:#22c55e;padding:2px 8px;border-radius:10px;font-size:11px;">✓ Contact</span>' if has_contact else ""
        rows_html += f"""
        <tr>
          <td><a href="{r['link']}" target="_blank" style="color:#3b82f6;">{r['address_full'] or 'N/A'}</a></td>
          <td>{r['address_city']}, {r['address_state']}</td>
          <td>{r['property_type']}</td>
          <td>{r['building_area_sf'] or ''}</td>
          <td>{r['year_built'] or ''}</td>
          <td>{r['reported_owner_name'] or ''}</td>
          <td>{r['contact_1_name'] or ''}<br><small style="color:#888;">{r['contact_1_phone_1'] or ''}</small></td>
          <td>{contact_badge}</td>
        </tr>"""

    pagination = ""
    if total_pages > 1:
        for p in range(max(1, page - 2), min(total_pages + 1, page + 3)):
            active = "background:#2563eb;color:#fff;" if p == page else "background:#1a1a2e;color:#aaa;"
            pagination += f'<a href="?page={p}&q={search}&state={state_filter}&type={type_filter}" style="{active}padding:6px 12px;border-radius:6px;text-decoration:none;font-size:13px;">{p}</a>'

    return RECORDS_HTML.replace("{{TOTAL}}", str(total)).replace(
        "{{STATE_OPTIONS}}", state_options).replace(
        "{{TYPE_OPTIONS}}", type_options).replace(
        "{{SEARCH_VAL}}", search).replace(
        "{{ROWS}}", rows_html).replace(
        "{{PAGINATION}}", pagination).replace(
        "{{PAGE}}", str(page)).replace(
        "{{TOTAL_PAGES}}", str(total_pages)).replace(
        "{{EXPORT_PARAMS}}", f"q={search}&state={state_filter}&type={type_filter}")


# ── HTML Templates ────────────────────────────────────────────────────────────
_NAV = """
<nav style="background:#0d0d14;border-bottom:1px solid #1e1e2e;padding:0 32px;display:flex;align-items:center;gap:0;height:56px;">
  <div style="display:flex;align-items:center;gap:10px;margin-right:40px;">
    <div style="width:32px;height:32px;background:#2563eb;border-radius:6px;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:15px;color:white;">A</div>
    <span style="font-weight:700;font-size:15px;color:#fff;">Atlas</span>
    <span style="font-size:11px;color:#555;margin-left:2px;">Wade Asset Management</span>
  </div>
  <a href="/" style="color:#aaa;text-decoration:none;padding:0 16px;height:56px;display:flex;align-items:center;font-size:13px;border-bottom:2px solid transparent;" onmouseover="this.style.color='#fff'" onmouseout="this.style.color='#aaa'">Dashboard</a>
  <a href="/scrape" style="color:#aaa;text-decoration:none;padding:0 16px;height:56px;display:flex;align-items:center;font-size:13px;border-bottom:2px solid transparent;" onmouseover="this.style.color='#fff'" onmouseout="this.style.color='#aaa'">New Scrape</a>
  <a href="/records" style="color:#aaa;text-decoration:none;padding:0 16px;height:56px;display:flex;align-items:center;font-size:13px;border-bottom:2px solid transparent;" onmouseover="this.style.color='#fff'" onmouseout="this.style.color='#aaa'">Records</a>
  <a href="/jobs" style="color:#aaa;text-decoration:none;padding:0 16px;height:56px;display:flex;align-items:center;font-size:13px;border-bottom:2px solid transparent;" onmouseover="this.style.color='#fff'" onmouseout="this.style.color='#aaa'">Job History</a>
  <div style="flex:1;"></div>
  <a href="/export" style="background:#1a3a1a;color:#22c55e;border:1px solid #22c55e33;padding:7px 16px;border-radius:6px;font-size:13px;text-decoration:none;margin-right:12px;">Export All CSV</a>
  <a href="/logout" style="color:#666;text-decoration:none;font-size:13px;">Sign out</a>
</nav>
"""

_BASE_STYLE = """
<style>
* { box-sizing: border-box; margin: 0; }
body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0a0a0f; color: #e8e8e8; min-height: 100vh; }
.page { max-width: 1400px; margin: 0 auto; padding: 32px 24px; }
.card { background: #111118; border: 1px solid #1e1e2e; border-radius: 12px; padding: 24px; }
label { display: block; font-size: 11px; font-weight: 600; letter-spacing: 0.08em; color: #888; text-transform: uppercase; margin-bottom: 6px; margin-top: 16px; }
input, select { width: 100%; padding: 10px 12px; background: #0a0a0f; border: 1px solid #2a2a3e; border-radius: 8px; color: #e8e8e8; font-size: 14px; outline: none; }
input:focus, select:focus { border-color: #2563eb; }
.btn { padding: 10px 20px; border: none; border-radius: 8px; font-size: 14px; font-weight: 600; cursor: pointer; text-decoration: none; display: inline-flex; align-items: center; gap: 6px; }
.btn-primary { background: #2563eb; color: white; }
.btn-primary:hover { background: #1d4ed8; }
.btn-success { background: #166534; color: #22c55e; border: 1px solid #22c55e33; }
.stat-card { background: #111118; border: 1px solid #1e1e2e; border-radius: 10px; padding: 20px 24px; }
.stat-num { font-size: 36px; font-weight: 700; color: #fff; }
.stat-label { font-size: 12px; color: #666; margin-top: 4px; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; padding: 10px 12px; color: #666; font-size: 11px; font-weight: 600; letter-spacing: 0.06em; text-transform: uppercase; border-bottom: 1px solid #1e1e2e; }
td { padding: 10px 12px; border-bottom: 1px solid #111118; color: #ccc; vertical-align: top; }
tr:hover td { background: #111118; }
.error { background: #2a1515; border: 1px solid #5a2020; border-radius: 8px; padding: 12px 14px; color: #f87171; font-size: 13px; margin-bottom: 20px; }
.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }
.grid-4 { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 16px; }
@media (max-width: 768px) { .grid-2, .grid-3, .grid-4 { grid-template-columns: 1fr; } }
</style>
"""

LOGIN_HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Atlas — Wade Asset Management</title>
{_BASE_STYLE}
<style>
body {{ display: flex; min-height: 100vh; }}
.left {{ flex: 1; padding: 80px 60px; display: flex; flex-direction: column; justify-content: center; background: #0a0a0f; }}
.right {{ width: 480px; padding: 60px 48px; display: flex; flex-direction: column; justify-content: center; background: #111118; border-left: 1px solid #1e1e2e; }}
h1 {{ font-size: 48px; font-weight: 700; line-height: 1.1; color: #fff; margin-bottom: 16px; }}
h1 span {{ color: #2563eb; }}
.subtitle {{ font-size: 16px; color: #888; line-height: 1.6; max-width: 480px; }}
.pills {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 40px; }}
.pill {{ background: #1a1a2e; border: 1px solid #2a2a3e; border-radius: 20px; padding: 6px 14px; font-size: 13px; color: #aaa; }}
.form-title {{ font-size: 24px; font-weight: 700; color: #fff; margin-bottom: 6px; }}
.form-sub {{ font-size: 14px; color: #666; margin-bottom: 32px; }}
.form-sub span {{ color: #2563eb; }}
button {{ width: 100%; padding: 13px; background: #2563eb; color: white; border: none; border-radius: 8px; font-size: 15px; font-weight: 600; cursor: pointer; }}
button:hover {{ background: #1d4ed8; }}
@media (max-width: 768px) {{ .left {{ display: none; }} .right {{ width: 100%; }} }}
</style>
</head>
<body>
<div class="left">
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:60px;">
    <div style="width:40px;height:40px;background:#2563eb;border-radius:8px;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:18px;color:white;">A</div>
    <div><div style="font-size:16px;font-weight:600;color:#e8e8e8;">Atlas</div><div style="font-size:12px;color:#666;">Wade Asset Management</div></div>
  </div>
  <h1>Your full-time<br><span>data team,</span><br>built into one app.</h1>
  <p class="subtitle">Atlas connects directly to Reonomy to surface motivated commercial sellers in your market — filtered exactly to your buy box, saved permanently.</p>
  <div class="pills">
    <div class="pill">🏭 Industrial Properties</div>
    <div class="pill">📞 Owner Contact Info</div>
    <div class="pill">💰 Tax &amp; Mortgage Data</div>
    <div class="pill">🎯 Owner Occupied</div>
    <div class="pill">📊 CSV Export</div>
    <div class="pill">💾 Permanent Records</div>
  </div>
</div>
<div class="right">
  <div class="form-title">Welcome back.</div>
  <div class="form-sub">I'm Atlas — <span>your full-time data agent.</span></div>
  {{ERROR}}
  <form method="POST" action="/login">
    <label>Email Address</label>
    <input type="text" name="username" placeholder="your@email.com" required>
    <label>Password</label>
    <input type="password" name="password" placeholder="••••••••••••" required>
    <button type="submit" style="margin-top:20px;">Access Atlas →</button>
  </form>
</div>
</body>
</html>"""

DASHBOARD_HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Atlas Dashboard</title>
{_BASE_STYLE}
</head>
<body>
{_NAV}
<div class="page">
  <div style="margin-bottom:32px;">
    <h2 style="font-size:24px;font-weight:700;color:#fff;">Dashboard</h2>
    <p style="color:#666;font-size:14px;margin-top:4px;">Commercial property intelligence — Wade Asset Management</p>
  </div>
  <div class="grid-4" style="margin-bottom:32px;">
    <div class="stat-card">
      <div class="stat-num">{{{{TOTAL_RECORDS}}}}</div>
      <div class="stat-label">Total Properties Saved</div>
    </div>
    <div class="stat-card" style="cursor:pointer;" onclick="window.location='/scrape'">
      <div class="stat-num" style="font-size:28px;color:#2563eb;">+</div>
      <div class="stat-label">New Scrape</div>
    </div>
    <div class="stat-card" style="cursor:pointer;" onclick="window.location='/records'">
      <div class="stat-num" style="font-size:28px;color:#22c55e;">→</div>
      <div class="stat-label">Browse Records</div>
    </div>
    <div class="stat-card" style="cursor:pointer;background:linear-gradient(135deg,#14532d,#166534);border:2px solid #22c55e;" onclick="window.location='/export'">
      <div class="stat-num" style="font-size:32px;color:#22c55e;">↓</div>
      <div class="stat-label" style="color:#86efac;font-weight:700;font-size:13px;">Download All Records CSV</div>
    </div>
  </div>
  <div class="card">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;">
      <h3 style="font-size:16px;font-weight:600;color:#fff;">Recent Scrape Jobs</h3>
      <a href="/jobs" style="font-size:13px;color:#3b82f6;text-decoration:none;">View all →</a>
    </div>
    {{{{RECENT_JOBS}}}}
  </div>
  <div style="margin-top:24px;padding:20px;background:#111118;border:1px solid #1a3a1a;border-radius:10px;display:flex;justify-content:space-between;align-items:center;">
    <div>
      <div style="font-size:14px;font-weight:600;color:#22c55e;">Ready to pull new leads?</div>
      <div style="font-size:13px;color:#666;margin-top:2px;">Configure your buy box and scrape Reonomy in one click.</div>
    </div>
    <a href="/scrape" class="btn btn-primary">Start New Scrape →</a>
  </div>
</div>
</body>
</html>"""

SCRAPE_HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>New Scrape — Atlas</title>
{_BASE_STYLE}
</head>
<body>
{_NAV}
<div class="page" style="max-width:900px;">
  <div style="margin-bottom:28px;">
    <h2 style="font-size:24px;font-weight:700;color:#fff;">Configure Buy Box</h2>
    <p style="color:#666;font-size:14px;margin-top:4px;">Set your search criteria. Atlas will pull matching properties from Reonomy and save them permanently.</p>
  </div>
  <form method="POST" action="/scrape" id="scrapeForm">
    <div class="card" style="margin-bottom:20px;">
      <h3 style="font-size:14px;font-weight:600;color:#aaa;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:16px;">Property Type</h3>
      <div class="grid-2">
        <div>
          <label>Property Type</label>
          <select name="property_type">
            <option value="Industrial">Industrial</option>
            <option value="Office">Office</option>
            <option value="Retail">Retail</option>
            <option value="Multifamily">Multifamily</option>
          </select>
        </div>
        <div>
          <label>Max Records to Pull</label>
          <select name="max_records">
            <option value="50">50 records</option>
            <option value="100" selected>100 records</option>
            <option value="250">250 records</option>
            <option value="500">500 records</option>
            <option value="1000">1,000 records</option>
            <option value="2500">2,500 records</option>
            <option value="5000">5,000 records</option>
          </select>
        </div>
      </div>
    </div>

    <div class="card" style="margin-bottom:20px;">
      <h3 style="font-size:14px;font-weight:600;color:#aaa;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:16px;">Geography</h3>
      <div class="grid-2">
        <div>
          <label>State(s) — comma separated (e.g. FL, TX, GA)</label>
          <input type="text" name="states_text" placeholder="FL, TX, GA" value="FL">
        </div>
        <div>
          <label>County — comma separated (optional)</label>
          <input type="text" name="counties" placeholder="Hillsborough, Pinellas">
        </div>
      </div>
    </div>

    <div class="card" style="margin-bottom:20px;">
      <h3 style="font-size:14px;font-weight:600;color:#aaa;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:16px;">Building Criteria</h3>
      <div class="grid-4">
        <div>
          <label>Building SF — Min</label>
          <input type="number" name="building_sf_min" placeholder="e.g. 5000">
        </div>
        <div>
          <label>Building SF — Max</label>
          <input type="number" name="building_sf_max" placeholder="e.g. 100000">
        </div>
        <div>
          <label>Year Built — Min</label>
          <input type="number" name="year_built_min" placeholder="e.g. 1980">
        </div>
        <div>
          <label>Year Built — Max</label>
          <input type="number" name="year_built_max" placeholder="e.g. 2010">
        </div>
      </div>
      <div class="grid-4" style="margin-top:0;">
        <div>
          <label>Lot Size SF — Min</label>
          <input type="number" name="lot_size_min" placeholder="e.g. 10000">
        </div>
        <div>
          <label>Lot Size SF — Max</label>
          <input type="number" name="lot_size_max" placeholder="e.g. 500000">
        </div>
        <div>
          <label>Assessed Value — Min ($)</label>
          <input type="number" name="assessed_value_min" placeholder="e.g. 100000">
        </div>
        <div>
          <label>Assessed Value — Max ($)</label>
          <input type="number" name="assessed_value_max" placeholder="e.g. 5000000">
        </div>
      </div>
    </div>

    <div class="card" style="margin-bottom:20px;">
      <h3 style="font-size:14px;font-weight:600;color:#aaa;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:16px;">Owner Filters</h3>
      <div class="grid-3">
        <div>
          <label>Owner Occupied Only</label>
          <select name="owner_occupied">
            <option value="">Any</option>
            <option value="true">Yes — Owner Occupied</option>
          </select>
        </div>
        <div>
          <label>Last Sale After (YYYY-MM-DD)</label>
          <input type="date" name="last_sale_after">
        </div>
        <div>
          <label>Include Contact Info</label>
          <select name="include_contacts">
            <option value="true" selected>Yes — Pull phones &amp; emails</option>
            <option value="false">No — Property data only (faster)</option>
          </select>
        </div>
      </div>
    </div>

    <div style="display:flex;gap:12px;align-items:center;">
      <button type="submit" class="btn btn-primary" style="font-size:15px;padding:13px 32px;">
        🚀 Start Scrape
      </button>
      <span style="font-size:13px;color:#666;">Results save permanently to your database</span>
    </div>
  </form>
</div>
</body>
</html>"""

JOB_STATUS_HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Scrape Job — Atlas</title>
{_BASE_STYLE}
</head>
<body>
{_NAV}
<div class="page" style="max-width:700px;">
  <h2 style="font-size:24px;font-weight:700;color:#fff;margin-bottom:8px;">Scrape Job</h2>
  <p style="color:#666;font-size:13px;margin-bottom:28px;">Job ID: {{{{JOB_ID}}}}</p>
  <div class="card" id="statusCard">
    <div id="statusBadge" style="display:inline-block;padding:4px 14px;border-radius:20px;font-size:12px;font-weight:700;margin-bottom:20px;background:#1a2a4a;color:#3b82f6;">QUEUED</div>
    <div style="margin-bottom:16px;">
      <div style="display:flex;justify-content:space-between;margin-bottom:6px;">
        <span style="font-size:13px;color:#aaa;">Progress</span>
        <span id="progressText" style="font-size:13px;color:#fff;">0 / 0</span>
      </div>
      <div style="background:#1a1a2e;border-radius:8px;height:10px;overflow:hidden;">
        <div id="progressBar" style="height:100%;background:#2563eb;width:0%;transition:width 0.5s;border-radius:8px;"></div>
      </div>
    </div>
    <div id="matchingCount" style="font-size:13px;color:#666;margin-bottom:20px;"></div>
    <div id="completedMsg" style="display:none;">
      <div style="background:#1a3a1a;border:1px solid #22c55e33;border-radius:8px;padding:16px;margin-bottom:16px;">
        <div style="font-size:16px;font-weight:700;color:#22c55e;margin-bottom:4px;">✓ Scrape Complete</div>
        <div id="recordsCount" style="font-size:14px;color:#aaa;"></div>
      </div>
      <div style="display:flex;gap:12px;">
        <a href="/records" class="btn btn-primary">View Records →</a>
        <a id="exportBtn" href="/export?job_id={{{{JOB_ID}}}}" class="btn btn-success">Export This Job CSV</a>
        <a href="/scrape" style="color:#666;text-decoration:none;font-size:13px;display:flex;align-items:center;">New Scrape</a>
      </div>
    </div>
    <div id="errorMsg" style="display:none;" class="error"></div>
  </div>
</div>
<script>
const jobData = {{{{JOB_DATA}}}};
let pollInterval;

function updateUI(data) {{
  const badge = document.getElementById('statusBadge');
  const colors = {{
    queued: ['#1a2a4a','#3b82f6'],
    running: ['#1a2a4a','#3b82f6'],
    completed: ['#1a3a1a','#22c55e'],
    failed: ['#2a1515','#ef4444']
  }};
  const [bg, fg] = colors[data.status] || ['#1a1a2e','#888'];
  badge.style.background = bg;
  badge.style.color = fg;
  badge.textContent = data.status.toUpperCase();

  const pct = data.total > 0 ? Math.round((data.progress / data.total) * 100) : 0;
  document.getElementById('progressBar').style.width = pct + '%';
  document.getElementById('progressText').textContent = data.progress + ' / ' + data.total;

  if (data.total_matching > 0) {{
    document.getElementById('matchingCount').textContent = data.total_matching.toLocaleString() + ' total matching properties found in Reonomy';
  }}

  if (data.status === 'completed') {{
    clearInterval(pollInterval);
    document.getElementById('completedMsg').style.display = 'block';
    document.getElementById('recordsCount').textContent = data.records + ' records saved to your database';
    document.getElementById('progressBar').style.background = '#22c55e';
  }} else if (data.status === 'failed') {{
    clearInterval(pollInterval);
    document.getElementById('errorMsg').style.display = 'block';
    document.getElementById('errorMsg').textContent = 'Error: ' + (data.error || 'Unknown error');
  }}
}}

updateUI(jobData);

if (jobData.status === 'queued' || jobData.status === 'running') {{
  pollInterval = setInterval(async () => {{
    try {{
      const r = await fetch('/api/job/{{{{JOB_ID}}}}/status');
      const data = await r.json();
      updateUI(data);
    }} catch(e) {{}}
  }}, 2000);
}}
</script>
</body>
</html>"""

RECORDS_HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Records — Atlas</title>
{_BASE_STYLE}
</head>
<body>
{_NAV}
<div class="page">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:24px;">
    <div>
      <h2 style="font-size:24px;font-weight:700;color:#fff;">Records</h2>
      <p style="color:#666;font-size:13px;margin-top:2px;">{{{{TOTAL}}}} total properties saved</p>
    </div>
    <a href="/export?{{{{EXPORT_PARAMS}}}}" class="btn btn-success">↓ Export CSV</a>
  </div>
  <form method="GET" style="display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap;">
    <input type="text" name="q" placeholder="Search address, owner, contact..." value="{{{{SEARCH_VAL}}}}" style="flex:1;min-width:200px;">
    <select name="state" style="width:120px;">
      <option value="">All States</option>
      {{{{STATE_OPTIONS}}}}
    </select>
    <select name="type" style="width:160px;">
      <option value="">All Types</option>
      {{{{TYPE_OPTIONS}}}}
    </select>
    <button type="submit" class="btn btn-primary">Filter</button>
    <a href="/records" style="color:#666;text-decoration:none;font-size:13px;display:flex;align-items:center;">Clear</a>
  </form>
  <div style="overflow-x:auto;">
    <table>
      <thead>
        <tr>
          <th>Address</th>
          <th>City, State</th>
          <th>Type</th>
          <th>Bldg SF</th>
          <th>Yr Built</th>
          <th>Owner</th>
          <th>Contact</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {{{{ROWS}}}}
      </tbody>
    </table>
  </div>
  <div style="display:flex;gap:8px;margin-top:20px;align-items:center;">
    {{{{PAGINATION}}}}
    <span style="font-size:13px;color:#666;margin-left:8px;">Page {{{{PAGE}}}} of {{{{TOTAL_PAGES}}}}</span>
  </div>
</div>
</body>
</html>"""

JOB_HISTORY_HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Job History — Atlas</title>
{_BASE_STYLE}
</head>
<body>
{_NAV}
<div class="page">
  <h2 style="font-size:24px;font-weight:700;color:#fff;margin-bottom:24px;">Job History</h2>
  <div style="overflow-x:auto;">
    <table>
      <thead>
        <tr>
          <th>Started</th>
          <th>Property Type</th>
          <th>States</th>
          <th>Status</th>
          <th>Records</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>
        {{{{JOBS_TABLE}}}}
      </tbody>
    </table>
  </div>
</div>
</body>
</html>"""


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)


@app.route("/api/test-reonomy")
@login_required
def test_reonomy():
    """Debug endpoint to test Reonomy API connectivity with multi-state"""
    import os, json, requests, time
    token_val = os.environ.get("REONOMY_TOKEN", "")
    token_expires = float(os.environ.get("REONOMY_TOKEN_EXPIRES_AT", "0") or "0")
    
    result = {
        "token_present": bool(token_val),
        "token_length": len(token_val),
        "token_expires_at": time.strftime("%Y-%m-%d %H:%M", time.localtime(token_expires)) if token_expires else "not set",
        "token_expired": token_expires < time.time() if token_expires else True,
    }
    
    if token_val:
        headers = {
            "Authorization": f"Bearer {token_val}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Origin": "https://app.reonomy.com",
            "Referer": "https://app.reonomy.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        # Test 1: single state
        payload1 = {"settings": {"land_use_code": ["806"], "state": ["FL"], "building_area": {"min": 10000, "max": 100000}}, "bounding_box": {}}
        try:
            r1 = requests.post("https://api.reonomy.com/v2/search/count", headers=headers, json=payload1, timeout=15)
            result["test1_single_state_status"] = r1.status_code
            result["test1_single_state_body"] = r1.text[:300]
        except Exception as e:
            result["test1_error"] = str(e)
        
        # Test 2: multi-state with full industrial codes
        from scraper import INDUSTRIAL_CODES
        payload2 = {"settings": {"land_use_code": INDUSTRIAL_CODES, "state": ["FL", "GA", "SC", "NC", "VA", "DE"], "building_area": {"min": 10000, "max": 100000}}, "bounding_box": {}}
        result["test2_payload_state_count"] = len(payload2["settings"]["state"])
        result["test2_payload_code_count"] = len(payload2["settings"]["land_use_code"])
        try:
            r2 = requests.post("https://api.reonomy.com/v2/search/count", headers=headers, json=payload2, timeout=15)
            result["test2_multi_state_status"] = r2.status_code
            result["test2_multi_state_body"] = r2.text[:500]
        except Exception as e:
            result["test2_error"] = str(e)
    
    return json.dumps(result, indent=2), 200, {"Content-Type": "application/json"}
