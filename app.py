"""
Karl Huth Properties — Atlas Reonomy Scraper
Flask web app with job queue, login gate, and CSV download.
"""

import os
import json
import uuid
import threading
import time
import logging
from pathlib import Path
from flask import (Flask, request, session, redirect, url_for,
                   jsonify, send_file, render_template_string, make_response)

logger = logging.getLogger("atlas-app")
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "karl-huth-atlas-secret-2024")

# ── Config ───────────────────────────────────────────────────────────────────
APP_USERNAME = os.environ.get("APP_USERNAME", "karlhuth@hotmail.com")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "Karl1074$")
REONOMY_EMAIL = os.environ.get("REONOMY_EMAIL", "kaleb@multifamilycartel.com")
REONOMY_PASSWORD = os.environ.get("REONOMY_PASSWORD", "97Transam!")
_default_jobs_dir = "/data/atlas_jobs" if os.path.exists("/data") else "/tmp/atlas_jobs"
JOBS_DIR = Path(os.environ.get("JOBS_DIR", _default_jobs_dir))
JOBS_DIR.mkdir(parents=True, exist_ok=True)

# In-memory job store
jobs = {}
jobs_lock = threading.Lock()

# ── HTML Templates ────────────────────────────────────────────────────────────

LOGIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Atlas — Karl Huth Properties</title>
<style>
  * { box-sizing: border-box; margin: 0; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0a0a0f; color: #e8e8e8; min-height: 100vh; display: flex; }
  .left { flex: 1; padding: 80px 60px; display: flex; flex-direction: column; justify-content: center; }
  .right { width: 480px; padding: 60px 48px; display: flex; flex-direction: column; justify-content: center; background: #111118; border-left: 1px solid #1e1e2e; }
  .logo { display: flex; align-items: center; gap: 12px; margin-bottom: 60px; }
  .logo-icon { width: 40px; height: 40px; background: #16a34a; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 18px; color: white; }
  .logo-text { font-size: 16px; font-weight: 600; color: #e8e8e8; }
  .logo-sub { font-size: 12px; color: #666; }
  h1 { font-size: 48px; font-weight: 700; line-height: 1.1; color: #fff; margin-bottom: 16px; }
  h1 span { color: #16a34a; }
  .subtitle { font-size: 16px; color: #888; line-height: 1.6; max-width: 480px; }
  .pills { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 40px; }
  .pill { background: #1a1a2e; border: 1px solid #2a2a3e; border-radius: 20px; padding: 6px 14px; font-size: 13px; color: #aaa; }
  .form-title { font-size: 24px; font-weight: 700; color: #fff; margin-bottom: 6px; }
  .form-sub { font-size: 14px; color: #666; margin-bottom: 32px; }
  .form-sub span { color: #16a34a; }
  label { display: block; font-size: 11px; font-weight: 600; letter-spacing: 0.08em; color: #888; text-transform: uppercase; margin-bottom: 6px; }
  input { width: 100%; padding: 12px 14px; background: #0a0a0f; border: 1px solid #2a2a3e; border-radius: 8px; color: #e8e8e8; font-size: 14px; outline: none; margin-bottom: 20px; transition: border-color 0.2s; }
  input:focus { border-color: #16a34a; }
  button { width: 100%; padding: 13px; background: #16a34a; color: white; border: none; border-radius: 8px; font-size: 15px; font-weight: 600; cursor: pointer; transition: background 0.2s; display: flex; align-items: center; justify-content: center; gap: 8px; }
  button:hover { background: #15803d; }
  .error { background: #2a1515; border: 1px solid #5a2020; border-radius: 8px; padding: 12px 14px; color: #f87171; font-size: 13px; margin-bottom: 20px; }
  @media (max-width: 768px) { .left { display: none; } .right { width: 100%; } }
</style>
</head>
<body>
<div class="left">
  <div class="logo">
    <div class="logo-icon">A</div>
    <div><div class="logo-text">Atlas</div><div class="logo-sub">Karl Huth Properties</div></div>
  </div>
  <h1>Your full-time<br><span>data team,</span><br>built into one app.</h1>
  <p class="subtitle">Atlas connects to Reonomy, county records, and AI to surface motivated commercial sellers in your market — every single day.</p>
  <div class="pills">
    <div class="pill">🏭 Industrial Properties</div>
    <div class="pill">📞 Owner Contact Info</div>
    <div class="pill">💰 Tax & Mortgage Data</div>
    <div class="pill">🎯 Owner Occupied</div>
    <div class="pill">📊 CSV Export</div>
  </div>
</div>
<div class="right">
  <div class="form-title">Welcome back.</div>
  <div class="form-sub">I'm Atlas — <span>your full-time data agent.</span></div>
  {% if error %}<div class="error">{{ error }}</div>{% endif %}
  <form method="POST" action="/login">
    <label>Email Address</label>
    <input type="text" name="username" placeholder="your@email.com" value="{{ username or '' }}" required>
    <label>Password</label>
    <input type="password" name="password" placeholder="••••••••••••" required>
    <button type="submit">Access Atlas &rarr;</button>
  </form>
</div>
</body>
</html>"""

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Atlas — Karl Huth Properties</title>
<style>
  * { box-sizing: border-box; margin: 0; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0a0a0f; color: #e8e8e8; min-height: 100vh; display: flex; }
  .sidebar { width: 220px; background: #0d0d14; border-right: 1px solid #1e1e2e; padding: 24px 0; display: flex; flex-direction: column; flex-shrink: 0; }
  .sidebar-logo { padding: 0 20px 24px; border-bottom: 1px solid #1e1e2e; margin-bottom: 16px; }
  .sidebar-logo .name { font-size: 15px; font-weight: 700; color: #fff; }
  .sidebar-logo .sub { font-size: 11px; color: #555; }
  .nav-item { padding: 10px 20px; font-size: 13px; color: #666; cursor: pointer; display: flex; align-items: center; gap: 10px; border-radius: 0; transition: all 0.15s; }
  .nav-item:hover { color: #ccc; background: #111118; }
  .nav-item.active { color: #16a34a; background: #0d1f14; border-right: 2px solid #16a34a; }
  .nav-item .icon { width: 16px; text-align: center; }
  .nav-section { padding: 16px 20px 6px; font-size: 10px; font-weight: 600; letter-spacing: 0.1em; color: #444; text-transform: uppercase; }
  .locked-item { padding: 10px 20px; font-size: 13px; color: #3a3a4a; cursor: default; display: flex; align-items: center; gap: 10px; }
  .locked-badge { font-size: 9px; background: #1a1a2e; color: #444; padding: 2px 6px; border-radius: 10px; margin-left: auto; }
  .main { flex: 1; padding: 40px; overflow-y: auto; }
  .page-header { margin-bottom: 32px; }
  .page-title { font-size: 26px; font-weight: 700; color: #fff; }
  .page-sub { font-size: 14px; color: #666; margin-top: 4px; }
  .card { background: #111118; border: 1px solid #1e1e2e; border-radius: 12px; padding: 28px; margin-bottom: 24px; }
  .card-title { font-size: 14px; font-weight: 600; color: #aaa; margin-bottom: 20px; text-transform: uppercase; letter-spacing: 0.06em; }
  .form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  .form-group { display: flex; flex-direction: column; gap: 6px; }
  .form-group.full { grid-column: 1 / -1; }
  label { font-size: 11px; font-weight: 600; letter-spacing: 0.08em; color: #666; text-transform: uppercase; }
  input, select { padding: 10px 12px; background: #0a0a0f; border: 1px solid #2a2a3e; border-radius: 8px; color: #e8e8e8; font-size: 14px; outline: none; transition: border-color 0.2s; }
  input:focus, select:focus { border-color: #16a34a; }
  select option { background: #111118; }
  .checkbox-row { display: flex; align-items: center; gap: 10px; padding: 10px 0; }
  .checkbox-row input[type="checkbox"] { width: 16px; height: 16px; accent-color: #16a34a; }
  .checkbox-row label { font-size: 13px; color: #aaa; text-transform: none; letter-spacing: 0; font-weight: 400; }
  .btn { padding: 12px 24px; border-radius: 8px; font-size: 14px; font-weight: 600; cursor: pointer; border: none; transition: all 0.2s; }
  .btn-primary { background: #16a34a; color: white; }
  .btn-primary:hover { background: #15803d; }
  .btn-primary:disabled { background: #0d2a1a; color: #555; cursor: not-allowed; }
  .status-card { background: #111118; border: 1px solid #1e1e2e; border-radius: 12px; padding: 24px; margin-bottom: 24px; display: none; }
  .status-card.visible { display: block; }
  .status-header { display: flex; align-items: center; gap: 12px; margin-bottom: 16px; }
  .status-dot { width: 10px; height: 10px; border-radius: 50%; background: #888; }
  .status-dot.running { background: #f59e0b; animation: pulse 1.5s infinite; }
  .status-dot.complete { background: #16a34a; }
  .status-dot.error { background: #ef4444; }
  @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
  .status-title { font-size: 15px; font-weight: 600; color: #fff; }
  .status-sub { font-size: 13px; color: #666; }
  .progress-bar { height: 4px; background: #1e1e2e; border-radius: 2px; overflow: hidden; margin: 12px 0; }
  .progress-fill { height: 100%; background: #16a34a; border-radius: 2px; transition: width 0.5s; }
  .log-box { background: #0a0a0f; border: 1px solid #1e1e2e; border-radius: 8px; padding: 16px; font-family: 'Courier New', monospace; font-size: 12px; color: #888; max-height: 200px; overflow-y: auto; white-space: pre-wrap; }
  .download-btn { display: inline-flex; align-items: center; gap: 8px; padding: 10px 20px; background: #16a34a; color: white; border-radius: 8px; font-size: 14px; font-weight: 600; text-decoration: none; margin-top: 16px; }
  .download-btn:hover { background: #15803d; }
  .jobs-table { width: 100%; border-collapse: collapse; }
  .jobs-table th { text-align: left; padding: 10px 12px; font-size: 11px; font-weight: 600; letter-spacing: 0.08em; color: #555; text-transform: uppercase; border-bottom: 1px solid #1e1e2e; }
  .jobs-table td { padding: 12px; font-size: 13px; color: #aaa; border-bottom: 1px solid #111118; }
  .badge { display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
  .badge-running { background: #2a1f00; color: #f59e0b; }
  .badge-complete { background: #0a2a1a; color: #16a34a; }
  .badge-error { background: #2a0a0a; color: #ef4444; }
  .badge-queued { background: #1a1a2e; color: #666; }
  .signout { margin-top: auto; padding: 20px; border-top: 1px solid #1e1e2e; }
  .signout a { font-size: 12px; color: #444; text-decoration: none; }
  .signout a:hover { color: #888; }
</style>
</head>
<body>
<div class="sidebar">
  <div class="sidebar-logo">
    <div style="display:flex;align-items:center;gap:10px;">
      <div style="width:32px;height:32px;background:#16a34a;border-radius:6px;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px;color:white;">A</div>
      <div><div class="name">Atlas</div><div class="sub">Karl Huth Properties</div></div>
    </div>
  </div>
  <div class="nav-section">Data Tools</div>
  <div class="nav-item active"><span class="icon">🏭</span> Reonomy Scraper</div>
  <div class="nav-item" onclick="showSection('jobs')"><span class="icon">📋</span> Job History</div>
  <div class="nav-section">Coming Soon</div>
  <div class="locked-item"><span class="icon">🔍</span> LLC Piercing <span class="locked-badge">LOCKED</span></div>
  <div class="locked-item"><span class="icon">📞</span> Skip Tracing <span class="locked-badge">LOCKED</span></div>
  <div class="locked-item"><span class="icon">🏢</span> County Records <span class="locked-badge">LOCKED</span></div>
  <div class="locked-item"><span class="icon">🛰️</span> Prop. Condition AI <span class="locked-badge">LOCKED</span></div>
  <div class="signout"><a href="/logout">Sign Out</a></div>
</div>

<div class="main">
  <div id="section-scraper">
    <div class="page-header">
      <div class="page-title">Reonomy Scraper</div>
      <div class="page-sub">Pull property + contact data from Reonomy without using credits.</div>
    </div>

    <div class="card">
      <div class="card-title">Search Filters</div>
      <form id="scrapeForm">
        <div class="form-grid">
          <div class="form-group">
            <label>State</label>
            <select name="state" id="state">
              <option value="FL">Florida (FL)</option>
              <option value="TX">Texas (TX)</option>
              <option value="CA">California (CA)</option>
              <option value="NY">New York (NY)</option>
              <option value="GA">Georgia (GA)</option>
              <option value="NC">North Carolina (NC)</option>
              <option value="SC">South Carolina (SC)</option>
              <option value="AL">Alabama (AL)</option>
              <option value="OH">Ohio (OH)</option>
              <option value="WI" selected>Wisconsin (WI)</option>
              <option value="ALL">All States</option>
            </select>
          </div>
          <div class="form-group">
            <label>Property Type</label>
            <select name="property_type" id="property_type">
              <option value="Industrial" selected>Industrial</option>
              <option value="Office">Office</option>
              <option value="Retail">Retail</option>
              <option value="Multifamily">Multifamily</option>
              <option value="Mixed Use">Mixed Use</option>
              <option value="Land">Land</option>
            </select>
          </div>
          <div class="form-group">
            <label>Max Building SF</label>
            <input type="number" name="building_sf_max" id="building_sf_max" value="100000" placeholder="e.g. 100000">
          </div>
          <div class="form-group">
            <label>Min Building SF</label>
            <input type="number" name="building_sf_min" id="building_sf_min" placeholder="e.g. 1000">
          </div>
          <div class="form-group">
            <label>Max Records to Pull</label>
            <input type="number" name="max_records" id="max_records" value="50" min="1" max="50000">
          </div>
          <div class="form-group">
            <label>Sort By</label>
            <select name="sort_by">
              <option value="recommended">Recommended</option>
              <option value="likely_to_sell">Likely to Sell</option>
              <option value="last_sale_date">Last Sale Date</option>
            </select>
          </div>
          <div class="form-group full">
            <div class="checkbox-row">
              <input type="checkbox" name="owner_occupied" id="owner_occupied" checked>
              <label for="owner_occupied">Owner Occupied Only</label>
            </div>
            <div class="checkbox-row">
              <input type="checkbox" name="include_contacts" id="include_contacts" checked>
              <label for="include_contacts">Include Contact Info (phones, emails)</label>
            </div>
          </div>
        </div>
        <div style="margin-top:20px;">
          <button type="submit" class="btn btn-primary" id="submitBtn">🚀 Start Scrape</button>
        </div>
      </form>
    </div>

    <div class="status-card" id="statusCard">
      <div class="status-header">
        <div class="status-dot" id="statusDot"></div>
        <div>
          <div class="status-title" id="statusTitle">Running...</div>
          <div class="status-sub" id="statusSub"></div>
        </div>
      </div>
      <div class="progress-bar"><div class="progress-fill" id="progressFill" style="width:0%"></div></div>
      <div class="log-box" id="logBox"></div>
      <div id="downloadArea"></div>
    </div>
  </div>

  <div id="section-jobs" style="display:none;">
    <div class="page-header">
      <div class="page-title">Job History</div>
      <div class="page-sub">All scrape jobs run in this session.</div>
    </div>
    <div class="card">
      <table class="jobs-table">
        <thead><tr><th>Job ID</th><th>Filters</th><th>Records</th><th>Status</th><th>Download</th></tr></thead>
        <tbody id="jobsTableBody"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
let currentJobId = null;
let pollInterval = null;

function showSection(name) {
  document.getElementById('section-scraper').style.display = name === 'scraper' ? 'block' : 'none';
  document.getElementById('section-jobs').style.display = name === 'jobs' ? 'block' : 'none';
  if (name === 'jobs') loadJobs();
}

document.getElementById('scrapeForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const btn = document.getElementById('submitBtn');
  btn.disabled = true;
  btn.textContent = 'Starting...';

  const form = e.target;
  const data = {
    state: form.state.value,
    property_type: form.property_type.value,
    building_sf_max: form.building_sf_max.value || '',
    building_sf_min: form.building_sf_min.value || '',
    max_records: parseInt(form.max_records.value) || 50,
    owner_occupied: form.owner_occupied.checked,
    include_contacts: form.include_contacts.checked,
    sort_by: form.sort_by.value,
  };

  try {
    const res = await fetch('/api/scrape', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data)
    });
    const json = await res.json();
    if (json.job_id) {
      currentJobId = json.job_id;
      showStatusCard();
      startPolling(json.job_id);
    } else {
      alert('Error: ' + (json.error || 'Unknown error'));
      btn.disabled = false;
      btn.textContent = '🚀 Start Scrape';
    }
  } catch (err) {
    alert('Request failed: ' + err.message);
    btn.disabled = false;
    btn.textContent = '🚀 Start Scrape';
  }
});

function showStatusCard() {
  const card = document.getElementById('statusCard');
  card.classList.add('visible');
  card.scrollIntoView({behavior: 'smooth', block: 'nearest'});
}

function startPolling(jobId) {
  if (pollInterval) clearInterval(pollInterval);
  pollInterval = setInterval(() => pollJob(jobId), 3000);
}

async function pollJob(jobId) {
  try {
    const res = await fetch('/api/status/' + jobId);
    const data = await res.json();
    updateStatusCard(data);
    if (data.status === 'complete' || data.status === 'error') {
      clearInterval(pollInterval);
      document.getElementById('submitBtn').disabled = false;
      document.getElementById('submitBtn').textContent = '🚀 Start Scrape';
    }
  } catch (err) {
    console.error('Poll error:', err);
  }
}

function updateStatusCard(data) {
  const dot = document.getElementById('statusDot');
  const title = document.getElementById('statusTitle');
  const sub = document.getElementById('statusSub');
  const fill = document.getElementById('progressFill');
  const log = document.getElementById('logBox');
  const dlArea = document.getElementById('downloadArea');

  dot.className = 'status-dot ' + (data.status || '');

  if (data.status === 'running') {
    title.textContent = 'Scraping Reonomy...';
    const pct = data.total > 0 ? Math.round((data.processed / data.total) * 100) : 0;
    sub.textContent = `${data.processed || 0} of ${data.total || '?'} properties processed`;
    fill.style.width = pct + '%';
  } else if (data.status === 'complete') {
    title.textContent = '✅ Scrape Complete';
    sub.textContent = `${data.processed || 0} properties extracted`;
    fill.style.width = '100%';
    dlArea.innerHTML = `<a href="/api/download/${data.job_id}" class="download-btn">⬇ Download CSV (${data.processed || 0} records)</a>`;
  } else if (data.status === 'error') {
    title.textContent = '❌ Scrape Failed';
    sub.textContent = data.error || 'Unknown error';
    fill.style.width = '0%';
  } else if (data.status === 'queued') {
    title.textContent = 'Queued...';
    sub.textContent = 'Job is waiting to start';
  }

  if (data.logs && data.logs.length > 0) {
    log.textContent = data.logs.slice(-30).join('\\n');
    log.scrollTop = log.scrollHeight;
  }
}

async function loadJobs() {
  try {
    const res = await fetch('/api/jobs');
    const data = await res.json();
    const tbody = document.getElementById('jobsTableBody');
    tbody.innerHTML = '';
    if (!data.jobs || data.jobs.length === 0) {
      tbody.innerHTML = '<tr><td colspan="5" style="color:#444;text-align:center;padding:24px;">No jobs yet</td></tr>';
      return;
    }
    for (const job of data.jobs) {
      const statusClass = {'complete':'badge-complete','error':'badge-error','running':'badge-running','queued':'badge-queued'}[job.status] || 'badge-queued';
      const dl = job.status === 'complete' ? `<a href="/api/download/${job.job_id}" style="color:#16a34a;font-size:12px;">Download</a>` : '—';
      tbody.innerHTML += `<tr>
        <td style="font-family:monospace;font-size:11px;">${job.job_id.slice(0,8)}...</td>
        <td>${job.state || ''} / ${job.property_type || ''} / ${job.building_sf_max ? '≤'+job.building_sf_max+' SF' : 'Any SF'}</td>
        <td>${job.processed || 0}</td>
        <td><span class="badge ${statusClass}">${job.status}</span></td>
        <td>${dl}</td>
      </tr>`;
    }
  } catch (err) {
    console.error('Jobs load error:', err);
  }
}
</script>
</body>
</html>"""


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    if session.get("logged_in"):
        return render_template_string(DASHBOARD_HTML)
    return render_template_string(LOGIN_HTML, error=None, username=None)


@app.route("/login", methods=["POST"])
def login_route():
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    if username == APP_USERNAME and password == APP_PASSWORD:
        session["logged_in"] = True
        session["username"] = username
        return redirect("/")
    return render_template_string(LOGIN_HTML, error="Invalid credentials. Please try again.", username=username)


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")


@app.route("/dashboard")
def dashboard():
    if not session.get("logged_in"):
        return redirect("/")
    return render_template_string(DASHBOARD_HTML)


@app.route("/api/test", methods=["POST"])
def api_test():
    """Test the Reonomy API connection."""
    if not session.get("logged_in"):
        return jsonify({"error": "Not authenticated"}), 401
    try:
        from scraper import get_reonomy_token, INDUSTRIAL_CODES
        import requests as req
        token = get_reonomy_token(REONOMY_EMAIL, REONOMY_PASSWORD)
        # Quick count query for WI industrial
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"filters": {"land_use_code": INDUSTRIAL_CODES, "state": ["WI"]}, "page": {"size": 1}}
        resp = req.post("https://api.reonomy.com/v2/properties/search", json=payload, headers=headers, timeout=15)
        if resp.status_code == 200:
            total = resp.json().get("total", {}).get("value", 0)
            return jsonify({"ok": True, "message": f"Connected — {total:,} Wisconsin industrial properties available"})
        else:
            return jsonify({"ok": False, "error": f"API returned {resp.status_code}: {resp.text[:200]}"})
    except Exception as e:
        logger.error("API test failed: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    if not session.get("logged_in"):
        return jsonify({"error": "Not authenticated"}), 401

    data = request.get_json() or {}
    job_id = str(uuid.uuid4())

    job_data = {
        "job_id": job_id,
        "status": "queued",
        "processed": 0,
        "total": 0,
        "logs": [],
        "error": None,
        # Scrape params
        "state": data.get("state", "FL"),
        "property_types": [data.get("property_type", "Industrial")],
        "building_sf_max": data.get("building_sf_max", ""),
        "building_sf_min": data.get("building_sf_min", ""),
        "owner_occupied": data.get("owner_occupied", True),
        "include_contacts": data.get("include_contacts", True),
        "max_records": int(data.get("max_records", 50)),
        "sort_by": data.get("sort_by", "recommended"),
        "reonomy_email": REONOMY_EMAIL,
        "reonomy_password": REONOMY_PASSWORD,
    }

    with jobs_lock:
        jobs[job_id] = job_data

    # Run in background thread
    thread = threading.Thread(target=run_job_thread, args=(job_id,), daemon=True)
    thread.start()

    return jsonify({"job_id": job_id, "status": "queued"})


def run_job_thread(job_id: str):
    """Background thread that runs the API-based scraper and updates job state."""
    job_logger = logging.getLogger("atlas-scraper")
    class JobLogHandler(logging.Handler):
        def emit(self, record):
            msg = self.format(record)
            with jobs_lock:
                if job_id in jobs:
                    jobs[job_id]["logs"].append(msg)
                    if len(jobs[job_id]["logs"]) > 200:
                        jobs[job_id]["logs"] = jobs[job_id]["logs"][-200:]
    handler = JobLogHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    job_logger.addHandler(handler)
    try:
        with jobs_lock:
            jobs[job_id]["status"] = "running"
        with jobs_lock:
            job_params = dict(jobs[job_id])
        from scraper import run_scrape_job, CSV_COLUMNS

        def progress_callback(event_type, value):
            with jobs_lock:
                if job_id not in jobs:
                    return
                if event_type == "total":
                    jobs[job_id]["total"] = value
                elif event_type == "progress":
                    jobs[job_id]["processed"] = value

        records, csv_string = run_scrape_job(job_params, progress_callback=progress_callback)
        job_dir = JOBS_DIR / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        csv_path = job_dir / "results.csv"
        csv_path.write_text(csv_string, encoding="utf-8")
        with jobs_lock:
            jobs[job_id]["status"] = "complete"
            jobs[job_id]["processed"] = len(records)
            jobs[job_id]["csv_path"] = str(csv_path)
        logger.info("Job %s complete: %d records", job_id, len(records))
    except Exception as e:
        logger.error("Job %s failed: %s", job_id, e, exc_info=True)
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"] = str(e)
    finally:
        job_logger.removeHandler(handler)


@app.route("/api/status/<job_id>")
def api_status(job_id: str):
    if not session.get("logged_in"):
        return jsonify({"error": "Not authenticated"}), 401
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({
        "job_id": job_id,
        "status": job["status"],
        "processed": job["processed"],
        "total": job["total"],
        "error": job.get("error"),
        "logs": job.get("logs", []),
    })


@app.route("/api/jobs")
def api_jobs():
    if not session.get("logged_in"):
        return jsonify({"error": "Not authenticated"}), 401
    with jobs_lock:
        job_list = [
            {
                "job_id": j["job_id"],
                "status": j["status"],
                "processed": j["processed"],
                "state": j.get("state", ""),
                "property_type": (j.get("property_types") or [""])[0],
                "building_sf_max": j.get("building_sf_max", ""),
            }
            for j in reversed(list(jobs.values()))
        ]
    return jsonify({"jobs": job_list})


@app.route("/api/download/<job_id>")
def api_download(job_id: str):
    if not session.get("logged_in"):
        return jsonify({"error": "Not authenticated"}), 401
    with jobs_lock:
        job = jobs.get(job_id)
    if not job or job["status"] != "complete":
        return jsonify({"error": "Job not complete"}), 404
    csv_path = job.get("csv_path")
    if not csv_path or not Path(csv_path).exists():
        return jsonify({"error": "CSV file not found"}), 404
    return send_file(
        csv_path,
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"atlas_reonomy_{job.get('state','WI')}_{job_id[:8]}.csv"
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
