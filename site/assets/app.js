/* IM Data Platform: static console over sealed JSON written by pipeline/build.py. */
(() => {
  "use strict";

  const FILES = ["catalog", "jobs", "governance", "dashboard"];
  const JOB_ID = "sheets_to_duckdb";
  const TASKS = [
    { key: "discover", desc: "List sheets in the Drive folder and config" },
    { key: "validate", desc: "Check the DATA / COLUMN_METADATA / TABLE_METADATA format" },
    { key: "ingest", desc: "Load registered sheets into DuckDB raw.*" },
    { key: "govern", desc: "Classify columns, build curated.* without PII" },
    { key: "publish", desc: "Aggregate, encrypt, deploy to Pages" },
  ];
  const CLASS_INFO = {
    personal: "Identifies a person. Never published, not even as counts.",
    sensitive: "Special-category data (health, disability, relationships). Never published.",
    free_text: "Open answers. Only completeness is published.",
    categorical: "Published as counts, small groups suppressed.",
    metadata: "Timestamps and consent flags. Published as counts.",
  };
  const PALETTE = ["--c1", "--c2", "--c3", "--c4", "--c5", "--c6", "--c7", "--c8", "--c9", "--c10"];

  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const fmtInt = (n) => (n == null ? "–" : Number(n).toLocaleString("en-US"));
  const pct = (x) => (x == null ? "–" : `${Math.round(x * 100)}%`);

  const store = {
    get(k) { try { return sessionStorage.getItem(k); } catch { return null; } },
    set(k, v) { try { sessionStorage.setItem(k, v); } catch { /* private mode: PIN asked again on reload */ } },
    del(k) { try { sessionStorage.removeItem(k); } catch { /* ignore */ } },
  };

  // ---------- crypto ----------
  const b64 = (s) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));
  const keyCache = new Map();
  async function keyFor(pin, kdf) {
    const id = `${kdf.salt}:${kdf.iterations}`;
    if (!keyCache.has(id)) {
      const base = await crypto.subtle.importKey("raw", new TextEncoder().encode(pin), "PBKDF2", false, ["deriveKey"]);
      keyCache.set(id, crypto.subtle.deriveKey(
        { name: "PBKDF2", hash: kdf.hash, salt: b64(kdf.salt), iterations: kdf.iterations },
        base, { name: "AES-GCM", length: 256 }, false, ["decrypt"]));
    }
    return keyCache.get(id);
  }
  async function unseal(envelope, pin) {
    const key = await keyFor(pin, envelope.kdf);
    const plain = await crypto.subtle.decrypt({ name: "AES-GCM", iv: b64(envelope.iv) }, key, b64(envelope.data));
    return JSON.parse(new TextDecoder().decode(plain));
  }
  async function loadAll(pin) {
    const stamp = Date.now();
    const envelopes = await Promise.all(FILES.map(async (f) => {
      const res = await fetch(`data/${f}.json?t=${stamp}`, { cache: "no-store" });
      if (!res.ok) throw new Error(`missing:${f}`);
      return res.json();
    }));
    const out = {};
    for (let i = 0; i < FILES.length; i++) {
      try { out[FILES[i]] = await unseal(envelopes[i], pin); } catch { throw new Error("pin"); }
    }
    return out;
  }

  // ---------- state ----------
  let D = null;
  const sources = () => D.catalog.sources || [];
  const registered = () => sources().filter((s) => s.status === "registered");
  const runs = () => D.jobs.runs || [];
  const tableName = (s) => (s.fqn || "").split(".").pop();
  const sourceByTable = (t) => registered().find((s) => tableName(s) === t);
  const repoUrl = () => `https://github.com/${D.jobs.workflow.repository}`;
  const workflowUrl = () => `${repoUrl()}/actions/workflows/${D.jobs.workflow.file}`;

  function when(iso) {
    if (!iso) return "–";
    const d = new Date(iso);
    return d.toLocaleString("en-GB", { day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" });
  }
  function ago(iso) {
    if (!iso) return "never";
    const s = (Date.now() - new Date(iso).getTime()) / 1000;
    if (s < 60) return "just now";
    const steps = [[60, "minute"], [3600, "hour"], [86400, "day"], [2592000, "month"]];
    let unit = steps[0];
    for (const st of steps) if (s >= st[0]) unit = st;
    const n = Math.floor(s / unit[0]);
    return `${n} ${unit[1]}${n === 1 ? "" : "s"} ago`;
  }
  const dur = (s) => (s == null ? "–" : s < 60 ? `${Math.round(s)}s` : `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`);
  const badge = (status, label) => `<span class="badge st-${esc(status)}">${esc(label ?? cap(status))}</span>`;
  const tag = (klass) => `<span class="tag tag-${esc(klass)}" title="${esc(CLASS_INFO[klass] || "")}">${esc(klass.replace("_", " "))}</span>`;
  const slug = (t) => String(t).toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "");
  const cap = (s) => String(s || "").charAt(0).toUpperCase() + String(s || "").slice(1);
  const triggerLabel = (t) => ({ schedule: "Scheduled", workflow_dispatch: "Manual", push: "Code change", local: "Local", seed: "Initial load" }[t] || t);

  // ---------- icons ----------
  const ICONS = {
    home: '<path d="M2.5 7.5 8 3l5.5 4.5V13H10V9.5H6V13H2.5z" fill="none" stroke="currentColor" stroke-width="1.3"/>',
    catalog: '<ellipse cx="8" cy="4" rx="5" ry="2" fill="none" stroke="currentColor" stroke-width="1.3"/><path d="M3 4v8c0 1.1 2.2 2 5 2s5-.9 5-2V4M3 8c0 1.1 2.2 2 5 2s5-.9 5-2" fill="none" stroke="currentColor" stroke-width="1.3"/>',
    jobs: '<rect x="2" y="2.5" width="5" height="4" rx="1" fill="none" stroke="currentColor" stroke-width="1.3"/><rect x="9" y="9.5" width="5" height="4" rx="1" fill="none" stroke="currentColor" stroke-width="1.3"/><path d="M4.5 6.5v5H9" fill="none" stroke="currentColor" stroke-width="1.3"/>',
    shield: '<path d="M8 2 3 4v4c0 3 2.2 5.2 5 6 2.8-.8 5-3 5-6V4z" fill="none" stroke="currentColor" stroke-width="1.3"/><path d="m6 8 1.5 1.5L10.5 6.5" fill="none" stroke="currentColor" stroke-width="1.3"/>',
    dashboard: '<rect x="2" y="2" width="12" height="12" rx="1.5" fill="none" stroke="currentColor" stroke-width="1.3"/><path d="M5 11V8M8 11V5M11 11V7" stroke="currentColor" stroke-width="1.5"/>',
    runs: '<circle cx="8" cy="8" r="5.5" fill="none" stroke="currentColor" stroke-width="1.3"/><path d="m6.8 5.5 3 2.5-3 2.5z" fill="currentColor"/>',
    ingest: '<path d="M8 2v8M4.5 6.5 8 10l3.5-3.5M3 13h10" fill="none" stroke="currentColor" stroke-width="1.3"/>',
    table: '<rect x="2" y="3" width="12" height="10" rx="1" fill="none" stroke="currentColor" stroke-width="1.2"/><path d="M2 6.5h12M6 6.5V13" stroke="currentColor" stroke-width="1.2"/>',
    view: '<rect x="2" y="3" width="12" height="10" rx="1" fill="none" stroke="currentColor" stroke-width="1.2" stroke-dasharray="2 1.5"/><path d="M2 6.5h12" stroke="currentColor" stroke-width="1.2"/>',
    schema: '<path d="M2 4.5h5l1 1.5h6V13H2z" fill="none" stroke="currentColor" stroke-width="1.2"/>',
    sheet: '<rect x="3" y="2" width="10" height="12" rx="1" fill="none" stroke="currentColor" stroke-width="1.2"/><path d="M3 6h10M3 9.5h10M7 6v8" stroke="currentColor" stroke-width="1.2"/>',
    play: '<path d="M5 3.5v9l7-4.5z" fill="currentColor"/>',
    filter: '<path d="M2 3.5h12l-4.5 5.2V13l-3-1.5V8.7z" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round"/>',
    ext: '<path d="M9 3h4v4M13 3 7.5 8.5M11 9.5V13H3V5h3.5" fill="none" stroke="currentColor" stroke-width="1.3"/>',
  };
  const icon = (name, size = 14) => `<svg viewBox="0 0 16 16" width="${size}" height="${size}" aria-hidden="true">${ICONS[name] || ""}</svg>`;

  // ---------- login ----------
  function showLogin(message) {
    $("#app").hidden = true;
    $("#login").hidden = false;
    const pin = $("#pin");
    if (!pin.children.length) {
      pin.innerHTML = Array.from({ length: 12 }, (_, i) =>
        `<input id="pin-${i}" inputmode="numeric" pattern="[0-9]" maxlength="1" aria-label="Digit ${i + 1}" autocomplete="off">`).join("");
      const boxes = $$("input", pin);
      const value = () => boxes.map((b) => b.value).join("");
      const sync = () => { $("#login-submit").disabled = !/^\d{12}$/.test(value()); pin.classList.remove("invalid"); $("#login-error").hidden = true; };
      boxes.forEach((box, i) => {
        box.addEventListener("input", () => {
          box.value = box.value.replace(/\D/g, "").slice(-1);
          if (box.value && i < 11) boxes[i + 1].focus();
          sync();
        });
        box.addEventListener("keydown", (e) => {
          if (e.key === "Backspace" && !box.value && i > 0) { boxes[i - 1].focus(); boxes[i - 1].value = ""; sync(); e.preventDefault(); }
          if (e.key === "ArrowLeft" && i > 0) boxes[i - 1].focus();
          if (e.key === "ArrowRight" && i < 11) boxes[i + 1].focus();
        });
        box.addEventListener("paste", (e) => {
          const digits = (e.clipboardData.getData("text") || "").replace(/\D/g, "").slice(0, 12);
          if (!digits) return;
          e.preventDefault();
          digits.split("").forEach((d, j) => { if (boxes[j]) boxes[j].value = d; });
          boxes[Math.min(digits.length, 11)].focus();
          sync();
        });
      });
      $("#login-form").addEventListener("submit", async (e) => {
        e.preventDefault();
        const btn = $("#login-submit");
        btn.disabled = true;
        btn.textContent = "Unlocking…";
        const ok = await tryPin(value());
        btn.textContent = "Continue";
        if (!ok) {
          btn.disabled = false;
          pin.classList.add("invalid");
          boxes.forEach((b) => (b.value = ""));
          boxes[0].focus();
          $("#login-submit").disabled = true;
        }
      });
    }
    const err = $("#login-error");
    err.hidden = !message;
    err.textContent = message || "";
    setTimeout(() => $("#pin-0")?.focus(), 0);
  }

  async function tryPin(pin) {
    try {
      D = await loadAll(pin);
    } catch (e) {
      keyCache.clear();
      const msg = e.message === "pin" ? "PIN salah. Coba lagi." :
        e.message.startsWith("missing") ? "No data published yet. Run the pipeline once (Actions → Pipeline → Run workflow)." :
        "Could not load data. Check your connection and try again.";
      showLogin(msg);
      return false;
    }
    store.set("im_pin", pin);
    $("#login").hidden = true;
    $("#app").hidden = false;
    buildSearch();
    route();
    return true;
  }

  // ---------- router ----------
  function route() {
    if (!D) return;
    const parts = location.hash.split("?")[0].replace(/^#\/?/, "").split("/").filter(Boolean).map(decodeURIComponent);
    const [page = "home", ...rest] = parts;
    const pages = { home, catalog, jobs, runs: runsPage, ingestion, dashboards, governance };
    const render = pages[page] || notFound;
    $$(".nav").forEach((a) => a.classList.toggle("active", a.dataset.route === (pages[page] ? page : "")));
    $("#content").innerHTML = render(...rest);
    $("#content").scrollTop = 0;
    $("#sidebar").classList.remove("open");
    const last = runs()[0];
    $("#sidebar-foot").innerHTML = last ? `Data refreshed ${esc(ago(last.finished_at))}<br>${badge(last.status)}` : "";
    afterRender();
  }
  const notFound = () => `<div class="empty"><h2>Page not found</h2><p><a href="#/">Back to workspace</a></p></div>`;

  function afterRender() {
    $$("[data-run-now]").forEach((b) => b.addEventListener("click", () => runNow(b.dataset.runNow)));
    const filter = $("#tree-filter");
    if (filter) filter.addEventListener("input", () => {
      const q = filter.value.toLowerCase();
      $$(".tree li[data-name]").forEach((li) => (li.hidden = q && !li.dataset.name.includes(q)));
    });
    const pick = $("#dash-pick");
    if (pick) pick.addEventListener("change", () => (location.hash = `#/dashboards/${encodeURIComponent(pick.value)}`));
    const dash = $("#dash");
    if (dash) mountDashboard(dash);
  }

  // ---------- pages ----------
  function head(title, sub, actions = "", crumbs = []) {
    const bc = crumbs.length ? `<div class="breadcrumb">${crumbs.map(([t, h], i) => (i ? "<span>" : "<b>") + (h ? `<a href="${h}">${esc(t)}</a>` : esc(t)) + (i ? "</span>" : "</b>")).join(" ")}</div>` : "";
    return `<div class="page-head"><div>${bc}<h1>${esc(title)}</h1>${sub ? `<p class="muted">${sub}</p>` : ""}</div><div>${actions}</div></div>`;
  }
  const runBtn = (src = "") => `<button class="btn btn-primary" data-run-now="${esc(src)}">${icon("play", 12)} Run now</button>`;

  function home() {
    const last = runs()[0];
    const counts = { registered: 0, rejected: 0, unreachable: 0 };
    sources().forEach((s) => counts[s.status]++);
    const rows = registered().reduce((a, s) => a + (s.rows || 0), 0);
    const withheld = D.governance.sources.reduce((a, s) => a + s.withheld.length, 0);
    return head("Welcome to Indonesia Mengglobal's data workspace", "Google Sheets → DuckDB on GitHub Actions → this site. No servers.", runBtn()) + `
      <div class="grid grid-4" style="margin-bottom:16px">
        <a class="card card-link stat" href="#/ingestion"><div class="label">Registered sources</div><div class="value">${counts.registered}</div><div class="muted small">${counts.rejected} rejected · ${counts.unreachable} unreachable</div></a>
        <a class="card card-link stat" href="#/catalog"><div class="label">Records in catalog</div><div class="value">${fmtInt(rows)}</div><div class="muted small">${registered().length} table${registered().length === 1 ? "" : "s"} in sheets.raw</div></a>
        <a class="card card-link stat" href="#/runs"><div class="label">Last pipeline run</div><div class="value" style="font-size:20px">${last ? badge(last.status) : "–"}</div><div class="muted small">${last ? `${esc(ago(last.finished_at))} · ${esc(triggerLabel(last.trigger))}` : "No runs yet"}</div></a>
        <a class="card card-link stat" href="#/governance"><div class="label">Columns withheld (PII)</div><div class="value">${withheld}</div><div class="muted small">Groups under ${D.governance.min_group_size} suppressed</div></a>
      </div>
      <div class="grid grid-2">
        <div class="card"><h3>Recents: tables</h3>${registered().length ? `<table><tbody>${registered().map((s) => `
          <tr><td style="overflow-wrap:anywhere">${icon("table")} <a href="#/catalog/${esc(tableName(s))}">${esc(tableName(s))}</a><div class="muted small">${fmtInt(s.rows)} rows · ${esc(ago(s.ingested_at))}</div></td></tr>`).join("")}</tbody></table>` : `<p class="muted">No registered sources yet.</p>`}</div>
        <div class="card"><h3>Recent job runs</h3>${runs().length ? `<table><tbody>${runs().slice(0, 6).map((r) => `
          <tr><td>${badge(r.status)}</td><td><a href="#/runs/${esc(r.run_id)}">${esc(when(r.started_at))}</a></td><td class="muted">${esc(triggerLabel(r.trigger))}</td><td class="num muted">${dur(r.duration_s)}</td></tr>`).join("")}</tbody></table>` : `<p class="muted">No runs yet.</p>`}</div>
        <div class="card"><h3>Dashboards</h3>${Object.entries(D.dashboard.sources || {}).map(([id, d]) => `<p style="margin:4px 0">${icon("dashboard")} <a href="#/dashboards/${esc(id)}">${esc(d.title)}</a></p>`).join("") || `<p class="muted">None yet.</p>`}</div>
        <div class="card"><h3>Need attention</h3>${sources().filter((s) => s.status !== "registered").map((s) => `<p style="margin:4px 0">${badge(s.status)} <a href="#/ingestion">${esc(s.title)}</a><br><span class="muted small">${esc(s.errors[0] || "")}</span></p>`).join("") || `<p class="muted">Every source is registered and valid.</p>`}</div>
      </div>`;
  }

  // Catalog explorer: catalog "sheets" > schemas raw / curated > tables
  function catalog(table, tab = "overview", schema) {
    const regs = registered();
    const tree = `<div class="tree"><input class="tree-filter" id="tree-filter" placeholder="Type to filter" aria-label="Filter tables">
      <ul><li><span class="node">${icon("catalog")} <b>sheets</b></span><ul>
        ${["raw", "curated"].map((sc) => `<li><span class="node">${icon("schema")} ${sc}</span><ul>
          ${regs.map((s) => `<li data-name="${esc(tableName(s).toLowerCase())}"><a href="#/catalog/${esc(tableName(s))}/overview/${sc}" class="${table === tableName(s) && (schema || "raw") === sc ? "active" : ""}">${icon(sc === "raw" ? "table" : "view")} ${esc(tableName(s))}</a></li>`).join("") || `<li class="muted small node">empty</li>`}
        </ul></li>`).join("")}
      </ul></li></ul></div>`;
    let main;
    const s = table && sourceByTable(table);
    if (!table) {
      main = head("Catalog", "Every registered Google Sheet becomes a table in <code>sheets.raw</code> and a governed view in <code>sheets.curated</code>.") +
        `<div class="table-wrap"><table><thead><tr><th>Name</th><th>Type</th><th>Owner</th><th class="num">Rows</th><th class="num">Columns</th><th>Updated</th></tr></thead><tbody>
        ${regs.map((x) => `<tr><td>${icon("table")} <a href="#/catalog/${esc(tableName(x))}">${esc(x.fqn)}</a></td><td>Table</td><td>${esc(x.table.owner_email)}</td><td class="num">${fmtInt(x.rows)}</td><td class="num">${x.columns.length}</td><td class="muted nowrap">${esc(ago(x.ingested_at))}</td></tr>
          <tr><td>${icon("view")} <a href="#/catalog/${esc(tableName(x))}/overview/curated">${esc(x.curated_fqn)}</a></td><td>View</td><td>${esc(x.table.owner_email)}</td><td class="num">${fmtInt(x.rows)}</td><td class="num">${x.columns.filter((c) => c.published).length}</td><td class="muted nowrap">${esc(ago(x.ingested_at))}</td></tr>`).join("") || `<tr><td colspan="6" class="empty">No tables yet. Register a sheet under Data Ingestion.</td></tr>`}
        </tbody></table></div>`;
    } else if (!s) {
      main = `<div class="empty">Table <code>${esc(table)}</code> not found.</div>`;
    } else {
      const sc = schema === "curated" ? "curated" : "raw";
      const cols = sc === "curated" ? s.columns.filter((c) => c.published) : s.columns;
      const base = `#/catalog/${esc(table)}`;
      const tabs = [["overview", "Overview"], ["sample", "Sample Data"], ["details", "Details"], ["permissions", "Permissions"], ["lineage", "Lineage"], ["quality", "Quality"]];
      main = head(`${table}`, esc(s.table.table_description || ""), `<a class="btn" href="${esc(s.url)}" target="_blank" rel="noopener">${icon("sheet")} Open sheet</a> ${runBtn(s.id)}`,
        [["sheets", "#/catalog"], [sc, "#/catalog"], [table, null]]) +
        `<p style="margin:-8px 0 12px" class="muted small">${sc === "raw" ? "Table" : "View"} · Owner <b>${esc(s.table.owner_email)}</b> · ${fmtInt(s.rows)} rows · Version ${esc(s.table.version || "–")} · Refreshed ${esc(ago(s.ingested_at))}</p>
        ${s.partial_load ? partialNote() : ""}
        <nav class="tabs">${tabs.map(([k, l]) => `<a href="${base}/${k}/${sc}" class="${tab === k ? "active" : ""}">${l}</a>`).join("")}</nav>`;
      if (tab === "overview") {
        main += `<div class="table-wrap"><table><thead><tr><th>#</th><th>Column</th><th>Type</th><th>Comment</th><th>Tags</th><th class="num">Filled</th></tr></thead><tbody>
          ${cols.map((c) => `<tr><td class="muted">${esc(c.location)}</td><td class="mono">${esc(c.name)}</td><td class="mono muted">string</td><td>${esc(c.description)}<div class="muted small">Form: ${esc(c.form_question.slice(0, 120))}${c.form_question.length > 120 ? "…" : ""}</div></td><td>${tag(c.class)}${c.published ? "" : ' <span class="tag tag-personal">withheld</span>'}</td><td class="num">${pct(c.completeness)}</td></tr>`).join("")}
          </tbody></table></div>`;
      } else if (tab === "sample") {
        main += `<div class="callout warn"><b>Sample data is not published.</b> This site is public and static, so rows never leave the pipeline run. Open the source sheet to see records (needs Google access), or see aggregated values on the <a href="#/dashboards/${esc(s.id)}">dashboard</a>.</div>`;
      } else if (tab === "details") {
        const t = s.table;
        const d = (v) => (v && /^\d{8}$/.test(v) ? `${v.slice(0, 4)}-${v.slice(4, 6)}-${v.slice(6)}` : v || "–");
        main += `<div class="grid grid-2"><div class="card"><h3>About this table</h3><dl class="kv">
          <dt>Full name</dt><dd class="mono">${esc(sc === "raw" ? s.fqn : s.curated_fqn)}</dd>
          <dt>Description</dt><dd>${esc(t.table_description)}</dd><dt>Purpose</dt><dd>${esc(t.purpose)}</dd>
          <dt>Owner</dt><dd>${esc(t.owner_email)}</dd><dt>Operation period</dt><dd>${esc(d(t.operation_date_start))} → ${esc(d(t.operation_date_end))}</dd>
          <dt>Populated</dt><dd>${esc(d(t.populated_date))}</dd><dt>Version</dt><dd>${esc(t.version)}</dd></dl></div>
          <div class="card"><h3>Source</h3><dl class="kv"><dt>Google Sheet</dt><dd><a href="${esc(s.url)}" target="_blank" rel="noopener">${esc(s.title)}</a></dd>
          <dt>Spreadsheet ID</dt><dd class="mono">${esc(s.id)}</dd><dt>Sheet modified</dt><dd>${esc(when(s.modified_time))}</dd>
          <dt>Last ingested</dt><dd>${esc(when(s.ingested_at))}</dd><dt>Format check</dt><dd>${badge("registered", "Passed")}</dd>
          <dt>Warnings</dt><dd>${s.warnings.length ? s.warnings.map(esc).join("<br>") : "None"}</dd></dl></div></div>`;
      } else if (tab === "permissions") {
        const g = D.governance.sources.find((x) => x.id === s.id);
        main += `<div class="callout">Access is enforced in three places: the Google Sheet's own sharing, the service account the pipeline reads with, and the PIN that encrypts this site.</div>
          <div class="table-wrap"><table><thead><tr><th>Principal</th><th>Privilege</th><th>Scope</th></tr></thead><tbody>
          <tr><td>${esc(s.table.owner_email)}</td><td>OWNER</td><td>Source sheet</td></tr>
          <tr><td>Pipeline service account</td><td>READ</td><td>Source sheet (Viewer)</td></tr>
          <tr><td>Site PIN holders</td><td>SELECT (aggregates only)</td><td>${esc(s.curated_fqn)}, ${g ? g.withheld.length : 0} columns withheld</td></tr>
          <tr><td>Public internet</td><td>None</td><td>Encrypted files only</td></tr></tbody></table></div>`;
      } else if (tab === "lineage") {
        main += `<div class="dag">${[["sheet", s.title, "Google Sheet"], ["table", s.fqn, "DuckDB table"], ["view", s.curated_fqn, "Governed view"], ["dashboard", "Dashboard", "Aggregates on Pages"]]
          .map(([i, k, d], n) => `${n ? '<div class="dag-edge"></div>' : ""}<div class="dag-node"><div class="k">${icon(i)} <span style="overflow:hidden;text-overflow:ellipsis">${esc(k)}</span></div><div class="d">${esc(d)}</div></div>`).join("")}</div>`;
      } else if (tab === "quality") {
        const q = (D.dashboard.sources[s.id] || {}).quality || [];
        const low = s.columns.filter((c) => c.completeness != null && c.completeness < 0.5);
        main += `<div class="table-wrap" style="margin-bottom:16px"><table><thead><tr><th>Check</th><th>Result</th><th>Status</th></tr></thead><tbody>
          <tr><td>Format contract (tabs, dictionary, header match)</td><td>Passed</td><td>${badge("ok", "Pass")}</td></tr>
          ${q.map((x) => `<tr><td>${esc(x.check)}</td><td>${esc(x.value)}</td><td>${badge(x.status, { ok: "Pass", warn: "Warning", fail: "Fail" }[x.status])}</td></tr>`).join("")}
          <tr><td>Columns under 50% filled</td><td>${low.length ? low.map((c) => `<code>${esc(c.name)}</code>`).join(", ") : "None"}</td><td>${badge(low.length ? "warn" : "ok", low.length ? "Warning" : "Pass")}</td></tr>
          </tbody></table></div>`;
      }
    }
    return `<div class="explorer">${tree}<div class="explorer-main">${main}</div></div>`;
  }

  function jobs(id, tab = "runs") {
    if (id === JOB_ID) return jobDetail(tab);
    const recent = runs().slice(0, 10).reverse();
    const last = runs()[0];
    return head("Jobs & Pipelines", "Scheduled on GitHub Actions. <b>Run now</b> opens the workflow on GitHub, where you start it with one click.", runBtn()) + `
      <div class="table-wrap"><table><thead><tr><th>Name</th><th>Type</th><th>Trigger</th><th>Recent runs</th><th>Last run</th><th>Status</th><th></th></tr></thead><tbody>
        <tr><td>${icon("jobs")} <a href="#/jobs/${JOB_ID}"><b>${JOB_ID}</b></a><div class="muted small">discover → validate → ingest → govern → publish</div></td><td>Job</td>
          <td class="nowrap">${esc(scheduleLabel())}</td>
          <td><span class="dots">${recent.map((r) => `<a class="dot ${esc(r.status)}" href="#/runs/${esc(r.run_id)}" title="${esc(r.status)} · ${esc(when(r.started_at))}"></a>`).join("")}</span></td>
          <td class="nowrap">${last ? esc(ago(last.finished_at)) : "–"}</td><td>${last ? badge(last.status) : "–"}</td><td>${runBtn()}</td></tr>
        ${sources().map((s) => `<tr><td>${icon("ingest")} <a href="#/ingestion">ingest_${esc(tableName(s) || slug(s.title))}</a><div class="muted small">${esc(s.title)}</div></td><td>Ingestion pipeline</td>
          <td class="nowrap">With ${JOB_ID}</td><td></td><td class="nowrap">${esc(ago(s.checked_at))}</td>
          <td>${badge(s.status, s.status === "registered" ? "Active" : cap(s.status))}</td><td>${s.status === "unreachable" ? "" : `<button class="btn btn-sm" data-run-now="${esc(s.id)}">${icon("play", 10)} Run</button>`}</td></tr>`).join("")}
      </tbody></table></div>`;
  }

  function scheduleLabel() {
    const cron = D.jobs.workflow.schedule;
    return cron ? `Scheduled (${cron} UTC)` : "Scheduled";
  }

  function jobDetail(tab) {
    const rs = runs();
    const shown = rs.slice(0, 20).reverse();
    const maxDur = Math.max(1, ...shown.map((r) => r.duration_s || 0));
    const tabs = [["runs", "Runs"], ["tasks", "Tasks"]];
    let body = "";
    if (tab === "tasks") {
      body = `<div class="dag">${TASKS.map((t, i) => `${i ? '<div class="dag-edge"></div>' : ""}<div class="dag-node"><div class="k">${icon("jobs")} ${t.key}</div><div class="d">${esc(t.desc)}</div></div>`).join("")}</div>
        <div class="grid grid-2" style="margin-top:16px"><div class="card"><h3>Job details</h3><dl class="kv">
          <dt>Job ID</dt><dd class="mono">${JOB_ID}</dd><dt>Runs on</dt><dd>GitHub Actions (ubuntu-latest)</dd>
          <dt>Schedule</dt><dd>${esc(scheduleLabel())}</dd><dt>Workflow</dt><dd><a href="${esc(workflowUrl())}" target="_blank" rel="noopener">${esc(D.jobs.workflow.file)} ${icon("ext", 12)}</a></dd>
          <dt>Compute</dt><dd>DuckDB, in-process (serverless)</dd><dt>Output</dt><dd>Encrypted JSON on GitHub Pages</dd></dl></div>
          <div class="card"><h3>Parameters</h3><dl class="kv"><dt>source</dt><dd>Spreadsheet ID to refresh alone. Blank = every source.</dd></dl></div></div>`;
    } else {
      body = `<div class="card" style="margin-bottom:16px"><h3>Run matrix</h3><div class="matrix"><table>
          <tr><th></th>${shown.map((r) => `<td title="${esc(dur(r.duration_s))}"><span class="bar-wrap"><span class="bar-duration" style="height:${Math.max(3, Math.round(40 * (r.duration_s || 0) / maxDur))}px;background:var(--${r.status === "failed" ? "danger" : r.status === "partial" ? "warning" : "c5"})"></span></span></td>`).join("")}</tr>
          ${TASKS.map((t) => `<tr><th class="mono">${t.key}</th>${shown.map((r) => {
            const task = (r.tasks || []).find((x) => x.key === t.key) || { status: "skipped" };
            return `<td><a class="cell ${esc(task.status)}" href="#/runs/${esc(r.run_id)}" title="${esc(t.key)}: ${esc(task.status)}${task.duration_ms != null ? ` · ${task.duration_ms} ms` : ""}"></a></td>`;
          }).join("")}</tr>`).join("")}
          <tr><th></th>${shown.map((r) => `<td class="muted small nowrap" style="writing-mode:vertical-rl;transform:rotate(180deg)">${esc(new Date(r.started_at).toLocaleDateString("en-GB", { day: "numeric", month: "short" }))}</td>`).join("")}</tr>
        </table></div></div>` + runsTable(rs);
    }
    return head(JOB_ID, "Google Sheets → DuckDB → encrypted aggregates on GitHub Pages", `<a class="btn" href="${esc(workflowUrl())}" target="_blank" rel="noopener">${icon("ext")} View on GitHub</a> ${runBtn()}`,
      [["Jobs & Pipelines", "#/jobs"], [JOB_ID, null]]) +
      `<nav class="tabs">${tabs.map(([k, l]) => `<a href="#/jobs/${JOB_ID}/${k}" class="${tab === k ? "active" : ""}">${l}</a>`).join("")}</nav>` + body;
  }

  function runsTable(rs) {
    if (!rs.length) return `<div class="empty">No runs yet.</div>`;
    return `<div class="table-wrap"><table><thead><tr><th>Start time</th><th>Run</th><th>Status</th><th>Trigger</th><th>Run as</th><th>Sources</th><th class="num">Duration</th></tr></thead><tbody>
      ${rs.map((r) => `<tr><td class="nowrap"><a href="#/runs/${esc(r.run_id)}">${esc(when(r.started_at))}</a></td><td class="mono">${esc(r.run_number ? `#${r.run_number}` : r.run_id)}</td>
        <td>${badge(r.status)}</td><td>${esc(triggerLabel(r.trigger))}${r.only_source ? `<div class="muted small">one source</div>` : ""}</td><td>${esc(r.actor)}</td>
        <td class="nowrap">${r.sources ? `${r.sources.registered} ok · ${r.sources.rejected} rejected · ${r.sources.unreachable} unreachable` : "–"}</td><td class="num">${dur(r.duration_s)}</td></tr>`).join("")}
      </tbody></table></div>`;
  }

  function runsPage(id) {
    if (!id) return head("Job Runs", "Every run of the pipeline, newest first.", runBtn()) + runsTable(runs());
    const r = runs().find((x) => String(x.run_id) === id);
    if (!r) return notFound();
    return head(`Run ${r.run_number ? `#${r.run_number}` : r.run_id}`, `${badge(r.status)} · ${esc(triggerLabel(r.trigger))} by ${esc(r.actor)} · ${esc(when(r.started_at))} · ${dur(r.duration_s)}`,
      `${r.url ? `<a class="btn" href="${esc(r.url)}" target="_blank" rel="noopener">${icon("ext")} Logs on GitHub</a> ` : ""}${runBtn(r.only_source || "")}`,
      [["Job Runs", "#/runs"], [String(r.run_id), null]]) +
      (r.error ? `<div class="callout danger"><b>Run failed.</b> <code>${esc(r.error)}</code><br><span class="muted small">The site kept the last good data; only the run history changed.</span></div>` : "") +
      `<div class="dag" style="margin-bottom:16px">${(r.tasks || []).map((t, i) => `${i ? '<div class="dag-edge"></div>' : ""}<div class="dag-node"><div class="k">${badge(t.status, "")} ${esc(t.key)}</div><div class="d">${t.duration_ms != null ? `${t.duration_ms.toLocaleString()} ms` : "not run"}${t.detail ? `<br><span style="color:var(--danger)">${esc(t.detail)}</span>` : ""}</div></div>`).join("")}</div>
      <div class="card"><h3>Parameters</h3><dl class="kv"><dt>source</dt><dd>${esc(r.only_source || "all sources")}</dd><dt>Reader</dt><dd>${esc(r.reader || "–")}</dd></dl></div>`;
  }

  function ingestion() {
    const c = D.catalog.contract;
    return head("Data Ingestion", "Sheets found in the live Drive folder and the config. Only sheets in the agreed format are registered.", runBtn()) + `
      <div class="table-wrap" style="margin-bottom:16px"><table><thead><tr><th>Source</th><th>Status</th><th>Owner</th><th class="num">Rows</th><th>Checked</th><th>Details</th></tr></thead><tbody>
      ${sources().map((s) => `<tr><td>${icon("sheet")} ${s.url ? `<a href="${esc(s.url)}" target="_blank" rel="noopener">${esc(s.title)}</a>` : esc(s.title)}<div class="muted small mono">${esc(s.id)}</div></td>
        <td>${badge(s.status)}</td><td>${esc(s.table?.owner_email || "–")}</td><td class="num">${fmtInt(s.rows)}</td><td class="nowrap muted">${esc(ago(s.checked_at))}</td>
        <td>${s.status === "registered" ? `<a href="#/catalog/${esc(tableName(s))}">${esc(s.fqn)}</a>${s.warnings.length ? `<div class="muted small">${s.warnings.map(esc).join("<br>")}</div>` : ""}` :
          `<ul class="list-plain small">${s.errors.slice(0, 6).map((e) => `<li>${esc(e)}</li>`).join("")}${s.errors.length > 6 ? `<li>and ${s.errors.length - 6} more</li>` : ""}</ul>`}</td></tr>`).join("") || `<tr><td colspan="6" class="empty">No sources found.</td></tr>`}
      </tbody></table></div>
      <div class="grid grid-2">
        <div class="card"><h3>Add data: the source format</h3><p class="muted">Put a Google Sheet in the live folder (or add its ID to <code>config/platform.yml</code>). It is registered on the next run when it has these tabs:</p>
          <dl class="kv"><dt><code>${esc(c.data_tab)}</code></dt><dd>Row 1 is the header (form questions), then one row per record.</dd>
          <dt><code>${esc(c.column_metadata_tab)}</code></dt><dd>Columns: ${c.column_metadata_headers.map((h) => `<code>${esc(h)}</code>`).join(", ")}. One row per ${esc(c.data_tab)} column; <code>form_question</code> must equal the header at <code>column_header_location</code>; <code>column_name</code> in snake_case.</dd>
          <dt><code>${esc(c.table_metadata_tab)}</code></dt><dd>Columns: ${c.table_metadata_headers.map((h) => `<code>${esc(h)}</code>`).join(", ")}. Needs a row with <code>table_name = data</code>, an owner, and dates as YYYYMMDD.</dd></dl></div>
        <div class="card"><h3>Access</h3><p class="muted">The pipeline reads with a Google service account. Share the live folder (or each sheet) with it as <b>Viewer</b>. A sheet it can't open shows here as <b>Unreachable</b>.</p>
          <p class="muted">Changed a sheet and don't want to wait for the schedule? Use <b>Run</b> on the Jobs page to refresh just that sheet.</p></div>
      </div>`;
  }

  function dashboards(id) {
    const all = D.dashboard.sources || {};
    const ids = Object.keys(all);
    if (!id) {
      return head("Dashboards", "One dashboard per registered source. Aggregates only; groups under " + D.governance.min_group_size + " are suppressed.") +
        `<div class="table-wrap"><table><thead><tr><th>Name</th><th class="num">Records</th><th class="num">Charts</th><th>Updated</th></tr></thead><tbody>
        ${ids.map((k) => `<tr><td>${icon("dashboard")} <a href="#/dashboards/${esc(k)}">${esc(all[k].title)}</a></td><td class="num">${fmtInt(all[k].kpis.records)}</td><td class="num">${all[k].charts.length}</td><td class="muted">${esc(ago(D.dashboard.generated_at))}</td></tr>`).join("") || `<tr><td colspan="4" class="empty">No dashboards yet.</td></tr>`}
        </tbody></table></div>`;
    }
    const d = all[id];
    if (!d) return notFound();
    const k = d.kpis;
    const pick = ids.length > 1 ? `<select class="select" id="dash-pick" aria-label="Dashboard">${ids.map((x) => `<option value="${esc(x)}" ${x === id ? "selected" : ""}>${esc(all[x].title)}</option>`).join("")}</select>` : "";
    const charts = d.charts.filter((c) => c.class === "categorical");
    const meta = d.charts.filter((c) => c.class === "metadata");
    const src = sources().find((x) => x.id === id);
    if (d.cube) {
      return head(d.title, `Published ${esc(when(D.dashboard.generated_at))}`, pick, [["Dashboards", "#/dashboards"], [d.title, null]]) +
        (src && src.partial_load ? partialNote() : "") + `<div id="dash" data-id="${esc(id)}"></div>`;
    }
    return head(d.title, `Published ${esc(when(D.dashboard.generated_at))}`, pick, [["Dashboards", "#/dashboards"], [d.title, null]]) +
      (src && src.partial_load ? partialNote() : "") + `
      <div class="canvas">
        <div class="widget w2 counter"><h3>Records</h3><div class="value">${fmtInt(k.records)}</div><div class="sub">rows in the DATA tab</div></div>
        <div class="widget w2 counter"><h3>Duplicate submissions</h3><div class="value">${fmtInt(k.duplicate_submissions)}</div><div class="sub">${k.duplicate_submissions == null ? "not measured yet" : "same email submitted again"}</div></div>
        <div class="widget w2 counter"><h3>Average completeness</h3><div class="value">${pct(k.avg_completeness)}</div><div class="sub">across ${k.columns_measured ?? k.columns} of ${k.columns} columns</div></div>
        ${d.timeline.length ? `<div class="widget w6"><h3>Submissions per day</h3><div class="sub">by form timestamp</div>${timelineChart(d.timeline)}</div>` : ""}
        ${charts.map((c, i) => `<div class="widget"><h3>${esc(label(c.column))}</h3><div class="sub">${c.multi ? `${c.distinct} options · people can pick several` : `${c.distinct} distinct answers`}</div>${hbar(c.values, PALETTE[i % PALETTE.length])}</div>`).join("")}
        ${meta.map((c) => `<div class="widget w2"><h3>${esc(label(c.column))}</h3><div class="sub">consent / declaration</div>${hbar(c.values, "--c3")}</div>`).join("")}
        <div class="widget w6"><h3>Data quality</h3><div class="table-wrap"><table><tbody>${d.quality.map((x) => `<tr><td>${esc(x.check)}</td><td class="num">${esc(x.value)}</td><td>${badge(x.status, { ok: "Pass", warn: "Warning", fail: "Fail" }[x.status])}</td></tr>`).join("") || `<tr><td class="muted">No checks.</td></tr>`}</tbody></table></div></div>
      </div>`;
  }


  // ---------- filtered dashboard ----------
  // Works on the grouped counts in d.cube: base = [dims..., day, count], charts = [dims..., day, value, count].
  // Every number shown goes through the same small-group rule as the pipeline.
  const FS = { id: null, sel: new Map(), from: null, to: null, open: null, q: "" };

  function readFilters(cube) {
    FS.sel = new Map(); FS.from = null; FS.to = null;
    const query = new URLSearchParams(location.hash.split("?")[1] || "");
    for (const part of (query.get("f") || "").split(";").filter(Boolean)) {
      const [col, vals] = part.split(":");
      const di = cube.dims.findIndex((d) => d.column === col);
      if (di < 0) continue;
      const idx = vals.split(",").map((v) => cube.dims[di].values.indexOf(decodeURIComponent(v))).filter((i) => i >= 0);
      if (idx.length) FS.sel.set(di, new Set(idx));
    }
    const from = cube.days.indexOf(query.get("from")), to = cube.days.indexOf(query.get("to"));
    FS.from = from >= 0 ? from : null;
    FS.to = to >= 0 ? to : null;
  }

  function writeFilters(cube) {
    const f = [...FS.sel].map(([di, set]) => `${cube.dims[di].column}:${[...set].map((i) => encodeURIComponent(cube.dims[di].values[i])).join(",")}`).join(";");
    const q = new URLSearchParams();
    if (f) q.set("f", f);
    if (FS.from != null) q.set("from", cube.days[FS.from]);
    if (FS.to != null) q.set("to", cube.days[FS.to]);
    const base = location.hash.split("?")[0];
    const next = q.toString() ? `${base}?${q}` : base;
    try { history.replaceState(null, "", next); } catch { /* sandboxed: filters still work, just not in the URL */ }
  }

  const dated = () => FS.from != null || FS.to != null;
  const active = () => FS.sel.size > 0 || dated();
  function keep(row, nd, skip) {
    for (const [di, set] of FS.sel) if (di !== skip && !set.has(row[di])) return false;
    if (dated()) {
      const day = row[nd];
      if (day < 0 || (FS.from != null && day < FS.from) || (FS.to != null && day > FS.to)) return false;
    }
    return true;
  }

  function suppressJS(counts, k, max) {
    const shown = []; let other = 0;
    counts.sort((a, b) => b[1] - a[1] || String(a[0]).localeCompare(b[0]));
    for (const [value, n] of counts) {
      if (n >= k && shown.length < max) shown.push({ value, count: n });
      else other += n;
    }
    if (other >= k) shown.push({ value: `Other (groups < ${k})`, count: other, folded: true });
    else if (other) shown.push({ value: "Suppressed", count: null, folded: true });
    return shown;
  }

  function mountDashboard(el) {
    const id = el.dataset.id;
    const d = D.dashboard.sources[id];
    const cube = d.cube;
    if (FS.id !== id) { FS.id = id; FS.open = null; }
    readFilters(cube);
    el.innerHTML = `<div class="filterbar" id="fbar"></div><div id="dash-body"></div>`;
    const draw = () => { drawFilterBar(d, cube); drawBody(d, cube); };
    const commit = () => { writeFilters(cube); draw(); };

    el.addEventListener("click", (e) => {
      const t = e.target.closest("[data-act]");
      if (!t) {
        if (FS.open != null && !e.target.closest(".popover")) { FS.open = null; drawFilterBar(d, cube); }
        return;
      }
      const act = t.dataset.act, di = Number(t.dataset.dim), vi = Number(t.dataset.val);
      if (act === "open") { FS.open = FS.open === t.dataset.key ? null : t.dataset.key; FS.q = ""; drawFilterBar(d, cube); return; }
      if (act === "toggle" || act === "bar") {
        const set = FS.sel.get(di) || new Set();
        set.has(vi) ? set.delete(vi) : set.add(vi);
        set.size ? FS.sel.set(di, set) : FS.sel.delete(di);
      }
      if (act === "only") FS.sel.set(di, new Set([vi]));
      if (act === "all") FS.sel.delete(di);
      if (act === "chip") { di >= 0 ? FS.sel.delete(di) : (FS.from = FS.to = null); }
      if (act === "reset") { FS.sel.clear(); FS.from = FS.to = null; FS.open = null; }
      if (act === "preset") {
        const n = cube.days.length, p = t.dataset.preset;
        if (p === "all") FS.from = FS.to = null;
        if (p === "last7") { FS.from = Math.max(0, n - 7); FS.to = n - 1; }
        if (p === "last") { FS.from = FS.to = n - 1; }
        if (p === "early") { FS.from = 0; FS.to = Math.max(0, Math.floor(n / 2) - 1); }
      }
      commit();
    });
    el.addEventListener("input", (e) => {
      if (e.target.id === "pop-search") {
        FS.q = e.target.value.toLowerCase();
        $$(".opt", el).forEach((o) => (o.hidden = FS.q && !o.dataset.label.includes(FS.q)));
      }
    });
    el.addEventListener("change", (e) => {
      if (e.target.dataset.date) {
        const i = cube.days.indexOf(e.target.value);
        if (e.target.dataset.date === "from") FS.from = i >= 0 ? i : null;
        else FS.to = i >= 0 ? i : null;
        if (FS.from != null && FS.to != null && FS.from > FS.to) [FS.from, FS.to] = [FS.to, FS.from];
        commit();
      }
    });
    el.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && FS.open != null) { FS.open = null; drawFilterBar(d, cube); }
      if ((e.key === "Enter" || e.key === " ") && e.target.matches(".hrow.clickable")) { e.preventDefault(); e.target.click(); }
    });
    draw();
  }

  function facet(cube, di) {
    const nd = cube.dims.length, counts = new Array(cube.dims[di].values.length).fill(0);
    for (const r of cube.base) if (keep(r, nd, di)) counts[r[di]] += r[nd + 1];
    return counts;
  }
  const shownCount = (n, k) => (n === 0 ? "0" : n < k ? `<${k}` : fmtInt(n));
  const fmtDay = (iso) => new Date(iso).toLocaleDateString("en-GB", { day: "numeric", month: "short" });

  function drawFilterBar(d, cube) {
    const k = cube.min_group_size, nd = cube.dims.length;
    const total = cube.base.reduce((a, r) => a + r[nd + 1], 0);
    const n = cube.base.reduce((a, r) => a + (keep(r, nd) ? r[nd + 1] : 0), 0);
    const pills = cube.dims.map((dim, di) => {
      const set = FS.sel.get(di);
      const value = !set ? "All" : set.size === 1 ? dim.values[[...set][0]] : `${set.size} selected`;
      const key = `d${di}`;
      return `<div class="pill-wrap"><button class="pill ${set ? "on" : ""}" data-act="open" data-key="${key}" aria-expanded="${FS.open === key}">
          <span class="pill-k">${esc(shortLabel(dim.column))}</span><span class="pill-v">${esc(value)}</span>${caret}</button>
        ${FS.open === key ? optionsPopover(cube, di, k) : ""}</div>`;
    }).join("");
    const dateValue = dated() ? `${fmtDay(cube.days[FS.from ?? 0])} – ${fmtDay(cube.days[FS.to ?? cube.days.length - 1])}` : "All dates";
    const datePill = cube.days.length ? `<div class="pill-wrap"><button class="pill ${dated() ? "on" : ""}" data-act="open" data-key="date" aria-expanded="${FS.open === "date"}">
        ${icon("runs", 12)}<span class="pill-k">Submitted</span><span class="pill-v">${esc(dateValue)}</span>${caret}</button>
        ${FS.open === "date" ? datePopover(cube) : ""}</div>` : "";
    const chips = [...FS.sel].map(([di, set]) => [...set].map((vi) => `<button class="chip" data-act="toggle" data-dim="${di}" data-val="${vi}" title="Remove">
        <span class="muted">${esc(shortLabel(cube.dims[di].column))}:</span> ${esc(cube.dims[di].values[vi])} <span aria-hidden="true">×</span></button>`).join("")).join("")
      + (dated() ? `<button class="chip" data-act="chip" data-dim="-1"><span class="muted">Submitted:</span> ${esc(dateValue)} <span aria-hidden="true">×</span></button>` : "");
    $("#fbar").innerHTML = `
      <div class="fb-row">
        <span class="fb-title">${icon("filter", 14)} Filters</span>
        ${datePill}${pills}
        <span class="fb-spacer"></span>
        <span class="fb-count"><b>${shownCount(n, k)}</b> of ${fmtInt(total)} applicants</span>
        ${active() ? `<button class="btn btn-sm" data-act="reset">Reset</button>` : ""}
      </div>
      ${active() ? `<div class="fb-chips">${chips}</div>` : ""}`;
    const search = $("#pop-search");
    if (search) { search.value = FS.q; search.focus(); }
  }

  function optionsPopover(cube, di, k) {
    const dim = cube.dims[di], counts = facet(cube, di), set = FS.sel.get(di);
    const max = Math.max(1, ...counts);
    const order = dim.values.map((v, i) => i).sort((a, b) => counts[b] - counts[a] || (dim.values[a] === "Other") - (dim.values[b] === "Other"));
    return `<div class="popover" role="dialog" aria-label="${esc(label(dim.column))}">
      <div class="pop-head"><b>${esc(label(dim.column))}</b><span class="pop-links"><button class="linkbtn" data-act="all" data-dim="${di}">All</button></span></div>
      ${dim.values.length > 6 ? `<input class="pop-search" id="pop-search" placeholder="Search ${esc(shortLabel(dim.column).toLowerCase())}…" autocomplete="off">` : ""}
      <div class="pop-list">${order.map((vi) => {
        const on = !!set && set.has(vi), c = counts[vi];
        const hide = FS.q && !dim.values[vi].toLowerCase().includes(FS.q);
        return `<div class="opt ${c === 0 ? "zero" : ""}" data-label="${esc(dim.values[vi].toLowerCase())}" ${hide ? "hidden" : ""}>
          <label class="opt-main" data-act="toggle" data-dim="${di}" data-val="${vi}">
            <span class="check ${on ? "on" : ""}" aria-hidden="true"></span>
            <span class="opt-text"><span class="opt-label">${esc(dim.values[vi])}</span>
              <span class="opt-bar"><span style="width:${(100 * c) / max}%"></span></span></span>
            <span class="opt-n">${shownCount(c, k)}</span></label>
          <button class="linkbtn only" data-act="only" data-dim="${di}" data-val="${vi}">Only</button></div>`;
      }).join("")}</div>
      <div class="pop-foot muted small">${dim.values.includes("Other") ? `"Other" groups answers given by fewer than ${k} people.` : "Counts update with your other filters."}</div>
    </div>`;
  }

  function datePopover(cube) {
    const days = cube.days, nd = cube.dims.length;
    const perDay = new Array(days.length).fill(0);
    for (const r of cube.base) {
      if (r[nd] < 0) continue;
      let ok = true;
      for (const [di, set] of FS.sel) if (!set.has(r[di])) { ok = false; break; }
      if (ok) perDay[r[nd]] += r[nd + 1];
    }
    const max = Math.max(1, ...perDay), from = FS.from ?? 0, to = FS.to ?? days.length - 1;
    return `<div class="popover pop-date" role="dialog" aria-label="Submission date">
      <div class="pop-head"><b>Submission date</b></div>
      <div class="presets">
        ${[["all", "All dates"], ["early", "First half"], ["last7", "Last 7 active days"], ["last", "Last day"]].map(([p, l]) => `<button class="seg" data-act="preset" data-preset="${p}">${l}</button>`).join("")}
      </div>
      <div class="spark">${perDay.map((c, i) => `<span class="${i >= from && i <= to ? "in" : ""}" style="height:${Math.max(2, (100 * c) / max)}%" title="${esc(fmtDay(days[i]))}"></span>`).join("")}</div>
      <div class="date-row">
        <label>From<input type="date" data-date="from" min="${days[0]}" max="${days[days.length - 1]}" value="${days[from]}"></label>
        <label>To<input type="date" data-date="to" min="${days[0]}" max="${days[days.length - 1]}" value="${days[to]}"></label>
      </div>
      <div class="pop-foot muted small">Only days with submissions can be picked.</div>
    </div>`;
  }

  function drawBody(d, cube) {
    const k = cube.min_group_size, nd = cube.dims.length;
    const n = cube.base.reduce((a, r) => a + (keep(r, nd) ? r[nd + 1] : 0), 0);
    const total = cube.base.reduce((a, r) => a + r[nd + 1], 0);
    const q = d.kpis;
    const charts = d.charts.filter((c) => cube.charts[c.column]);
    const computed = {};
    for (const c of charts) {
      const cc = cube.charts[c.column], sums = new Array(cc.values.length).fill(0);
      for (const r of cc.rows) if (keep(r, nd)) sums[r[nd + 1]] += r[nd + 2];
      computed[c.column] = { raw: sums, values: suppressJS(cc.values.map((v, i) => [v, sums[i]]).filter((x) => x[1] > 0), k, cube.max_categories) };
    }
    const top = (col) => { const v = computed[col]?.values.find((x) => !x.folded); return v ? v : null; };
    const topArea = top("phd_area_of_study"), topFund = top("funding_plan_name_of_scholarship");
    const counters = `
      <div class="widget w2 counter"><h3>Applicants</h3><div class="value">${shownCount(n, k)}</div><div class="sub">${active() ? `${n >= k ? Math.round((100 * n) / total) + "% of " : "of "}${fmtInt(total)} in this sheet` : "rows in the DATA tab"}</div></div>
      <div class="widget w2 counter"><h3>Top PhD area</h3><div class="value value-text">${esc(topArea ? topArea.value : "–")}</div><div class="sub">${topArea ? `${fmtInt(topArea.count)} applicants` : "not enough data"}</div></div>
      <div class="widget w2 counter"><h3>Top funding plan</h3><div class="value value-text">${esc(topFund ? topFund.value : "–")}</div><div class="sub">${topFund ? `${fmtInt(topFund.count)} applicants` : "not enough data"}</div></div>`;
    if (n < k) {
      $("#dash-body").innerHTML = `<div class="canvas">${counters}<div class="widget w6 privacy-note">${icon("shield", 18)}
        <div><b>Fewer than ${k} applicants match these filters.</b><br><span class="muted">Charts are hidden so no one can be singled out. Widen or reset the filters.</span></div>
        <button class="btn" data-act="reset">Reset filters</button></div></div>${qualityBlock(d)}`;
      return;
    }
    const filterable = new Map(cube.dims.map((dim, di) => [dim.column, di]));
    const timeline = filteredTimeline(cube, k);
    $("#dash-body").innerHTML = `<div class="canvas">${counters}
      ${timeline}
      ${charts.filter((c) => c.class === "categorical").map((c, i) => chartWidget(c, computed[c.column], filterable.get(c.column), cube, PALETTE[i % PALETTE.length], "")).join("")}
      ${charts.filter((c) => c.class === "metadata").map((c) => chartWidget(c, computed[c.column], undefined, cube, "--c3", "w2")).join("")}
      </div>${qualityBlock(d)}`;
  }

  function chartWidget(c, comp, di, cube, color, size) {
    const set = di != null ? FS.sel.get(di) : null;
    const max = Math.max(1, ...comp.values.map((v) => v.count || 0));
    const k = cube.min_group_size;
    const rows = comp.values.map((v) => {
      const vi = di != null && !v.folded ? cube.dims[di].values.indexOf(v.value) : -1;
      const click = vi >= 0 ? `data-act="bar" data-dim="${di}" data-val="${vi}" role="button" tabindex="0"` : "";
      const state = set ? (vi >= 0 && set.has(vi) ? "sel" : "dim") : "";
      return `<div class="hrow ${click ? "clickable" : ""} ${state}" ${click} title="${esc(v.value)}">
        <span class="lbl">${esc(v.value)}</span>
        <span class="track"><span class="fill ${v.folded ? "folded" : ""}" style="width:${v.count ? (100 * v.count) / max : 0}%;${v.folded ? "" : `background:var(${color})`}"></span></span>
        <span class="n">${v.count == null ? `<${k}` : fmtInt(v.count)}</span></div>`;
    }).join("");
    const sub = c.multi ? "people can pick several" : di != null ? "click a bar to filter" : `${comp.values.length} answers shown`;
    return `<div class="widget ${size}"><h3>${esc(label(c.column))}</h3><div class="sub">${sub}</div><div class="hbar2">${rows || `<p class="muted">No answers.</p>`}</div></div>`;
  }

  function filteredTimeline(cube, k) {
    if (!cube.days.length) return "";
    const nd = cube.dims.length, perDay = new Array(cube.days.length).fill(0);
    for (const r of cube.base) if (r[nd] >= 0 && keep(r, nd)) perDay[r[nd]] += r[nd + 1];
    if (!active()) {
      return `<div class="widget w6"><h3>Submissions per day</h3><div class="sub">by form timestamp</div>${timelineChart(cube.days.map((date, i) => ({ date, count: perDay[i] })))}</div>`;
    }
    // Filtered: weekly totals, and weeks under k are drawn as a stub without a number.
    const weeks = new Map();
    cube.days.forEach((iso, i) => {
      const dt = new Date(iso); const monday = new Date(dt); monday.setDate(dt.getDate() - ((dt.getDay() + 6) % 7));
      const key = monday.toISOString().slice(0, 10);
      weeks.set(key, (weeks.get(key) || 0) + perDay[i]);
    });
    const pts = [...weeks].filter(([, c]) => c > 0);
    const W = 960, H = 180, L = 36, B = 24, T = 8;
    const max = Math.max(k, ...pts.map(([, c]) => c));
    const step = (W - L) / Math.max(1, pts.length), bw = Math.max(4, step * 0.6);
    const y = (v) => T + (H - T - B) * (1 - v / max);
    return `<div class="widget w6"><h3>Submissions per week</h3><div class="sub">filtered · weeks with fewer than ${k} are shown as a grey stub</div>
      <svg class="chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="Submissions per week">
      ${[0, Math.round(max / 2), max].map((t) => `<line class="grid-line" x1="${L}" x2="${W}" y1="${y(t)}" y2="${y(t)}"/><text x="${L - 6}" y="${y(t) + 4}" text-anchor="end">${t}</text>`).join("")}
      ${pts.map(([wk, c], i) => {
        const x = L + i * step + (step - bw) / 2, small = c < k, h = small ? 6 : H - B - y(c);
        return `<rect x="${x}" y="${H - B - h}" width="${bw}" height="${h}" rx="2" fill="${small ? "var(--c9)" : "var(--c1)"}"><title>Week of ${esc(fmtDay(wk))}: ${small ? `<${k}` : c}</title></rect>
          <text x="${x + bw / 2}" y="${H - 6}" text-anchor="middle">${esc(fmtDay(wk))}</text>`;
      }).join("")}</svg></div>`;
  }

  function qualityBlock(d) {
    const q = d.kpis;
    return `<h2 class="section-title">Data quality <span class="muted small">· all records, not filtered</span></h2>
      <div class="canvas">
        <div class="widget w2 counter"><h3>Duplicate submissions</h3><div class="value">${fmtInt(q.duplicate_submissions)}</div><div class="sub">${q.duplicate_submissions == null ? "not measured yet" : "same email submitted again"}</div></div>
        <div class="widget w2 counter"><h3>Average completeness</h3><div class="value">${pct(q.avg_completeness)}</div><div class="sub">across ${q.columns_measured ?? q.columns} of ${q.columns} columns</div></div>
        <div class="widget w2"><h3>Checks</h3><div class="table-wrap"><table><tbody>${d.quality.map((x) => `<tr><td>${esc(x.check)}</td><td class="num">${esc(x.value)}</td><td>${badge(x.status, { ok: "Pass", warn: "Warning", fail: "Fail" }[x.status])}</td></tr>`).join("") || `<tr><td class="muted">No checks.</td></tr>`}</tbody></table></div></div>
      </div>`;
  }

  const SHORT = {
    your_current_residency_province: "Province", current_occupation_job_sector: "Sector", phd_area_of_study: "PhD area",
    funding_plan_name_of_scholarship: "Funding", when_do_you_plan_to_start_your_phd: "PhD start", prior_research_experience: "Research exp.",
  };
  const shortLabel = (column) => SHORT[column] || label(column);
  const caret = `<svg class="caret" viewBox="0 0 16 16" width="12" height="12" aria-hidden="true"><path d="m4 6 4 4 4-4" fill="none" stroke="currentColor" stroke-width="1.6"/></svg>`;

  const partialNote = () => `<div class="callout warn"><b>Initial load.</b> This data was loaded once through a Google connector, reading only the columns that feed the charts. Personal and free-text columns show "–" until the scheduled pipeline (service account) runs.</div>`;
  const label = (column) => cap(column.replace(/_/g, " ").replace(/\bphd\b/g, "PhD"));

  function hbar(values, color) {
    const max = Math.max(1, ...values.map((v) => v.count || 0));
    return `<div class="hbar">${values.map((v) => `<span class="lbl" title="${esc(v.value)}">${esc(v.value)}</span>
      <span class="track"><span class="fill ${v.folded ? "folded" : ""}" style="width:${v.count ? (100 * v.count) / max : 0}%;${v.folded ? "" : `background:var(${color})`}"></span></span>
      <span class="n">${v.count == null ? "<5" : fmtInt(v.count)}</span>`).join("")}</div>`;
  }

  function timelineChart(points) {
    const pts = points.filter((p) => p.date);
    if (!pts.length) return `<p class="muted">No readable timestamps.</p>`;
    const W = 960, H = 200, L = 36, B = 24, T = 8;
    const max = Math.max(...pts.map((p) => p.count));
    const step = (W - L) / pts.length;
    const bw = Math.max(2, step * 0.7);
    const y = (v) => T + (H - T - B) * (1 - v / max);
    const ticks = [0, Math.round(max / 2), max];
    const every = Math.ceil(pts.length / 10);
    return `<svg class="chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="Submissions per day">
      ${ticks.map((t) => `<line class="grid-line" x1="${L}" x2="${W}" y1="${y(t)}" y2="${y(t)}"/><text x="${L - 6}" y="${y(t) + 4}" text-anchor="end">${t}</text>`).join("")}
      ${pts.map((p, i) => `<rect x="${L + i * step + (step - bw) / 2}" y="${y(p.count)}" width="${bw}" height="${H - B - y(p.count)}" rx="1.5" fill="var(--c1)"><title>${esc(p.date)}: ${p.count}</title></rect>
        ${i % every === 0 ? `<text x="${L + i * step + step / 2}" y="${H - 6}" text-anchor="middle">${esc(new Date(p.date).toLocaleDateString("en-GB", { day: "numeric", month: "short" }))}</text>` : ""}`).join("")}
    </svg>`;
  }

  function governance() {
    const g = D.governance;
    const classes = ["personal", "sensitive", "free_text", "categorical", "metadata"];
    return head("Governance", "What is collected, what is withheld, and the rules that decide it.") + `
      <div class="callout"><b>Policy.</b> Rows never leave the pipeline run. Columns tagged <b>personal</b> or <b>sensitive</b> are dropped before anything is counted; free text is only measured for completeness; categories with fewer than <b>${g.min_group_size}</b> people are folded into one group, and that group is hidden too when it is itself under ${g.min_group_size}. Published files are encrypted with the site PIN (AES-256-GCM, PBKDF2 key).</div>
      <div class="table-wrap" style="margin-bottom:16px"><table><thead><tr><th>Tag</th><th>Meaning</th><th>Published as</th></tr></thead><tbody>
        ${classes.map((k) => `<tr><td>${tag(k)}</td><td>${esc(CLASS_INFO[k])}</td><td>${g.withheld_classes.includes(k) ? badge("fail", "Never") : k === "free_text" ? badge("warn", "Completeness only") : badge("ok", "Suppressed counts")}</td></tr>`).join("")}
      </tbody></table></div>
      ${g.sources.map((s) => {
        const total = classes.reduce((a, k) => a + (s.classes[k] || 0), 0) || 1;
        return `<div class="card" style="margin-bottom:16px"><div class="page-head" style="margin-bottom:8px"><div><h2>${esc(s.title)}</h2><p class="muted">Owner ${esc(s.owner)} · ${esc(s.purpose)}</p></div></div>
          <div style="display:flex;height:14px;border-radius:3px;overflow:hidden;margin-bottom:6px">${classes.map((k, i) => s.classes[k] ? `<span title="${k}: ${s.classes[k]}" style="width:${(100 * s.classes[k]) / total}%;background:var(${["--danger", "--c6", "--warning", "--primary", "--success"][i]})"></span>` : "").join("")}</div>
          <p class="small" style="margin:0 0 12px">${classes.map((k) => `${tag(k)} ${s.classes[k] || 0}`).join(" &nbsp; ")}</p>
          <div class="grid grid-2"><div><h3>Withheld columns (${s.withheld.length})</h3><p>${s.withheld.map((c) => `<code>${esc(c)}</code>`).join(", ") || "None"}</p></div>
          <div><h3>Consent coverage</h3>${s.consent.map((c) => `<p style="margin:2px 0"><code>${esc(c.column)}</code> answered by ${pct(c.completeness)} ${badge(c.completeness >= 0.999 ? "ok" : "fail", c.completeness >= 0.999 ? "Complete" : "Gaps")}</p>`).join("") || `<p class="muted">No consent column found. Add one to the form.</p>`}</div></div></div>`;
      }).join("") || `<div class="empty">No registered sources.</div>`}
      <div class="card"><h3>Classification rules</h3><p class="muted small">From <code>config/platform.yml</code>, first match on <code>column_name</code> wins.</p>
        <div class="table-wrap"><table><thead><tr><th>Pattern</th><th>Tag</th></tr></thead><tbody>${g.rules.map((r) => `<tr><td class="mono">${esc(r.match)}</td><td>${tag(r.class)}</td></tr>`).join("")}</tbody></table></div></div>`;
  }

  // ---------- run now ----------
  function runNow(sourceId) {
    const m = $("#modal");
    m.innerHTML = `<div class="modal" role="dialog" aria-modal="true" aria-labelledby="rn-title">
      <header><h2 id="rn-title">Run now with parameters</h2><button class="icon-btn" data-close aria-label="Close">✕</button></header>
      <div class="body"><div class="field"><label for="rn-source">source</label>
        <select id="rn-source"><option value="">All sources</option>${sources().filter((s) => s.status !== "unreachable").map((s) => `<option value="${esc(s.id)}" ${s.id === sourceId ? "selected" : ""}>${esc(s.title)}</option>`).join("")}</select></div>
        <p class="muted small" style="margin:0">GitHub opens the workflow page. Click <b>Run workflow</b>, paste the source ID (copied for you) or leave it blank for all, then <b>Run workflow</b> again. Needs write access to <code>${esc(D.jobs.workflow.repository)}</code>. New data shows here when the run finishes, usually in 1–2 minutes.</p></div>
      <footer><button class="btn" data-close>Cancel</button><button class="btn btn-primary" id="rn-go">${icon("play", 12)} Open Run workflow</button></footer></div>`;
    m.hidden = false;
    const close = () => { m.hidden = true; m.innerHTML = ""; };
    $$("[data-close]", m).forEach((b) => b.addEventListener("click", close));
    m.addEventListener("click", (e) => { if (e.target === m) close(); }, { once: true });
    $("#rn-go").addEventListener("click", async () => {
      const id = $("#rn-source").value;
      if (id) { try { await navigator.clipboard.writeText(id); } catch { /* clipboard blocked: ID is visible in the list */ } }
      window.open(workflowUrl(), "_blank", "noopener");
      close();
    });
    $("#rn-source").focus();
  }

  // ---------- search ----------
  let index = [];
  function buildSearch() {
    index = [
      ...registered().flatMap((s) => [
        { kind: "Table", label: s.fqn, href: `#/catalog/${tableName(s)}` },
        ...s.columns.map((c) => ({ kind: "Column", label: `${tableName(s)}.${c.name}`, extra: c.description, href: `#/catalog/${tableName(s)}` })),
      ]),
      ...sources().map((s) => ({ kind: "Source", label: s.title, href: "#/ingestion" })),
      ...Object.entries(D.dashboard.sources || {}).map(([id, d]) => ({ kind: "Dashboard", label: d.title, href: `#/dashboards/${id}` })),
      { kind: "Job", label: JOB_ID, href: `#/jobs/${JOB_ID}` },
      { kind: "Page", label: "Governance", href: "#/governance" },
      { kind: "Page", label: "Job Runs", href: "#/runs" },
    ];
  }
  function wireSearch() {
    const input = $("#search");
    const box = $("#search-results");
    let active = 0;
    const draw = () => {
      const q = input.value.trim().toLowerCase();
      if (!q) { box.hidden = true; return; }
      const hits = index.filter((x) => x.label.toLowerCase().includes(q) || (x.extra || "").toLowerCase().includes(q)).slice(0, 12);
      active = Math.min(active, Math.max(0, hits.length - 1));
      box.innerHTML = hits.length ? hits.map((h, i) => `<a href="${esc(h.href)}" class="${i === active ? "active" : ""}"><span class="kind">${esc(h.kind)}</span><span>${esc(h.label)}</span></a>`).join("") : `<div class="muted" style="padding:8px">No results</div>`;
      box.hidden = false;
    };
    input.addEventListener("input", () => { active = 0; draw(); });
    input.addEventListener("keydown", (e) => {
      const links = $$("a", box);
      if (e.key === "ArrowDown") { active = Math.min(active + 1, links.length - 1); draw(); e.preventDefault(); }
      if (e.key === "ArrowUp") { active = Math.max(active - 1, 0); draw(); e.preventDefault(); }
      if (e.key === "Enter" && links[active]) { location.hash = links[active].getAttribute("href"); input.value = ""; box.hidden = true; input.blur(); }
      if (e.key === "Escape") { input.value = ""; box.hidden = true; input.blur(); }
    });
    box.addEventListener("click", () => { input.value = ""; box.hidden = true; });
    input.addEventListener("blur", () => setTimeout(() => (box.hidden = true), 150));
    document.addEventListener("keydown", (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "p" && !$("#app").hidden) { e.preventDefault(); input.focus(); }
    });
  }

  // ---------- boot ----------
  function boot() {
    $$("i[data-icon]").forEach((i) => (i.innerHTML = icon(i.dataset.icon, 16)));
    $("#nav-toggle").addEventListener("click", () => $("#sidebar").classList.toggle("open"));
    $("#sign-out").addEventListener("click", () => { store.del("im_pin"); D = null; keyCache.clear(); location.hash = ""; showLogin(); });
    wireSearch();
    window.addEventListener("hashchange", route);
    document.addEventListener("keydown", (e) => { if (e.key === "Escape" && !$("#modal").hidden) { $("#modal").hidden = true; $("#modal").innerHTML = ""; } });
    if (!window.crypto?.subtle) { showLogin("This browser can't decrypt the data (needs HTTPS and a modern browser)."); return; }
    const saved = store.get("im_pin");
    if (saved) tryPin(saved); else showLogin();
  }
  boot();
})();
