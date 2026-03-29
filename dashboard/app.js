/* Context Engine Dashboard — app.js */

const REPO = "conorpmuldoon-cpm/context-engine";
const PER_PAGE = 25;

let allRecords = [];
let filteredRecords = [];
let clusters = {};
let stats = {};
let briefings = [];
let taxonomy = {};
let currentPage = 1;

/* ── Data Loading ── */

async function loadJSON(url) {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Failed to load ${url}`);
  return resp.json();
}

async function init() {
  try {
    [allRecords, clusters, stats, briefings, taxonomy] = await Promise.all([
      loadJSON("data/records-index.json"),
      loadJSON("data/clusters.json"),
      loadJSON("data/stats.json"),
      loadJSON("data/briefings-index.json"),
      loadJSON("data/taxonomy.json"),
    ]);
  } catch (e) {
    document.getElementById("stats-grid").innerHTML =
      '<p style="color:#e74c3c">Error loading data. Run <code>python scripts/build_dashboard.py</code> first.</p>';
    console.error(e);
    return;
  }

  // Sort records newest first
  allRecords.sort((a, b) => (b.date || "").localeCompare(a.date || ""));
  filteredRecords = allRecords;

  populateFilters();
  setupEventListeners();
  handleRoute();

  // Footer
  const gen = document.getElementById("footer-generated");
  if (gen && stats.generated) gen.textContent = `Data generated: ${stats.generated}`;

  // Issue links
  const base = `https://github.com/${REPO}/issues/new`;
  const recLink = document.getElementById("new-record-link");
  if (recLink) recLink.href = `${base}?template=new-record.yml`;
  const brLink = document.getElementById("briefing-request-link");
  if (brLink) brLink.href = `${base}?template=briefing-request.yml`;
}

/* ── Routing ── */

function handleRoute() {
  const hash = location.hash || "#/";
  const view = hash.replace("#/", "") || "dashboard";

  // Update tabs
  document.querySelectorAll(".tab").forEach((t) => {
    t.classList.toggle("active", t.dataset.view === view);
  });

  // Show correct view
  document.querySelectorAll(".view").forEach((v) => {
    v.style.display = v.id === `view-${view}` ? "block" : "none";
  });

  // Render view
  switch (view) {
    case "dashboard": renderDashboard(); break;
    case "records": renderRecords(); break;
    case "clusters": renderClusters(); break;
    case "briefings": renderBriefings(); break;
  }
}

/* ── Dashboard View ── */

function renderDashboard() {
  // Stats cards
  const grid = document.getElementById("stats-grid");
  grid.innerHTML = [
    statCard(stats.total_records, "Total Records"),
    statCard(stats.clustered_records, "Clustered"),
    statCard(`${stats.cluster_pct}%`, "Cluster Coverage"),
    statCard(Object.keys(clusters).length, "Clusters"),
    statCard(briefings.length, "Briefings"),
    statCard(Object.keys(stats.by_source || {}).length, "Source Types"),
  ].join("");

  // Source chart
  renderBarChart("chart-source", stats.by_source || {}, formatSourceType);

  // Top tags
  renderBarChart("chart-tags", stats.top_tags || {});

  // Timeline
  renderBarChart("chart-timeline", stats.by_month || {});

  // Recent records
  const recent = allRecords.slice(0, 10);
  document.getElementById("recent-records").innerHTML = recent
    .map(recordCard)
    .join("");
}

function statCard(value, label) {
  return `<div class="stat-card"><div class="stat-value">${value}</div><div class="stat-label">${label}</div></div>`;
}

function renderBarChart(containerId, data, labelFn) {
  const el = document.getElementById(containerId);
  if (!el) return;
  const entries = Object.entries(data);
  if (!entries.length) { el.innerHTML = "<p>No data</p>"; return; }
  const max = Math.max(...entries.map(([, v]) => v));
  el.innerHTML = entries
    .map(([k, v]) => {
      const pct = (v / max) * 100;
      const label = labelFn ? labelFn(k) : k;
      return `<div class="bar-row">
        <span class="bar-label" title="${k}">${label}</span>
        <div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div></div>
        <span class="bar-value">${v}</span>
      </div>`;
    })
    .join("");
}

/* ── Records View ── */

function renderRecords() {
  const start = (currentPage - 1) * PER_PAGE;
  const page = filteredRecords.slice(start, start + PER_PAGE);

  document.getElementById("search-count").textContent =
    `${filteredRecords.length} of ${allRecords.length} records`;

  document.getElementById("records-list").innerHTML = page.length
    ? page.map(recordCard).join("")
    : '<p style="color:var(--text-muted);padding:1rem">No records match your filters.</p>';

  renderPagination();
}

function recordCard(rec) {
  const sentClass = rec.sentiment === "critical" ? "tag-sentiment-critical"
    : rec.sentiment === "positive" ? "tag-sentiment-positive"
    : rec.sentiment === "advocacy" ? "tag-sentiment-advocacy" : "";

  const tags = (rec.tags || []).slice(0, 4).map((t) => `<span class="tag">${t}</span>`).join("");
  const source = `<span class="tag tag-source">${formatSourceType(rec.source_type)}</span>`;
  const sentiment = rec.sentiment ? `<span class="tag ${sentClass}">${rec.sentiment}</span>` : "";
  const signal = rec.signal ? `<span class="tag tag-signal">${rec.signal}</span>` : "";

  return `<div class="record-card" data-id="${rec.id}" onclick="showRecord('${rec.id}')">
    <div class="record-header">
      <span class="record-title">${esc(rec.title)}</span>
      <span class="record-date">${rec.date || "n/a"}</span>
    </div>
    <div class="record-summary">${esc(rec.summary)}</div>
    <div class="record-meta">${source}${sentiment}${signal}${tags}</div>
  </div>`;
}

function renderPagination() {
  const totalPages = Math.ceil(filteredRecords.length / PER_PAGE);
  const el = document.getElementById("pagination");
  if (totalPages <= 1) { el.innerHTML = ""; return; }

  let html = "";
  if (currentPage > 1) html += `<button onclick="goPage(${currentPage - 1})">&laquo;</button>`;

  const start = Math.max(1, currentPage - 3);
  const end = Math.min(totalPages, currentPage + 3);
  if (start > 1) html += `<button onclick="goPage(1)">1</button><button disabled>...</button>`;
  for (let i = start; i <= end; i++) {
    html += `<button class="${i === currentPage ? 'active' : ''}" onclick="goPage(${i})">${i}</button>`;
  }
  if (end < totalPages) html += `<button disabled>...</button><button onclick="goPage(${totalPages})">${totalPages}</button>`;

  if (currentPage < totalPages) html += `<button onclick="goPage(${currentPage + 1})">&raquo;</button>`;
  el.innerHTML = html;
}

function goPage(n) {
  currentPage = n;
  renderRecords();
  document.getElementById("view-records").scrollIntoView({ behavior: "smooth" });
}

/* ── Record Detail Modal ── */

function showRecord(id) {
  const rec = allRecords.find((r) => r.id === id);
  if (!rec) return;

  const entities = (rec.entities || []).map((e) => `<span class="tag">${esc(e)}</span>`).join("");
  const clusterTags = (rec.clusters || []).map((c) =>
    `<span class="tag" style="cursor:pointer" onclick="filterByCluster('${c}')">${c}</span>`
  ).join("");
  const tags = (rec.tags || []).map((t) => `<span class="tag">${t}</span>`).join("");
  const depts = (rec.depts || []).map((d) => `<span class="tag">${d}</span>`).join("");

  document.getElementById("modal-content").innerHTML = `
    <h2>${esc(rec.title)}</h2>
    <div class="detail-row"><span class="detail-label">Record ID</span><span class="detail-value">${rec.id}</span></div>
    <div class="detail-row"><span class="detail-label">Date</span><span class="detail-value">${rec.date || "n/a"}</span></div>
    <div class="detail-row"><span class="detail-label">Source</span><span class="detail-value">${formatSourceType(rec.source_type)}</span></div>
    ${rec.source_url ? `<div class="detail-row"><span class="detail-label">URL</span><span class="detail-value"><a href="${rec.source_url}" target="_blank">${rec.source_url}</a></span></div>` : ""}
    <div class="detail-row"><span class="detail-label">Sentiment</span><span class="detail-value">${rec.sentiment || "n/a"}</span></div>
    ${rec.signal ? `<div class="detail-row"><span class="detail-label">Signal</span><span class="detail-value"><span class="tag tag-signal">${rec.signal}</span></span></div>` : ""}
    <div class="detail-summary">${esc(rec.summary)}</div>
    ${tags ? `<div class="detail-row"><span class="detail-label">Tags</span><div class="detail-value" style="display:flex;flex-wrap:wrap;gap:0.3rem">${tags}</div></div>` : ""}
    ${depts ? `<div class="detail-row"><span class="detail-label">Departments</span><div class="detail-value" style="display:flex;flex-wrap:wrap;gap:0.3rem">${depts}</div></div>` : ""}
    ${entities ? `<div class="detail-row"><span class="detail-label">Entities</span><div class="entity-list">${entities}</div></div>` : ""}
    ${clusterTags ? `<div class="detail-row"><span class="detail-label">Clusters</span><div class="cluster-list">${clusterTags}</div></div>` : ""}
  `;
  document.getElementById("modal-overlay").style.display = "flex";
}

function closeModal() {
  document.getElementById("modal-overlay").style.display = "none";
}

function filterByCluster(clusterId) {
  closeModal();
  location.hash = "#/clusters";
  setTimeout(() => {
    const el = document.getElementById(`cluster-${clusterId}`);
    if (el) {
      el.scrollIntoView({ behavior: "smooth" });
      el.querySelector(".cluster-records").classList.add("expanded");
    }
  }, 100);
}

/* ── Clusters View ── */

function renderClusters() {
  const searchVal = (document.getElementById("cluster-search")?.value || "").toLowerCase();
  const entries = Object.entries(clusters)
    .filter(([id]) => !searchVal || id.toLowerCase().includes(searchVal))
    .sort((a, b) => b[1].count - a[1].count);

  document.getElementById("clusters-list").innerHTML = entries.length
    ? entries.map(([id, data]) => clusterCard(id, data)).join("")
    : '<p style="color:var(--text-muted);padding:1rem">No clusters match.</p>';
}

function clusterCard(id, data) {
  const recs = data.records
    .map((rid) => allRecords.find((r) => r.id === rid))
    .filter(Boolean)
    .sort((a, b) => (b.date || "").localeCompare(a.date || ""));

  const recordsHtml = recs
    .map((r) => `<div class="record-card" onclick="showRecord('${r.id}')" style="margin:0.25rem 0">
      <div class="record-header">
        <span class="record-title">${esc(r.title)}</span>
        <span class="record-date">${r.date || ""}</span>
      </div>
    </div>`)
    .join("");

  const name = formatClusterName(id);

  return `<div class="cluster-card" id="cluster-${id}">
    <h3 onclick="toggleCluster(this)">${name}</h3>
    <div class="cluster-count">${data.count} records &mdash; ${id}</div>
    <div class="cluster-records">${recordsHtml}</div>
  </div>`;
}

function toggleCluster(el) {
  el.parentElement.querySelector(".cluster-records").classList.toggle("expanded");
}

/* ── Briefings View ── */

function renderBriefings() {
  const el = document.getElementById("briefings-list");
  if (!briefings.length) {
    el.innerHTML = '<p style="color:var(--text-muted)">No briefings generated yet.</p>';
    return;
  }
  el.innerHTML = briefings
    .sort((a, b) => (b.date || "").localeCompare(a.date || ""))
    .map((b) => `<div class="briefing-card">
      <div>
        <div class="briefing-title">${esc(b.title)}</div>
        <div class="briefing-meta">${b.date} &mdash; ${b.records} records reviewed</div>
      </div>
      <a href="${b.file}" class="btn">Read</a>
    </div>`)
    .join("");
}

/* ── Filters ── */

function populateFilters() {
  const sources = [...new Set(allRecords.map((r) => r.source_type).filter(Boolean))].sort();
  const depts = [...new Set(allRecords.flatMap((r) => r.depts))].sort();
  const tags = [...new Set(allRecords.flatMap((r) => r.tags))].sort();
  const sentiments = [...new Set(allRecords.map((r) => r.sentiment).filter(Boolean))].sort();
  const signals = [...new Set(allRecords.map((r) => r.signal).filter(Boolean))].sort();

  fillSelect("filter-source", sources, formatSourceType);
  fillSelect("filter-dept", depts);
  fillSelect("filter-tag", tags);
  fillSelect("filter-sentiment", sentiments);
  fillSelect("filter-signal", signals);
}

function fillSelect(id, values, labelFn) {
  const sel = document.getElementById(id);
  if (!sel) return;
  const defaultOpt = sel.options[0].textContent;
  sel.innerHTML = `<option value="">${defaultOpt}</option>` +
    values.map((v) => `<option value="${v}">${labelFn ? labelFn(v) : v}</option>`).join("");
}

function applyFilters() {
  const search = (document.getElementById("search-input")?.value || "").toLowerCase();
  const source = document.getElementById("filter-source")?.value || "";
  const dept = document.getElementById("filter-dept")?.value || "";
  const tag = document.getElementById("filter-tag")?.value || "";
  const sentiment = document.getElementById("filter-sentiment")?.value || "";
  const signal = document.getElementById("filter-signal")?.value || "";

  filteredRecords = allRecords.filter((r) => {
    if (source && r.source_type !== source) return false;
    if (dept && !r.depts.includes(dept)) return false;
    if (tag && !r.tags.includes(tag)) return false;
    if (sentiment && r.sentiment !== sentiment) return false;
    if (signal && r.signal !== signal) return false;
    if (search) {
      const hay = `${r.title} ${r.summary} ${r.entities.join(" ")} ${r.id}`.toLowerCase();
      if (!hay.includes(search)) return false;
    }
    return true;
  });

  currentPage = 1;
  renderRecords();
}

/* ── Event Listeners ── */

function setupEventListeners() {
  window.addEventListener("hashchange", handleRoute);

  // Search & filters
  const searchInput = document.getElementById("search-input");
  if (searchInput) {
    let timer;
    searchInput.addEventListener("input", () => {
      clearTimeout(timer);
      timer = setTimeout(applyFilters, 200);
    });
  }

  ["filter-source", "filter-dept", "filter-tag", "filter-sentiment", "filter-signal"].forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.addEventListener("change", applyFilters);
  });

  const clearBtn = document.getElementById("clear-filters");
  if (clearBtn) clearBtn.addEventListener("click", () => {
    document.getElementById("search-input").value = "";
    ["filter-source", "filter-dept", "filter-tag", "filter-sentiment", "filter-signal"]
      .forEach((id) => { document.getElementById(id).value = ""; });
    applyFilters();
  });

  // Cluster search
  const clusterSearch = document.getElementById("cluster-search");
  if (clusterSearch) {
    let timer;
    clusterSearch.addEventListener("input", () => {
      clearTimeout(timer);
      timer = setTimeout(renderClusters, 200);
    });
  }

  // Modal
  document.getElementById("modal-close")?.addEventListener("click", closeModal);
  document.getElementById("modal-overlay")?.addEventListener("click", (e) => {
    if (e.target.id === "modal-overlay") closeModal();
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeModal();
  });
}

/* ── Helpers ── */

function esc(str) {
  if (!str) return "";
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

function formatSourceType(st) {
  return (st || "unknown")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatClusterName(id) {
  return id
    .replace(/^(CLUSTER-|DEDUP-URL-|DEDUP-EVENT-)/, "")
    .replace(/-\d{4}Q?\d?$/, "")
    .replace(/-/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/* ── Boot ── */
init();
