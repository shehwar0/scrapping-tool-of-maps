const startBtn = document.getElementById("startBtn");
const stopBtn = document.getElementById("stopBtn");
const downloadBtn = document.getElementById("downloadBtn");
const statusEl = document.getElementById("status");
const bodyEl = document.getElementById("resultsBody");
const statsPanel = document.getElementById("statsPanel");
const modeDescription = document.getElementById("modeDescription");
const extractionModeSelect = document.getElementById("extractionMode");
const historyInfo = document.getElementById("historyInfo");
const historyCount = document.getElementById("historyCount");
const clearHistoryBtn = document.getElementById("clearHistoryBtn");
const historyFilesList = document.getElementById("historyFilesList");
const refreshHistoryFilesBtn = document.getElementById("refreshHistoryFilesBtn");
const selectAllHistoryFilesBtn = document.getElementById("selectAllHistoryFilesBtn");
const clearSelectedHistoryFilesBtn = document.getElementById("clearSelectedHistoryFilesBtn");
const MAX_RESULTS_LIMIT = 500;

let pollingId = null;
let outputHistoryFiles = [];
const selectedHistoryFiles = new Set();

// Mode descriptions for the UI
const modeDescriptions = {
  ultra: `<small><strong>Ultra Deep:</strong> Uses ALL extraction engines (business_extractor, email_extractor, enhanced_scraper, deep_scraper) in parallel with cross-verification. Highest accuracy, slowest speed. Best for important lead generation.</small>`,
  deep: `<small><strong>Deep:</strong> Multi-source extraction - Google Maps → Website analysis → Google Search cross-verification. Finds Instagram, Facebook, WhatsApp and emails from multiple sources.</small>`,
  enhanced: `<small><strong>Enhanced:</strong> Google Maps + comprehensive website analysis. Extracts tech stack, chatbots, analytics. Good balance of speed and data quality.</small>`,
  basic: `<small><strong>Basic:</strong> Fast Maps-only extraction. Gets name, phone, address, rating, website from Google Maps only. Fastest option when you need quick results.</small>`
};

function updateModeDescription() {
  const mode = extractionModeSelect.value;
  if (modeDescription && modeDescriptions[mode]) {
    modeDescription.innerHTML = modeDescriptions[mode];
  }
}

// Initialize mode description
if (extractionModeSelect) {
  extractionModeSelect.addEventListener("change", updateModeDescription);
  updateModeDescription();
}

// ============================================================================
// HISTORY MANAGEMENT
// ============================================================================

async function fetchHistoryStats() {
  const keyword = document.getElementById("keyword").value.trim();
  const location = document.getElementById("location").value.trim();
  
  if (!keyword || !location) {
    historyInfo.style.display = "none";
    return;
  }
  
  try {
    const res = await fetch(`/history/stats?keyword=${encodeURIComponent(keyword)}&location=${encodeURIComponent(location)}`);
    const data = await res.json();
    
    if (data.search_total > 0) {
      historyCount.textContent = data.search_total;
      historyInfo.style.display = "block";
    } else {
      historyInfo.style.display = "none";
    }
  } catch {
    historyInfo.style.display = "none";
  }
}

async function clearHistory() {
  const keyword = document.getElementById("keyword").value.trim();
  const location = document.getElementById("location").value.trim();
  
  if (!keyword || !location) {
    alert("Please enter keyword and location first.");
    return;
  }
  
  if (!confirm(`Clear history for "${keyword}" in "${location}"?\n\nThis will allow you to scrape the same businesses again.`)) {
    return;
  }
  
  try {
    const res = await fetch("/history/clear", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ keyword, location }),
    });
    const data = await res.json();
    
    if (data.ok) {
      setStatus(`✓ ${data.message}`);
      historyInfo.style.display = "none";
    } else {
      setStatus(`Error: ${data.error || "Failed to clear history"}`);
    }
  } catch {
    setStatus("Error clearing history.");
  }
}

async function fetchOutputHistoryFiles() {
  if (!historyFilesList) {
    return;
  }

  try {
    const res = await fetch("/history/output-files");
    const data = await res.json();
    outputHistoryFiles = Array.isArray(data.files) ? data.files : [];

    const availableNames = new Set(outputHistoryFiles.map((file) => file.name));
    for (const selected of [...selectedHistoryFiles]) {
      if (!availableNames.has(selected)) {
        selectedHistoryFiles.delete(selected);
      }
    }

    renderOutputHistoryFiles();
  } catch {
    historyFilesList.innerHTML = "Could not load output history files.";
  }
}

function renderOutputHistoryFiles() {
  if (!historyFilesList) {
    return;
  }

  historyFilesList.innerHTML = "";

  if (!outputHistoryFiles.length) {
    historyFilesList.textContent = "No output CSV files found yet.";
    return;
  }

  const fragment = document.createDocumentFragment();

  outputHistoryFiles.forEach((file) => {
    const row = document.createElement("label");
    row.className = "history-file-row";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = selectedHistoryFiles.has(file.name);
    checkbox.addEventListener("change", () => {
      if (checkbox.checked) {
        selectedHistoryFiles.add(file.name);
      } else {
        selectedHistoryFiles.delete(file.name);
      }
    });

    const nameEl = document.createElement("span");
    nameEl.className = "history-file-name";
    nameEl.textContent = file.name;

    const metaEl = document.createElement("small");
    metaEl.className = "history-file-meta";
    metaEl.textContent = `${file.rows || 0} rows | ${file.modified || "unknown"}`;

    row.appendChild(checkbox);
    row.appendChild(nameEl);
    row.appendChild(metaEl);
    fragment.appendChild(row);
  });

  historyFilesList.appendChild(fragment);
}

function getSelectedHistoryFiles() {
  return [...selectedHistoryFiles];
}

// Set up history listeners
if (clearHistoryBtn) {
  clearHistoryBtn.addEventListener("click", clearHistory);
}

if (refreshHistoryFilesBtn) {
  refreshHistoryFilesBtn.addEventListener("click", fetchOutputHistoryFiles);
}

if (selectAllHistoryFilesBtn) {
  selectAllHistoryFilesBtn.addEventListener("click", () => {
    outputHistoryFiles.forEach((file) => selectedHistoryFiles.add(file.name));
    renderOutputHistoryFiles();
  });
}

if (clearSelectedHistoryFilesBtn) {
  clearSelectedHistoryFilesBtn.addEventListener("click", () => {
    selectedHistoryFiles.clear();
    renderOutputHistoryFiles();
  });
}

// Check history when keyword/location changes
const keywordInput = document.getElementById("keyword");
const locationInput = document.getElementById("location");

if (keywordInput && locationInput) {
  let historyTimeout;
  const checkHistory = () => {
    clearTimeout(historyTimeout);
    historyTimeout = setTimeout(fetchHistoryStats, 500);
  };
  
  keywordInput.addEventListener("input", checkHistory);
  locationInput.addEventListener("input", checkHistory);
}

function setStatus(message) {
  statusEl.textContent = message;
}

function setRunningState(isRunning) {
  startBtn.disabled = isRunning;
  stopBtn.disabled = !isRunning;
}

function updateStats(rows) {
  if (!rows || rows.length === 0) {
    statsPanel.style.display = "none";
    return;
  }
  
  statsPanel.style.display = "block";
  document.getElementById("statTotal").textContent = rows.length;
  document.getElementById("statWithEmail").textContent = rows.filter(r => r.email).length;
  document.getElementById("statWithWhatsapp").textContent = rows.filter(r => r.whatsapp).length;
  document.getElementById("statWithInstagram").textContent = rows.filter(r => r.instagram).length;
  document.getElementById("statWithFacebook").textContent = rows.filter(r => r.facebook).length;
  document.getElementById("statWithWebsite").textContent = rows.filter(r => r.has_website === "Yes").length;
  
  // Handle both verified count and high quality count
  const verifiedEl = document.getElementById("statVerified");
  const highQualityEl = document.getElementById("statHighQuality");
  
  if (verifiedEl) {
    const verifiedCount = rows.filter(r => r.verified === true || r.verification_score > 50).length;
    verifiedEl.textContent = verifiedCount;
  }
  if (highQualityEl) {
    highQualityEl.textContent = rows.filter(r => r.quality_score === "high").length;
  }
}

function truncate(str, maxLen) {
  if (!str) return "";
  return str.length > maxLen ? str.substring(0, maxLen) + "..." : str;
}

function renderRows(rows) {
  bodyEl.innerHTML = "";
  updateStats(rows);

  if (!rows || rows.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = "<td colspan='11'>No results yet.</td>";
    bodyEl.appendChild(tr);
    return;
  }

  rows.forEach((row) => {
    const tr = document.createElement("tr");

    // Address cell (truncated)
    const addressCell = row.address ? `<span title="${row.address}">${truncate(row.address, 30)}</span>` : "—";

    // Website cell with link
    const websiteCell = row.website
      ? `<a href="${row.website}" target="_blank" rel="noopener noreferrer" title="${row.website}">🔗 Visit</a>`
      : "❌";

    // Instagram with link
    const instagramCell = row.instagram
      ? `<a href="${row.instagram}" target="_blank" title="Instagram">📸 View</a>`
      : "—";

    // Facebook with link
    const facebookCell = row.facebook
      ? `<a href="${row.facebook}" target="_blank" title="Facebook">👤 View</a>`
      : "—";

    // Rating display
    const ratingCell = row.rating ? `⭐ ${row.rating}` : "—";

    // Quality badge
    const qualityClass = row.quality_score === "high" ? "quality-high" : row.quality_score === "medium" ? "quality-medium" : "quality-low";
    const qualityCell = `<span class="quality ${qualityClass}">${(row.quality_score || "?").toUpperCase()}</span>`;

    const whatsappCell = row.whatsapp || "—";

    const waMeLinks = (row.whatsapp_wa_me_links || "").split(";").map((entry) => entry.trim()).filter(Boolean);
    const whatsappLinkCell = waMeLinks.length > 0
      ? waMeLinks
          .map((link) => `<a href="${link}" target="_blank" rel="noopener noreferrer" title="Open WhatsApp">Open</a>`)
          .join(" | ")
      : (row.whatsapp
          ? `<a href="https://wa.me/${row.whatsapp.replace(/[^0-9]/g, "")}" target="_blank" rel="noopener noreferrer" title="Open WhatsApp">Open</a>`
          : "—");

    tr.innerHTML = `
      <td title="${row.name || ''}">${truncate(row.name, 25) || "—"}</td>
      <td>${addressCell}</td>
      <td>${row.phone || "—"}</td>
      <td>${whatsappCell}</td>
      <td>${whatsappLinkCell}</td>
      <td>${row.email || "—"}</td>
      <td>${websiteCell}</td>
      <td>${instagramCell}</td>
      <td>${facebookCell}</td>
      <td>${ratingCell}</td>
      <td>${qualityCell}</td>
    `;

    bodyEl.appendChild(tr);
  });
}

function normalizeMaxResults(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 50;
  }
  return Math.min(MAX_RESULTS_LIMIT, Math.max(1, Math.trunc(numeric)));
}

async function fetchStatus() {
  try {
    const res = await fetch("/status");
    const data = await res.json();
    if (data?.message) {
      setStatus(data.message);
    }
  } catch {
    // Keep existing UI status if polling fails.
  }
}

function startPolling() {
  stopPolling();
  pollingId = setInterval(fetchStatus, 2000);
}

function stopPolling() {
  if (pollingId) {
    clearInterval(pollingId);
    pollingId = null;
  }
}

startBtn.addEventListener("click", async () => {
  const keyword = document.getElementById("keyword").value.trim();
  const location = document.getElementById("location").value.trim();
  const maxResultsInput = document.getElementById("maxResults");
  const maxResults = normalizeMaxResults(maxResultsInput.value || 50);
  const websiteFilter = document.getElementById("websiteFilter").value || "all";
  const extractionMode = document.getElementById("extractionMode").value || "deep";
  const headless = document.getElementById("headless").checked;
  const deepSearch = document.getElementById("deepSearch").checked;
  const verifySocials = document.getElementById("verifySocials")?.checked ?? true;
  const skipDuplicates = document.getElementById("skipDuplicates")?.checked ?? true;
  const chosenHistoryFiles = getSelectedHistoryFiles();

  maxResultsInput.value = String(maxResults);

  if (!keyword || !location) {
    setStatus("Keyword and location are required.");
    return;
  }

  // Update status based on mode
  const modeNames = {
    ultra: "🚀 Ultra Deep: ALL engines + Cross-verification",
    deep: "🔍 Deep: Maps → Website → Google Search",
    enhanced: "⚙️ Enhanced: Maps + Website analysis",
    basic: "⚡ Basic: Maps only (fast)"
  };
  
  let statusMsg = modeNames[extractionMode] || "Scraping...";
  if (skipDuplicates) {
    statusMsg += " (skipping previous results)";
  }
  if (chosenHistoryFiles.length > 0) {
    statusMsg += ` + ${chosenHistoryFiles.length} selected history file(s)`;
  }
  
  setRunningState(true);
  downloadBtn.disabled = true;
  renderRows([]);
  setStatus(statusMsg);
  startPolling();

  try {
    const res = await fetch("/scrape", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        keyword,
        location,
        max_results: maxResults,
        website_filter: websiteFilter,
        extraction_mode: extractionMode,
        deep_search: deepSearch,
        verify_socials: verifySocials,
        skip_duplicates: skipDuplicates,
        selected_history_files: chosenHistoryFiles,
        headless,
      }),
    });

    const data = await res.json();
    if (!res.ok) {
      setStatus(data.error || "Scraping failed.");
      return;
    }

    renderRows(data.results || []);
    setStatus(data.message || `Completed. ${data.count || 0} NEW leads collected.`);
    downloadBtn.disabled = !(data.count > 0);
    
    // Refresh history stats after scraping
    fetchHistoryStats();
    fetchOutputHistoryFiles();
  } catch (err) {
    setStatus("Network error while scraping. Check backend logs.");
  } finally {
    setRunningState(false);
    stopPolling();
  }
});

stopBtn.addEventListener("click", async () => {
  try {
    const res = await fetch("/stop", { method: "POST" });
    const data = await res.json();
    setStatus(data.message || "Stop requested.");
  } catch {
    setStatus("Could not send stop request.");
  }
});

downloadBtn.addEventListener("click", () => {
  window.location.href = "/download";
});

renderRows([]);
fetchOutputHistoryFiles();
