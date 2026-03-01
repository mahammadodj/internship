/* ====================================================================

   DCA Pro – Application JavaScript

   ==================================================================== */



/* ====================================================================

   Tab Switching

   ==================================================================== */

document.querySelectorAll('.tab-btn').forEach(btn => {

  btn.addEventListener('click', () => {

    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));

    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));

    btn.classList.add('active');

    document.getElementById('tab-' + btn.dataset.tab).classList.add('active');

    Object.values(chartInstances).forEach(c => c && c.resize());

    if (btn.dataset.tab === 'editor') loadEditorPage(editorPage);

  });

});



/* ====================================================================

   Theme Toggle

   ==================================================================== */

document.getElementById('themeToggle').addEventListener('click', function () {

  const root = document.documentElement;

  const isDark = !root.hasAttribute('data-theme');

  if (isDark) {

    root.setAttribute('data-theme', 'light');

    this.textContent = '☀️ Light';

  } else {

    root.removeAttribute('data-theme');

    this.textContent = '🌙 Dark';

  }

});



/* ====================================================================

   Global State

   ==================================================================== */

let uploadedColumns = [];

let numericColumns = [];

let allWells = [];

let hasDiskPath = false;

const chartInstances = {};

const cardExclusions = {};

const cardOptions = {};

const cardZoomState = {};

const cardStyles = {};

const cardTitles = {};

const cardLastData = {};

const cardPCurveState = {};  // { enabled, p10Di, p90Di } — decline rates per P-curve

const cardUserLines = {};    // { cardId: [{id, type:'h'|'v', value, color, name}] }
const cardAnnotations = {};  // { cardId: [{id, x, y, xLabel, text, fontSize}] }
const cardValueLabels = {};  // { cardId: 'none'|'top'|'bottom'|'left'|'right' }
const cardLogScale = {};     // { cardId: boolean }

/* Parse date string "DD.MM.YYYY" → JS timestamp */
function parseDateStr(s) {
  const p = s.split('.');
  return new Date(+p[2], p[1] - 1, +p[0]).getTime();
}

/* Format timestamp → "DD.MM.YYYY" */
function formatDateTs(ts) {
  const d = new Date(ts);
  return String(d.getDate()).padStart(2, '0') + '.' + String(d.getMonth() + 1).padStart(2, '0') + '.' + d.getFullYear();
}
const cardPctChange = {};    // { cardId: boolean }
const cardAxisLabels = {};   // { cardId: {x:string, y:string} }
const cardAxisPositions = {}; // { cardId: {x:'bottom'|'top', y:'left'|'right'} }
const cardTableData = {};    // { cardId: [{well, section, time, actual, fitted}] }
const cardTableSort = {};    // { cardId: {col:string, asc:boolean} }
const cardHeaders = {};      // { cardId: string }
let _ctxMenuCardId = null;
let _ctxMenuCoord = null;



let editorPage = 1;

const EDITOR_PAGE_SIZE = 50;

let editorState = { sortCol: null, sortAsc: true, filterCol: null, filterVal: '' };

let previewState = { data: [], columns: [], sortCol: null, sortAsc: true, filterCol: null, filterVal: '' };

let activeDatasetId = null;

const CHUNK_SIZE = 5 * 1024 * 1024; // 5 MB



// --- Sync/Pipeline state ---

let storedFileHandle = null;      // File System Access API handle

let syncMode = 'manual';          // 'manual' | 'auto' | 'scheduled'

let syncScheduleTimer = null;     // setInterval id for scheduled mode

let autoSyncTimer = null;         // setInterval id for auto (on-change) mode

let lastKnownModified = 0;        // lastModified timestamp of file

let lastImportTimestamp = null;    // ISO string from server

let currentVersion = 0;           // current version number

let isSyncing = false;            // prevent concurrent syncs



/* ====================================================================

   IndexedDB – File Handle Persistence

   ==================================================================== */

const IDB_NAME = 'DCAProSync';

const IDB_STORE = 'fileHandles';

const IDB_WS_STORE = 'workspace';

const IDB_VERSION = 2;



function openIDB() {

  return new Promise((resolve, reject) => {

    const req = indexedDB.open(IDB_NAME, IDB_VERSION);

    req.onupgradeneeded = () => {

      const db = req.result;

      if (!db.objectStoreNames.contains(IDB_STORE)) {

        db.createObjectStore(IDB_STORE, { keyPath: 'id' });

      }

      if (!db.objectStoreNames.contains(IDB_WS_STORE)) {

        db.createObjectStore(IDB_WS_STORE, { keyPath: 'id' });

      }

    };

    req.onsuccess = () => resolve(req.result);

    req.onerror = () => reject(req.error);

  });

}



async function saveFileHandleToIDB(handle, filename) {

  const db = await openIDB();

  return new Promise((resolve, reject) => {

    const tx = db.transaction(IDB_STORE, 'readwrite');

    tx.objectStore(IDB_STORE).put({ id: 'currentFile', handle, filename, savedAt: Date.now() });

    tx.oncomplete = () => resolve();

    tx.onerror = () => reject(tx.error);

  });

}



async function loadFileHandleFromIDB() {

  const db = await openIDB();

  return new Promise((resolve, reject) => {

    const tx = db.transaction(IDB_STORE, 'readonly');

    const req = tx.objectStore(IDB_STORE).get('currentFile');

    req.onsuccess = () => resolve(req.result || null);

    req.onerror = () => reject(req.error);

  });

}



async function clearFileHandleFromIDB() {

  const db = await openIDB();

  return new Promise((resolve, reject) => {

    const tx = db.transaction(IDB_STORE, 'readwrite');

    tx.objectStore(IDB_STORE).delete('currentFile');

    tx.oncomplete = () => resolve();

    tx.onerror = () => reject(tx.error);

  });

}



/* ====================================================================

   Chunked Upload System

   ==================================================================== */

const uploadZone = document.getElementById('uploadZone');

const fileInput = document.getElementById('fileInput');

['dragenter', 'dragover'].forEach(e => uploadZone.addEventListener(e, ev => { ev.preventDefault(); uploadZone.classList.add('dragover'); }));

['dragleave', 'drop'].forEach(e => uploadZone.addEventListener(e, ev => { ev.preventDefault(); uploadZone.classList.remove('dragover'); }));

uploadZone.addEventListener('drop', e => { if (e.dataTransfer.files.length) { fileInput.files = e.dataTransfer.files; handleFile(); } });



// Use File System Access API (if available) for persistent file handles

fileInput.addEventListener('click', async function (e) {

  if (window.showOpenFilePicker) {

    e.preventDefault(); // prevent default file dialog

    try {

      const [handle] = await window.showOpenFilePicker({

        types: [{ description: 'Data files', accept: { 'text/csv': ['.csv'], 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx', '.xls'] } }],

        multiple: false

      });

      storedFileHandle = handle;

      const file = await handle.getFile();

      lastKnownModified = file.lastModified;

      // Save handle to IndexedDB for persistence across refreshes

      await saveFileHandleToIDB(handle, file.name);

      // Create a synthetic FileList-like flow

      await handleFileWithObj(file);

    } catch (err) {

      if (err.name !== 'AbortError') console.error('File picker error:', err);

    }

  }

});

// Fallback for browsers without File System Access API

fileInput.addEventListener('change', function () {

  if (!window.showOpenFilePicker) handleFile();

});



async function handleFile() {

  const file = fileInput.files[0];

  if (!file) return;

  lastKnownModified = file.lastModified;

  await handleFileWithObj(file);

}



async function handleFileWithObj(file) {

  const info = document.getElementById('fileInfo');

  const progressWrap = document.getElementById('uploadProgressWrap');

  const progressBar = document.getElementById('uploadProgressBar');

  const progressText = document.getElementById('uploadProgressText');



  info.className = 'file-info show';

  info.innerHTML = '<span class="loader"></span> Initializing upload…';

  progressWrap.style.display = 'block';

  progressBar.style.width = '0%';

  progressText.textContent = '0%';



  try {

    // Step 1: Initialize upload — get dataset_id

    const initRes = await fetch('/api/upload/init', {

      method: 'POST',

      headers: { 'Content-Type': 'application/json' },

      body: JSON.stringify({ filename: file.name, file_size: file.size })

    });

    const initData = await initRes.json();

    if (!initRes.ok) throw new Error(initData.detail || 'Init failed');

    const datasetId = initData.dataset_id;

    activeDatasetId = datasetId;

    info.innerHTML = `<span class="loader"></span> Uploading <strong>${file.name}</strong> (${formatBytes(file.size)})…`;



    // Step 2: Upload chunks

    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    for (let i = 0; i < totalChunks; i++) {

      const start = i * CHUNK_SIZE;

      const end = Math.min(start + CHUNK_SIZE, file.size);

      const blob = file.slice(start, end);

      const chunkForm = new FormData();

      chunkForm.append('file', blob, `chunk_${i}`);

      const chunkRes = await fetch(`/api/upload/chunk?dataset_id=${datasetId}&chunk_index=${i}`, {

        method: 'POST', body: chunkForm

      });

      if (!chunkRes.ok) {

        const err = await chunkRes.json();

        throw new Error(err.detail || `Chunk ${i} failed`);

      }

      const pct = Math.round(((i + 1) / totalChunks) * 100);

      progressBar.style.width = pct + '%';

      progressText.textContent = pct + '%';

    }



    // Step 3: Finalize upload — trigger server-side parsing

    info.innerHTML = '<span class="loader"></span> Processing file on server…';

    const finRes = await fetch(`/api/upload/finalize?dataset_id=${datasetId}`, { method: 'POST' });

    if (!finRes.ok) {

      const err = await finRes.json();

      throw new Error(err.detail || 'Finalize failed');

    }



    // Step 4: Poll for processing status

    await pollDatasetStatus(datasetId);



  } catch (err) {

    info.className = 'file-info show error';

    info.innerHTML = `✗ Error: ${err.message}`;

    progressWrap.style.display = 'none';

  }

}



async function pollDatasetStatus(datasetId) {

  const info = document.getElementById('fileInfo');

  const progressBar = document.getElementById('uploadProgressBar');

  const progressText = document.getElementById('uploadProgressText');

  const progressWrap = document.getElementById('uploadProgressWrap');



  return new Promise((resolve, reject) => {

    const interval = setInterval(async () => {

      try {

        const res = await fetch(`/api/dataset/${datasetId}/status`);

        const data = await res.json();

        if (!res.ok) throw new Error(data.detail || 'Status check failed');



        // Update progress bar for processing phase

        const pct = data.progress || 0;

        progressBar.style.width = pct + '%';

        progressText.textContent = pct + '% — ' + (data.status === 'processing' ? 'Converting to Parquet…' : data.status);



        if (data.status === 'ready') {

          clearInterval(interval);

          progressWrap.style.display = 'none';

          applyUploadData(data);

          resolve();

        } else if (data.status === 'error') {

          clearInterval(interval);

          progressWrap.style.display = 'none';

          throw new Error(data.error || 'Processing failed');

        }

      } catch (err) {

        clearInterval(interval);

        info.className = 'file-info show error';

        info.innerHTML = `✗ Error: ${err.message}`;

        progressWrap.style.display = 'none';

        reject(err);

      }

    }, 500); // Poll every 500ms

  });

}



function formatBytes(bytes) {

  if (bytes < 1024) return bytes + ' B';

  if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';

  if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';

  return (bytes / 1073741824).toFixed(2) + ' GB';

}



function applyUploadData(data) {

  const info = document.getElementById('fileInfo');

  info.className = 'file-info show';

  info.innerHTML = `✓ <strong>${data.filename}</strong> — ${data.rows.toLocaleString()} rows × ${data.columns.length} columns` +

    (activeDatasetId ? ` <span style="color:var(--text-dim);font-size:.72rem;margin-left:8px;">ID: ${activeDatasetId}</span>` : '');

  document.getElementById('headerStatus').textContent = `${data.filename} • ${data.rows.toLocaleString()} rows`;

  uploadedColumns = data.columns;

  numericColumns = data.numeric_columns;

  hasDiskPath = data.has_disk_path || false;



  // Track version & timestamp

  lastImportTimestamp = data.last_import || null;

  currentVersion = data.version || 0;

  if (data.dataset_id) activeDatasetId = data.dataset_id;



  // Virtual-scroll preview: store total row count, fetch on demand

  previewState.totalRows = data.rows || 0;

  previewState.columns = data.columns;

  previewState.sortCol = null;

  previewState.filterCol = null;

  previewState.filterVal = '';



  renderPreview();

  populateSelectors();

  // Clear all plot cards

  document.querySelectorAll('.plot-card').forEach(card => removeCard(card.id));




  // Show sync panel (always show after first upload)

  showSyncPanel();

  updateSyncUI();



  // Show replay errors if any

  if (data.replay_errors && data.replay_errors.length > 0) {

    data.replay_errors.forEach(err => {

      showToast(`Pipeline replay failed for column "${err.name}": ${err.error}`, 'error', 6000);

    });

  }

}



/* ====================================================================

   Foundry-style Data Preview — Virtual Scroll + Stats

   ==================================================================== */

const ROW_HEIGHT = 32;          // px per row (matches CSS padding)

const OVERSCAN = 8;            // extra rows rendered above/below viewport

const FETCH_BATCH = 100;        // rows fetched per server request

let vsTotalRows = 0;

let vsColumns = [];

let vsCache = {};               // offset -> row-data array (sparse cache)

let vsFetching = new Set();     // offsets currently being fetched

let vsCurrentView = 'table';    // 'table' | 'stats'

let vsSearchQuery = '';

let vsStatsData = null;

let vsColTypes = {};             // column -> detected type hint



function renderPreview() {

  document.getElementById('previewCard').style.display = 'block';

  // Reset virtual-scroll state

  vsCache = {};

  vsFetching.clear();

  vsTotalRows = previewState.totalRows || 0;

  vsColumns = previewState.columns || [];



  document.getElementById('previewNote').textContent =

    `${vsTotalRows.toLocaleString()} rows · ${vsColumns.length} columns`;

  document.getElementById('foundryRowInfo').textContent = `${vsTotalRows.toLocaleString()} rows`;

  document.getElementById('foundryColInfo').textContent = `${vsColumns.length} columns`;



  // Build header

  buildVScrollHeader();

  // Size spacer for total scrollable height

  const spacer = document.getElementById('vscrollSpacer');

  spacer.style.height = (vsTotalRows * ROW_HEIGHT) + 'px';

  // Reset scroll

  const container = document.getElementById('vscrollContainer');

  container.scrollTop = 0;

  // Render visible rows

  renderVisibleRows();

  // Reset stats

  vsStatsData = null;

  if (vsCurrentView === 'stats') loadStats();

}



function buildVScrollHeader() {

  const head = document.getElementById('vscrollHead');

  let html = '<tr><th class="row-idx-head">#</th>';

  vsColumns.forEach(c => {

    const sortIcon = (previewState.sortCol === c) ? (previewState.sortAsc ? ' ▲' : ' ▼') : '';

    const filterIcon = (previewState.filterCol === c) ? ' 🔍' : '';

    const typeHint = vsColTypes[c] || '';

    html += `<th onclick="handlePreviewSort(event, '${c.replace(/'/g, "\\'")}')" title="${c}">

      ${c}${filterIcon}${sortIcon}

      <span class="col-type">${typeHint}</span>

      <span class="th-menu" onclick="event.stopPropagation(); showHeaderMenu(event, '${c.replace(/'/g, "\\'")}', 'preview')">⋮</span>

    </th>`;

  });

  html += '</tr>';

  head.innerHTML = html;

}



function handlePreviewSort(e, col) {

  if (e.target.closest('.th-menu')) return;

  if (previewState.sortCol === col) previewState.sortAsc = !previewState.sortAsc;

  else { previewState.sortCol = col; previewState.sortAsc = true; }

  vsCache = {};

  vsFetching.clear();

  buildVScrollHeader();

  renderVisibleRows();

}



/* --- Virtual scroll rendering --- */

function renderVisibleRows() {

  const container = document.getElementById('vscrollContainer');

  const scrollTop = container.scrollTop;

  const viewH = container.clientHeight;

  const headerH = document.getElementById('vscrollHead').offsetHeight || 32;



  const firstVisible = Math.max(0, Math.floor((scrollTop - headerH) / ROW_HEIGHT));

  const visibleCount = Math.ceil(viewH / ROW_HEIGHT) + 1;

  const startRow = Math.max(0, firstVisible - OVERSCAN);

  const endRow = Math.min(vsTotalRows, firstVisible + visibleCount + OVERSCAN);



  // Check which batches we need

  const neededBatchStart = Math.floor(startRow / FETCH_BATCH) * FETCH_BATCH;

  const neededBatchEnd = Math.floor((endRow - 1) / FETCH_BATCH) * FETCH_BATCH;

  for (let b = neededBatchStart; b <= neededBatchEnd; b += FETCH_BATCH) {

    if (!vsCache.hasOwnProperty(b) && !vsFetching.has(b)) {

      fetchPreviewBatch(b);

    }

  }



  // Build rows HTML

  const tbody = document.getElementById('vscrollBody');

  let html = '';

  // Spacer row to push content down

  if (startRow > 0) {

    html += `<tr style="height:${startRow * ROW_HEIGHT}px"><td colspan="${vsColumns.length + 1}"></td></tr>`;

  }

  for (let i = startRow; i < endRow; i++) {

    const batchKey = Math.floor(i / FETCH_BATCH) * FETCH_BATCH;

    const batchData = vsCache[batchKey];

    const row = batchData ? batchData[i - batchKey] : null;

    html += '<tr>';

    html += `<td class="row-idx">${i + 1}</td>`;

    if (row) {

      vsColumns.forEach(c => html += `<td>${row[c] ?? ''}</td>`);

    } else {

      vsColumns.forEach(() => html += '<td style="color:var(--text-dim)">…</td>');

    }

    html += '</tr>';

  }

  // Bottom spacer

  const remaining = vsTotalRows - endRow;

  if (remaining > 0) {

    html += `<tr style="height:${remaining * ROW_HEIGHT}px"><td colspan="${vsColumns.length + 1}"></td></tr>`;

  }

  tbody.innerHTML = html;

}



async function fetchPreviewBatch(offset) {

  vsFetching.add(offset);

  try {

    let url = `/api/preview/rows?offset=${offset}&limit=${FETCH_BATCH}`;

    if (previewState.sortCol) url += `&sort_col=${enc(previewState.sortCol)}&sort_asc=${previewState.sortAsc}`;

    if (previewState.filterCol && previewState.filterVal) url += `&filter_col=${enc(previewState.filterCol)}&filter_val=${enc(previewState.filterVal)}`;

    const res = await fetch(url);

    if (!res.ok) return;

    const data = await res.json();

    vsCache[offset] = data.rows;

    vsTotalRows = data.total;

    // Update row count display

    document.getElementById('previewNote').textContent =

      `${vsTotalRows.toLocaleString()} rows · ${vsColumns.length} columns`;

    document.getElementById('foundryRowInfo').textContent = `${vsTotalRows.toLocaleString()} rows`;

    renderVisibleRows();

  } catch (e) {

    console.error('Fetch preview batch error:', e);

  } finally {

    vsFetching.delete(offset);

  }

}



// Scroll handler — throttled

let vsScrollTimer = null;

document.addEventListener('DOMContentLoaded', () => {

  const container = document.getElementById('vscrollContainer');

  if (container) {

    container.addEventListener('scroll', () => {

      if (vsScrollTimer) return;

      vsScrollTimer = requestAnimationFrame(() => {

        vsScrollTimer = null;

        renderVisibleRows();

      });

    });

  }

});



/* --- View toggle --- */

function switchPreviewView(view) {

  vsCurrentView = view;

  document.getElementById('btnViewTable').classList.toggle('active', view === 'table');

  document.getElementById('btnViewStats').classList.toggle('active', view === 'stats');

  document.getElementById('foundryTableView').style.display = view === 'table' ? '' : 'none';

  document.getElementById('foundryStatsView').style.display = view === 'stats' ? '' : 'none';

  if (view === 'stats' && !vsStatsData) loadStats();

}



/* --- Column search --- */

function onPreviewSearch(q) {

  vsSearchQuery = q.toLowerCase().trim();

  if (vsCurrentView === 'stats') {

    renderStatsCards();

  }

}



/* --- Stats view --- */

async function loadStats() {

  const grid = document.getElementById('foundryStatsGrid');

  grid.innerHTML = '<div style="padding:20px;color:var(--text-dim);"><span class="loader"></span> Computing column statistics…</div>';

  try {

    const res = await fetch('/api/preview/stats');

    if (!res.ok) throw new Error('Failed to load stats');

    vsStatsData = await res.json();

    // Extract column type hints

    vsColTypes = {};

    vsStatsData.columns.forEach(c => { vsColTypes[c.column] = c.type; });

    buildVScrollHeader(); // refresh type badges

    renderStatsCards();

  } catch (e) {

    grid.innerHTML = `<div style="padding:20px;color:var(--red);">Error: ${e.message}</div>`;

  }

}



function renderStatsCards() {

  const grid = document.getElementById('foundryStatsGrid');

  if (!vsStatsData) return;

  let cols = vsStatsData.columns;

  if (vsSearchQuery) {

    cols = cols.filter(c => c.column.toLowerCase().includes(vsSearchQuery));

  }

  if (cols.length === 0) {

    grid.innerHTML = '<div style="padding:20px;color:var(--text-dim);">No matching columns</div>';

    return;

  }

  let html = '';

  cols.forEach(c => {

    html += `<div class="foundry-stat-card">`;

    html += `<div class="stat-col-name">${c.column} <span class="stat-col-type">${c.type}</span></div>`;

    // Null bar

    const nullPct = c.total > 0 ? ((c.null_count / c.total) * 100) : 0;

    html += `<div class="stat-null-bar"><div class="stat-null-fill" style="width:${100 - nullPct}%"></div></div>`;

    html += `<div class="stat-summary">`;

    html += `<div><span class="stat-label">Non-null</span><br><span class="stat-value">${(c.non_null || 0).toLocaleString()}</span></div>`;

    html += `<div><span class="stat-label">Unique</span><br><span class="stat-value">${(c.unique || 0).toLocaleString()}</span></div>`;

    if (c.type === 'numeric') {

      html += `<div><span class="stat-label">Min</span><br><span class="stat-value">${fmtNum(c.min)}</span></div>`;

      html += `<div><span class="stat-label">Max</span><br><span class="stat-value">${fmtNum(c.max)}</span></div>`;

      html += `<div><span class="stat-label">Mean</span><br><span class="stat-value">${fmtNum(c.mean)}</span></div>`;

      html += `<div><span class="stat-label">Std</span><br><span class="stat-value">${fmtNum(c.std)}</span></div>`;

      html += `<div><span class="stat-label">Median</span><br><span class="stat-value">${fmtNum(c.median)}</span></div>`;

      html += `<div><span class="stat-label">Nulls</span><br><span class="stat-value">${(c.null_count || 0).toLocaleString()}</span></div>`;

    } else if (c.type === 'datetime') {

      html += `<div><span class="stat-label">Min</span><br><span class="stat-value">${c.min || '—'}</span></div>`;

      html += `<div><span class="stat-label">Max</span><br><span class="stat-value">${c.max || '—'}</span></div>`;

    }

    html += `</div>`;

    // Histogram for numeric

    if (c.type === 'numeric' && c.histogram) {

      html += `<div class="stat-bar-wrap"><canvas id="hist_${c.column.replace(/\W/g, '_')}"></canvas></div>`;

    }

    // Top values for string

    if (c.type === 'string' && c.top_values && c.top_values.length) {

      html += `<div class="stat-top-values">`;

      c.top_values.slice(0, 6).forEach(tv => {

        html += `<div class="stat-top-row"><span class="stv-name">${tv.value}</span><span class="stv-count">${tv.count.toLocaleString()}</span></div>`;

      });

      html += `</div>`;

    }

    html += `</div>`;

  });

  grid.innerHTML = html;

  // Draw histograms

  cols.forEach(c => {

    if (c.type === 'numeric' && c.histogram) {

      drawHistogram(`hist_${c.column.replace(/\W/g, '_')}`, c.histogram);

    }

  });

}



function fmtNum(v) {

  if (v === null || v === undefined) return '—';

  if (Math.abs(v) >= 1e6) return v.toExponential(2);

  if (Number.isInteger(v)) return v.toLocaleString();

  return v.toLocaleString(undefined, { maximumFractionDigits: 4 });

}



function drawHistogram(canvasId, hist) {

  const canvas = document.getElementById(canvasId);

  if (!canvas) return;

  const ctx = canvas.getContext('2d');

  const dpr = window.devicePixelRatio || 1;

  const w = canvas.parentElement.clientWidth;

  const h = 48;

  canvas.width = w * dpr;

  canvas.height = h * dpr;

  canvas.style.width = w + 'px';

  canvas.style.height = h + 'px';

  ctx.scale(dpr, dpr);

  const counts = hist.counts;

  const maxCount = Math.max(...counts, 1);

  const barW = w / counts.length;

  const accentColor = getComputedStyle(document.documentElement).getPropertyValue('--accent').trim() || '#f59e0b';

  counts.forEach((count, i) => {

    const barH = (count / maxCount) * (h - 4);

    ctx.fillStyle = accentColor;

    ctx.globalAlpha = 0.3 + (count / maxCount) * 0.7;

    ctx.fillRect(i * barW + 1, h - barH, barW - 2, barH);

  });

  ctx.globalAlpha = 1;

}



/* ====================================================================

   Selectors

   ==================================================================== */

function populateSelectors() {

  const selX = document.getElementById('selX');

  const selY = document.getElementById('selY');

  const selWellCol = document.getElementById('selWellCol');

  selX.innerHTML = '<option value="">— select —</option>';

  selY.innerHTML = '<option value="">— select —</option>';

  selWellCol.innerHTML = '<option value="">— select —</option>';

  [...numericColumns, ...uploadedColumns.filter(c => !numericColumns.includes(c))].forEach(c => selX.innerHTML += `<option value="${c}">${c}</option>`);

  numericColumns.forEach(c => selY.innerHTML += `<option value="${c}">${c}</option>`);

  const catCols = uploadedColumns.filter(c => !numericColumns.includes(c));

  [...catCols, ...numericColumns].forEach(c => selWellCol.innerHTML += `<option value="${c}">${c}</option>`);

  selWellCol.onchange = fetchWells;

}



async function fetchWells() {

  const wellCol = document.getElementById('selWellCol').value;

  if (!wellCol) { allWells = []; return; }

  const res = await fetch(`/api/wells?well_col=${encodeURIComponent(wellCol)}`);

  const data = await res.json();

  allWells = data.wells || [];

}



/* ====================================================================

   Pages  (formerly "Workspaces")

   ==================================================================== */

const pages = [];

let activePageId = null;



function initPages() {

  if (pages.length === 0) addPage('Page 1');

}



function addPage(name) {

  const id = 'page-' + Date.now();

  pages.push({ id, name });

  renderPageTabs();

  switchPage(id);

  return id;

}



function addPagePrompt() {

  showPageNameModal('New Page', 'Page ' + (pages.length + 1), (name) => {

    if (name && name.trim()) addPage(name.trim());

  });

}



function renamePage(pageId) {

  const pg = pages.find(p => p.id === pageId);

  if (!pg) return;

  showPageNameModal('Rename Page', pg.name, (name) => {

    if (name && name.trim()) { pg.name = name.trim(); renderPageTabs(); }

  });

}



function showPageNameModal(title, defaultVal, onConfirm) {

  const mc = document.getElementById('modalContainer');

  mc.innerHTML = `

    <div class="modal-overlay" onclick="if(event.target===this)this.remove()">

      <div class="modal">

        <h3>${title}</h3>

        <div class="form-group"><label>Page Name</label><input type="text" id="pageNameInput" value="${defaultVal}" placeholder="Enter page name"></div>

        <div class="modal-actions">

          <button class="btn btn-outline btn-sm" onclick="this.closest('.modal-overlay').remove()">Cancel</button>

          <button class="btn btn-sm" id="pageNameConfirmBtn">OK</button>

        </div>

      </div>

    </div>`;

  const inp = document.getElementById('pageNameInput');

  const confirmBtn = document.getElementById('pageNameConfirmBtn');

  setTimeout(() => { inp.focus(); inp.select(); }, 50);

  const doConfirm = () => { const v = inp.value; mc.innerHTML = ''; onConfirm(v); };

  confirmBtn.onclick = doConfirm;

  inp.addEventListener('keydown', e => { if (e.key === 'Enter') doConfirm(); });

}



function deletePage(pageId) {

  if (pages.length <= 1) { alert('Cannot delete the last page.'); return; }

  if (!confirm('Delete this page and all its plots?')) return;

  document.querySelectorAll(`.plot-card[data-page="${pageId}"]`).forEach(card => removeCard(card.id));

  const idx = pages.findIndex(p => p.id === pageId);

  if (idx >= 0) pages.splice(idx, 1);

  if (activePageId === pageId) switchPage(pages[0].id);

  renderPageTabs();

}



function switchPage(pageId) {

  activePageId = pageId;

  document.querySelectorAll('.plot-card').forEach(card => {

    card.style.display = card.dataset.page === pageId ? '' : 'none';

  });

  renderPageTabs();

  setTimeout(() => {

    document.querySelectorAll(`.plot-card[data-page="${pageId}"]`).forEach(card => {

      const ch = chartInstances[card.id]; if (ch) ch.resize();

    });

  }, 50);

}



function renderPageTabs() {

  const tabs = document.getElementById('pageTabs');

  tabs.innerHTML = '';

  pages.forEach(pg => {

    const btn = document.createElement('button');

    btn.className = 'page-tab' + (pg.id === activePageId ? ' active' : '');

    const nameSpan = document.createElement('span');

    nameSpan.textContent = pg.name;

    nameSpan.addEventListener('dblclick', function (e) {

      e.stopPropagation();

      const input = document.createElement('input');

      input.type = 'text';

      input.className = 'page-rename-input';

      input.value = pg.name;

      input.style.width = Math.max(60, pg.name.length * 9) + 'px';

      nameSpan.replaceWith(input);

      input.focus();

      input.select();

      const finishRename = () => {

        const newName = input.value.trim();

        if (newName && newName !== pg.name) {

          pg.name = newName;

          if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();

        }

        renderPageTabs();

      };

      input.addEventListener('blur', finishRename);

      input.addEventListener('keydown', (ev) => {

        if (ev.key === 'Enter') { ev.preventDefault(); input.blur(); }

        if (ev.key === 'Escape') { input.value = pg.name; input.blur(); }

      });

    });

    btn.appendChild(nameSpan);

    if (pages.length > 1) {

      const closeSpan = document.createElement('span');

      closeSpan.className = 'page-close';

      closeSpan.textContent = '×';

      closeSpan.onclick = (e) => { e.stopPropagation(); deletePage(pg.id); };

      btn.appendChild(closeSpan);

    }

    btn.onclick = () => switchPage(pg.id);

    tabs.appendChild(btn);

  });

}



initPages();



/* ====================================================================

   Dynamic Plots

   ==================================================================== */

const container = document.getElementById('plotsContainer');



function addPlotCardToActive() {

  if (!document.getElementById('selX').value || !document.getElementById('selY').value || !document.getElementById('selWellCol').value) {

    alert('Please select Time, Rate, and Well columns first.');

    return;

  }

  addPlotCard();

}



function addPlotCard(presetWell, presetModel, presetForecast, presetTitle, presetCombine, presetHeader, presetCombineAgg) {

  const cardId = 'card-' + Date.now() + '-' + Math.floor(Math.random() * 10000);

  const card = document.createElement('div');

  card.className = 'plot-card';

  card.id = cardId;

  card.dataset.page = activePageId;

  cardExclusions[cardId] = new Set();

  cardStyles[cardId] = getDefaultStyles();

  cardTitles[cardId] = presetTitle || '';

  cardHeaders[cardId] = presetHeader || '';

  card._presetCombine = presetCombine || false;

  card._presetCombineAgg = presetCombineAgg || 'sum';



  const presetWells = Array.isArray(presetWell) ? presetWell : (presetWell ? [presetWell] : []);



  card.innerHTML = `

    <button class="plot-remove" onclick="removeCard('${cardId}')">×</button>

    <div class="plot-card-header">
      <input type="text" class="plot-card-header-input p-header" placeholder="Click to add section header…" value="${presetHeader || ''}">
    </div>

    <div class="plot-header">

      <div class="control-group"><label>Well(s)</label>

        <div class="well-picker" id="wp-${cardId}">

          <div class="well-picker-display" onclick="toggleWellPicker('${cardId}')">

            <span class="well-picker-text">Select wells…</span>

            <span class="well-picker-arrow">▾</span>

          </div>

          <div class="well-picker-dropdown">

            <input type="text" class="well-picker-search" placeholder="Search wells…" oninput="filterWells('${cardId}', this.value)">

            <label class="well-picker-combine"><input type="checkbox" class="p-combine"> Combine</label>

            <div class="well-picker-agg-row">
              <span>Aggregate</span>
              <select class="p-combine-agg">
                <option value="sum">Sum</option>
                <option value="mean">Average</option>
                <option value="median">Median</option>
                <option value="min">Min</option>
                <option value="max">Max</option>
              </select>
            </div>

            <div class="well-picker-list"></div>

          </div>

        </div>

      </div>

      <div class="control-group"><label>Model</label>

        <select class="p-model">

          <option value="exponential" ${(presetModel || '') === 'exponential' ? 'selected' : ''}>Exponential</option>

          <option value="hyperbolic" ${presetModel === 'hyperbolic' ? 'selected' : ''}>Hyperbolic</option>

          <option value="harmonic" ${presetModel === 'harmonic' ? 'selected' : ''}>Harmonic</option>

        </select>

      </div>

      <div class="control-group"><label>Forecast (months)</label><input type="number" class="p-forecast" value="${presetForecast || 0}" min="0" style="width:100px;"></div>

      <div class="control-group"><label>X Labels</label><input type="number" class="p-xlabels" value="8" min="2" max="50" style="width:70px;"></div>

      <div class="control-group"><label>Title</label><input type="text" class="p-title" value="${presetTitle || ''}" placeholder="Auto (well name)" style="width:150px;"></div>

      <div class="control-group"><label>&nbsp;</label>
        <div style="display:flex;gap:4px;">
          <button class="btn" onclick="runSingleDCA('${cardId}')" style="flex:1;">Plot</button>
          <button class="btn btn-outline" title="Full View" onclick="openFullView('${cardId}')" style="padding:0 8px;">⛶</button>
        </div>
      </div>

    </div>

    <div class="style-panel" id="style-${cardId}">
      <div class="style-row">
        <span class="style-label">Plot Theme</span>
        <select class="s-plot-theme">
          <option value="classic">Classic</option>
          <option value="ocean">Ocean</option>
          <option value="sunset">Sunset</option>
          <option value="forest">Forest</option>
          <option value="mono">Mono</option>
          <option value="custom">Custom</option>
        </select>
      </div>
      <hr style="border:none;border-top:1px solid var(--border);margin:4px 0;">

      <div class="style-row">

        <span class="style-label">Actual Pts</span>

        <input type="color" class="s-actual-color" value="#3b82f6">

        <select class="s-actual-symbol"><option value="circle">● Circle</option><option value="diamond">◆ Diamond</option><option value="rect">■ Square</option><option value="triangle">▲ Triangle</option></select>

        <input type="range" class="s-actual-size" min="2" max="20" value="6" title="Size">

        <span class="s-actual-size-val" style="font-size:.7rem;color:var(--text-dim);width:20px;">6</span>

      </div>

      <div class="style-row">

        <span class="style-label">Fitted Curve</span>

        <input type="color" class="s-fitted-color" value="#f59e0b">

        <select class="s-fitted-style"><option value="solid">Solid</option><option value="dashed">Dashed</option><option value="dotted">Dotted</option></select>

        <input type="range" class="s-fitted-width" min="1" max="6" value="2" title="Width">

        <span class="s-fitted-width-val" style="font-size:.7rem;color:var(--text-dim);width:20px;">2</span>

      </div>

      <div class="style-row">

        <span class="style-label">Fitted Markers</span>

        <label style="font-size:.78rem;display:flex;align-items:center;gap:4px;cursor:pointer;"><input type="checkbox" class="s-fitted-markers"> Show</label>

        <select class="s-fitted-symbol"><option value="circle">● Circle</option><option value="diamond">◆ Diamond</option><option value="rect">■ Square</option><option value="triangle" selected>▲ Triangle</option></select>

        <input type="range" class="s-fitted-msize" min="2" max="20" value="6" title="Size">

        <span class="s-fitted-msize-val" style="font-size:.7rem;color:var(--text-dim);width:20px;">6</span>

      </div>

      <div class="style-row">

        <span class="style-label">Forecast</span>

        <input type="color" class="s-forecast-color" value="#22c55e">

        <select class="s-forecast-style"><option value="solid">Solid</option><option value="dashed" selected>Dashed</option><option value="dotted">Dotted</option></select>

        <input type="range" class="s-forecast-width" min="1" max="6" value="2" title="Width">

        <span class="s-forecast-width-val" style="font-size:.7rem;color:var(--text-dim);width:20px;">2</span>

      </div>

      <div class="style-row">

        <span class="style-label">Fcst Markers</span>

        <label style="font-size:.78rem;display:flex;align-items:center;gap:4px;cursor:pointer;"><input type="checkbox" class="s-forecast-markers"> Show</label>

        <select class="s-forecast-symbol"><option value="circle">● Circle</option><option value="diamond">◆ Diamond</option><option value="rect">■ Square</option><option value="triangle">▲ Triangle</option></select>

        <input type="range" class="s-forecast-msize" min="2" max="20" value="8" title="Size">

        <span class="s-forecast-msize-val" style="font-size:.7rem;color:var(--text-dim);width:20px;">8</span>

      </div>
      <hr style="border:none;border-top:1px solid var(--border);margin:4px 0;">
      <div class="style-row">
        <span class="style-label">P10 Curve</span>
        <input type="color" class="s-p10-color" value="#22c55e">
        <label style="font-size:.78rem;display:flex;align-items:center;gap:4px;cursor:pointer;"><input type="checkbox" class="s-p10-line" checked> Line</label>
        <label style="font-size:.78rem;display:flex;align-items:center;gap:4px;cursor:pointer;"><input type="checkbox" class="s-p10-marker"> Markers</label>
      </div>
      <div class="style-row">
        <span class="style-label">P90 Curve</span>
        <input type="color" class="s-p90-color" value="#ef4444">
        <label style="font-size:.78rem;display:flex;align-items:center;gap:4px;cursor:pointer;"><input type="checkbox" class="s-p90-line" checked> Line</label>
        <label style="font-size:.78rem;display:flex;align-items:center;gap:4px;cursor:pointer;"><input type="checkbox" class="s-p90-marker"> Markers</label>
      </div>
      <hr style="border:none;border-top:1px solid var(--border);margin:4px 0;">
      <div class="style-row">
        <span class="style-label">Gridlines</span>
        <label style="font-size:.78rem;display:flex;align-items:center;gap:4px;cursor:pointer;"><input type="checkbox" class="s-grid-x" checked> Horizontal</label>
        <label style="font-size:.78rem;display:flex;align-items:center;gap:4px;cursor:pointer;"><input type="checkbox" class="s-grid-y" checked> Vertical</label>
      </div>
      <hr style="border:none;border-top:1px solid var(--border);margin:4px 0;">
      <div class="style-row">
        <span class="style-label">Section Header</span>
        <input type="color" class="s-header-color" value="#334155" title="Color">
        <input type="range" class="s-header-fsize" min="0.8" max="4.0" step="0.1" value="1.8" title="Size">
        <span class="s-header-fsize-val" style="font-size:.7rem;color:var(--text-dim);width:20px;">1.8</span>
      </div>
      <div class="style-row">
        <span class="style-label">&nbsp;</span>
        <label style="font-size:.78rem;display:flex;align-items:center;gap:4px;cursor:pointer;"><input type="checkbox" class="s-header-bold"> Bold</label>
        <select class="s-header-align" style="margin-left:8px;">
          <option value="left">Left</option>
          <option value="center">Center</option>
          <option value="right">Right</option>
        </select>
      </div>
    </div>

    <div class="chart-area" style="display:none;">

      <div class="chart-toolbar">

        <button onclick="toggleStylePanel('${cardId}')" title="Customize Style">🎨 Style</button>

        <button id="pcurveBtn-${cardId}" onclick="togglePCurves('${cardId}')" title="Toggle P10/P50/P90 uncertainty curves" style="position:relative;">📊 P10/P90</button>

        <button onclick="downloadChart('${cardId}','png')" title="Download PNG">PNG</button>

        <button onclick="downloadChart('${cardId}','jpg')" title="Download JPG">JPG</button>

        <button onclick="downloadChart('${cardId}','pdf')" title="Download PDF">PDF</button>

        <button class="reset-zoom-btn" id="resetZoom-${cardId}" onclick="resetZoom('${cardId}')">Reset Zoom</button>

        <button class="reset-all-btn" id="resetAll-${cardId}" onclick="resetAll('${cardId}')">⟲ Reset All</button>

      </div>

      <div class="mini-chart" id="chart-${cardId}"></div>

      <div class="formula-display" id="formula-${cardId}" style="display:none;"></div>

      <div class="param-display" id="params-${cardId}"></div>

      <div class="dca-stats-summary" id="dcaStats-${cardId}" style="display:none;"></div>

      <div class="excl-hint" id="exclHint-${cardId}">Click scatter points to exclude them from curve fitting</div>

      <div id="forecastTableSection-${cardId}" style="display:none;">
        <div class="data-table-toolbar">
          <span class="dt-title">📊 Data Table</span>
          <div style="display:flex;gap:6px;">
            <button onclick="downloadTableCSV('${cardId}')" title="Download as CSV">↓ CSV</button>
          </div>
        </div>
        <div class="table-wrap" id="forecastTable-${cardId}" style="max-height:250px; border:1px solid var(--border);"></div>
      </div>

    </div>

  `;

  container.appendChild(card);



  // Populate the custom well picker and set combine checkbox

  populateWellPicker(cardId, presetWells);

  if (card._presetCombine) {

    const combCb = card.querySelector('.p-combine');

    if (combCb) combCb.checked = true;

  }

  const combAggSel = card.querySelector('.p-combine-agg');
  if (combAggSel) {
    combAggSel.value = card._presetCombineAgg || 'sum';
    const combCb = card.querySelector('.p-combine');
    combAggSel.disabled = !(combCb && combCb.checked);
  }

  const combCb = card.querySelector('.p-combine');
  if (combCb) {
    combCb.addEventListener('change', () => {
      const aggSel = card.querySelector('.p-combine-agg');
      if (aggSel) aggSel.disabled = !combCb.checked;
      _debouncedAutoSave();
    });
  }

  if (combAggSel) {
    combAggSel.addEventListener('change', _debouncedAutoSave);
  }



  // Wire up range label displays AND live style re-render

  const liveStyleRerender = () => {
    const st = readCardStyles(cardId);
    cardStyles[cardId] = st;

    const headerEl = card.querySelector('.p-header');
    if (headerEl) {
      headerEl.style.fontSize = st.headerFontSize + 'rem';
      headerEl.style.color = st.headerColor;
      headerEl.style.fontWeight = st.headerFontWeight || 'normal';
      headerEl.style.textAlign = st.headerTextAlign || 'left';
    }
    if (chartInstances[cardId] && cardLastData[cardId]) {
      saveZoomState(cardId);
      renderSingleChart(cardId, cardLastData[cardId], card.querySelector('.p-forecast')?.value || 0);
    }
  };
  const plotThemeSelect = card.querySelector('.s-plot-theme');

  card.querySelectorAll('input[type="range"]').forEach(r => {

    const valSpan = r.nextElementSibling;

    r.addEventListener('input', () => {

      if (valSpan) valSpan.textContent = r.value;

      liveStyleRerender();

    });

  });

  card.querySelectorAll('.style-panel input[type="color"], .style-panel select, .style-panel input[type="checkbox"]').forEach(el => {

    el.addEventListener('change', () => {
      if (!el.classList.contains('s-plot-theme') && plotThemeSelect && plotThemeSelect.value !== 'custom') {
        plotThemeSelect.value = 'custom';
      }
      liveStyleRerender();
    });

  });

  const titleInput = card.querySelector('.p-title');
  if (titleInput) titleInput.addEventListener('input', liveStyleRerender);

  const xLabelsInput = card.querySelector('.p-xlabels');
  if (xLabelsInput) xLabelsInput.addEventListener('input', liveStyleRerender);

  if (plotThemeSelect) {
    plotThemeSelect.addEventListener('change', () => {
      const selectedTheme = plotThemeSelect.value;
      if (selectedTheme !== 'custom') {
        applyPlotThemePresetToCard(cardId, selectedTheme);
        liveStyleRerender();
      }
    });
  }

  const headerInput = card.querySelector('.p-header');
  if (headerInput) {
    headerInput.addEventListener('input', () => {
      cardHeaders[cardId] = headerInput.value;
      // No need to re-render chart for header text change as it's outside the canvas
    });
  }



  return cardId;

}



function getDefaultStyles() {

  return { plotTheme: 'classic', actualColor: '#3b82f6', actualSymbol: 'circle', actualSize: 6, fittedColor: '#f59e0b', fittedStyle: 'solid', fittedWidth: 2, fittedMarkers: true, fittedSymbol: 'triangle', fittedSymbolSize: 6, forecastColor: '#22c55e', forecastStyle: 'dashed', forecastWidth: 2, forecastMarkers: false, forecastSymbol: 'circle', forecastSymbolSize: 8, p10Color: '#22c55e', p10Line: true, p10Marker: false, p90Color: '#ef4444', p90Line: true, p90Marker: false, gridX: true, gridY: true, headerFontSize: 1.8, headerColor: '#334155', headerFontWeight: 'normal', headerTextAlign: 'left' };
}

const PLOT_THEME_PRESETS = {
  classic: {
    actualColor: '#3b82f6', fittedColor: '#f59e0b', forecastColor: '#22c55e', p10Color: '#22c55e', p90Color: '#ef4444',
    palette: ['#3b82f6', '#f59e0b', '#22c55e', '#ef4444', '#8b5cf6', '#ec4899', '#14b8a6', '#f97316']
  },
  ocean: {
    actualColor: '#2563eb', fittedColor: '#0ea5e9', forecastColor: '#14b8a6', p10Color: '#06b6d4', p90Color: '#0891b2',
    palette: ['#2563eb', '#0ea5e9', '#14b8a6', '#06b6d4', '#0f766e', '#38bdf8', '#1d4ed8', '#67e8f9']
  },
  sunset: {
    actualColor: '#f97316', fittedColor: '#ef4444', forecastColor: '#f59e0b', p10Color: '#fb7185', p90Color: '#dc2626',
    palette: ['#f97316', '#ef4444', '#f59e0b', '#fb7185', '#eab308', '#dc2626', '#f43f5e', '#fdba74']
  },
  forest: {
    actualColor: '#16a34a', fittedColor: '#15803d', forecastColor: '#65a30d', p10Color: '#22c55e', p90Color: '#14532d',
    palette: ['#16a34a', '#15803d', '#65a30d', '#22c55e', '#166534', '#84cc16', '#4d7c0f', '#86efac']
  },
  mono: {
    actualColor: '#64748b', fittedColor: '#334155', forecastColor: '#0f172a', p10Color: '#475569', p90Color: '#1e293b',
    palette: ['#64748b', '#334155', '#0f172a', '#475569', '#1e293b', '#94a3b8', '#0b1220', '#7f8ea3']
  },
};

function getPlotThemePalette(themeName) {
  return (PLOT_THEME_PRESETS[themeName] || PLOT_THEME_PRESETS.classic).palette;
}

function applyPlotThemePresetToCard(cardId, themeName) {
  const card = document.getElementById(cardId);
  const preset = PLOT_THEME_PRESETS[themeName];
  if (!card || !preset) return;
  const setVal = (sel, val) => {
    const el = card.querySelector(sel);
    if (el) el.value = val;
  };
  setVal('.s-actual-color', preset.actualColor);
  setVal('.s-fitted-color', preset.fittedColor);
  setVal('.s-forecast-color', preset.forecastColor);
  setVal('.s-p10-color', preset.p10Color);
  setVal('.s-p90-color', preset.p90Color);
}



function readCardStyles(cardId) {

  const card = document.getElementById(cardId);

  if (!card) return getDefaultStyles();

  return {
    plotTheme: card.querySelector('.s-plot-theme')?.value || 'classic',

    actualColor: card.querySelector('.s-actual-color')?.value || '#3b82f6',

    actualSymbol: card.querySelector('.s-actual-symbol')?.value || 'circle',

    actualSize: parseInt(card.querySelector('.s-actual-size')?.value || '6'),

    fittedColor: card.querySelector('.s-fitted-color')?.value || '#f59e0b',

    fittedStyle: card.querySelector('.s-fitted-style')?.value || 'solid',

    fittedWidth: parseInt(card.querySelector('.s-fitted-width')?.value || '2'),

    fittedMarkers: card.querySelector('.s-fitted-markers')?.checked || false,

    fittedSymbol: card.querySelector('.s-fitted-symbol')?.value || 'triangle',

    fittedSymbolSize: parseInt(card.querySelector('.s-fitted-msize')?.value || '6'),

    forecastColor: card.querySelector('.s-forecast-color')?.value || '#22c55e',

    forecastStyle: card.querySelector('.s-forecast-style')?.value || 'dashed',

    forecastWidth: parseInt(card.querySelector('.s-forecast-width')?.value || '2'),

    forecastMarkers: card.querySelector('.s-forecast-markers')?.checked || false,

    forecastSymbol: card.querySelector('.s-forecast-symbol')?.value || 'circle',

    forecastSymbolSize: parseInt(card.querySelector('.s-forecast-msize')?.value || '8'),

    p10Color: card.querySelector('.s-p10-color')?.value || '#22c55e',

    p10Line: card.querySelector('.s-p10-line')?.checked !== false,

    p10Marker: card.querySelector('.s-p10-marker')?.checked || false,

    p90Color: card.querySelector('.s-p90-color')?.value || '#ef4444',

    p90Line: card.querySelector('.s-p90-line')?.checked !== false,

    p90Marker: card.querySelector('.s-p90-marker')?.checked || false,

    gridX: card.querySelector('.s-grid-x')?.checked !== false,

    gridY: card.querySelector('.s-grid-y')?.checked !== false,

    headerFontSize: parseFloat(card.querySelector('.s-header-fsize')?.value || '1.8'),
    headerColor: card.querySelector('.s-header-color')?.value || '#334155',
    headerFontWeight: card.querySelector('.s-header-bold')?.checked ? 'bold' : 'normal',
    headerTextAlign: card.querySelector('.s-header-align')?.value || 'left',
  };

}



function applyStylesToCard(cardId, styles) {

  const card = document.getElementById(cardId);

  if (!card || !styles) return;
  const merged = { ...getDefaultStyles(), ...styles };

  const s = (sel, val) => {
    if (val === undefined || val === null) return;

    const el = card.querySelector(sel);

    if (el) {

      el.value = val;

      if (el.type === 'range' && el.nextElementSibling) {

        el.nextElementSibling.textContent = val;

      }

    }

  };

  s('.s-plot-theme', merged.plotTheme || 'classic');

  s('.s-actual-color', merged.actualColor);

  s('.s-actual-symbol', merged.actualSymbol);

  s('.s-actual-size', merged.actualSize);

  s('.s-fitted-color', merged.fittedColor);

  s('.s-fitted-style', merged.fittedStyle);

  s('.s-fitted-width', merged.fittedWidth);

  const ftm = card.querySelector('.s-fitted-markers'); if (ftm) ftm.checked = merged.fittedMarkers || false;

  s('.s-fitted-symbol', merged.fittedSymbol);

  s('.s-fitted-msize', merged.fittedSymbolSize);

  s('.s-forecast-color', merged.forecastColor);

  s('.s-forecast-style', merged.forecastStyle);

  s('.s-forecast-width', merged.forecastWidth);

  const fcm = card.querySelector('.s-forecast-markers'); if (fcm) fcm.checked = merged.forecastMarkers || false;

  s('.s-forecast-symbol', merged.forecastSymbol);

  s('.s-forecast-msize', merged.forecastSymbolSize);

  s('.s-p10-color', merged.p10Color || '#22c55e');

  const p10l = card.querySelector('.s-p10-line'); if (p10l) p10l.checked = merged.p10Line !== false;

  const p10m = card.querySelector('.s-p10-marker'); if (p10m) p10m.checked = merged.p10Marker || false;

  s('.s-p90-color', merged.p90Color || '#ef4444');

  const p90l = card.querySelector('.s-p90-line'); if (p90l) p90l.checked = merged.p90Line !== false;

  const p90m = card.querySelector('.s-p90-marker'); if (p90m) p90m.checked = merged.p90Marker || false;

  const gx = card.querySelector('.s-grid-x'); if (gx) gx.checked = merged.gridX !== false;

  const gy = card.querySelector('.s-grid-y'); if (gy) gy.checked = merged.gridY !== false;

  s('.s-header-fsize', merged.headerFontSize || 1.8);
  s('.s-header-color', merged.headerColor || '#334155');
  const hb = card.querySelector('.s-header-bold'); if (hb) hb.checked = merged.headerFontWeight === 'bold';
  s('.s-header-align', merged.headerTextAlign || 'left');
}



function toggleStylePanel(cardId) {

  const panel = document.getElementById('style-' + cardId);

  if (panel) panel.classList.toggle('show');

}



function removeCard(id) {

  if (chartInstances[id]) { chartInstances[id].dispose(); delete chartInstances[id]; }

  delete cardExclusions[id]; delete cardOptions[id]; delete cardZoomState[id];

  delete cardStyles[id]; delete cardTitles[id]; delete cardLastData[id]; delete cardPCurveState[id];

  delete cardUserLines[id]; delete cardAnnotations[id]; delete cardValueLabels[id];

  delete cardLogScale[id]; delete cardPctChange[id]; delete cardAxisLabels[id]; delete cardAxisPositions[id];

  const el = document.getElementById(id);

  if (el) el.remove();

}



/* ====================================================================

   Run DCA

   ==================================================================== */

async function runSingleDCA(cardId) {

  const card = document.getElementById(cardId);

  const selectedWells = getSelectedWells(cardId);

  const well = selectedWells.join(',');

  const model = card.querySelector('.p-model').value;

  const months = card.querySelector('.p-forecast').value;

  const combine = isCombineMode(cardId);

  const combineAgg = getCombineAggMode(cardId);

  if (!well) { alert('Please select at least one well.'); return; }

  const xVal = document.getElementById('selX').value;

  const yVal = document.getElementById('selY').value;

  const wellCol = document.getElementById('selWellCol').value;

  const btn = card.querySelector('.btn');

  btn.innerHTML = '<span class="loader"></span>';

  btn.disabled = true;

  const excl = cardExclusions[cardId] || new Set();

  const exclStr = [...excl].join(',');

  try {

    const url = `/api/dca?x=${enc(xVal)}&y=${enc(yVal)}&well_col=${enc(wellCol)}&wells=${enc(well)}&model=${enc(model)}&forecast_months=${months}&exclude_indices=${enc(exclStr)}&combine=${combine}&combine_func=${enc(combineAgg)}`;

    const res = await fetch(url);

    const data = await res.json();

    if (!res.ok) throw new Error(data.detail || 'Error');

    cardLastData[cardId] = data;

    cardStyles[cardId] = readCardStyles(cardId);

    renderSingleChart(cardId, data, months);

  } catch (err) { alert(err.message); }

  finally { btn.innerHTML = 'Plot'; btn.disabled = false; }

}



function toggleExclusion(cardId, index) {

  if (!cardExclusions[cardId]) cardExclusions[cardId] = new Set();

  const s = cardExclusions[cardId];

  if (s.has(index)) s.delete(index); else s.add(index);

  saveZoomState(cardId);

  runSingleDCA(cardId);

}

/* ====================================================================

   Full View Modal

   ==================================================================== */

function openFullView(cardId) {
  const data = cardLastData[cardId];
  if (!data) {
    alert("Please generate the plot first!");
    return;
  }

  const existingChart = chartInstances[cardId];
  if (!existingChart) {
    alert("Chart instance not found.");
    return;
  }

  const fullId = 'fullChart-' + cardId;
  const cardTitle = document.querySelector(`#${cardId} .p-title`)?.value || 'Full View';

  const modalHtml = `
    <div class="modal-overlay" onclick="if(event.target===this)this.remove()">
      <div class="modal modal-full-view">
        <div class="modal-full-view-header">
          <span class="modal-full-view-title">${cardTitle}</span>
          <div style="display:flex;gap:8px;align-items:center;">
            <button class="btn btn-outline btn-sm reset-zoom-btn" id="resetZoomFull-${cardId}" onclick="resetZoom('${cardId}')">Reset Zoom</button>
            <button class="btn btn-outline btn-sm" onclick="this.closest('.modal-overlay').remove()">Close</button>
          </div>
        </div>
        <div class="modal-full-view-content">
          <div id="${fullId}" class="modal-full-view-chart"></div>
        </div>
      </div>
    </div>
  `;

  document.getElementById('modalContainer').innerHTML = modalHtml;

  requestAnimationFrame(() => {
    const chartDiv = document.getElementById(fullId);
    if (!chartDiv) return;

    const isLight = document.documentElement.getAttribute('data-theme') === 'light';
    const fullChart = echarts.init(chartDiv, isLight ? null : 'dark');
    
    const opts = existingChart.getOption();
    if(opts) {
        // Ensure the chart resizes properly
        opts.grid = opts.grid || {};
        // Reset grid to be responsive or fit the new container
        // Actually, copying options might carry over fixed pixel sizes if they were set.
        // Let's rely on ECharts default responsiveness if 'containLabel' is true.
        
        fullChart.setOption(opts);
    }
    const z = cardZoomState[cardId];
    setResetZoomButtonsVisible(cardId, !!(z && (z.xMin != null || z.xMax != null || z.yMin != null || z.yMax != null)));
    
    // Store full chart instance
    chartInstances[fullId] = fullChart;

    // Attach interactive events (same as main chart)
    const isSingle = data.wells.length === 1;
    if (isSingle) {
      fullChart.on('click', function (params) {
        if (params.componentType === 'markPoint' && params.name && params.name.startsWith('ann_')) {
          const annId = parseInt(params.name.replace('ann_', ''));
          const anns = cardAnnotations[cardId] || [];
          const idx = anns.findIndex(a => a.id === annId);
          if (idx >= 0) showAnnotationEditor(cardId, idx, params.event);
          return;
        }
        if (!params.seriesName.endsWith('Actual') && params.seriesName !== 'Excluded') return;
        let idx = params.data && params.data[2];
        if (idx !== undefined && idx !== null) toggleExclusion(cardId, idx);
      });
    } else {
      fullChart.on('click', function (params) {
        if (params.componentType === 'markPoint' && params.name && params.name.startsWith('ann_')) {
          const annId = parseInt(params.name.replace('ann_', ''));
          const anns = cardAnnotations[cardId] || [];
          const idx = anns.findIndex(a => a.id === annId);
          if (idx >= 0) showAnnotationEditor(cardId, idx, params.event);
        }
      });
    }

    setupLegendColorPicker(cardId, fullChart);
    setupPCurveDragHandles(cardId, fullChart);
    setupQiDragHandle(cardId, fullChart);
    setupUserLineDrag(cardId, fullChart);
    setupAxisDragHandles(cardId, fullChart);
    setupBoxSelection(cardId, fullChart, chartDiv);
    setupAxisHoverTooltip(cardId, chartDiv); // cardId allows axis hover to read same global axis styles
    
    // Resize observer to handle window resize while modal is open
    const resizeObserver = new ResizeObserver(() => fullChart.resize());
    resizeObserver.observe(chartDiv);
    
    // Clean up observer when modal is removed
    const overlay = document.querySelector('.modal-overlay');
    if(overlay) {
        const originalRemove = overlay.remove.bind(overlay);
        overlay.remove = function() {
            resizeObserver.disconnect();
            fullChart.dispose();
            delete chartInstances[fullId]; // Clean up instance
            originalRemove();
        };
    }
  });
}

function saveZoomState(cardId) {

  const chart = chartInstances[cardId];

  if (!chart) return;

  const opt = chart.getOption();

  if (!opt) return;

  const xa = opt.xAxis && opt.xAxis[0];

  const ya = opt.yAxis && opt.yAxis[0];

  cardZoomState[cardId] = { xMin: xa ? xa.min : null, xMax: xa ? xa.max : null, yMin: ya ? ya.min : null, yMax: ya ? ya.max : null };

}



/* ====================================================================

   P10 / P50 / P90 – Physics-based drag (recalculates decline rate D)

   ==================================================================== */



/* ---- Decline model evaluation functions (mirror backend) ---- */

function evalDeclineModel(model, t, params) {

  const qi = params.qi, di = params.di;

  if (model === 'exponential') {

    return qi * Math.exp(-di * t);

  } else if (model === 'hyperbolic') {

    const b = params.b || 0.5;

    const denom = Math.max(1e-12, 1 + b * di * t);

    return qi / Math.pow(denom, 1 / b);

  } else if (model === 'harmonic') {

    return qi / (1 + di * t);

  }

  return qi * Math.exp(-di * t); // fallback to exponential

}



/* Given model, qi (and b for hyperbolic), a known point (tAnchor, qAnchor),

   solve for the decline rate D that passes through that point.

   Keep qi (and b) fixed.  */

function solveForDi(model, qi, b, tAnchor, qAnchor) {

  if (tAnchor <= 0 || qAnchor <= 0 || qi <= 0) return 0.01;

  const ratio = qi / qAnchor;         // always >= 1 for decline

  if (model === 'exponential') {

    // qi * exp(-D*t) = q  =>  D = -ln(q/qi)/t = ln(qi/q)/t

    return Math.max(1e-6, Math.log(ratio) / tAnchor);

  } else if (model === 'hyperbolic') {

    // qi / (1+b*D*t)^(1/b) = q  =>  (1+b*D*t)^(1/b) = qi/q

    // => 1+b*D*t = (qi/q)^b  =>  D = ((qi/q)^b - 1)/(b*t)

    const bSafe = b || 0.5;

    return Math.max(1e-6, (Math.pow(ratio, bSafe) - 1) / (bSafe * tAnchor));

  } else if (model === 'harmonic') {

    // qi / (1+D*t) = q  =>  D = (qi/q - 1)/t

    return Math.max(1e-6, (ratio - 1) / tAnchor);

  }

  return Math.max(1e-6, Math.log(ratio) / tAnchor);

}



/* Evaluate a full curve array at given t values */

function evalCurveArray(model, tArr, params) {

  return tArr.map(t => {

    const v = evalDeclineModel(model, t, params);

    return (isFinite(v) && v > 0) ? v : null;

  });

}



function togglePCurves(cardId) {

  const data = cardLastData[cardId];

  const model = data ? data.model : 'exponential';

  const w = data && data.wells && data.wells[0];

  const baseDi = (w && w.params && w.params.di) ? w.params.di : 0.01;



  if (!cardPCurveState[cardId]) {

    cardPCurveState[cardId] = {

      enabled: false,

      p10Di: baseDi * 0.7,   // lower D → slower decline → higher rates → P10

      p90Di: baseDi * 1.5,   // higher D → faster decline → lower rates → P90

    };

  }
  const ps = cardPCurveState[cardId];
  ps.enabled = !ps.enabled;
  const btn = document.getElementById('pcurveBtn-' + cardId);
  if (btn) {
    btn.style.borderColor = ps.enabled ? 'var(--accent)' : '';
    btn.style.color = ps.enabled ? 'var(--accent)' : '';
  }
  if (cardLastData[cardId]) {
    saveZoomState(cardId);
    renderSingleChart(cardId, cardLastData[cardId], document.getElementById(cardId)?.querySelector('.p-forecast')?.value || 0);
  }
}



function setupPCurveDragHandles(cardId, myChart) {

  const ps = cardPCurveState[cardId];

  if (!ps || !ps.enabled) return;

  const data = cardLastData[cardId];

  if (!data || !data.wells || data.wells.length === 0) return;

  const w = data.wells[0];

  if (!w.y_fitted || !w.params || !w.t) return;

  const model = data.model || 'exponential';

  const qi = w.params.qi;

  const bParam = w.params.b;

  if (!qi) return;



  const isDate = w.is_date || false;

  const zr = myChart.getZr();

  let dragging = null;   // 'p10' | 'p90' | null

  let startPxY = 0;

  let anchorT = 0;       // t value of the anchor (where user clicked)

  let anchorDi = 0;      // starting Di for the dragged curve



  /* Hit-test: is the pixel point close to a P10 or P90 series?

     Also returns the nearest data-point index for the anchor */

  function hitTestSeries(px, py) {

    const THRESHOLD = 10;

    const opt = myChart.getOption();

    if (!opt || !opt.series) return null;

    for (let si = 0; si < opt.series.length; si++) {

      const s = opt.series[si];

      if (!s.name) continue;

      const isP10 = s.name.startsWith('P10');

      const isP90 = s.name.startsWith('P90');

      if (!isP10 && !isP90) continue;

      const sData = s.data;

      if (!sData || sData.length === 0) continue;

      let prevPt = null;

      for (let di = 0; di < sData.length; di++) {

        let val, xIdx;

        if (!Array.isArray(sData[di]) || sData[di][1] == null) { prevPt = null; continue; }

        val = sData[di][1]; xIdx = sData[di][0];

        let ptPx;

        ptPx = myChart.convertToPixel('grid', [xIdx, val]);

        if (!ptPx) { prevPt = null; continue; }

        if (Math.abs(ptPx[0] - px) < THRESHOLD * 3 && Math.abs(ptPx[1] - py) < THRESHOLD) {

          return { which: isP10 ? 'p10' : 'p90', dataIdx: di };

        }

        if (prevPt) {

          const dist = pointToSegmentDist(px, py, prevPt[0], prevPt[1], ptPx[0], ptPx[1]);

          if (dist < THRESHOLD) {

            return { which: isP10 ? 'p10' : 'p90', dataIdx: di };

          }

        }

        prevPt = ptPx;

      }

    }

    return null;

  }



  function pointToSegmentDist(px, py, x1, y1, x2, y2) {

    const dx = x2 - x1, dy = y2 - y1;

    const lenSq = dx * dx + dy * dy;

    if (lenSq === 0) return Math.hypot(px - x1, py - y1);

    let t = ((px - x1) * dx + (py - y1) * dy) / lenSq;

    t = Math.max(0, Math.min(1, t));

    return Math.hypot(px - (x1 + t * dx), py - (y1 + t * dy));

  }



  /* Resolve the t value at a given pixel X position */

  function getTAtPixelX(pxX) {

    /* Use convertFromPixel to get the data-space X, then map to t */

    let dataX;

    if (isDate) {

      const dp = myChart.convertFromPixel('grid', [pxX, 0]);

      if (!dp) return null;

      const tsVal = dp[0]; // timestamp

      /* Find the closest t value by matching date */

      let bestIdx = 0, bestDiff = Infinity;

      for (let i = 0; i < w.x.length; i++) {

        const ts = parseDateStr(w.x[i]);

        const d = Math.abs(ts - tsVal);

        if (d < bestDiff) { bestDiff = d; bestIdx = i; }

      }

      /* Also check forecast dates */

      if (w.forecast && w.forecast.x) {

        for (let i = 0; i < w.forecast.x.length; i++) {

          const ts = parseDateStr(w.forecast.x[i]);

          const d = Math.abs(ts - tsVal);

          if (d < bestDiff) { bestDiff = d; bestIdx = -1; /* use forecast t */ }

        }

        if (bestIdx === -1) {

          /* Re-find in forecast */

          let bfi = 0, bfd = Infinity;

          for (let i = 0; i < w.forecast.x.length; i++) {

            const ts = parseDateStr(w.forecast.x[i]);

            const d = Math.abs(ts - tsVal);

            if (d < bfd) { bfd = d; bfi = i; }

          }

          if (w.forecast.t) return w.forecast.t[bfi];

        }

      }

      return w.t[bestIdx];

    } else {

      const dp = myChart.convertFromPixel('grid', [pxX, 0]);

      if (!dp) return null;

      dataX = dp[0];  // data-space X value

      /* Convert display-X back to t: t = x_display - x_min, and x[0] corresponds to t[0] */

      /* For numeric mode, x_display = x_numeric, t = x_numeric - x_numeric.min() */

      /* Since w.x[0] corresponds to t=0 (approximately), t ≈ dataX - w.x[0] */

      /* But we have the actual t array, so find the closest */

      if (w.t.length === 0) return null;

      const approxT = dataX - w.x[0] + w.t[0];

      return Math.max(0, approxT);

    }

  }



  function onMouseDown(e) {

    const px = e.offsetX, py = e.offsetY;

    const hit = hitTestSeries(px, py);

    if (hit) {

      dragging = hit.which;

      startPxY = py;

      anchorDi = dragging === 'p10' ? ps.p10Di : ps.p90Di;

      /* Determine the t value at the click position */

      const tVal = getTAtPixelX(px);

      anchorT = (tVal != null && tVal > 0) ? tVal : 1;  // avoid t=0 (qi is fixed there)

      myChart.dispatchAction({ type: 'takeGlobalCursor', key: 'dataZoomSelect', dataZoomSelectActive: false });

      e.event && e.event.preventDefault && e.event.preventDefault();

    }

  }



  function onMouseMove(e) {

    if (!dragging) {

      const hit = hitTestSeries(e.offsetX, e.offsetY);

      myChart.getDom().style.cursor = hit ? 'ns-resize' : '';

      return;

    }

    const py = e.offsetY;



    /* Convert current mouse Y to data Y at the anchor's X pixel */

    const anchorXPx = isDate

      ? (() => {

        /* Find pixel X for anchorT — find closest t in the data */

        let bestIdx = 0, bestDiff = Infinity;

        for (let i = 0; i < w.t.length; i++) {

          const d = Math.abs(w.t[i] - anchorT);

          if (d < bestDiff) { bestDiff = d; bestIdx = i; }

        }

        const ts = parseDateStr(w.x[bestIdx]);

        const cp = myChart.convertToPixel('grid', [ts, 0]);

        return cp ? cp[0] : e.offsetX;

      })()

      : (() => {

        const xVal = w.x[0] + anchorT - w.t[0];

        const cp = myChart.convertToPixel('grid', [xVal, 0]);

        return cp ? cp[0] : e.offsetX;

      })();



    let newDataY;

    if (isDate) {

      const dp = myChart.convertFromPixel('grid', [anchorXPx, py]);

      newDataY = dp ? dp[1] : 0;

    } else {

      const dp = myChart.convertFromPixel('grid', [anchorXPx, py]);

      newDataY = dp ? dp[1] : 0;

    }

    if (newDataY <= 0) return;



    /* Solve for new D such that q(anchorT) = newDataY, keeping qi fixed */

    const newDi = solveForDi(model, qi, bParam, anchorT, newDataY);

    const diKey = dragging === 'p10' ? 'p10Di' : 'p90Di';

    ps[diKey] = newDi;

    updatePCurveSeries(cardId, myChart);

    e.event && e.event.preventDefault && e.event.preventDefault();

  }



  function onMouseUp() {

    if (dragging) {

      dragging = null;

      myChart.getDom().style.cursor = '';

    }

  }



  if (myChart.__pcurveDragCleanup) myChart.__pcurveDragCleanup();



  zr.on('mousedown', onMouseDown);

  zr.on('mousemove', onMouseMove);

  zr.on('mouseup', onMouseUp);

  zr.on('globalout', onMouseUp);



  myChart.__pcurveDragCleanup = () => {

    zr.off('mousedown', onMouseDown);

    zr.off('mousemove', onMouseMove);

    zr.off('mouseup', onMouseUp);

    zr.off('globalout', onMouseUp);

    delete myChart.__pcurveDragCleanup;

  };

}



/* Build P-curve data arrays for a given Di, using the model function */

function buildPCurveData(w, data, diValue, isDate, categoryData) {

  const model = data.model || 'exponential';

  const qi = w.params.qi;

  const pParams = { qi: qi, di: diValue, b: w.params.b };

  const hasForecast = w.forecast && w.forecast.x && w.forecast.x.length > 0;



  /* Find first/last non-null fitted indices (same region as P50) */

  let firstFit = -1, lastFit = -1;

  for (let i = 0; i < (w.y_fitted || []).length; i++) {

    if (w.y_fitted[i] != null) { if (firstFit === -1) firstFit = i; lastFit = i; }

  }



  /* Both date and numeric modes now return [x, y] pair arrays */

  const pts = [];

  /* Fitted region */

  for (let i = 0; i < w.t.length; i++) {

    if (w.y_fitted && w.y_fitted[i] != null) {

      const xVal = isDate ? parseDateStr(w.x[i]) : w.x[i];

      pts.push([xVal, evalDeclineModel(model, w.t[i], pParams)]);

    }

  }

  /* Forecast region */

  if (hasForecast && w.forecast.t) {

    /* Bridge from last fitted point */

    if (lastFit >= 0) {

      const lastX = isDate ? parseDateStr(w.x[lastFit]) : w.x[lastFit];

      if (pts.length === 0 || pts[pts.length - 1][0] !== lastX) {

        pts.push([lastX, evalDeclineModel(model, w.t[lastFit], pParams)]);

      }

    }

    for (let i = 0; i < w.forecast.t.length; i++) {

      const xVal = isDate ? parseDateStr(w.forecast.x[i]) : w.forecast.x[i];

      pts.push([xVal, evalDeclineModel(model, w.forecast.t[i], pParams)]);

    }

  }

  return pts;

}



function updatePCurveSeries(cardId, myChart) {

  const ps = cardPCurveState[cardId];

  const data = cardLastData[cardId];

  if (!ps || !data || !data.wells) return;

  const w = data.wells[0];

  if (!w.y_fitted || !w.params || !w.t) return;

  const st = readCardStyles(cardId);

  const isDate = w.is_date || false;

  const P10_COLOR = st.p10Color || '#22c55e', P90_COLOR = st.p90Color || '#ef4444';

  const p10ShowLine = st.p10Line !== false, p10ShowMarker = st.p10Marker || false;

  const p90ShowLine = st.p90Line !== false, p90ShowMarker = st.p90Marker || false;

  const p10DiRound = Math.round(ps.p10Di * 1e6) / 1e6;

  const p90DiRound = Math.round(ps.p90Di * 1e6) / 1e6;



  const opt = myChart.getOption();

  const seriesOpt = opt.series;



  let p10Idx = -1, p90Idx = -1;

  seriesOpt.forEach((s, i) => {

    if (s.name && s.name.startsWith('P10')) p10Idx = i;

    if (s.name && s.name.startsWith('P90')) p90Idx = i;

  });

  if (p10Idx === -1 || p90Idx === -1) return;



  const p10Data = buildPCurveData(w, data, ps.p10Di, isDate, null);

  const p90Data = buildPCurveData(w, data, ps.p90Di, isDate, null);



  seriesOpt[p10Idx].data = p10Data;

  seriesOpt[p10Idx].name = 'P10 (Di=' + p10DiRound + ')';

  seriesOpt[p10Idx].lineStyle = { color: P10_COLOR, width: p10ShowLine ? 1.5 : 0, type: 'dashed' };

  seriesOpt[p10Idx].itemStyle = { color: P10_COLOR };

  seriesOpt[p10Idx].showSymbol = p10ShowMarker;

  seriesOpt[p90Idx].data = p90Data;

  seriesOpt[p90Idx].name = 'P90 (Di=' + p90DiRound + ')';

  seriesOpt[p90Idx].lineStyle = { color: P90_COLOR, width: p90ShowLine ? 1.5 : 0, type: 'dashed' };

  seriesOpt[p90Idx].itemStyle = { color: P90_COLOR };

  seriesOpt[p90Idx].showSymbol = p90ShowMarker;

  myChart.setOption({ series: seriesOpt }, false);

}



/* ====================================================================

   Qi Drag – change initial rate by dragging the first fitted point

   ==================================================================== */

function setupQiDragHandle(cardId, myChart) {

  const data = cardLastData[cardId];

  if (!data || !data.wells || data.wells.length !== 1) return; // single-well only

  const w = data.wells[0];

  if (!w.y_fitted || !w.params || !w.t) return;

  if (!w.params.qi) return;

  const model = data.model || 'exponential';

  const isDate = w.is_date || false;

  const zr = myChart.getZr();

  let dragging = false;



  /* Index of the first non-null fitted point */

  function getFirstFittedIdx() {

    for (let i = 0; i < (w.y_fitted || []).length; i++) {

      if (w.y_fitted[i] != null) return i;

    }

    return -1;

  }



  /* Hit-test: is pixel within threshold of the first fitted point? */

  function hitTestInitialPoint(px, py) {

    const THRESHOLD = 14;

    const opt = myChart.getOption();

    if (!opt || !opt.series) return false;

    const firstIdx = getFirstFittedIdx();

    if (firstIdx < 0) return false;

    for (let si = 0; si < opt.series.length; si++) {

      const s = opt.series[si];

      if (!s.name || !s.name.endsWith('Fitted')) continue;

      const sData = s.data;

      if (!sData || sData.length === 0) continue;

      /* Find the first non-null data point in this series */

      for (let di = 0; di < sData.length; di++) {

        if (!Array.isArray(sData[di]) || sData[di][1] == null) continue;

        const ptPx = myChart.convertToPixel('grid', [sData[di][0], sData[di][1]]);

        if (!ptPx) continue;

        if (Math.abs(ptPx[0] - px) < THRESHOLD && Math.abs(ptPx[1] - py) < THRESHOLD) return true;

        break; // only check the first valid point

      }

    }

    return false;

  }



  function onMouseDown(e) {

    if (hitTestInitialPoint(e.offsetX, e.offsetY)) {

      dragging = true;

      myChart.dispatchAction({ type: 'takeGlobalCursor', key: 'dataZoomSelect', dataZoomSelectActive: false });

      e.event && e.event.preventDefault && e.event.preventDefault();

    }

  }



  function onMouseMove(e) {

    if (!dragging) {

      /* Only set cursor when actually over the point; don't clear it (other handlers manage cursor too) */

      if (hitTestInitialPoint(e.offsetX, e.offsetY)) myChart.getDom().style.cursor = 'ns-resize';

      return;

    }

    const py = e.offsetY;

    const firstIdx = getFirstFittedIdx();

    if (firstIdx < 0) return;

    /* X-pixel of the initial point (keep it fixed) */

    const xVal = isDate ? parseDateStr(w.x[firstIdx]) : w.x[firstIdx];

    const ptPx = myChart.convertToPixel('grid', [xVal, 0]);

    const xPx = ptPx ? ptPx[0] : e.offsetX;

    /* Convert mouse Y → data Y at that X */

    const dp = myChart.convertFromPixel('grid', [xPx, py]);

    const newQi = dp ? dp[1] : null;

    if (!newQi || newQi <= 0) return;

    /* Update qi in the stored data */

    w.params.qi = Math.round(newQi * 1e6) / 1e6;

    /* Recalculate fitted values */

    for (let i = 0; i < w.t.length; i++) {

      if (w.y_fitted[i] != null) w.y_fitted[i] = evalDeclineModel(model, w.t[i], w.params);

    }

    /* Recalculate forecast values */

    if (w.forecast && w.forecast.t) {

      for (let i = 0; i < w.forecast.t.length; i++) {

        w.forecast.y[i] = evalDeclineModel(model, w.forecast.t[i], w.params);

      }

    }

    /* Patch the chart series in-place */

    const opt = myChart.getOption();

    const seriesOpt = opt.series;

    for (let si = 0; si < seriesOpt.length; si++) {

      const s = seriesOpt[si];

      if (!s.name) continue;

      if (s.name.endsWith('Fitted')) {

        /* Rebuild fitted data */

        seriesOpt[si].data = isDate

          ? w.x.map((xv, i) => [parseDateStr(xv), w.y_fitted[i]])

          : w.x.map((xv, i) => [xv, w.y_fitted[i]]);

      }

      if (s.name.endsWith('Forecast') && w.forecast && w.forecast.x) {

        /* Rebuild forecast data — include bridge point from last fitted to avoid gap */

        const fData = [];

        let lfi = (w.y_fitted || []).length - 1;

        while (lfi >= 0 && w.y_fitted[lfi] == null) lfi--;

        if (lfi >= 0) {

          const bx = isDate ? parseDateStr(w.x[lfi]) : w.x[lfi];

          fData.push([bx, w.y_fitted[lfi]]);

        }

        for (let i = 0; i < w.forecast.x.length; i++) {

          const xv = isDate ? parseDateStr(w.forecast.x[i]) : w.forecast.x[i];

          fData.push([xv, w.forecast.y[i]]);

        }

        seriesOpt[si].data = fData;

      }

    }

    myChart.setOption({ series: seriesOpt }, false);

    /* Update P10/P90 curves if enabled (they depend on qi) */

    const ps = cardPCurveState[cardId];

    if (ps && ps.enabled) updatePCurveSeries(cardId, myChart);

    /* Update formula display */

    updateQiDisplay(cardId);

    e.event && e.event.preventDefault && e.event.preventDefault();

  }



  function onMouseUp() {

    if (dragging) {

      dragging = false;

      myChart.getDom().style.cursor = '';

      /* Also sync the other chart (mini ↔ full view) */

      const fullId = 'fullChart-' + cardId;

      const otherChart = myChart === chartInstances[fullId] ? chartInstances[cardId] : chartInstances[fullId];

      if (otherChart) {

        const seriesSync = myChart.getOption().series;

        otherChart.setOption({ series: seriesSync }, false);

        const psSync = cardPCurveState[cardId];

        if (psSync && psSync.enabled) updatePCurveSeries(cardId, otherChart);

      }

    }

  }



  if (myChart.__qiDragCleanup) myChart.__qiDragCleanup();

  zr.on('mousedown', onMouseDown);

  zr.on('mousemove', onMouseMove);

  zr.on('mouseup', onMouseUp);

  zr.on('globalout', onMouseUp);

  myChart.__qiDragCleanup = () => {

    zr.off('mousedown', onMouseDown);

    zr.off('mousemove', onMouseMove);

    zr.off('mouseup', onMouseUp);

    zr.off('globalout', onMouseUp);

    delete myChart.__qiDragCleanup;

  };

}



/* Helper: update the formula + parameter display after qi change */

function updateQiDisplay(cardId) {

  const data = cardLastData[cardId];

  if (!data || !data.wells || data.wells.length === 0) return;

  const w = data.wells[0];

  if (!w.params) return;

  const p = w.params;

  const fmt = (v) => typeof v === 'number' ? (Number.isInteger(v) ? v.toString() : v.toFixed(4)) : v;

  /* Update parameter badges */

  const paramDiv = document.getElementById('params-' + cardId);

  if (paramDiv) {

    let pHtml = `<span>Model: <strong>${data.model}</strong></span>`;

    for (const [k, v] of Object.entries(p)) pHtml += `<span>${k}: <strong>${typeof v === 'number' ? fmt(v) : v}</strong></span>`;

    paramDiv.innerHTML = pHtml;

  }

  /* Update formula */

  const formulaDiv = document.getElementById('formula-' + cardId);

  if (formulaDiv && w.equation) {

    const modelName = (data.model || '').toLowerCase();

    let formulaHtml = '';

    if (modelName === 'exponential') {

      formulaHtml = `<span class="formula-generic">q(t) = q<sub>i</sub> &middot; e<sup>&minus;d<sub>i</sub>&middot;t</sup></span>`

        + `<span class="formula-fitted">q(t) = ${fmt(p.qi)} &middot; e<sup>&minus;${fmt(p.di)}&middot;t</sup></span>`;

    } else if (modelName === 'hyperbolic') {

      formulaHtml = `<span class="formula-generic">q(t) = q<sub>i</sub> / (1 + b&middot;d<sub>i</sub>&middot;t)<sup>1/b</sup></span>`

        + `<span class="formula-fitted">q(t) = ${fmt(p.qi)} / (1 + ${fmt(p.b)}&middot;${fmt(p.di)}&middot;t)<sup>1/${fmt(p.b)}</sup></span>`;

    } else if (modelName === 'harmonic') {

      formulaHtml = `<span class="formula-generic">q(t) = q<sub>i</sub> / (1 + d<sub>i</sub>&middot;t)</span>`

        + `<span class="formula-fitted">q(t) = ${fmt(p.qi)} / (1 + ${fmt(p.di)}&middot;t)</span>`;

    } else {

      formulaHtml = `<span class="formula-fitted">${w.equation}</span>`;

    }

    formulaDiv.innerHTML = formulaHtml;

  }

  /* Update DCA stats */

  const dcaStatsDiv = document.getElementById('dcaStats-' + cardId);

  if (dcaStatsDiv) {

    const fittedVals = w.y_fitted ? w.y_fitted.filter(v => v != null) : [];

    if (fittedVals.length >= 2) {

      const entries = dcaStatsDiv.querySelectorAll('span');

      entries.forEach(sp => {

        if (sp.textContent.includes('Fitted Start')) {

          sp.innerHTML = `Fitted Start: <strong>${fittedVals[0].toFixed(2)}</strong>`;

        } else if (sp.textContent.includes('Fitted End')) {

          sp.innerHTML = `Fitted End: <strong>${fittedVals[fittedVals.length - 1].toFixed(2)}</strong>`;

        }

      });

    }

  }

}



/* ====================================================================

   Axis Panning (Drag Axes)

   ==================================================================== */

function setupAxisDragHandles(cardId, myChart) {

  const zr = myChart.getZr();

  let draggingAxis = null;

  let startPx = 0, startPy = 0;

  let initialExtent = null;



  function getCoordSys() {

    const gridModel = myChart.getModel().getComponent('grid');

    return gridModel ? gridModel.coordinateSystem : null;

  }



  function isOverYAxis(px, py) {

    const coordSys = getCoordSys();

    if (!coordSys) return false;

    const rect = coordSys.getRect();

    const axPos = cardAxisPositions[cardId] || { x: 'bottom', y: 'left' };

    if (axPos.y === 'left' && px < rect.x && py >= rect.y && py <= rect.y + rect.height) return true;

    if (axPos.y === 'right' && px > rect.x + rect.width && py >= rect.y && py <= rect.y + rect.height) return true;

    return false;

  }



  function isOverXAxis(px, py) {

    const coordSys = getCoordSys();

    if (!coordSys) return false;

    const rect = coordSys.getRect();

    const axPos = cardAxisPositions[cardId] || { x: 'bottom', y: 'left' };

    if (axPos.x === 'bottom' && py > rect.y + rect.height && px >= rect.x && px <= rect.x + rect.width) return true;

    if (axPos.x === 'top' && py < rect.y && px >= rect.x && px <= rect.x + rect.width) return true;

    return false;

  }



  function onMouseDown(e) {

    const px = e.offsetX, py = e.offsetY;

    if (isOverYAxis(px, py)) {

      draggingAxis = 'y';

      startPy = py;

      const coordSys = getCoordSys();

      initialExtent = coordSys.getAxis('y').scale.getExtent();

      myChart.dispatchAction({ type: 'takeGlobalCursor', key: 'dataZoomSelect', dataZoomSelectActive: false });

      if (e.event && e.event.preventDefault) e.event.preventDefault();

    } else if (isOverXAxis(px, py)) {

      draggingAxis = 'x';

      startPx = px;

      const coordSys = getCoordSys();

      initialExtent = coordSys.getAxis('x').scale.getExtent();

      myChart.dispatchAction({ type: 'takeGlobalCursor', key: 'dataZoomSelect', dataZoomSelectActive: false });

      if (e.event && e.event.preventDefault) e.event.preventDefault();

    }

  }



  function onMouseMove(e) {

    const px = e.offsetX, py = e.offsetY;

    if (!draggingAxis) {

      if (isOverYAxis(px, py) || isOverXAxis(px, py)) {

        myChart.getDom().style.cursor = 'move';

        myChart.__isOverAxis = true;

      } else if (myChart.__isOverAxis) {

        myChart.getDom().style.cursor = '';

        myChart.__isOverAxis = false;

      }

      return;

    }



    const coordSys = getCoordSys();

    if (!coordSys) return;

    const rect = coordSys.getRect();



    if (draggingAxis === 'y') {

      const yAxis = coordSys.getAxis('y');

      const dy = py - startPy;

      const isLog = yAxis.type === 'log';

      let newMin, newMax;



      if (isLog) {

        const logMin = Math.log(initialExtent[0]);

        const logMax = Math.log(initialExtent[1]);

        const logRange = logMax - logMin;

        const diff = (dy / rect.height) * logRange;

        newMin = Math.exp(logMin + diff);

        newMax = Math.exp(logMax + diff);

      } else {

        const range = initialExtent[1] - initialExtent[0];

        const diff = (dy / rect.height) * range;

        newMin = initialExtent[0] + diff;

        newMax = initialExtent[1] + diff;

      }



      myChart.setOption({ yAxis: { min: newMin, max: newMax } });

      cardZoomState[cardId] = cardZoomState[cardId] || {};

      cardZoomState[cardId].yMin = newMin;

      cardZoomState[cardId].yMax = newMax;

    } else if (draggingAxis === 'x') {

      const dx = px - startPx;

      const range = initialExtent[1] - initialExtent[0];

      const diff = -(dx / rect.width) * range;

      const newMin = initialExtent[0] + diff;

      const newMax = initialExtent[1] + diff;



      myChart.setOption({ xAxis: { min: newMin, max: newMax } });

      cardZoomState[cardId] = cardZoomState[cardId] || {};

      cardZoomState[cardId].xMin = newMin;

      cardZoomState[cardId].xMax = newMax;

    }

    if (e.event && e.event.preventDefault) e.event.preventDefault();

  }



  function onMouseUp() {

    if (draggingAxis) {

      draggingAxis = null;

      myChart.getDom().style.cursor = '';

      myChart.__isOverAxis = false;

    }

  }



  if (myChart.__axisDragCleanup) myChart.__axisDragCleanup();



  zr.on('mousedown', onMouseDown);

  zr.on('mousemove', onMouseMove);

  zr.on('mouseup', onMouseUp);

  zr.on('globalout', onMouseUp);



  myChart.__axisDragCleanup = () => {

    zr.off('mousedown', onMouseDown);

    zr.off('mousemove', onMouseMove);

    zr.off('mouseup', onMouseUp);

    zr.off('globalout', onMouseUp);

    delete myChart.__axisDragCleanup;

  };

}



/* ====================================================================

   Render Chart

   ==================================================================== */

function renderSingleChart(cardId, data, forecastMonths) {

  const card = document.getElementById(cardId);

  card.querySelector('.chart-area').style.display = 'block';

  const chartDiv = document.getElementById('chart-' + cardId);

  const paramDiv = document.getElementById('params-' + cardId);

  const exclHint = document.getElementById('exclHint-' + cardId);



  const allWellsData = data.wells || [];

  if (allWellsData.length === 0) return;



  const firstW = allWellsData[0];

  const excluded = new Set(firstW.excluded_indices || []);

  cardExclusions[cardId] = excluded;

  exclHint.textContent = excluded.size > 0 ? `${excluded.size} point(s) excluded — click to toggle` : 'Click scatter points to exclude / include';



  let pHtml = `<span>Model: <strong>${data.model}</strong></span>`;

  if (firstW.params) { for (const [k, v] of Object.entries(firstW.params)) pHtml += `<span>${k}: <strong>${v}</strong></span>`; }

  if (allWellsData.length > 1) pHtml += `<span>Wells: <strong>${allWellsData.length}</strong></span>`;

  paramDiv.innerHTML = pHtml;



  /* --- Formula display below chart --- */

  const formulaDiv = document.getElementById('formula-' + cardId);

  if (formulaDiv && firstW.equation && firstW.params) {

    const modelName = (data.model || '').toLowerCase();

    let formulaHtml = '';

    const p = firstW.params;

    const fmt = (v) => typeof v === 'number' ? (Number.isInteger(v) ? v.toString() : v.toFixed(4)) : v;

    if (modelName === 'exponential') {

      formulaHtml = `<span class="formula-generic">q(t) = q<sub>i</sub> &middot; e<sup>&minus;d<sub>i</sub>&middot;t</sup></span>`

        + `<span class="formula-fitted">q(t) = ${fmt(p.qi)} &middot; e<sup>&minus;${fmt(p.di)}&middot;t</sup></span>`;

    } else if (modelName === 'hyperbolic') {

      formulaHtml = `<span class="formula-generic">q(t) = q<sub>i</sub> / (1 + b&middot;d<sub>i</sub>&middot;t)<sup>1/b</sup></span>`

        + `<span class="formula-fitted">q(t) = ${fmt(p.qi)} / (1 + ${fmt(p.b)}&middot;${fmt(p.di)}&middot;t)<sup>1/${fmt(p.b)}</sup></span>`;

    } else if (modelName === 'harmonic') {

      formulaHtml = `<span class="formula-generic">q(t) = q<sub>i</sub> / (1 + d<sub>i</sub>&middot;t)</span>`

        + `<span class="formula-fitted">q(t) = ${fmt(p.qi)} / (1 + ${fmt(p.di)}&middot;t)</span>`;

    } else {

      formulaHtml = `<span class="formula-fitted">${firstW.equation}</span>`;

    }

    formulaDiv.innerHTML = formulaHtml;

    formulaDiv.style.display = '';

  } else if (formulaDiv) {

    formulaDiv.style.display = 'none';

  }



  /* --- DCA Stats: first/last values for actual and fitted --- */

  const dcaStatsDiv = document.getElementById('dcaStats-' + cardId);

  if (dcaStatsDiv && allWellsData.length > 0) {

    const sw = allWellsData[0];

    const exclSet = new Set(sw.excluded_indices || []);

    const actualVals = sw.y_actual.filter((v, i) => v != null && !exclSet.has(i));

    const fittedVals = sw.y_fitted ? sw.y_fitted.filter(v => v != null) : [];

    if (actualVals.length >= 2 || fittedVals.length >= 2) {

      let statsHtml = '';

      if (actualVals.length >= 2) {

        const aFirst = actualVals[0], aLast = actualVals[actualVals.length - 1];

        const aDiff = aLast - aFirst;

        const aPct = aFirst !== 0 ? ((aDiff / Math.abs(aFirst)) * 100) : 0;

        const aSign = aDiff >= 0 ? '+' : '';

        const aCls = aDiff >= 0 ? 'positive' : 'negative';

        statsHtml += `<div class="dca-stat-item"><div class="dca-stat-label">Actual First</div><div class="dca-stat-value">${aFirst.toFixed(2)}</div></div>`;

        statsHtml += `<div class="dca-stat-item"><div class="dca-stat-label">Actual Last</div><div class="dca-stat-value">${aLast.toFixed(2)}</div></div>`;

        statsHtml += `<div class="dca-stat-item"><div class="dca-stat-label">Actual Δ</div><div class="dca-stat-value ${aCls}">${aSign}${aDiff.toFixed(2)}</div></div>`;

        statsHtml += `<div class="dca-stat-item"><div class="dca-stat-label">Actual %</div><div class="dca-stat-value ${aCls}">${aSign}${aPct.toFixed(1)}%</div></div>`;

      }

      if (fittedVals.length >= 2) {

        const fFirst = fittedVals[0], fLast = fittedVals[fittedVals.length - 1];

        const fDiff = fLast - fFirst;

        const fPct = fFirst !== 0 ? ((fDiff / Math.abs(fFirst)) * 100) : 0;

        const fSign = fDiff >= 0 ? '+' : '';

        const fCls = fDiff >= 0 ? 'positive' : 'negative';

        statsHtml += `<div class="dca-stat-item"><div class="dca-stat-label">Fitted First</div><div class="dca-stat-value">${fFirst.toFixed(2)}</div></div>`;

        statsHtml += `<div class="dca-stat-item"><div class="dca-stat-label">Fitted Last</div><div class="dca-stat-value">${fLast.toFixed(2)}</div></div>`;

        statsHtml += `<div class="dca-stat-item"><div class="dca-stat-label">Fitted Δ</div><div class="dca-stat-value ${fCls}">${fSign}${fDiff.toFixed(2)}</div></div>`;

        statsHtml += `<div class="dca-stat-item"><div class="dca-stat-label">Fitted %</div><div class="dca-stat-value ${fCls}">${fSign}${fPct.toFixed(1)}%</div></div>`;

      }

      dcaStatsDiv.innerHTML = statsHtml;

      dcaStatsDiv.style.display = 'flex';

    } else {

      dcaStatsDiv.style.display = 'none';

    }

  }



  const st = readCardStyles(cardId);

  const customTitle = card.querySelector('.p-title')?.value || '';



  if (chartInstances[cardId]) chartInstances[cardId].dispose();

  const isLight = document.documentElement.getAttribute('data-theme') === 'light';

  const myChart = echarts.init(chartDiv, isLight ? null : 'dark');

  chartInstances[cardId] = myChart;



  const isDate = firstW.is_date || false;

  const xAxisType = isDate ? 'time' : 'value';

  const series = [];

  const wellColors = getPlotThemePalette(st.plotTheme);



  /* Compute date range for time axis */

  let dateMin = Infinity, dateMax = -Infinity;

  if (isDate) {

    allWellsData.forEach(w => {

      w.x.forEach(d => { const ts = parseDateStr(d); if (ts < dateMin) dateMin = ts; if (ts > dateMax) dateMax = ts; });

      if (w.forecast && w.forecast.x) w.forecast.x.forEach(d => { const ts = parseDateStr(d); if (ts < dateMin) dateMin = ts; if (ts > dateMax) dateMax = ts; });

    });

    /* Add a small padding (2% of range on each side) */

    const dateRange = dateMax - dateMin;

    const datePad = Math.max(dateRange * 0.02, 86400000); /* at least 1 day */

    dateMin -= datePad;

    dateMax += datePad;

  }



  const isSingle = allWellsData.length === 1;



  allWellsData.forEach((w, wIdx) => {

    const wColor = isSingle ? st.actualColor : wellColors[wIdx % wellColors.length];

    const wFitColor = isSingle ? st.fittedColor : wColor;

    const wFcColor = isSingle ? st.forecastColor : wColor;

    const prefix = isSingle ? '' : w.well + ' ';

    const actualLen = w.x.length;

    const hasForecast = w.forecast && w.forecast.x && w.forecast.x.length > 0;



    if (isDate) {

      const incD = [], exclD = [];

      for (let i = 0; i < actualLen; i++) {

        const ts = parseDateStr(w.x[i]);

        const pt = [ts, w.y_actual[i], i];

        if (isSingle && excluded.has(i)) exclD.push(pt);

        else incD.push(pt);

      }

      series.push({ name: prefix + 'Actual', type: 'scatter', symbolSize: st.actualSize, symbol: st.actualSymbol, itemStyle: { color: wColor }, data: incD });

      if (isSingle && excluded.size > 0) series.push({ name: 'Excluded', type: 'scatter', symbolSize: st.actualSize, symbol: 'diamond', itemStyle: { color: '#ef4444', opacity: 0.5 }, data: exclD });

      if (w.y_fitted) {

        series.push({
          name: prefix + 'Fitted',
          type: 'line',
          showSymbol: st.fittedMarkers,
          symbol: st.fittedSymbol,
          symbolSize: st.fittedSymbolSize,
          smooth: false,
          lineStyle: { color: wFitColor, width: st.fittedWidth, type: st.fittedStyle },
          itemStyle: { color: wFitColor },
          data: w.x.map((xv, i) => [parseDateStr(xv), w.y_fitted[i]])
        });

      }

      if (hasForecast) {

        const bridgeArr = [];

        if (w.y_fitted && w.y_fitted.length > 0 && w.x.length > 0) {

          let lfi = w.y_fitted.length - 1;

          while (lfi >= 0 && w.y_fitted[lfi] == null) lfi--;

          if (lfi >= 0) bridgeArr.push([parseDateStr(w.x[lfi]), w.y_fitted[lfi]]);

        }

        series.push({
          name: prefix + 'Forecast',
          type: 'line',
          showSymbol: st.forecastMarkers,
          symbol: st.forecastSymbol,
          symbolSize: st.forecastSymbolSize,
          smooth: false,
          lineStyle: { type: st.forecastStyle, color: wFcColor, width: st.forecastWidth },
          itemStyle: { color: wFcColor },
          data: [...bridgeArr, ...w.forecast.x.map((xv, i) => [parseDateStr(xv), w.forecast.y[i]])]
        });

      }

    } else {

      const incD = [], exclD = [];

      for (let i = 0; i < actualLen; i++) { const pt = [w.x[i], w.y_actual[i], i]; if (isSingle && excluded.has(i)) exclD.push(pt); else incD.push(pt); }

      series.push({ name: prefix + 'Actual', type: 'scatter', symbolSize: st.actualSize, symbol: st.actualSymbol, itemStyle: { color: wColor }, data: incD });

      if (isSingle && excluded.size > 0) series.push({ name: 'Excluded', type: 'scatter', symbolSize: st.actualSize, symbol: 'diamond', itemStyle: { color: '#ef4444', opacity: 0.5 }, data: exclD });

      if (w.y_fitted) {
        series.push({
          name: prefix + 'Fitted',
          type: 'line',
          showSymbol: st.fittedMarkers,
          symbol: st.fittedSymbol,
          symbolSize: st.fittedSymbolSize,
          smooth: false,
          lineStyle: { color: wFitColor, width: st.fittedWidth, type: st.fittedStyle },
          itemStyle: { color: wFitColor },
          data: w.x.map((xv, i) => [xv, w.y_fitted[i]])
        });
      }

      if (hasForecast) {

        const bridgeArr = [];

        if (w.y_fitted && w.y_fitted.length > 0 && w.x.length > 0) { let lfi = w.y_fitted.length - 1; while (lfi >= 0 && w.y_fitted[lfi] == null) lfi--; if (lfi >= 0) bridgeArr.push([w.x[lfi], w.y_fitted[lfi]]); }

        series.push({ name: prefix + 'Forecast', type: 'line', showSymbol: st.forecastMarkers, symbol: st.forecastSymbol, symbolSize: st.forecastSymbolSize, smooth: false, lineStyle: { type: st.forecastStyle, color: wFcColor, width: st.forecastWidth }, itemStyle: { color: wFcColor }, data: [...bridgeArr, ...w.forecast.x.map((xv, i) => [xv, w.forecast.y[i]])] });

      }

    }



    /* ---- P10 / P90 decline curves (physics-based, drag recalculates D) ---- */

    const ps = cardPCurveState[cardId];

    if (ps && ps.enabled && w.y_fitted && w.params && w.t) {

      const P10_COLOR = st.p10Color || '#22c55e', P90_COLOR = st.p90Color || '#ef4444';

      const p10ShowLine = st.p10Line !== false, p10ShowMarker = st.p10Marker || false;

      const p90ShowLine = st.p90Line !== false, p90ShowMarker = st.p90Marker || false;

      const p10DiRound = Math.round(ps.p10Di * 1e6) / 1e6;

      const p90DiRound = Math.round(ps.p90Di * 1e6) / 1e6;



      const p10Data = buildPCurveData(w, data, ps.p10Di, isDate, null);

      const p90Data = buildPCurveData(w, data, ps.p90Di, isDate, null);

      series.push({ name: prefix + 'P10 (Di=' + p10DiRound + ')', type: 'line', z: 1, showSymbol: p10ShowMarker, symbol: 'circle', symbolSize: 6, smooth: false, lineStyle: { color: P10_COLOR, width: p10ShowLine ? 1.5 : 0, type: 'dashed' }, itemStyle: { color: P10_COLOR }, data: p10Data });

      series.push({ name: prefix + 'P90 (Di=' + p90DiRound + ')', type: 'line', z: 1, showSymbol: p90ShowMarker, symbol: 'circle', symbolSize: 6, smooth: false, lineStyle: { color: P90_COLOR, width: p90ShowLine ? 1.5 : 0, type: 'dashed' }, itemStyle: { color: P90_COLOR }, data: p90Data });

    }

  });



  /* --- User Lines (H/V) --- */

  const userLines = cardUserLines[cardId] || [];

  userLines.forEach(ul => {

    const mlDataItem = ul.type === 'h' ? { yAxis: ul.value } : { xAxis: ul.value };

    series.push({

      name: ul.name, type: 'line', data: [],

      markLine: {

        silent: false, symbol: ['none', 'none'],

        lineStyle: { color: ul.color, type: 'solid', width: 2 },

        label: { show: true, formatter: ul.name, color: ul.color, fontSize: 11 },

        data: [mlDataItem]

      },

      itemStyle: { color: ul.color }, lineStyle: { color: ul.color }

    });

  });



  /* --- Annotations as markPoint on a utility series --- */

  const annotations = cardAnnotations[cardId] || [];

  if (annotations.length > 0) {

    const annotPoints = annotations.map(ann => {

      const coord = [ann.x, ann.y];

      return {

        name: 'ann_' + ann.id, coord: coord, symbol: 'circle', symbolSize: 1,

        itemStyle: { color: 'transparent' },

        label: {

          show: true, formatter: ann.text, position: 'top', fontSize: ann.fontSize || 12,

          color: ann.color || (isLight ? '#0f172a' : '#e2e8f0'),

          backgroundColor: isLight ? 'rgba(255,255,255,.92)' : 'rgba(34,37,51,.92)',

          borderColor: isLight ? '#cbd5e1' : '#444', borderWidth: 1,

          padding: [4, 8], borderRadius: 4, distance: 15

        }

      };

    });

    series.push({

      name: '_annotations', type: 'scatter', data: [],

      markPoint: { data: annotPoints, animation: false },

      tooltip: { show: false }, silent: false

    });

  }



  /* --- Data Labels on scatter series --- */

  const labelPos = cardValueLabels[cardId] || 'none';

  if (labelPos !== 'none') {

    series.forEach(s => {

      if (s.type === 'scatter' && s.name && !s.name.startsWith('_') && s.name !== 'Excluded') {

        s.label = {

          show: true, position: labelPos,

          formatter: function (params) {

            let val = Array.isArray(params.data) ? params.data[1] : params.data;

            if (val == null) return '';

            return typeof val === 'number' ? val.toFixed(1) : val;

          },

          fontSize: 9, color: isLight ? '#475569' : '#94a3b8'

        };

      }

    });

  }



  const legendData = series.filter(s => !s.name.endsWith('_bridge') && !s.name.startsWith('_')).map(s => s.name);

  const axC = isLight ? '#94a3b8' : '#333750'; // Darker axis lines for light theme

  const spC = isLight ? '#d1d5db' : '#262a3a'; // Darker grid lines for light theme

  const lbC = isLight ? '#475569' : '#94a3b8'; // Darker labels for light theme

  const ttC = isLight ? '#020617' : '#e2e8f0'; // Almost black title for light theme



  const chartTitle = customTitle || (isSingle ? firstW.well : allWellsData.map(w => w.well).join(', '));

  const customLabels = cardAxisLabels[cardId] || {};

  const useLogScale = cardLogScale[cardId] || false;



  /* --- % Change & Difference graphic --- */

  const showPct = cardPctChange[cardId] || false;

  let graphicElems = [];
  if (showPct && allWellsData.length > 0) {
    const pw = allWellsData[0];
    const validIndices = [];
    pw.y_actual.forEach((v, i) => { if (v != null) validIndices.push(i); });
    if (validIndices.length >= 2) {
      const firstIdx = validIndices[0], lastIdx = validIndices[validIndices.length - 1];
      const firstVal = pw.y_actual[firstIdx], lastVal = pw.y_actual[lastIdx];
      const firstDate = pw.x[firstIdx], lastDate = pw.x[lastIdx];
      const diff = lastVal - firstVal;
      const pct = firstVal !== 0 ? ((diff / Math.abs(firstVal)) * 100) : 0;
      const sign = diff >= 0 ? '+' : '';
      const pctText = `[${firstDate}] ${firstVal.toFixed(2)} \u2192 [${lastDate}] ${lastVal.toFixed(2)}  |  \u0394 ${sign}${diff.toFixed(2)} (${sign}${pct.toFixed(1)}%)`;
      graphicElems.push({
        type: 'text', right: 40, top: 55, z: 100,
        style: {

          text: pctText, fontSize: 11.5, fontWeight: 'bold',

          fill: diff >= 0 ? '#22c55e' : '#ef4444',

          backgroundColor: isLight ? 'rgba(255,255,255,.88)' : 'rgba(34,37,51,.88)',

          borderColor: diff >= 0 ? 'rgba(34,197,94,.4)' : 'rgba(239,68,68,.4)',

          borderWidth: 1, padding: [6, 12], borderRadius: 4

        }

      });

    }

  }


  const axPos = cardAxisPositions[cardId] || { x: 'bottom', y: 'left' };

  const gridLeft = axPos.y === 'right' ? 30 : 60;
  const gridRight = axPos.y === 'right' ? 60 : 30;
  const gridTop = axPos.x === 'top' ? 70 : 50;
  const gridBottom = axPos.x === 'top' ? (isDate ? 40 : 30) : (isDate ? 70 : 40);
  const dzBottom = axPos.x === 'top' ? (isDate ? 8 : 4) : (isDate ? 8 : 4);

  const option = {

    backgroundColor: 'transparent',

    graphic: graphicElems,

    title: {

      text: chartTitle,

      left: 'center',

      textStyle: { color: ttC, fontSize: 15, fontWeight: 'bold' }

    },

    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' }, formatter: isDate ? function(params) {
      if (!params || params.length === 0) return '';
      let ts = params[0].axisValue;
      let header = '<b>' + formatDateTs(ts) + '</b>';
      params.forEach(p => {
        if (p.seriesName && !p.seriesName.endsWith('_bridge') && !p.seriesName.startsWith('_')) {
          let val = Array.isArray(p.data) ? p.data[1] : p.data;
          if (val != null) header += '<br/>' + p.marker + ' ' + p.seriesName + ': <b>' + (typeof val === 'number' ? val.toFixed(2) : val) + '</b>';
        }
      });
      return header;
    } : undefined },

    legend: { data: legendData, top: 4, left: 60, textStyle: { color: lbC, fontSize: 11 } },

    grid: { left: gridLeft, right: gridRight, top: gridTop, bottom: gridBottom },

    xAxis: {

      type: xAxisType,

      position: axPos.x,

      min: isDate ? dateMin : undefined,

      max: isDate ? dateMax : undefined,

      name: customLabels.x || data.x_label,

      nameLocation: 'middle',

      nameGap: isDate ? 50 : 25,

      nameTextStyle: { color: lbC, fontSize: 13, fontWeight: 'bold' },

      splitLine: { show: st.gridY !== false, lineStyle: { color: spC, type: 'dashed' } },

      axisLine: { lineStyle: { color: axC } },

      splitNumber: isDate ? (parseInt(card.querySelector('.p-xlabels')?.value) || 8) : undefined,

      axisLabel: {
        color: lbC, rotate: isDate ? 45 : 0, fontSize: isDate ? 10 : 12, hideOverlap: true, showMinLabel: true, showMaxLabel: true,
        formatter: isDate ? function(val) { return formatDateTs(val); } : undefined
      }

    },

    yAxis: {

      type: useLogScale ? 'log' : 'value',

      position: axPos.y,

      name: customLabels.y || data.y_label,

      nameTextStyle: { color: lbC, fontSize: 13, fontWeight: 'bold' },

      splitLine: { show: st.gridX !== false, lineStyle: { color: spC, type: 'dashed' } },

      axisLine: { lineStyle: { color: axC } },

      axisLabel: { color: lbC },

      min: useLogScale ? 0.01 : undefined

    },

    dataZoom: [{ type: 'slider', xAxisIndex: 0, bottom: dzBottom, height: 18 }],

    series: series

  };

  myChart.setOption(option);

  cardOptions[cardId] = JSON.parse(JSON.stringify(option));



  const savedZoom = cardZoomState[cardId];

  if (savedZoom && (savedZoom.xMin != null || savedZoom.yMin != null)) {

    const zo = {};

    if (savedZoom.xMin != null || savedZoom.xMax != null) zo.xAxis = { min: savedZoom.xMin, max: savedZoom.xMax };

    if (savedZoom.yMin != null || savedZoom.yMax != null) zo.yAxis = { min: savedZoom.yMin, max: savedZoom.yMax };

    myChart.setOption(zo);

    setResetZoomButtonsVisible(cardId, true);

  }

  const hasExcl = excluded.size > 0;

  const hasZoom = savedZoom && (savedZoom.xMin != null || savedZoom.yMin != null);

  const ra = document.getElementById('resetAll-' + cardId); if (ra) { if (hasExcl || hasZoom) ra.classList.add('show'); else ra.classList.remove('show'); }



  if (isSingle) {

    const actualLen = firstW.x.length;

    myChart.on('click', function (params) {

      /* Handle annotation clicks */

      if (params.componentType === 'markPoint' && params.name && params.name.startsWith('ann_')) {

        const annId = parseInt(params.name.replace('ann_', ''));

        const anns = cardAnnotations[cardId] || [];

        const idx = anns.findIndex(a => a.id === annId);

        if (idx >= 0) showAnnotationEditor(cardId, idx, params.event);

        return;

      }

      if (!params.seriesName.endsWith('Actual') && params.seriesName !== 'Excluded') return;

      let idx;

      idx = params.data && params.data[2];

      if (idx !== undefined && idx !== null) toggleExclusion(cardId, idx);

    });

  } else {

    /* Multi-well mode: still handle annotation clicks */

    myChart.on('click', function (params) {

      if (params.componentType === 'markPoint' && params.name && params.name.startsWith('ann_')) {

        const annId = parseInt(params.name.replace('ann_', ''));

        const anns = cardAnnotations[cardId] || [];

        const idx = anns.findIndex(a => a.id === annId);

        if (idx >= 0) showAnnotationEditor(cardId, idx, params.event);

      }

    });

  }



  /* Setup legend click → color picker for user lines */

  setupLegendColorPicker(cardId, myChart);



  /* Setup P10/P90 curve drag via zr mouse events */

  setupPCurveDragHandles(cardId, myChart);



  /* Setup qi drag on the initial fitted point */

  setupQiDragHandle(cardId, myChart);



  /* Setup user line dragging */

  setupUserLineDrag(cardId, myChart);



  /* Setup explicit axis panning */

  setupAxisDragHandles(cardId, myChart);



  // Populate data table with sorting support

  const ft = document.getElementById('forecastTable-' + cardId);

  const ftSection = document.getElementById('forecastTableSection-' + cardId);

  if (ft) {

    const tableRows = [];

    allWellsData.forEach(w => {

      const wExcl = new Set(w.excluded_indices || []);

      for (let i = 0; i < w.x.length; i++) {

        if (wExcl.has(i)) continue;

        let xDisp = w.x[i];

        if (typeof xDisp === 'number' && !Number.isInteger(xDisp)) xDisp = parseFloat(xDisp.toFixed(4));

        const yAct = typeof w.y_actual[i] === 'number' ? w.y_actual[i] : null;

        const yFit = w.y_fitted ? (typeof w.y_fitted[i] === 'number' ? w.y_fitted[i] : null) : null;

        tableRows.push({ well: w.well, section: 'Actual', time: xDisp, actual: yAct, fitted: yFit });

      }

      if (w.forecast && w.forecast.x && w.forecast.x.length > 0) {

        for (let i = 0; i < w.forecast.x.length; i++) {

          let xDisp = w.forecast.x[i];

          if (typeof xDisp === 'number' && !Number.isInteger(xDisp)) xDisp = parseFloat(xDisp.toFixed(4));

          const yFc = typeof w.forecast.y[i] === 'number' ? w.forecast.y[i] : null;

          tableRows.push({ well: w.well, section: 'Forecast', time: xDisp, actual: null, fitted: yFc });

        }

      }

    });

    cardTableData[cardId] = tableRows;

    if (tableRows.length > 0) {

      if (ftSection) ftSection.style.display = 'block';

      renderSortableTable(cardId);

    } else {

      if (ftSection) ftSection.style.display = 'none';

      ft.innerHTML = '';

    }

  }



  setupBoxSelection(cardId, myChart, chartDiv);

  setupAxisHoverTooltip(cardId, chartDiv);

  const ro = new ResizeObserver(() => myChart.resize());

  ro.observe(chartDiv);

  // Sync back to full view if it is open
  const fullId = 'fullChart-' + cardId;
  const fullChart = chartInstances[fullId];
  if (fullChart) {
    // Merge options properly avoiding axis reset anomalies or pass `true` to replace options
    fullChart.setOption(option, true);
  }

}



/* ====================================================================

   Box Selection & Zoom Menu

   ==================================================================== */

let _activeSelection = null;

function setResetZoomButtonsVisible(cardId, visible) {
  const miniBtn = document.getElementById('resetZoom-' + cardId);
  const fullBtn = document.getElementById('resetZoomFull-' + cardId);
  if (miniBtn) miniBtn.classList.toggle('show', !!visible);
  if (fullBtn) fullBtn.classList.toggle('show', !!visible);
}



function setupBoxSelection(cardId, chart, chartDiv) {

  chartDiv.addEventListener('mousedown', function (e) {

    if (e.button !== 0) return;

    hideZoomMenu();

    const rect = chartDiv.getBoundingClientRect();

    const offsetX = e.clientX - rect.left;

    const offsetY = e.clientY - rect.top;

    const dp = chart.convertFromPixel('grid', [offsetX, offsetY]);

    if (!dp) return;

    _activeSelection = { cardId, chart, chartDiv, startX: offsetX, startY: offsetY, overlay: null };

  });

}



document.addEventListener('mousemove', function (e) {

  if (!_activeSelection) return;

  const { chartDiv, startX, startY } = _activeSelection;

  const rect = chartDiv.getBoundingClientRect();

  const cx = e.clientX - rect.left, cy = e.clientY - rect.top;

  if (Math.abs(cx - startX) < 5 && Math.abs(cy - startY) < 5) return;

  if (!_activeSelection.overlay) {

    const ov = document.createElement('div'); ov.className = 'select-overlay'; chartDiv.appendChild(ov); _activeSelection.overlay = ov;

  }

  const ov = _activeSelection.overlay;

  ov.style.left = Math.min(startX, cx) + 'px'; ov.style.top = Math.min(startY, cy) + 'px';

  ov.style.width = Math.abs(cx - startX) + 'px'; ov.style.height = Math.abs(cy - startY) + 'px';

});



document.addEventListener('mouseup', function (e) {

  if (!_activeSelection) return;

  const sel = _activeSelection; _activeSelection = null;

  const rect = sel.chartDiv.getBoundingClientRect();

  const endX = e.clientX - rect.left, endY = e.clientY - rect.top;

  const dx = Math.abs(endX - sel.startX), dy = Math.abs(endY - sel.startY);

  if (dx < 10 && dy < 10) { if (sel.overlay) sel.overlay.remove(); return; }

  const selRect = { x1: Math.min(sel.startX, endX), y1: Math.min(sel.startY, endY), x2: Math.max(sel.startX, endX), y2: Math.max(sel.startY, endY) };

  showZoomMenu(e.clientX, e.clientY, sel.cardId, sel.chart, selRect, sel.overlay);

});



const zoomMenuEl = document.getElementById('zoomMenu');

let _pendingZoom = null;



function showZoomMenu(clientX, clientY, cardId, chart, selRect, overlay) {

  _pendingZoom = { cardId, chart, selRect, overlay };

  zoomMenuEl.style.left = clientX + 'px'; zoomMenuEl.style.top = clientY + 'px';

  zoomMenuEl.classList.add('show');

}

function hideZoomMenu() {

  zoomMenuEl.classList.remove('show');

  if (_pendingZoom && _pendingZoom.overlay) _pendingZoom.overlay.remove();

  _pendingZoom = null;

}

document.addEventListener('mousedown', function (e) { if (!zoomMenuEl.contains(e.target)) hideZoomMenu(); });



zoomMenuEl.querySelectorAll('.zoom-menu-item').forEach(item => {

  item.addEventListener('click', function () {

    if (!_pendingZoom) { hideZoomMenu(); return; }

    const action = this.dataset.action;

    const { cardId, chart, selRect } = _pendingZoom;

    if (action === 'reset') resetZoom(cardId);

    else if (action === 'fitsel') fitSelectionOnly(cardId, chart, selRect);

    else if (action === 'exclsel') excludeSelection(cardId, chart, selRect);

    else applyZoom(chart, selRect, action, cardId);

    hideZoomMenu();

  });

});



function applyZoom(chart, rect, mode, cardId) {

  const p1 = chart.convertFromPixel('grid', [rect.x1, rect.y1]);

  const p2 = chart.convertFromPixel('grid', [rect.x2, rect.y2]);

  if (!p1 || !p2) return;

  const xMin = Math.min(p1[0], p2[0]), xMax = Math.max(p1[0], p2[0]), yMin = Math.min(p1[1], p2[1]), yMax = Math.max(p1[1], p2[1]);

  const opts = {};

  if (mode === 'box' || mode === 'x') opts.xAxis = { min: xMin, max: xMax };

  if (mode === 'box' || mode === 'y') opts.yAxis = { min: yMin, max: yMax };

  chart.setOption(opts);

  cardZoomState[cardId] = { xMin: (mode === 'box' || mode === 'x') ? xMin : null, xMax: (mode === 'box' || mode === 'x') ? xMax : null, yMin: (mode === 'box' || mode === 'y') ? yMin : null, yMax: (mode === 'box' || mode === 'y') ? yMax : null };

  setResetZoomButtonsVisible(cardId, true);
  
  // Sync to parallel chart instances depending on which triggered the zoom
  const fullId = 'fullChart-' + cardId;
  const miniChart = chartInstances[cardId];
  const fullChart = chartInstances[fullId];
  if (chart === miniChart && fullChart) fullChart.setOption(opts);
  if (chart === fullChart && miniChart) miniChart.setOption(opts);

}



function resetZoom(cardId) {

  const chart = chartInstances[cardId], origOpt = cardOptions[cardId];

  if (chart && origOpt) chart.setOption({ xAxis: { min: origOpt.xAxis.min || null, max: origOpt.xAxis.max || null }, yAxis: { min: origOpt.yAxis.min || null, max: origOpt.yAxis.max || null } });
  
  const fullChart = chartInstances['fullChart-' + cardId];
  if (fullChart && origOpt) fullChart.setOption({ xAxis: { min: origOpt.xAxis.min || null, max: origOpt.xAxis.max || null }, yAxis: { min: origOpt.yAxis.min || null, max: origOpt.yAxis.max || null } });

  delete cardZoomState[cardId];

  setResetZoomButtonsVisible(cardId, false);

}



function resetAll(cardId) {

  cardExclusions[cardId] = new Set();

  delete cardZoomState[cardId];

  setResetZoomButtonsVisible(cardId, false);

  const ra = document.getElementById('resetAll-' + cardId); if (ra) ra.classList.remove('show');

  runSingleDCA(cardId);

}



/* Fit curve to selected area only */

function fitSelectionOnly(cardId, chart, selRect) {

  const lastData = cardLastData[cardId];

  if (!lastData || !lastData.wells || !lastData.wells[0]) return;

  const w = lastData.wells[0];

  const p1 = chart.convertFromPixel('grid', [selRect.x1, selRect.y1]);

  const p2 = chart.convertFromPixel('grid', [selRect.x2, selRect.y2]);

  if (!p1 || !p2) return;



  const isDate = w.is_date || false;

  let xMin, xMax, yMin, yMax;

  if (isDate) {

    xMin = Math.floor(Math.min(p1[0], p2[0]));

    xMax = Math.ceil(Math.max(p1[0], p2[0]));

    yMin = Math.min(p1[1], p2[1]);

    yMax = Math.max(p1[1], p2[1]);

  } else {

    xMin = Math.min(p1[0], p2[0]); xMax = Math.max(p1[0], p2[0]);

    yMin = Math.min(p1[1], p2[1]); yMax = Math.max(p1[1], p2[1]);

  }



  const newExcl = new Set();

  for (let i = 0; i < w.x.length; i++) {

    let xVal, yVal = w.y_actual[i];

    if (isDate) { xVal = parseDateStr(w.x[i]); } else { xVal = w.x[i]; }

    if (xVal < xMin || xVal > xMax || yVal < yMin || yVal > yMax) {

      newExcl.add(i);

    }

  }

  cardExclusions[cardId] = newExcl;

  saveZoomState(cardId);

  runSingleDCA(cardId);

}



/* Exclude selected points */

function excludeSelection(cardId, chart, selRect) {

  const lastData = cardLastData[cardId];

  if (!lastData || !lastData.wells || !lastData.wells[0]) return;

  const w = lastData.wells[0];

  const p1 = chart.convertFromPixel('grid', [selRect.x1, selRect.y1]);

  const p2 = chart.convertFromPixel('grid', [selRect.x2, selRect.y2]);

  if (!p1 || !p2) return;



  const isDate = w.is_date || false;

  let xMin, xMax, yMin, yMax;

  if (isDate) {

    xMin = Math.floor(Math.min(p1[0], p2[0]));

    xMax = Math.ceil(Math.max(p1[0], p2[0]));

    yMin = Math.min(p1[1], p2[1]);

    yMax = Math.max(p1[1], p2[1]);

  } else {

    xMin = Math.min(p1[0], p2[0]); xMax = Math.max(p1[0], p2[0]);

    yMin = Math.min(p1[1], p2[1]); yMax = Math.max(p1[1], p2[1]);

  }



  if (!cardExclusions[cardId]) cardExclusions[cardId] = new Set();

  const existing = cardExclusions[cardId];



  for (let i = 0; i < w.x.length; i++) {

    let xVal, yVal = w.y_actual[i];

    if (isDate) { xVal = parseDateStr(w.x[i]); } else { xVal = w.x[i]; }

    if (xVal >= xMin && xVal <= xMax && yVal >= yMin && yVal <= yMax) {

      existing.add(i);

    }

  }

  saveZoomState(cardId);

  runSingleDCA(cardId);

}



/* ====================================================================

   Download Chart

   ==================================================================== */

function downloadChart(cardId, format) {

  const chart = chartInstances[cardId];

  if (!chart) return;

  const card = document.getElementById(cardId);

  const title = card?.querySelector('.p-title')?.value || card?.querySelector('.p-well')?.value || 'chart';

  const filename = title.replace(/[^a-zA-Z0-9_-]/g, '_');



  if (format === 'png' || format === 'jpg') {

    const dataUrl = chart.getDataURL({ type: format === 'jpg' ? 'jpeg' : 'png', pixelRatio: 2, backgroundColor: '#fff' });

    const a = document.createElement('a');

    a.href = dataUrl;

    a.download = `${filename}.${format}`;

    a.click();

  } else if (format === 'pdf') {

    try {

      const { jsPDF } = window.jspdf;

      const doc = new jsPDF('landscape', 'mm', 'a4');

      const dataUrl = chart.getDataURL({ type: 'png', pixelRatio: 2, backgroundColor: '#fff' });

      doc.addImage(dataUrl, 'PNG', 10, 15, 277, 150);

      doc.save(`${filename}.pdf`);

    } catch (e) {

      alert('PDF export failed. jsPDF library may not be loaded.');

    }

  }

}



/* ====================================================================

   Chart Context Menu & Interactive Features

   ==================================================================== */

function reRenderChart(cardId) {

  if (cardLastData[cardId]) {

    saveZoomState(cardId);

    const card = document.getElementById(cardId);

    const months = card?.querySelector('.p-forecast')?.value || 0;

    renderSingleChart(cardId, cardLastData[cardId], months);

    const fullId = 'fullChart-' + cardId;
    if (chartInstances[fullId]) {
      const fullChart = chartInstances[fullId];
      const origOpts = chartInstances[cardId].getOption();
      if (origOpts) {
        fullChart.setOption(origOpts, true);
      }
    }

  }

}



/* --- Context Menu Show/Hide --- */

document.addEventListener('contextmenu', function (e) {
  const chartDiv = e.target.closest('.mini-chart, .modal-full-view-chart');
  if (!chartDiv) return;

  e.preventDefault();

  let cardId;
  let chart;

  if (chartDiv.classList.contains('modal-full-view-chart')) {
    const fullId = chartDiv.id;
    cardId = fullId.replace('fullChart-', '');
    chart = chartInstances[fullId];
  } else {
    cardId = chartDiv.id.replace('chart-', '');
    chart = chartInstances[cardId];
  }

  if (!chart) return;

  const rect = chartDiv.getBoundingClientRect();

  const px = e.clientX - rect.left, py = e.clientY - rect.top;

  const opt = chart.getOption();

  const xType = opt.xAxis[0].type;

  let dataCoord;

  if (xType === 'category') {

    dataCoord = chart.convertFromPixel({ seriesIndex: 0 }, [px, py]);

  } else {

    dataCoord = chart.convertFromPixel('grid', [px, py]);

  }

  if (!dataCoord) return;

  const catIdx = Math.round(dataCoord[0]);

  _ctxMenuCardId = cardId;

  _ctxMenuCoord = {

    x: dataCoord[0], y: dataCoord[1],

    xType: xType,

    xLabel: xType === 'category' ? (opt.xAxis[0].data[catIdx] || catIdx) : (xType === 'time' ? formatDateTs(dataCoord[0]) : dataCoord[0])

  };

  /* Update toggle states in menu */

  const menu = document.getElementById('chartCtxMenu');

  const isLog = cardLogScale[cardId] || false;

  const logItem = menu.querySelector('[data-action="log-scale"]');

  if (logItem) logItem.innerHTML = '<span class="ccm-icon">㏒</span> ' + (isLog ? '✓ Y-Axis Log Scale' : '　Y-Axis Log Scale');

  const showPct = cardPctChange[cardId] || false;

  const pctItem = menu.querySelector('[data-action="pct-change"]');

  if (pctItem) pctItem.innerHTML = '<span class="ccm-icon">Δ</span> ' + (showPct ? '✓ Show % Change & Diff' : '　Show % Change & Diff');

  const labelPos = cardValueLabels[cardId] || 'none';

  menu.querySelectorAll('.ccm-label-opt').forEach(el => {

    const pos = el.dataset.pos;

    el.textContent = (pos === labelPos ? '✓ ' : '　') + pos.charAt(0).toUpperCase() + pos.slice(1);

  });

  menu.style.left = e.clientX + 'px'; menu.style.top = e.clientY + 'px';

  menu.style.display = 'block';

  requestAnimationFrame(() => {

    const mr = menu.getBoundingClientRect();

    if (mr.right > window.innerWidth) menu.style.left = (e.clientX - mr.width) + 'px';

    if (mr.bottom > window.innerHeight) menu.style.top = (e.clientY - mr.height) + 'px';

  });

});



function hideChartCtxMenu() {

  const m = document.getElementById('chartCtxMenu'); if (m) m.style.display = 'none';

}

document.addEventListener('click', function (e) {

  if (!e.target.closest('#chartCtxMenu')) hideChartCtxMenu();

});



/* --- Context Menu Actions --- */

document.querySelectorAll('#chartCtxMenu > .ccm-item[data-action]').forEach(item => {

  item.addEventListener('click', function () {

    const action = this.dataset.action;

    if (!_ctxMenuCardId) { hideChartCtxMenu(); return; }

    if (action === 'add-hline') addUserLine(_ctxMenuCardId, 'h');

    else if (action === 'add-vline') addUserLine(_ctxMenuCardId, 'v');

    else if (action === 'add-annotation') addAnnotationPrompt(_ctxMenuCardId);

    else if (action === 'pct-change') togglePctChange(_ctxMenuCardId);

    else if (action === 'log-scale') toggleLogScale(_ctxMenuCardId);

    hideChartCtxMenu();

  });

});

document.querySelectorAll('.ccm-label-opt').forEach(item => {

  item.addEventListener('click', function () {

    if (!_ctxMenuCardId) return;

    cardValueLabels[_ctxMenuCardId] = this.dataset.pos;

    reRenderChart(_ctxMenuCardId);

    hideChartCtxMenu();

  });

});



/* --- Feature 1: User Horizontal/Vertical Lines --- */

function addUserLine(cardId, type) {

  if (!_ctxMenuCoord) return;

  if (!cardUserLines[cardId]) cardUserLines[cardId] = [];

  const value = type === 'h' ? _ctxMenuCoord.y : _ctxMenuCoord.x;

  const displayVal = type === 'h'

    ? (typeof value === 'number' ? value.toFixed(2) : value)

    : (_ctxMenuCoord.xType === 'category' ? _ctxMenuCoord.xLabel : (_ctxMenuCoord.xType === 'time' ? formatDateTs(value) : (typeof value === 'number' ? value.toFixed(2) : value)));

  const id = Date.now() + Math.floor(Math.random() * 1000);

  const hue = Math.floor(Math.random() * 360);

  const color = `hsl(${hue}, 70%, 55%)`;

  const name = (type === 'h' ? 'H: ' : 'V: ') + displayVal;

  cardUserLines[cardId].push({ id, type, value, color, name, xType: _ctxMenuCoord.xType });

  reRenderChart(cardId);

}



/* Legend click → color picker for user lines */

function setupLegendColorPicker(cardId, myChart) {

  myChart.on('legendselectchanged', function (params) {

    const name = params.name;

    const lines = cardUserLines[cardId] || [];

    const line = lines.find(l => l.name === name);

    if (!line) return;

    /* Re-select the legend (prevent hide) */

    myChart.dispatchAction({ type: 'legendSelect', name: name });

    showLineColorPicker(cardId, line);

  });

}



function showLineColorPicker(cardId, line) {

  const popup = document.getElementById('lineColorPopup');

  const input = document.getElementById('lineColorInput');

  input.value = rgbToHex(line.color);

  const chartDiv = document.getElementById('chart-' + cardId);

  const rect = chartDiv.getBoundingClientRect();

  popup.style.left = (rect.left + rect.width / 2 - 80) + 'px';

  popup.style.top = (rect.top + 30) + 'px';

  popup.style.display = 'flex';

  popup._lineId = line.id;

  popup._cardId = cardId;

}



function rgbToHex(c) {

  if (c.startsWith('#')) return c;

  const el = document.createElement('span');

  el.style.color = c; document.body.appendChild(el);

  const computed = getComputedStyle(el).color;

  el.remove();

  const m = computed.match(/\d+/g);

  if (!m) return '#ff0000';

  return '#' + m.slice(0, 3).map(n => parseInt(n).toString(16).padStart(2, '0')).join('');

}



document.getElementById('lineColorOk').addEventListener('click', function () {

  const popup = document.getElementById('lineColorPopup');

  const lines = cardUserLines[popup._cardId] || [];

  const line = lines.find(l => l.id === popup._lineId);

  if (line) { line.color = document.getElementById('lineColorInput').value; reRenderChart(popup._cardId); }

  popup.style.display = 'none';

});

document.getElementById('lineColorDel').addEventListener('click', function () {

  const popup = document.getElementById('lineColorPopup');

  if (cardUserLines[popup._cardId]) {

    cardUserLines[popup._cardId] = cardUserLines[popup._cardId].filter(l => l.id !== popup._lineId);

    reRenderChart(popup._cardId);

  }

  popup.style.display = 'none';

});



/* --- Feature 2: Annotations --- */

function addAnnotationPrompt(cardId) {

  if (!_ctxMenuCoord) return;

  const text = prompt('Enter annotation text:');

  if (!text) return;

  if (!cardAnnotations[cardId]) cardAnnotations[cardId] = [];

  cardAnnotations[cardId].push({

    id: Date.now() + Math.floor(Math.random() * 1000),

    x: _ctxMenuCoord.x,

    y: _ctxMenuCoord.y,

    xLabel: _ctxMenuCoord.xLabel,

    xType: _ctxMenuCoord.xType,

    text, fontSize: 12,

    color: document.documentElement.getAttribute('data-theme') === 'light' ? '#0f172a' : '#e2e8f0'

  });

  reRenderChart(cardId);

}



function showAnnotationEditor(cardId, annIdx, evt) {

  const popup = document.getElementById('annotationPopup');

  const anns = cardAnnotations[cardId] || [];

  const ann = anns[annIdx];

  if (!ann) return;

  document.getElementById('annotText').value = ann.text;

  document.getElementById('annotFontSize').value = ann.fontSize || 12;

  document.getElementById('annotFontSizeVal').textContent = ann.fontSize || 12;

  document.getElementById('annotColor').value = ann.color || (document.documentElement.getAttribute('data-theme') === 'light' ? '#0f172a' : '#e2e8f0');

  const x = evt ? (evt.offsetX || evt.clientX || 300) : 300;

  const y = evt ? (evt.offsetY || evt.clientY || 200) : 200;

  popup.style.left = x + 'px'; popup.style.top = y + 'px';

  popup.style.display = 'block';

  popup._cardId = cardId; popup._annIdx = annIdx;

  document.getElementById('annotText').focus();

}



document.getElementById('annotFontSize').addEventListener('input', function () {

  document.getElementById('annotFontSizeVal').textContent = this.value;

});

document.getElementById('annotOk').addEventListener('click', function () {

  const popup = document.getElementById('annotationPopup');

  const anns = cardAnnotations[popup._cardId] || [];

  if (anns[popup._annIdx]) {

    anns[popup._annIdx].text = document.getElementById('annotText').value;

    anns[popup._annIdx].fontSize = parseInt(document.getElementById('annotFontSize').value);

    anns[popup._annIdx].color = document.getElementById('annotColor').value;

    reRenderChart(popup._cardId);

  }

  popup.style.display = 'none';

});

document.getElementById('annotDel').addEventListener('click', function () {

  const popup = document.getElementById('annotationPopup');

  if (cardAnnotations[popup._cardId]) {

    cardAnnotations[popup._cardId].splice(popup._annIdx, 1);

    reRenderChart(popup._cardId);

  }

  popup.style.display = 'none';

});

/* Close popups on outside click */

document.addEventListener('mousedown', function (e) {

  if (!e.target.closest('#lineColorPopup')) document.getElementById('lineColorPopup').style.display = 'none';

  if (!e.target.closest('#annotationPopup') && !e.target.closest('.mini-chart, .modal-full-view-chart')) document.getElementById('annotationPopup').style.display = 'none';

});



/* --- Feature 4: % Change & Difference --- */

function togglePctChange(cardId) {

  cardPctChange[cardId] = !cardPctChange[cardId];

  reRenderChart(cardId);

}



/* --- Feature 5: Log Scale Y-axis --- */

function toggleLogScale(cardId) {

  cardLogScale[cardId] = !cardLogScale[cardId];

  reRenderChart(cardId);

}



/* --- Feature 6: Editable Axis Labels (double-click on axis) --- */

document.addEventListener('dblclick', function (e) {
  const chartDiv = e.target.closest('.mini-chart, .modal-full-view-chart');
  if (!chartDiv) return;

  let cardId;
  let chart;
  if (chartDiv.classList.contains('modal-full-view-chart')) {
    const fullId = chartDiv.id;
    cardId = fullId.replace('fullChart-', '');
    chart = chartInstances[fullId];
  } else {
    cardId = chartDiv.id.replace('chart-', '');
    chart = chartInstances[cardId];
  }

  if (!chart) return;

  const rect = chartDiv.getBoundingClientRect();

  const px = e.clientX - rect.left, py = e.clientY - rect.top;

  const h = rect.height, w = rect.width;

  const pos = cardAxisPositions[cardId] || { x: 'bottom', y: 'left' };

  /* If click is near X axis area */

  if ((pos.x === 'bottom' && py > h - 70) || (pos.x === 'top' && py < 70 && py > 30)) { showAxisLabelInput(cardId, 'x', e.clientX, e.clientY); return; }

  /* If click is near Y axis area */

  if ((pos.y === 'left' && px < 65) || (pos.y === 'right' && px > w - 65)) { showAxisLabelInput(cardId, 'y', e.clientX, e.clientY); return; }

});



function showAxisLabelInput(cardId, axis, cx, cy) {

  const data = cardLastData[cardId];

  const defaultLabel = data ? (axis === 'x' ? data.x_label : data.y_label) : '';

  const current = (cardAxisLabels[cardId] && cardAxisLabels[cardId][axis]) || defaultLabel;

  const input = document.getElementById('axisLabelInput');

  input.value = current;

  input.style.left = cx + 'px'; input.style.top = cy + 'px';

  input.style.display = 'block';

  input._cardId = cardId; input._axis = axis;

  input.focus(); input.select();

}



document.getElementById('axisLabelInput').addEventListener('keydown', function (e) {

  if (e.key === 'Enter') { finalizeAxisLabel(this); }

  if (e.key === 'Escape') { this.style.display = 'none'; }

});

document.getElementById('axisLabelInput').addEventListener('blur', function () {

  finalizeAxisLabel(this);

});



function finalizeAxisLabel(input) {

  if (input.style.display === 'none') return;

  const cardId = input._cardId, axis = input._axis;

  if (!cardAxisLabels[cardId]) cardAxisLabels[cardId] = {};

  cardAxisLabels[cardId][axis] = input.value.trim();

  input.style.display = 'none';

  reRenderChart(cardId);

}


/* ====================================================================
   Axis Hover Toolbar – Zoom, Fit, Range, Move
   ==================================================================== */
const _axisToolbar = { cardId: null, chartKey: null, axis: null, hideTimer: null };

function setupAxisHoverTooltip(cardId, chartDiv) {
  if (chartDiv._axisHoverBound) return;
  chartDiv._axisHoverBound = true;
  const tb = document.getElementById('axisHoverToolbar');

  chartDiv.addEventListener('mousemove', function (e) {
    const isFullView = chartDiv.classList.contains('modal-full-view-chart');
    const chartKey = isFullView ? chartDiv.id : cardId;
    const chart = chartInstances[chartKey];
    if (!chart) return;
    if (tb.classList.contains('show') && _axisToolbar.chartKey === chartKey) return; // don't reposition while open
    const rect = chartDiv.getBoundingClientRect();
    const px = e.clientX - rect.left, py = e.clientY - rect.top;
    const h = rect.height, w = rect.width;
    const pos = cardAxisPositions[cardId] || { x: 'bottom', y: 'left' };
    let hitAxis = null;

    if (pos.x === 'bottom' && py > h - 70 && px > 50 && px < w - 30) hitAxis = 'x';
    if (pos.x === 'top' && py < 70 && py > 30 && px > 50 && px < w - 30) hitAxis = 'x';
    if (!hitAxis && pos.y === 'left' && px < 65 && py > 30 && py < h - 40) hitAxis = 'y';
    if (!hitAxis && pos.y === 'right' && px > w - 65 && py > 30 && py < h - 40) hitAxis = 'y';

    if (hitAxis) {
      _showAxisToolbar(cardId, chartKey, hitAxis, e.clientX, e.clientY);
    } else {
      if (!tb.matches(':hover')) _hideAxisToolbar();
    }
  });

  chartDiv.addEventListener('mouseleave', function () {
    const tb = document.getElementById('axisHoverToolbar');
    const rp = document.getElementById('axisRangePopup');
    if (!tb.matches(':hover') && rp.style.display === 'none') _hideAxisToolbar();
  });
}

function _showAxisToolbar(cardId, chartKey, axis, cx, cy) {
  const tb = document.getElementById('axisHoverToolbar');
  _axisToolbar.cardId = cardId;
  _axisToolbar.chartKey = chartKey;
  _axisToolbar.axis = axis;
  clearTimeout(_axisToolbar.hideTimer);
  /* Position: for Y axis place it to the right of cursor, for X axis place it above */
  if (axis === 'y') {
    tb.style.left = (cx + 14) + 'px';
    tb.style.top = (cy - 60) + 'px';
  } else {
    tb.style.left = (cx - 14) + 'px';
    tb.style.top = (cy - 170) + 'px';
  }
  /* Update move button icon based on current position */
  const pos = cardAxisPositions[cardId] || { x: 'bottom', y: 'left' };
  const moveBtn = tb.querySelector('[data-action="move"]');
  if (axis === 'x') {
    moveBtn.title = 'Move axis to ' + (pos.x === 'bottom' ? 'top' : 'bottom');
    moveBtn.innerHTML = pos.x === 'bottom'
      ? '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/></svg>'
      : '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><polyline points="19 12 12 19 5 12"/></svg>';
  } else {
    moveBtn.title = 'Move axis to ' + (pos.y === 'left' ? 'right' : 'left');
    moveBtn.innerHTML = pos.y === 'left'
      ? '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>'
      : '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="19" y1="12" x2="5" y2="12"/><polyline points="12 19 5 12 12 5"/></svg>';
  }
  tb.classList.add('show');
}

function _hideAxisToolbar() {
  const tb = document.getElementById('axisHoverToolbar');
  const rp = document.getElementById('axisRangePopup');
  _axisToolbar.hideTimer = setTimeout(() => {
    if (rp.style.display !== 'none') return; // keep open while range popup is shown
    tb.classList.remove('show');
    _axisToolbar.cardId = null;
    _axisToolbar.chartKey = null;
    _axisToolbar.axis = null;
  }, 250);
}

/* --- Axis toolbar: zoom in/out on a single axis --- */
function _axisZoom(cardId, axis, factor, chartKey) {
  const activeKey = chartKey || cardId;
  const chart = chartInstances[activeKey];
  if (!chart) return;
  const grid = chart.getModel().getComponent('grid', 0);
  if (!grid) return;
  const rect = grid.coordinateSystem.getRect();

  let curMin, curMax;
  if (axis === 'x') {
    curMin = chart.convertFromPixel({ xAxisIndex: 0 }, rect.x);
    curMax = chart.convertFromPixel({ xAxisIndex: 0 }, rect.x + rect.width);
  } else {
    // Y pixel increases downwards
    curMin = chart.convertFromPixel({ yAxisIndex: 0 }, rect.y + rect.height);
    curMax = chart.convertFromPixel({ yAxisIndex: 0 }, rect.y);
  }

  if (curMin == null || curMax == null || isNaN(curMin) || isNaN(curMax)) return;

  const range = curMax - curMin;
  const center = (curMin + curMax) / 2;
  const newRange = range * factor;
  const mn = center - newRange / 2;
  const mx = center + newRange / 2;

  const setObj = {};
  if (axis === 'x') setObj.xAxis = { min: mn, max: mx };
  else setObj.yAxis = { min: mn, max: mx };
  chart.setOption(setObj);
  const fullId = 'fullChart-' + cardId;
  const miniChart = chartInstances[cardId];
  const fullChart = chartInstances[fullId];
  if (chart === miniChart && fullChart) fullChart.setOption(setObj);
  if (chart === fullChart && miniChart) miniChart.setOption(setObj);

  /* Save zoom state */
  if (!cardZoomState[cardId]) cardZoomState[cardId] = {};
  if (axis === 'x') { cardZoomState[cardId].xMin = mn; cardZoomState[cardId].xMax = mx; }
  else { cardZoomState[cardId].yMin = mn; cardZoomState[cardId].yMax = mx; }
  setResetZoomButtonsVisible(cardId, true);
}

/* --- Axis toolbar: fit single axis to data --- */
function _axisFit(cardId, axis, chartKey) {
  const activeKey = chartKey || cardId;
  const chart = chartInstances[activeKey], origOpt = cardOptions[cardId];
  if (!chart || !origOpt) return;
  const setObj = {};
  if (axis === 'x') {
    setObj.xAxis = { min: origOpt.xAxis.min || null, max: origOpt.xAxis.max || null };
    if (cardZoomState[cardId]) { delete cardZoomState[cardId].xMin; delete cardZoomState[cardId].xMax; }
  } else {
    setObj.yAxis = { min: origOpt.yAxis.min || null, max: origOpt.yAxis.max || null };
    if (cardZoomState[cardId]) { delete cardZoomState[cardId].yMin; delete cardZoomState[cardId].yMax; }
  }
  chart.setOption(setObj);
  const fullId = 'fullChart-' + cardId;
  const miniChart = chartInstances[cardId];
  const fullChart = chartInstances[fullId];
  if (chart === miniChart && fullChart) fullChart.setOption(setObj);
  if (chart === fullChart && miniChart) miniChart.setOption(setObj);
  /* Hide reset button if no zoom remaining */
  const z = cardZoomState[cardId];
  if (!z || (z.xMin == null && z.xMax == null && z.yMin == null && z.yMax == null)) {
    delete cardZoomState[cardId];
    setResetZoomButtonsVisible(cardId, false);
  }
}

/* --- Axis toolbar: show range popup --- */
function _axisShowRange(cardId, axis, chartKey) {
  const activeKey = chartKey || cardId;
  const chart = chartInstances[activeKey];
  if (!chart) return;
  const tb = document.getElementById('axisHoverToolbar');
  const popup = document.getElementById('axisRangePopup');

  let curMin, curMax;
  const grid = chart.getModel().getComponent('grid', 0);
  if (grid) {
    const rect = grid.coordinateSystem.getRect();
    if (axis === 'x') {
      curMin = chart.convertFromPixel({ xAxisIndex: 0 }, rect.x);
      curMax = chart.convertFromPixel({ xAxisIndex: 0 }, rect.x + rect.width);
    } else {
      curMin = chart.convertFromPixel({ yAxisIndex: 0 }, rect.y + rect.height);
      curMax = chart.convertFromPixel({ yAxisIndex: 0 }, rect.y);
    }
  }

  document.getElementById('axisRangeMin').value = (curMin != null && !isNaN(curMin)) ? +parseFloat(curMin).toFixed(4) : '';
  document.getElementById('axisRangeMax').value = (curMax != null && !isNaN(curMax)) ? +parseFloat(curMax).toFixed(4) : '';
  popup._cardId = cardId;
  popup._chartKey = activeKey;
  popup._axis = axis;
  /* Position near toolbar */
  const tbRect = tb.getBoundingClientRect();
  popup.style.left = (tbRect.right + 6) + 'px';
  popup.style.top = tbRect.top + 'px';
  popup.style.display = 'block';
}

/* --- Axis toolbar: move axis to opposite side --- */
function _axisMoveToOpposite(cardId, axis) {
  if (!cardAxisPositions[cardId]) cardAxisPositions[cardId] = { x: 'bottom', y: 'left' };
  if (axis === 'x') {
    cardAxisPositions[cardId].x = cardAxisPositions[cardId].x === 'bottom' ? 'top' : 'bottom';
  } else {
    cardAxisPositions[cardId].y = cardAxisPositions[cardId].y === 'left' ? 'right' : 'left';
  }
  reRenderChart(cardId);
}

/* --- Wire up axis toolbar buttons and range popup --- */
(function () {
  const tb = document.getElementById('axisHoverToolbar');
  const rp = document.getElementById('axisRangePopup');

  tb.addEventListener('mouseenter', function () { clearTimeout(_axisToolbar.hideTimer); });
  tb.addEventListener('mouseleave', function () {
    if (rp.style.display !== 'none') return;
    _hideAxisToolbar();
  });

  tb.querySelectorAll('.axis-tb-btn').forEach(btn => {
    btn.addEventListener('click', function (e) {
      e.stopPropagation();
      const action = this.dataset.action;
      const cardId = _axisToolbar.cardId, chartKey = _axisToolbar.chartKey, axis = _axisToolbar.axis;
      if (!cardId || !axis) return;
      switch (action) {
        case 'zoom-in': _axisZoom(cardId, axis, 0.5, chartKey); break;
        case 'zoom-out': _axisZoom(cardId, axis, 2.0, chartKey); break;
        case 'fit': _axisFit(cardId, axis, chartKey); tb.classList.remove('show'); break;
        case 'range': _axisShowRange(cardId, axis, chartKey); break;
        case 'move': _axisMoveToOpposite(cardId, axis); tb.classList.remove('show'); break;
      }
    });
  });

  /* Range popup: Apply */
  document.getElementById('axisRangeOk').addEventListener('click', function () {
    const cardId = rp._cardId, chartKey = rp._chartKey, axis = rp._axis;
    if (!cardId) return;
    const activeKey = chartKey || cardId;
    const chart = chartInstances[activeKey];
    if (!chart) return;
    const mn = document.getElementById('axisRangeMin').value;
    const mx = document.getElementById('axisRangeMax').value;
    const setObj = {};
    const minVal = mn !== '' ? parseFloat(mn) : null;
    const maxVal = mx !== '' ? parseFloat(mx) : null;
    if (axis === 'x') setObj.xAxis = { min: minVal, max: maxVal };
    else setObj.yAxis = { min: minVal, max: maxVal };
    chart.setOption(setObj);
    const fullId = 'fullChart-' + cardId;
    const miniChart = chartInstances[cardId];
    const fullChart = chartInstances[fullId];
    if (chart === miniChart && fullChart) fullChart.setOption(setObj);
    if (chart === fullChart && miniChart) miniChart.setOption(setObj);
    if (!cardZoomState[cardId]) cardZoomState[cardId] = {};
    if (axis === 'x') { cardZoomState[cardId].xMin = minVal; cardZoomState[cardId].xMax = maxVal; }
    else { cardZoomState[cardId].yMin = minVal; cardZoomState[cardId].yMax = maxVal; }
    if (minVal != null || maxVal != null) {
      setResetZoomButtonsVisible(cardId, true);
    }
    rp.style.display = 'none';
    tb.classList.remove('show');
  });

  /* Range popup: Auto (reset) */
  document.getElementById('axisRangeReset').addEventListener('click', function () {
    const cardId = rp._cardId, chartKey = rp._chartKey, axis = rp._axis;
    if (!cardId) return;
    _axisFit(cardId, axis, chartKey);
    rp.style.display = 'none';
    tb.classList.remove('show');
  });

  /* Close range popup when clicking elsewhere */
  document.addEventListener('mousedown', function (e) {
    if (!rp.contains(e.target) && !tb.contains(e.target)) {
      rp.style.display = 'none';
    }
  });
})();


/* ====================================================================

   Data Editor

   ==================================================================== */

async function loadEditorPage(page) {

  try {

    const url = `/api/data?page=${page}&page_size=${EDITOR_PAGE_SIZE}` +

      (editorState.sortCol ? `&sort_col=${enc(editorState.sortCol)}&sort_asc=${editorState.sortAsc}` : '') +

      (editorState.filterCol ? `&filter_col=${enc(editorState.filterCol)}&filter_val=${enc(editorState.filterVal)}` : '');



    const res = await fetch(url);

    const data = await res.json();

    if (!res.ok) { document.getElementById('editorTableWrap').innerHTML = '<p style="color:var(--text-dim);padding:20px;">No data loaded. Import a file first.</p>'; return; }

    editorPage = data.page;

    // Sync editorState columns if changed

    uploadedColumns = data.columns;

    renderEditorTable(data);

    renderEditorPagination(data);

  } catch (e) {

    document.getElementById('editorTableWrap').innerHTML = '<p style="color:var(--text-dim);padding:20px;">No data loaded.</p>';

  }

}



function renderEditorTable(data) {

  const startRow = (data.page - 1) * data.page_size;

  let html = '<table><thead><tr><th>#</th>';

  data.columns.forEach(c => {

    const sortIcon = (editorState.sortCol === c) ? (editorState.sortAsc ? '▲' : '▼') : '';

    const filterIcon = (editorState.filterCol === c) ? '🔍' : '';

    html += `<th class="th-sortable" onclick="handleHeaderStart(event, '${c}', 'editor')">

      ${c} ${filterIcon} ${sortIcon}

      <span class="th-menu" onclick="event.stopPropagation(); showHeaderMenu(event, '${c}', 'editor')">⋮</span>

    </th>`;

  });

  html += '</tr></thead><tbody>';

  data.rows.forEach((r, idx) => {

    const absRow = startRow + idx;

    html += `<tr><td style="color:var(--text-dim)">${absRow}</td>`;

    data.columns.forEach(c => {

      html += `<td contenteditable="true" data-row="${absRow}" data-col="${c}" onblur="handleCellEdit(this)">${r[c] ?? ''}</td>`;

    });

    html += '</tr>';

  });

  html += '</tbody></table>';

  document.getElementById('editorTableWrap').innerHTML = html;

}



function renderEditorPagination(data) {

  const div = document.getElementById('editorPagination');

  div.innerHTML = `

    <button ${data.page <= 1 ? 'disabled' : ''} onclick="loadEditorPage(1)">«</button>

    <button ${data.page <= 1 ? 'disabled' : ''} onclick="loadEditorPage(${data.page - 1})">‹</button>

    <span>Page ${data.page} of ${data.total_pages} (${data.total.toLocaleString()} rows)</span>

    <button ${data.page >= data.total_pages ? 'disabled' : ''} onclick="loadEditorPage(${data.page + 1})">›</button>

    <button ${data.page >= data.total_pages ? 'disabled' : ''} onclick="loadEditorPage(${data.total_pages})">»</button>

  `;

}



async function handleCellEdit(td) {

  const row = parseInt(td.dataset.row);

  const col = td.dataset.col;

  const value = td.textContent.trim();

  try {

    await fetch('/api/data/update', {

      method: 'POST',

      headers: { 'Content-Type': 'application/json' },

      body: JSON.stringify({ row, column: col, value })

    });

  } catch (e) { /* silent */ }

}



function showAddColumnModal() {

  const mc = document.getElementById('modalContainer');

  mc.innerHTML = `

    <div class="modal-overlay" onclick="if(event.target===this)this.remove()">

      <div class="modal">

        <h3>Add Computed Column</h3>

        <div class="form-group"><label>Column Name</label><input type="text" id="newColName" placeholder="e.g. total_rate"></div>

        <div class="form-group"><label>Formula (use existing column names)</label><input type="text" id="newColFormula" placeholder="e.g. orate + wrate"></div>

        <p style="font-size:.72rem;color:var(--text-dim);margin-top:4px;">Supports: +, -, *, /, **, column names. Example: <code>orate / (orate + wrate) * 100</code></p>

        <div class="modal-actions">

          <button class="btn btn-outline btn-sm" onclick="this.closest('.modal-overlay').remove()">Cancel</button>

          <button class="btn btn-sm" onclick="doAddColumn()">Add Column</button>

        </div>

      </div>

    </div>`;

}



async function doAddColumn() {

  const name = document.getElementById('newColName').value.trim();

  const formula = document.getElementById('newColFormula').value.trim();

  if (!name || !formula) { alert('Please fill in both fields.'); return; }

  try {

    const res = await fetch('/api/data/add_column', {

      method: 'POST', headers: { 'Content-Type': 'application/json' },

      body: JSON.stringify({ name, formula })

    });

    const data = await res.json();

    if (!res.ok) throw new Error(data.detail || 'Error');

    uploadedColumns = data.columns;

    numericColumns = data.numeric_columns;

    populateSelectors();

    loadEditorPage(editorPage);

    document.querySelector('.modal-overlay')?.remove();

    showNotification(`Column "${name}" added`);

  } catch (e) { alert(e.message); }

}



function showDeleteColumnModal() {

  let opts = '';

  uploadedColumns.forEach(c => opts += `<option value="${c}">${c}</option>`);

  const mc = document.getElementById('modalContainer');

  mc.innerHTML = `

    <div class="modal-overlay" onclick="if(event.target===this)this.remove()">

      <div class="modal">

        <h3>Delete Column</h3>

        <div class="form-group"><label>Column to delete</label><select id="delColName">${opts}</select></div>

        <div class="modal-actions">

          <button class="btn btn-outline btn-sm" onclick="this.closest('.modal-overlay').remove()">Cancel</button>

          <button class="btn btn-sm" style="background:var(--red)" onclick="doDeleteColumn()">Delete</button>

        </div>

      </div>

    </div>`;

}



async function doDeleteColumn() {

  const col = document.getElementById('delColName').value;

  if (!col) return;

  try {

    const res = await fetch(`/api/data/column?column=${enc(col)}`, { method: 'DELETE' });

    const data = await res.json();

    if (!res.ok) throw new Error(data.detail || 'Error');

    uploadedColumns = data.columns;

    numericColumns = data.numeric_columns;

    populateSelectors();

    loadEditorPage(editorPage);

    document.querySelector('.modal-overlay')?.remove();

    showNotification(`Column "${col}" deleted`);

  } catch (e) { alert(e.message); }

}



async function exportCSV() {

  try {

    const res = await fetch('/api/data/export');

    const data = await res.json();

    if (!res.ok) throw new Error('Export failed');

    const blob = new Blob([data.csv], { type: 'text/csv' });

    const a = document.createElement('a');

    a.href = URL.createObjectURL(blob);

    a.download = 'data_export.csv';

    a.click();

  } catch (e) { alert(e.message); }

}



/* ====================================================================

   Save / Load Workspace

   ==================================================================== */

function saveWorkspaceToFile() {

  const currentPage = pages.find(p => p.id === activePageId);

  const state = {

    selX: document.getElementById('selX').value,

    selY: document.getElementById('selY').value,

    selWellCol: document.getElementById('selWellCol').value,

    pages: currentPage ? [currentPage] : [],

    activePageId: activePageId,

    cards: []

  };

  document.querySelectorAll('.plot-card').forEach(card => {

    if (card.dataset.page !== activePageId) return;

    const cardId = card.id;

    state.cards.push({

      page: activePageId,

      well: getSelectedWells(cardId),

      model: card.querySelector('.p-model')?.value || 'exponential',

      forecast: card.querySelector('.p-forecast')?.value || '0',

      title: card.querySelector('.p-title')?.value || '',

      combine: isCombineMode(cardId),

      combineAgg: getCombineAggMode(cardId),

      exclusions: [...(cardExclusions[cardId] || [])],

      styles: readCardStyles(cardId),

      zoom: cardZoomState[cardId] || null

    });

  });

  const blob = new Blob([JSON.stringify(state, null, 2)], { type: 'application/json' });

  const a = document.createElement('a');

  a.href = URL.createObjectURL(blob);

  a.download = `workspace_${currentPage ? currentPage.name.replace(/\s+/g, '_') : 'page'}.dcapro`;

  a.click();

  showNotification('Current page saved');

}



async function loadWorkspaceFromFile() {

  const input = document.createElement('input');

  input.type = 'file';

  input.accept = '.dcapro,.json';

  input.onchange = async () => {

    const file = input.files[0];

    if (!file) return;

    try {

      const text = await file.text();

      const state = JSON.parse(text);

      // Restore column selectors

      if (state.selX) document.getElementById('selX').value = state.selX;

      if (state.selY) document.getElementById('selY').value = state.selY;

      if (state.selWellCol) { document.getElementById('selWellCol').value = state.selWellCol; await fetchWells(); }

      // Clear existing cards and restore pages

      document.querySelectorAll('.plot-card').forEach(card => removeCard(card.id));

      pages.length = 0;

      // Support old "workspaces" format for backward compatibility

      const savedPages = state.pages || state.workspaces || [];

      if (savedPages.length > 0) {

        savedPages.forEach(pg => pages.push({ id: pg.id, name: pg.name }));

      } else {

        pages.push({ id: 'page-legacy', name: 'Page 1' });

      }

      activePageId = state.activePageId || state.activeWsId || pages[0].id;

      renderPageTabs();

      // Recreate cards in their pages

      for (const cs of (state.cards || [])) {

        const savedPage = activePageId;

        activePageId = cs.page || cs.workspace || pages[0].id;

        const newId = addPlotCard(cs.well, cs.model, cs.forecast, cs.title, cs.combine || false, '', cs.combineAgg || 'sum');

        if (cs.exclusions) cardExclusions[newId] = new Set(cs.exclusions);

        if (cs.styles) {
          const mergedStyles = { ...getDefaultStyles(), ...cs.styles };
          cardStyles[newId] = mergedStyles;
          applyStylesToCard(newId, mergedStyles);
        }

        if (cs.zoom) cardZoomState[newId] = cs.zoom;

        activePageId = savedPage;

        await runSingleDCA(newId);

      }

      switchPage(activePageId);

      showNotification('Workspace loaded');

      // Switch to DCA tab

      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));

      document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));

      document.querySelector('[data-tab="dca"]').classList.add('active');

      document.getElementById('tab-dca').classList.add('active');

    } catch (e) { alert('Failed to load workspace: ' + e.message); }

  };

  input.click();

}



/* ====================================================================

   Auto-Refresh (File Watcher) — Legacy (kept for refreshBar compat)

   ==================================================================== */

let autoRefreshTimer = null;



function toggleAutoRefresh(enabled) {

  if (autoRefreshTimer) { clearInterval(autoRefreshTimer); autoRefreshTimer = null; }

  if (enabled) {

    const ms = parseInt(document.getElementById('autoRefreshInterval').value) || 10000;

    autoRefreshTimer = setInterval(checkFileStatus, ms);

    document.getElementById('refreshStatus').textContent = 'Watching…';

  } else {

    document.getElementById('refreshStatus').textContent = '';

  }

}



async function checkFileStatus() {

  try {

    const res = await fetch('/api/file_status');

    const data = await res.json();

    if (data.modified) {

      await reloadData();

      showToast('Data auto-reloaded from disk', 'success');

    }

  } catch (e) { /* silent */ }

}



async function reloadData() {

  try {

    const res = await fetch('/api/reload');

    const data = await res.json();

    if (!res.ok) throw new Error(data.detail || 'Reload failed');

    applyUploadData(data);

    showToast('Data reloaded', 'success');

  } catch (e) { alert(e.message); }

}



/* ====================================================================

   Enhanced Toast Notification System

   ==================================================================== */

function getToastContainer() {

  let c = document.querySelector('.toast-container');

  if (!c) {

    c = document.createElement('div');

    c.className = 'toast-container';

    document.body.appendChild(c);

  }

  return c;

}



function showToast(msg, type = 'info', duration = 3500) {

  const container = getToastContainer();

  const icons = { success: '✓', error: '✗', info: 'ℹ' };

  const toast = document.createElement('div');

  toast.className = `toast ${type}`;

  toast.innerHTML = `<span class="toast-icon">${icons[type] || 'ℹ'}</span><span class="toast-msg">${msg}</span><span class="toast-close" onclick="this.parentElement.remove()">×</span>`;

  container.appendChild(toast);

  setTimeout(() => { toast.style.opacity = '0'; toast.style.transform = 'translateX(40px)'; setTimeout(() => toast.remove(), 300); }, duration);

}



// Backward-compat wrapper

function showNotification(msg) { showToast(msg, 'success'); }



/* ====================================================================

   Persistent Pipeline — Sync Engine

   ==================================================================== */



function showSyncPanel() {

  document.getElementById('syncPanel').style.display = '';

}



function updateSyncUI() {

  // Update timestamp display

  const tsEl = document.getElementById('syncTimestamp');

  if (lastImportTimestamp) {

    const d = new Date(lastImportTimestamp);

    tsEl.textContent = 'Last import: ' + d.toLocaleString();

  } else {

    tsEl.textContent = '';

  }

  // Update version badge

  const verEl = document.getElementById('syncVersionBadge');

  verEl.textContent = currentVersion > 0 ? `v${currentVersion}` : '';

  // Update status indicator

  updateSyncStatus();

}



function updateSyncStatus(status, text) {

  const indicator = document.getElementById('syncStatusIndicator');

  const textEl = document.getElementById('syncStatusText');

  if (status && text) {

    indicator.className = 'sync-status-indicator ' + status;

    textEl.textContent = text;

    return;

  }

  // Default based on syncMode

  if (isSyncing) {

    indicator.className = 'sync-status-indicator syncing';

    textEl.textContent = 'Syncing…';

  } else if (syncMode === 'auto') {

    indicator.className = 'sync-status-indicator watching';

    textEl.textContent = 'Watching for changes';

  } else if (syncMode === 'scheduled') {

    indicator.className = 'sync-status-indicator watching';

    const ms = parseInt(document.getElementById('syncIntervalSelect').value) || 600000;

    textEl.textContent = `Scheduled (every ${ms / 60000} min)`;

  } else {

    indicator.className = 'sync-status-indicator idle';

    textEl.textContent = 'Manual mode — click Build to refresh';

  }

}



function setSyncMode(mode) {

  syncMode = mode;

  // Clear existing timers

  if (syncScheduleTimer) { clearInterval(syncScheduleTimer); syncScheduleTimer = null; }

  if (autoSyncTimer) { clearInterval(autoSyncTimer); autoSyncTimer = null; }



  // Show/hide schedule config

  document.getElementById('syncScheduleConfig').style.display = mode === 'scheduled' ? 'flex' : 'none';



  if (mode === 'auto') {

    // Poll for file changes every 3 seconds using File System Access API

    autoSyncTimer = setInterval(autoSyncCheck, 3000);

  } else if (mode === 'scheduled') {

    updateSyncSchedule();

  }

  updateSyncStatus();

}



function updateSyncSchedule() {

  if (syncScheduleTimer) { clearInterval(syncScheduleTimer); syncScheduleTimer = null; }

  if (syncMode !== 'scheduled') return;

  const ms = parseInt(document.getElementById('syncIntervalSelect').value) || 600000;

  syncScheduleTimer = setInterval(scheduledSyncCheck, ms);

  updateSyncStatus();

}



async function autoSyncCheck() {

  // Use File System Access API handle to check lastModified

  if (!storedFileHandle || isSyncing) return;

  try {

    const perm = await storedFileHandle.queryPermission({ mode: 'read' });

    if (perm !== 'granted') return;

    const file = await storedFileHandle.getFile();

    if (file.lastModified > lastKnownModified) {

      lastKnownModified = file.lastModified;

      await performSync(file);

    }

  } catch (e) {

    // File might be locked or handle invalid — silent

    console.warn('Auto-sync check error:', e.message);

  }

}



async function scheduledSyncCheck() {

  if (isSyncing) return;

  // Try File System Access API first

  if (storedFileHandle) {

    try {

      const perm = await storedFileHandle.queryPermission({ mode: 'read' });

      if (perm === 'granted') {

        const file = await storedFileHandle.getFile();

        if (file.lastModified > lastKnownModified) {

          lastKnownModified = file.lastModified;

          await performSync(file);

          return;

        } else {

          // No changes

          return;

        }

      }

    } catch (e) { /* fall through to server-side check */ }

  }

  // Fallback: check server-side file_status

  try {

    const res = await fetch('/api/file_status');

    const data = await res.json();

    if (data.modified) {

      await reloadData();

      showToast('Data synced (scheduled)', 'success');

    }

  } catch (e) { /* silent */ }

}



async function triggerManualBuild() {

  if (isSyncing) { showToast('Sync already in progress', 'info'); return; }

  // Try File System Access API handle first

  if (storedFileHandle) {

    try {

      let perm = await storedFileHandle.queryPermission({ mode: 'read' });

      if (perm !== 'granted') {

        perm = await storedFileHandle.requestPermission({ mode: 'read' });

      }

      if (perm === 'granted') {

        const file = await storedFileHandle.getFile();

        lastKnownModified = file.lastModified;

        await performSync(file);

        return;

      }

    } catch (e) {

      console.warn('File handle error, falling back to server reload:', e.message);

    }

  }

  // Fallback: server-side reload from disk

  setSyncingUI(true);

  try {

    await reloadData();

    showToast('Build complete — data refreshed', 'success');

  } catch (e) {

    showToast('Build failed: ' + e.message, 'error');

  } finally {

    setSyncingUI(false);

  }

}



async function performSync(file) {

  if (isSyncing) return;

  setSyncingUI(true);

  const syncProgressWrap = document.getElementById('syncProgressWrap');

  const syncProgressBar = document.getElementById('syncProgressBar');

  const syncProgressText = document.getElementById('syncProgressText');

  syncProgressWrap.style.display = '';

  syncProgressBar.style.width = '10%';

  syncProgressText.textContent = 'Reading file…';



  try {

    const formData = new FormData();

    formData.append('file', file, file.name);



    syncProgressBar.style.width = '40%';

    syncProgressText.textContent = 'Uploading to server…';



    const res = await fetch('/api/sync/upload', { method: 'POST', body: formData });



    syncProgressBar.style.width = '80%';

    syncProgressText.textContent = 'Processing & replaying pipeline…';



    if (!res.ok) {

      const err = await res.json();

      throw new Error(err.detail || 'Sync failed');

    }



    const data = await res.json();

    syncProgressBar.style.width = '100%';

    syncProgressText.textContent = 'Complete!';



    // Apply the updated data (this also re-renders preview, selectors, etc.)

    applyUploadData(data);

    showToast(`Sync complete — v${data.version || currentVersion}, ${data.rows.toLocaleString()} rows`, 'success');



    // Re-run all DCA cards (pipeline replay for dashboard widgets)

    await rerunAllDCACards();



  } catch (e) {

    showToast('Sync failed: ' + e.message, 'error', 5000);

    updateSyncStatus('error', 'Sync failed');

  } finally {

    setTimeout(() => { syncProgressWrap.style.display = 'none'; }, 1500);

    setSyncingUI(false);

  }

}



function setSyncingUI(syncing) {

  isSyncing = syncing;

  const btn = document.getElementById('syncBuildBtn');

  if (syncing) {

    btn.disabled = true;

    btn.innerHTML = '<span class="loader"></span> Syncing…';

  } else {

    btn.disabled = false;

    btn.innerHTML = '⚡ Build';

  }

  updateSyncStatus();

}



async function grantFileAccess() {

  if (!window.showOpenFilePicker) {

    showToast('File System Access API not supported in this browser', 'error');

    return;

  }

  try {

    const [handle] = await window.showOpenFilePicker({

      types: [{ description: 'Data files', accept: { 'text/csv': ['.csv'], 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx', '.xls'] } }],

      multiple: false

    });

    storedFileHandle = handle;

    const file = await handle.getFile();

    lastKnownModified = file.lastModified;

    await saveFileHandleToIDB(handle, file.name);

    showToast(`File access granted: ${file.name}`, 'success');

    // If auto mode is active, restart watcher

    if (syncMode === 'auto' && !autoSyncTimer) {

      autoSyncTimer = setInterval(autoSyncCheck, 3000);

    }

  } catch (err) {

    if (err.name !== 'AbortError') showToast('Failed to get file access: ' + err.message, 'error');

  }

}



async function rerunAllDCACards() {

  // Re-run DCA for all existing plot cards to replay dashboard widgets

  const cards = document.querySelectorAll('.plot-card');

  for (const card of cards) {

    try {

      await runSingleDCA(card.id);

    } catch (e) {

      console.warn('DCA re-run failed for', card.id, e);

    }

  }

}



/* ====================================================================

   Version Control Panel

   ==================================================================== */

async function showVersionPanel() {

  try {

    const res = await fetch('/api/versions');

    if (!res.ok) throw new Error('Failed to load versions');

    const data = await res.json();

    const versions = data.versions || [];

    const current = data.current_version;



    let listHtml = '';

    if (versions.length === 0) {

      listHtml = '<div style="padding:16px;color:var(--text-dim);text-align:center;">No versions yet</div>';

    } else {

      versions.slice().reverse().forEach(v => {

        const ts = new Date(v.timestamp).toLocaleString();

        const isCurrent = v.version === current;

        listHtml += `<div class="version-item">

          <span class="ver-num">v${v.version}</span>

          <span class="ver-time">${ts}</span>

          <span class="ver-rows">${v.rows.toLocaleString()} rows × ${v.columns} cols</span>

          <span class="ver-actions">

            ${isCurrent ? '<span class="version-current">Current</span>' : `<button class="btn btn-outline btn-sm" onclick="rollbackToVersion(${v.version})">Rollback</button>`}

          </span>

        </div>`;

      });

    }



    const mc = document.getElementById('modalContainer');

    mc.innerHTML = `

      <div class="modal-overlay" onclick="if(event.target===this)this.remove()">

        <div class="modal" style="min-width:500px;">

          <h3>Version History</h3>

          <p style="font-size:.76rem;color:var(--text-dim);margin-bottom:12px;">Dataset: ${data.dataset_id || activeDatasetId || '—'} · ${versions.length} version(s) stored</p>

          <div class="version-list">${listHtml}</div>

          <div class="modal-actions" style="margin-top:16px;">

            <button class="btn btn-outline btn-sm" onclick="this.closest('.modal-overlay').remove()">Close</button>

          </div>

        </div>

      </div>`;

  } catch (e) {

    showToast('Failed to load versions: ' + e.message, 'error');

  }

}



async function rollbackToVersion(version) {

  if (!confirm(`Rollback to version ${version}? This will replace current data.`)) return;

  try {

    const res = await fetch(`/api/versions/rollback?version=${version}`, { method: 'POST' });

    if (!res.ok) {

      const err = await res.json();

      throw new Error(err.detail || 'Rollback failed');

    }

    const data = await res.json();

    applyUploadData(data);

    showToast(`Rolled back to v${version}`, 'success');

    // Close the modal

    document.querySelector('.modal-overlay')?.remove();

    // Re-run DCA cards

    await rerunAllDCACards();

  } catch (e) {

    showToast('Rollback failed: ' + e.message, 'error');

  }

}



/* ====================================================================

   Restore File Handle on Page Load

   ==================================================================== */

document.addEventListener('DOMContentLoaded', async () => {

  try {

    const saved = await loadFileHandleFromIDB();

    if (saved && saved.handle) {

      storedFileHandle = saved.handle;

      // Try to verify permission (won't prompt — just checks)

      const perm = await storedFileHandle.queryPermission({ mode: 'read' });

      if (perm === 'granted') {

        showToast(`File handle restored: ${saved.filename}`, 'info', 2500);

      }

    }

  } catch (e) {

    // IndexedDB or permission check failed — silent

    console.warn('Could not restore file handle:', e.message);

  }

});



/* ====================================================================

   Utility

   ==================================================================== */

function enc(s) { return encodeURIComponent(s); }

window.addEventListener('resize', () => Object.values(chartInstances).forEach(c => c && c.resize()));



/* ====================================================================

   Well Picker (searchable multi-select)

   ==================================================================== */



function populateWellPicker(cardId, selectedWells) {

  const wp = document.getElementById('wp-' + cardId);

  if (!wp) return;

  const list = wp.querySelector('.well-picker-list');

  list.innerHTML = '';

  allWells.forEach(w => {

    const label = document.createElement('label');

    label.className = 'well-picker-item';

    const cb = document.createElement('input');

    cb.type = 'checkbox';

    cb.value = w;

    cb.checked = selectedWells.includes(w);

    cb.addEventListener('change', () => updateWellPickerDisplay(cardId));

    label.appendChild(cb);

    label.appendChild(document.createTextNode(' ' + w));

    list.appendChild(label);

  });

  updateWellPickerDisplay(cardId);

}



function toggleWellPicker(cardId) {

  const wp = document.getElementById('wp-' + cardId);

  if (!wp) return;

  const dd = wp.querySelector('.well-picker-dropdown');

  const isOpen = dd.classList.contains('open');

  // Close all other pickers first

  document.querySelectorAll('.well-picker-dropdown.open').forEach(d => d.classList.remove('open'));

  if (!isOpen) {

    dd.classList.add('open');

    const search = dd.querySelector('.well-picker-search');

    if (search) { search.value = ''; filterWells(cardId, ''); setTimeout(() => search.focus(), 50); }

  }

}



function filterWells(cardId, query) {

  const wp = document.getElementById('wp-' + cardId);

  if (!wp) return;

  const items = wp.querySelectorAll('.well-picker-item');

  const q = query.toLowerCase();

  items.forEach(item => {

    const text = item.textContent.toLowerCase();

    item.style.display = text.includes(q) ? '' : 'none';

  });

}



function updateWellPickerDisplay(cardId) {

  const wp = document.getElementById('wp-' + cardId);

  if (!wp) return;

  const selected = getSelectedWells(cardId);

  const display = wp.querySelector('.well-picker-text');

  if (selected.length === 0) display.textContent = 'Select wells…';

  else if (selected.length <= 2) display.textContent = selected.join(', ');

  else display.textContent = selected.length + ' wells selected';

}



function getSelectedWells(cardId) {

  const wp = document.getElementById('wp-' + cardId);

  if (!wp) return [];

  return Array.from(wp.querySelectorAll('.well-picker-list input:checked')).map(cb => cb.value);

}



function isCombineMode(cardId) {

  const card = document.getElementById(cardId);

  if (!card) return false;

  return card.querySelector('.p-combine')?.checked || false;

}

function getCombineAggMode(cardId) {

  const card = document.getElementById(cardId);

  if (!card) return 'sum';

  const value = card.querySelector('.p-combine-agg')?.value || 'sum';

  const allowed = new Set(['sum', 'mean', 'median', 'min', 'max']);

  return allowed.has(value) ? value : 'sum';

}



// Close well picker when clicking outside

document.addEventListener('click', function (e) {

  if (!e.target.closest('.well-picker')) {

    document.querySelectorAll('.well-picker-dropdown.open').forEach(d => d.classList.remove('open'));

  }

});

/* ====================================================================
   Header Menu Logic (Sort/Filter/Remove/Copy)
   ==================================================================== */
let activeHeader = null; // { col, scope }

function handleHeaderStart(e, col, scope) {
  if (e.target.closest('.th-menu')) return;

  if (scope === 'editor') {
    if (editorState.sortCol === col) editorState.sortAsc = !editorState.sortAsc;
    else { editorState.sortCol = col; editorState.sortAsc = true; }
    loadEditorPage(editorPage);
  } else if (scope === 'preview') {
    handlePreviewSort(e, col);
  }
}

function showHeaderMenu(e, col, scope) {
  e.preventDefault();
  activeHeader = { col, scope };
  const menu = document.getElementById('headerMenu');

  // Position the menu
  menu.style.left = e.clientX + 'px';
  menu.style.top = e.clientY + 'px';
  menu.classList.add('show');

  // Set filter value if exists
  const input = document.getElementById('hmFilterInput');
  if (scope === 'editor') {
    input.value = (editorState.filterCol === col) ? editorState.filterVal : '';
  } else {
    input.value = (previewState.filterCol === col) ? previewState.filterVal : '';
  }

  document.getElementById('hmTitle').textContent = col;
  setTimeout(() => input.focus(), 50);
}

// Close Header Menu on outside click
document.addEventListener('click', function (e) {
  const menu = document.getElementById('headerMenu');
  if (!menu.contains(e.target)) {
    menu.classList.remove('show');
    activeHeader = null;
  }
});

// Header Menu Actions
document.querySelectorAll('.header-menu-item').forEach(item => {
  item.addEventListener('click', function (e) {
    if (!activeHeader) return;
    const action = this.dataset.action;
    const { col, scope } = activeHeader;

    if (action === 'sort-asc') {
      applySort(col, true, scope);
    } else if (action === 'sort-desc') {
      applySort(col, false, scope);
    } else if (action === 'copy') {
      copyColumnValues(col, scope);
    } else if (action === 'remove') {
      // For remove, we delete the column from the DF entirely via API
      if (confirm(`Remove column "${col}" from the dataset? This action cannot be undone.`)) {
        document.getElementById('delColName').value = col; // Hack reuse existing logic? No, create dedicated function.
        doDeleteColumnByName(col);
      }
    }

    if (action) document.getElementById('headerMenu').classList.remove('show');
  });
});

// Filter Input Logic
document.getElementById('hmFilterInput').addEventListener('keydown', function (e) {
  if (e.key === 'Enter') {
    if (!activeHeader) return;
    const val = this.value.trim();
    applyFilter(activeHeader.col, val, activeHeader.scope);
    document.getElementById('headerMenu').classList.remove('show');
  }
});

function applySort(col, asc, scope) {
  if (scope === 'editor') {
    editorState.sortCol = col;
    editorState.sortAsc = asc;
    loadEditorPage(editorPage);
  } else {
    previewState.sortCol = col;
    previewState.sortAsc = asc;
    vsCache = {};
    vsFetching.clear();
    buildVScrollHeader();
    renderVisibleRows();
  }
}

function applyFilter(col, val, scope) {
  if (scope === 'editor') {
    editorState.filterCol = val ? col : null;
    editorState.filterVal = val;
    loadEditorPage(1); // Reset to page 1 on filter change
  } else {
    previewState.filterCol = val ? col : null;
    previewState.filterVal = val;
    vsCache = {};
    vsFetching.clear();
    document.getElementById('vscrollContainer').scrollTop = 0;
    renderVisibleRows();
  }
}

function copyColumnValues(col, scope) {
  let values = [];
  if (scope === 'editor') {
    const cells = document.querySelectorAll(`#editorTableWrap td[data-col="${col}"]`);
    cells.forEach(td => values.push(td.textContent.trim()));
  } else {
    // Preview: copy visible rows from virtual scroll table
    const colIdx = vsColumns.indexOf(col);
    if (colIdx >= 0) {
      document.querySelectorAll('#vscrollBody tr').forEach(tr => {
        const cell = tr.children[colIdx + 1]; // +1 for row-idx column
        if (cell && !cell.classList.contains('row-idx') && cell.textContent !== '…') {
          values.push(cell.textContent.trim());
        }
      });
    }
  }

  if (values.length > 0) {
    navigator.clipboard.writeText(values.join('\n')).then(() => {
      showNotification(`Copied ${values.length} values from column "${col}"`);
    }).catch(err => alert('Failed to copy: ' + err));
  } else {
    showNotification('No values to copy');
  }
}

async function doDeleteColumnByName(col) {
  const overlay = document.querySelector('.modal-overlay');
  if (overlay) overlay.remove(); // Close any open modal just in case

  try {
    const res = await fetch(`/api/data/column?column=${enc(col)}`, { method: 'DELETE' });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Error');
    uploadedColumns = data.columns;
    numericColumns = data.numeric_columns;
    populateSelectors();

    // Update both views
    if (previewState.columns.includes(col)) {
      previewState.columns = previewState.columns.filter(c => c !== col);
      vsColumns = previewState.columns;
      vsCache = {};
      vsFetching.clear();
      renderPreview();
    }

    loadEditorPage(editorPage);
    showNotification(`Column "${col}" deleted`);
  } catch (e) { alert(e.message); }
}



/* ====================================================================

   Sortable Data Table

   ==================================================================== */

function renderSortableTable(cardId) {

  const ft = document.getElementById('forecastTable-' + cardId);

  if (!ft) return;

  const rows = cardTableData[cardId] || [];

  const sortState = cardTableSort[cardId] || { col: null, asc: true };



  // Sort rows if a sort column is set

  const sorted = [...rows];

  if (sortState.col) {

    sorted.sort((a, b) => {

      let va = a[sortState.col], vb = b[sortState.col];

      if (va == null && vb == null) return 0;

      if (va == null) return 1;

      if (vb == null) return -1;

      if (typeof va === 'string' && typeof vb === 'string') {

        return sortState.asc ? va.localeCompare(vb) : vb.localeCompare(va);

      }

      return sortState.asc ? va - vb : vb - va;

    });

  }



  const cols = [

    { key: 'well', label: 'Well' },

    { key: 'section', label: 'Section' },

    { key: 'time', label: 'Time' },

    { key: 'actual', label: 'Actual Rate' },

    { key: 'fitted', label: 'Fitted Rate' }

  ];



  let html = '<table style="font-size:0.75rem;"><thead><tr>';

  cols.forEach(c => {

    const isActive = sortState.col === c.key;

    const arrow = isActive ? (sortState.asc ? ' ▲' : ' ▼') : '';

    const cls = isActive ? 'sortable-th active' : 'sortable-th';

    html += `<th class="${cls}" style="padding:6px 10px;background:var(--bg-input);cursor:pointer;" onclick="sortTable('${cardId}','${c.key}')">${c.label}<span class="sort-arrow${isActive ? ' active' : ''}">${arrow}</span></th>`;

  });

  html += '</tr></thead><tbody>';



  sorted.forEach(r => {

    const isFc = r.section === 'Forecast';

    const bg = isFc ? 'background:rgba(34,197,94,.06);' : '';

    const secColor = isFc ? 'color:var(--green);' : 'color:var(--accent);';

    const actDisp = r.actual != null ? r.actual.toFixed(2) : (isFc ? '—' : '');

    const fitDisp = r.fitted != null ? r.fitted.toFixed(2) : '';

    const timeDisp = typeof r.time === 'number' && !Number.isInteger(r.time) ? r.time.toFixed(4) : r.time;

    html += `<tr style="${bg}"><td style="padding:4px 10px;">${r.well}</td><td style="padding:4px 10px;${secColor}">${r.section}</td><td style="padding:4px 10px;">${timeDisp}</td><td style="padding:4px 10px;">${actDisp}</td><td style="padding:4px 10px;">${fitDisp}</td></tr>`;

  });

  html += '</tbody></table>';

  ft.innerHTML = html;

}



function sortTable(cardId, colKey) {

  const st = cardTableSort[cardId] || { col: null, asc: true };

  if (st.col === colKey) { st.asc = !st.asc; }

  else { st.col = colKey; st.asc = true; }

  cardTableSort[cardId] = st;

  renderSortableTable(cardId);

}



function downloadTableCSV(cardId) {

  const rows = cardTableData[cardId] || [];

  if (rows.length === 0) return;

  const sortState = cardTableSort[cardId] || { col: null, asc: true };

  const sorted = [...rows];

  if (sortState.col) {

    sorted.sort((a, b) => {

      let va = a[sortState.col], vb = b[sortState.col];

      if (va == null && vb == null) return 0;

      if (va == null) return 1;

      if (vb == null) return -1;

      if (typeof va === 'string' && typeof vb === 'string') return sortState.asc ? va.localeCompare(vb) : vb.localeCompare(va);

      return sortState.asc ? va - vb : vb - va;

    });

  }

  let csv = 'Well,Section,Time,Actual Rate,Fitted Rate\n';

  sorted.forEach(r => {

    const act = r.actual != null ? r.actual.toFixed(2) : '';

    const fit = r.fitted != null ? r.fitted.toFixed(2) : '';

    const time = typeof r.time === 'number' && !Number.isInteger(r.time) ? r.time.toFixed(4) : r.time;

    csv += `"${r.well}","${r.section}","${time}","${act}","${fit}"\n`;

  });

  const blob = new Blob([csv], { type: 'text/csv' });

  const url = URL.createObjectURL(blob);

  const a = document.createElement('a');

  a.href = url;

  a.download = `dca_table_${cardId.replace('card-', '')}.csv`;

  a.click();

  URL.revokeObjectURL(url);

}



/* ====================================================================

   Draggable User Lines (H/V)

   ==================================================================== */

function setupUserLineDrag(cardId, myChart) {

  const zr = myChart.getZr();

  if (!zr) return;

  // Clean up previous drag handler

  if (zr.__userLineDragCleanup) zr.__userLineDragCleanup();



  let dragging = null; // { lineObj, seriesIdx }

  const SNAP_DIST = 12; // px tolerance for clicking near a line



  function findNearestLine(px, py) {

    const lines = cardUserLines[cardId] || [];

    if (lines.length === 0) return null;

    const opt = myChart.getOption();

    if (!opt || !opt.xAxis || !opt.yAxis) return null;

    let bestLine = null, bestDist = SNAP_DIST;

    lines.forEach(ul => {

      let dist;

      if (ul.type === 'h') {

        const pxY = myChart.convertToPixel('grid', [0, ul.value]);

        if (pxY) { dist = Math.abs(py - pxY[1]); }

      } else {

        const xType = opt.xAxis[0].type;

        let coord;

        if (xType === 'category') {

          coord = myChart.convertToPixel({ seriesIndex: 0 }, [Math.round(ul.value), 0]);

        } else {

          coord = myChart.convertToPixel('grid', [ul.value, 0]);

        }

        if (coord) { dist = Math.abs(px - coord[0]); }

      }

      if (dist !== undefined && dist < bestDist) {

        bestDist = dist;

        bestLine = ul;

      }

    });

    return bestLine;

  }


  /* Find the series index in current option that corresponds to a user line */
  function findSeriesIdx(line) {
    const opt = myChart.getOption();
    if (!opt || !opt.series) return -1;
    return opt.series.findIndex(s => s.name === line.name && s.markLine);
  }

  /* Lightweight update: patch only the markLine data for the dragged series */
  function patchMarkLine(line) {
    const opt = myChart.getOption();
    if (!opt || !opt.series) return;
    const isDate = opt.xAxis[0].type === 'category';
    const idx = opt.series.findIndex(s => s.markLine && s.name === line.name);
    if (idx === -1) {
      // name changed during drag, find by old series index
      if (dragging && dragging.seriesIdx >= 0 && dragging.seriesIdx < opt.series.length) {
        const sOpt = {};
        const newSeries = opt.series.map((s, i) => {
          if (i !== dragging.seriesIdx) return s;
          const mlDataItem = line.type === 'h' ? { yAxis: line.value } : { xAxis: isDate ? Math.round(line.value) : line.value };
          return Object.assign({}, s, {
            name: line.name,
            markLine: Object.assign({}, s.markLine, {
              label: Object.assign({}, (s.markLine || {}).label, { formatter: line.name, color: line.color }),
              data: [mlDataItem]
            })
          });
        });
        myChart.setOption({ series: newSeries }, false);
      }
      return;
    }
    const mlDataItem = line.type === 'h' ? { yAxis: line.value } : { xAxis: isDate ? Math.round(line.value) : line.value };
    const newSeries = opt.series.map((s, i) => {
      if (i !== idx) return s;
      return Object.assign({}, s, {
        name: line.name,
        markLine: Object.assign({}, s.markLine, {
          label: Object.assign({}, (s.markLine || {}).label, { formatter: line.name, color: line.color }),
          data: [mlDataItem]
        })
      });
    });
    if (dragging) dragging.seriesIdx = idx;
    myChart.setOption({ series: newSeries }, false);
  }


  function onMouseDown(e) {

    if (e.which !== 1) return; // left button only

    const chartDiv = document.getElementById('chart-' + cardId);

    if (!chartDiv) return;

    const px = e.offsetX, py = e.offsetY;

    const line = findNearestLine(px, py);

    if (!line) return;

    dragging = { lineObj: line, startPx: { x: px, y: py }, startVal: line.value, seriesIdx: findSeriesIdx(line) };

    e.event && e.event.preventDefault && e.event.preventDefault();

    // Prevent ECharts tooltip/dataZoom from interfering
    myChart.dispatchAction({ type: 'takeGlobalCursor', key: 'userLineDrag', userLineDragging: true });

    chartDiv.style.cursor = line.type === 'h' ? 'ns-resize' : 'ew-resize';

  }



  function onMouseMove(e) {

    if (!dragging) {

      // Show cursor hint when near a line

      const chartDiv = document.getElementById('chart-' + cardId);

      if (!chartDiv) return;

      const px = e.offsetX, py = e.offsetY;

      const nearLine = findNearestLine(px, py);

      if (nearLine) {

        chartDiv.style.cursor = nearLine.type === 'h' ? 'ns-resize' : 'ew-resize';

      } else {

        chartDiv.style.cursor = '';

      }

      return;

    }

    const chartDiv = document.getElementById('chart-' + cardId);

    if (!chartDiv) return;

    const px = e.offsetX, py = e.offsetY;

    const opt = myChart.getOption();

    const xType = opt.xAxis[0].type;



    if (dragging.lineObj.type === 'h') {

      const dataCoord = myChart.convertFromPixel('grid', [0, py]);

      if (dataCoord) {

        dragging.lineObj.value = dataCoord[1];

        const displayVal = typeof dataCoord[1] === 'number' ? dataCoord[1].toFixed(2) : dataCoord[1];

        dragging.lineObj.name = 'H: ' + displayVal;

      }

    } else {

      if (xType === 'category') {

        const dataCoord = myChart.convertFromPixel({ seriesIndex: 0 }, [px, py]);

        if (dataCoord) {

          dragging.lineObj.value = dataCoord[0];

          const catLabel = opt.xAxis[0].data[Math.round(dataCoord[0])] || Math.round(dataCoord[0]);

          dragging.lineObj.name = 'V: ' + catLabel;

        }

      } else {

        const dataCoord = myChart.convertFromPixel('grid', [px, 0]);

        if (dataCoord) {

          dragging.lineObj.value = dataCoord[0];

          const displayVal = xType === 'time' ? formatDateTs(dataCoord[0]) : (typeof dataCoord[0] === 'number' ? dataCoord[0].toFixed(2) : dataCoord[0]);

          dragging.lineObj.name = 'V: ' + displayVal;

        }

      }

    }

    // Lightweight update — only patch the markLine, don't re-render the whole chart
    patchMarkLine(dragging.lineObj);

  }



  function onMouseUp() {

    if (!dragging) return;

    const chartDiv = document.getElementById('chart-' + cardId);

    if (chartDiv) chartDiv.style.cursor = '';

    dragging = null;

    // Full re-render to finalize position and sync all state
    reRenderChart(cardId);

  }



  zr.on('mousedown', onMouseDown);

  zr.on('mousemove', onMouseMove);

  zr.on('mouseup', onMouseUp);

  zr.on('globalout', onMouseUp);



  zr.__userLineDragCleanup = function () {

    zr.off('mousedown', onMouseDown);

    zr.off('mousemove', onMouseMove);

    zr.off('mouseup', onMouseUp);

    zr.off('globalout', onMouseUp);

  };

}


/* ====================================================================

   Workspace Persistence – Auto-save / Auto-restore

   ==================================================================== */

const WS_LS_KEY = 'dcapro_workspace';
let _wsSaveTimer = null;

function _collectWorkspaceState() {
  const cardEls = document.querySelectorAll('.plot-card');
  const cards = [];
  cardEls.forEach(card => {
    const cid = card.id;
    const wells = [];
    card.querySelectorAll('.well-picker-list input[type="checkbox"]:checked').forEach(cb => {
      wells.push(cb.value);
    });
    cards.push({
      cardId: cid,
      pageId: card.dataset.page,
      wells: wells,
      model: card.querySelector('.p-model')?.value || 'exponential',
      forecast: card.querySelector('.p-forecast')?.value || '0',
      title: card.querySelector('.p-title')?.value || '',
      header: card.querySelector('.p-header')?.value || '',
      combine: card.querySelector('.p-combine')?.checked || false,
      combineAgg: card.querySelector('.p-combine-agg')?.value || 'sum',
      styles: cardStyles[cid] || null,
      exclusions: cardExclusions[cid] ? [...cardExclusions[cid]] : [],
      userLines: cardUserLines[cid] || [],
      annotations: cardAnnotations[cid] || [],
      valueLabels: cardValueLabels[cid] || 'none',
      logScale: cardLogScale[cid] || false,
      pctChange: cardPctChange[cid] || false,
      axisLabels: cardAxisLabels[cid] || null,
      axisPositions: cardAxisPositions[cid] || null,
      headers: cardHeaders[cid] || '',
      zoomState: cardZoomState[cid] || null,
      pCurveState: cardPCurveState[cid] || null,
    });
  });

  return {
    version: 1,
    savedAt: Date.now(),
    selX: document.getElementById('selX')?.value || '',
    selY: document.getElementById('selY')?.value || '',
    selWellCol: document.getElementById('selWellCol')?.value || '',
    uploadedColumns: uploadedColumns,
    numericColumns: numericColumns,
    allWells: allWells,
    activeDatasetId: activeDatasetId,
    pages: pages.map(p => ({ id: p.id, name: p.name })),
    activePageId: activePageId,
    cards: cards,
    theme: document.documentElement.getAttribute('data-theme') || '',
  };
}

async function _saveCardDataToIDB(state) {
  try {
    const db = await openIDB();
    const tx = db.transaction(IDB_WS_STORE, 'readwrite');
    const store = tx.objectStore(IDB_WS_STORE);

    const cardData = {};
    state.cards.forEach(c => {
      if (cardLastData[c.cardId]) {
        cardData[c.cardId] = cardLastData[c.cardId];
      }
    });

    store.put({ id: 'cardLastData', data: cardData });
    store.put({ id: 'workspaceState', data: state });

    await new Promise((resolve, reject) => {
      tx.oncomplete = resolve;
      tx.onerror = () => reject(tx.error);
    });
  } catch (e) {
    console.warn('Failed to save workspace to IDB:', e);
  }
}

async function _loadCardDataFromIDB() {
  try {
    const db = await openIDB();
    const tx = db.transaction(IDB_WS_STORE, 'readonly');
    const store = tx.objectStore(IDB_WS_STORE);

    const [stateResult, dataResult] = await Promise.all([
      new Promise((resolve, reject) => {
        const r = store.get('workspaceState');
        r.onsuccess = () => resolve(r.result);
        r.onerror = () => reject(r.error);
      }),
      new Promise((resolve, reject) => {
        const r = store.get('cardLastData');
        r.onsuccess = () => resolve(r.result);
        r.onerror = () => reject(r.error);
      }),
    ]);

    return {
      state: stateResult?.data || null,
      cardData: dataResult?.data || {},
    };
  } catch (e) {
    console.warn('Failed to load workspace from IDB:', e);
    return { state: null, cardData: {} };
  }
}

function _autoSaveWorkspace() {
  const state = _collectWorkspaceState();
  _saveCardDataToIDB(state).then(() => {
    console.log('Workspace auto-saved at', new Date().toLocaleTimeString());
  });
}

// saveWorkspace: called by 💾 Save button AND auto-save
function saveWorkspace() {
  _autoSaveWorkspace();
  if (typeof showToast === 'function') showToast('Workspace saved', 'success', 2000);
}

function _debouncedAutoSave() {
  if (_wsSaveTimer) clearTimeout(_wsSaveTimer);
  _wsSaveTimer = setTimeout(() => {
    _autoSaveWorkspace();
  }, 1500);
}

// Restore the Import Data tab from server-side dataset info
async function _restoreImportTab() {
  try {
    const res = await fetch('/api/current');
    if (!res.ok) return;
    const data = await res.json();

    // Populate file info
    const info = document.getElementById('fileInfo');
    if (info) {
      info.className = 'file-info show';
      info.innerHTML = `\u2713 <strong>${data.filename}</strong> \u2014 ${data.rows.toLocaleString()} rows \u00D7 ${data.columns.length} columns` +
        (data.dataset_id ? ` <span style="color:var(--text-dim);font-size:.72rem;margin-left:8px;">ID: ${data.dataset_id}</span>` : '');
    }

    // Update header status
    document.getElementById('headerStatus').textContent = `${data.filename} \u2022 ${data.rows.toLocaleString()} rows`;

    // Update globals
    uploadedColumns = data.columns;
    numericColumns = data.numeric_columns;
    hasDiskPath = data.has_disk_path || false;
    lastImportTimestamp = data.last_import || null;
    currentVersion = data.version || 0;
    if (data.dataset_id) activeDatasetId = data.dataset_id;

    // Virtual-scroll preview
    previewState.totalRows = data.rows || 0;
    previewState.columns = data.columns;
    previewState.sortCol = null;
    previewState.filterCol = null;
    previewState.filterVal = '';
    renderPreview();

    // Column selectors
    populateSelectors();


    // Sync panel
    if (typeof showSyncPanel === 'function') showSyncPanel();
    if (typeof updateSyncUI === 'function') updateSyncUI();

  } catch (e) {
    console.warn('Could not restore Import Data tab:', e);
  }
}

// loadWorkspace: called by \ud83d\udcc2 Load button AND auto-restore
async function loadWorkspace() {
  try {
    const { state, cardData } = await _loadCardDataFromIDB();
    if (!state || !state.cards || state.cards.length === 0) {
      console.log('No saved workspace found.');
      return false;
    }

    // Check if the server still has data loaded
    let serverHasData = false;
    try {
      const res = await fetch('/api/columns');
      if (res.ok) {
        const colData = await res.json();
        uploadedColumns = colData.columns || [];
        numericColumns = colData.numeric_columns || [];
        serverHasData = uploadedColumns.length > 0;
      }
    } catch (e) { /* server not available */ }

    if (serverHasData) {
      // Restore Import Data tab from server
      await _restoreImportTab();

      // Fetch wells for the saved well column
      if (state.selWellCol) {
        try {
          const wRes = await fetch('/api/wells?well_col=' + encodeURIComponent(state.selWellCol));
          const wData = await wRes.json();
          allWells = wData.wells || [];
        } catch (e) { allWells = state.allWells || []; }
      }
    } else {
      if (state.uploadedColumns?.length > 0) {
        uploadedColumns = state.uploadedColumns;
        numericColumns = state.numericColumns || [];
        allWells = state.allWells || [];
        populateSelectors();
        const msg = 'Server restarted \u2013 please re-import your data file. Your dashboard layout has been preserved.';
        if (typeof showToast === 'function') showToast(msg, 'warning', 6000);
        else alert(msg);
      }
    }

    // Restore theme
    if (state.theme) {
      document.documentElement.setAttribute('data-theme', state.theme);
      const tb = document.getElementById('themeToggle');
      if (tb) tb.textContent = state.theme === 'light' ? '\u2600\uFE0F Light' : '\uD83C\uDF19 Dark';
    }

    // Restore column selections
    if (state.selX) { const el = document.getElementById('selX'); if (el) el.value = state.selX; }
    if (state.selY) { const el = document.getElementById('selY'); if (el) el.value = state.selY; }
    if (state.selWellCol) { const el = document.getElementById('selWellCol'); if (el) el.value = state.selWellCol; }

    // Restore pages
    pages.length = 0;
    (state.pages || []).forEach(p => pages.push({ id: p.id, name: p.name }));
    if (pages.length === 0) pages.push({ id: 'page-default', name: 'Page 1' });
    activePageId = state.activePageId || pages[0].id;
    renderPageTabs();

    // Remove existing cards
    document.querySelectorAll('.plot-card').forEach(card => {
      if (chartInstances[card.id]) { chartInstances[card.id].dispose(); delete chartInstances[card.id]; }
      card.remove();
    });

    // Restore cards
    for (const c of state.cards) {
      const cardId = addPlotCard(c.wells, c.model, c.forecast, c.title, c.combine, c.header, c.combineAgg || 'sum');

      const cardEl = document.getElementById(cardId);
      if (cardEl) cardEl.dataset.page = c.pageId;

      if (c.exclusions?.length) cardExclusions[cardId] = new Set(c.exclusions);
      if (c.userLines?.length) cardUserLines[cardId] = c.userLines;
      if (c.annotations?.length) cardAnnotations[cardId] = c.annotations;
      if (c.valueLabels && c.valueLabels !== 'none') cardValueLabels[cardId] = c.valueLabels;
      if (c.logScale) cardLogScale[cardId] = true;
      if (c.pctChange) cardPctChange[cardId] = true;
      if (c.axisLabels) cardAxisLabels[cardId] = c.axisLabels;
      if (c.axisPositions) cardAxisPositions[cardId] = c.axisPositions;
      if (c.headers) cardHeaders[cardId] = c.headers;
      if (c.zoomState) cardZoomState[cardId] = c.zoomState;
      if (c.pCurveState) cardPCurveState[cardId] = c.pCurveState;

      if (c.styles) {
        const mergedStyles = { ...getDefaultStyles(), ...c.styles };
        cardStyles[cardId] = mergedStyles;
        applyStylesToCard(cardId, mergedStyles);
        const headerEl = cardEl?.querySelector('.p-header');
        if (headerEl && mergedStyles.headerFontSize) {
          headerEl.style.fontSize = mergedStyles.headerFontSize + 'rem';
          headerEl.style.color = mergedStyles.headerColor || '';
        }
      }

      const savedData = cardData[c.cardId];
      if (savedData && serverHasData) {
        cardLastData[cardId] = savedData;
        try {
          renderSingleChart(cardId, savedData, c.forecast);
        } catch (e) {
          console.warn('Failed to render chart for', cardId, e);
        }
      }
    }

    switchPage(activePageId);

    // Switch to DCA tab so charts are visible
    if (state.cards.length > 0) {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
      const dcaBtn = document.querySelector('[data-tab="dca"]');
      const dcaTab = document.getElementById('tab-dca');
      if (dcaBtn) dcaBtn.classList.add('active');
      if (dcaTab) dcaTab.classList.add('active');
    }

    // Update header status if server has data
    if (serverHasData) {
      const hs = document.getElementById('headerStatus');
      if (hs) hs.textContent = 'Workspace restored';
    }

    console.log('Workspace restored with', state.cards.length, 'cards');
    if (typeof showToast === 'function') showToast('Workspace restored', 'success', 2500);
    return true;

  } catch (e) {
    console.error('Failed to restore workspace:', e);
    return false;
  }
}


// --- Auto-save hooks ---

const _plotsObserverTarget = document.getElementById('plotsContainer');
if (_plotsObserverTarget) {
  const observer = new MutationObserver(() => _debouncedAutoSave());
  observer.observe(_plotsObserverTarget, { childList: true });
}

const _origRunSingleDCA = runSingleDCA;
runSingleDCA = async function (cardId) {
  await _origRunSingleDCA(cardId);
  _debouncedAutoSave();
};

const _origRemoveCard = removeCard;
removeCard = function (id) {
  _origRemoveCard(id);
  _debouncedAutoSave();
};

const _origAddPage = addPage;
addPage = function (name) {
  const result = _origAddPage(name);
  _debouncedAutoSave();
  return result;
};

const _origDeletePage = deletePage;
deletePage = function (pageId) {
  _origDeletePage(pageId);
  _debouncedAutoSave();
};

const _origSwitchPage = switchPage;
switchPage = function (pageId) {
  _origSwitchPage(pageId);
  _debouncedAutoSave();
};

const _origReRenderChart = reRenderChart;
reRenderChart = function (cardId) {
  _origReRenderChart(cardId);
  _debouncedAutoSave();
};

['selX', 'selY', 'selWellCol'].forEach(id => {
  const el = document.getElementById(id);
  if (el) el.addEventListener('change', _debouncedAutoSave);
});

document.getElementById('themeToggle')?.addEventListener('click', () => {
  setTimeout(_debouncedAutoSave, 200);
});


// --- Auto-restore on page load ---

document.addEventListener('DOMContentLoaded', async () => {
  setTimeout(async () => {
    try {
      const restored = await loadWorkspace();
      if (restored) {
        console.log('Auto-restored workspace on page load.');
      }
    } catch (e) {
      console.warn('Auto-restore failed:', e);
    }
  }, 500);
});
