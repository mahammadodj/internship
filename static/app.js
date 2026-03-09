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

    // Show/hide the DCA sidebar toggle based on active tab
    const sidebarToggle = document.getElementById('dcaSidebarToggle');
    if (sidebarToggle) {
      if (btn.dataset.tab === 'dca') {
        sidebarToggle.style.display = _dcaSidebarOpen ? 'none' : 'flex';
      } else {
        sidebarToggle.style.display = 'none';
      }
    }
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

  /* Re-render all charts so ECharts picks up the new theme and label colours */

  requestAnimationFrame(() => {

    Object.keys(cardLastData).forEach(cardId => {

      if (cardLastData[cardId]) {

        reRenderChart(cardId);

      }

    });

  });

});



/* ====================================================================

   Global State

   ==================================================================== */

let uploadedColumns = [];

let numericColumns = [];

let derivedColumnNames = [];

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
const cardLogScaleX = {};    // { cardId: boolean }  – X-axis log scale
const cardAxisAutoFit = {};  // { cardId: { x: bool, y: bool } } – "Fit to frame" state per axis

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
const cardLabelCollapseStates = {}; // { cardId: { actual: bool, p10: bool, ... } }
const cardAxisLabels = {};   // { cardId: {x:string, y:string} }
const cardAxisPositions = {}; // { cardId: {x:'bottom'|'top', y:'left'|'right'} }
const cardTableData = {};    // { cardId: [{well, section, time, actual, fitted}] }
const cardTableSort = {};    // { cardId: {col:string, asc:boolean} }
const cardTableFilter = {};  // { cardId: {well:string, section:string} }
const cardHeaders = {};      // { cardId: string }
let _ctxMenuCardId = null;
let _ctxMenuCoord = null;
let _ctxMenuClientX = 0;
let _ctxMenuClientY = 0;
let _anchorMenuCardId = null;
let _anchorMenuIdx = null;
let _anchorMenuJustOpened = false; // prevents doc-click from closing on same tick
let _pointMenuCardId = null;
let _pointMenuIdx = null;
let _pointMenuJustOpened = false;

const cardCtrlSelected = {};      // cardId -> Set<index> for Ctrl+click multi-select
const cardMultiFits = {};         // cardId -> Array<{id, model, params, equation, indices:[], color, fittedData:[[x,y],...], forecastData:[[x,y],...]}>  
const cardHiddenSeries = {};      // cardId -> Set<seriesName> of manually removed series
const cardActiveCurveStyle = {};  // cardId -> 'well:wellName' or 'mf:id' identifying the visible style section
let _multiFitNextId = 1;
const MULTI_FIT_COLORS = ['#e040fb', '#00bcd4', '#ff9800', '#8bc34a', '#ff5722', '#9c27b0', '#009688', '#ffc107', '#795548', '#607d8b'];
let _multiPointMenuJustOpened = false;



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

  renderDerivedColumnsPanel();

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

    const isDerived = derivedColumnNames.includes(c);

    const derivedCls = isDerived ? ' th-derived' : '';

    const derivedBadge = isDerived ? '<span class="derived-badge">ƒ</span>' : '';

    html += `<th class="${derivedCls}" onclick="handlePreviewSort(event, '${c.replace(/'/g, "\\'")}')" title="${c}${isDerived ? ' (calculated)' : ''}">

      ${derivedBadge}${c}${filterIcon}${sortIcon}

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

      vsColumns.forEach(c => {
        const isDerived = derivedColumnNames.includes(c);
        const cls = isDerived ? ' class="td-derived"' : '';
        html += `<td${cls}>${row[c] ?? ''}</td>`;
      });

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

    if (data.derived_columns) derivedColumnNames = data.derived_columns.map(d => d.name || d);

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

   Selectors  (per-card — column selectors are on each plot card)

   ==================================================================== */

// Legacy stubs for backward compatibility (no-ops)
const pageColumns = {};
function getPageColumns(pageId) { return { selX: '', selY: '', selWellCol: '', allWells: [] }; }
function getActivePageColumns() { return getPageColumns(activePageId); }
function populatePageSelectors(pageId) { /* no-op: columns are per-card now */ }
function createPageColumnsUI(pageId) { /* no-op: columns are per-card now */ }
async function fetchWellsForPage(pageId) { /* no-op: wells are fetched per-card now */ }

function populateSelectors() {
  // After data upload, re-populate column selectors on all existing cards
  populateAllCardColumnSelectors();
}

async function fetchWells() {
  // no-op: wells are fetched per-card now
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

  createPageColumnsUI(id);

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

  // Remove page columns UI
  const colsEl = document.getElementById('pageCols-' + pageId);
  if (colsEl) colsEl.remove();
  delete pageColumns[pageId];

  const idx = pages.findIndex(p => p.id === pageId);

  if (idx >= 0) pages.splice(idx, 1);

  if (activePageId === pageId) switchPage(pages[0].id);

  renderPageTabs();

}



function switchPage(pageId) {

  if (pageId === activePageId) return;

  activePageId = pageId;

  // Show/hide plot cards
  document.querySelectorAll('.plot-card').forEach(card => {

    card.style.display = card.dataset.page === pageId ? '' : 'none';

  });

  // Update active class inline – no full re-render so existing nameSpan elements stay alive
  document.querySelectorAll('#pageTabs .page-tab').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.pageId === pageId);
  });

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

    btn.dataset.pageId = pg.id;

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

    btn.onclick = (e) => { if (e.detail >= 2) return; switchPage(pg.id); };

    tabs.appendChild(btn);

  });

}



initPages();



/* ====================================================================

   Dynamic Plots

   ==================================================================== */

const container = document.getElementById('plotsContainer');



function populateCardColumnSelectors(cardId, presetX, presetY, presetWellCol) {
  const card = document.getElementById(cardId);
  if (!card) return;
  const selX = card.querySelector('.p-selX');
  const selY = card.querySelector('.p-selY');
  const selWellCol = card.querySelector('.p-selWellCol');
  if (!selX || !selY || !selWellCol) return;

  selX.innerHTML = '<option value="">— select —</option>';
  selY.innerHTML = '<option value="">— select —</option>';
  selWellCol.innerHTML = '<option value="">— select —</option>';

  // X: all columns (numeric first, then categorical)
  [...numericColumns, ...uploadedColumns.filter(c => !numericColumns.includes(c))].forEach(c => {
    selX.innerHTML += `<option value="${c}">${c}</option>`;
  });
  // Y: numeric columns
  numericColumns.forEach(c => {
    selY.innerHTML += `<option value="${c}">${c}</option>`;
  });
  // Well column: categorical first, then numeric
  const catCols = uploadedColumns.filter(c => !numericColumns.includes(c));
  [...catCols, ...numericColumns].forEach(c => {
    selWellCol.innerHTML += `<option value="${c}">${c}</option>`;
  });

  if (presetX) selX.value = presetX;
  if (presetY) selY.value = presetY;
  if (presetWellCol) selWellCol.value = presetWellCol;
}

// Re-populate column selectors on all existing cards (after new data upload)
function populateAllCardColumnSelectors() {
  document.querySelectorAll('.plot-card').forEach(card => {
    const curX = card.querySelector('.p-selX')?.value || '';
    const curY = card.querySelector('.p-selY')?.value || '';
    const curW = card.querySelector('.p-selWellCol')?.value || '';
    populateCardColumnSelectors(card.id, curX, curY, curW);
  });
}


function addPlotCardToActive() {
  if (uploadedColumns.length === 0) {
    alert('Please upload a dataset first.');
    return;
  }
  // Prevent adding a new card if any existing card has no plot yet
  const existingCards = container.querySelectorAll('.plot-card');
  for (const c of existingCards) {
    if (!chartInstances[c.id]) {
      alert('Please create a plot for the existing card before adding a new one.');
      c.scrollIntoView({ behavior: 'smooth', block: 'center' });
      return;
    }
  }
  addPlotCard();
}



function addPlotCard(presetWell, presetModel, presetForecast, presetTitle, presetCombine, presetHeader, presetCombineAgg, presetSelX, presetSelY, presetSelWellCol) {

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

    <div class="plot-header">

      <div class="control-group"><label>X Axis</label>
        <select class="p-selX"><option value="">— select —</option></select>
      </div>
      <div class="control-group"><label>Y Axis</label>
        <select class="p-selY"><option value="">— select —</option></select>
      </div>
      <div class="control-group"><label>Well Column</label>
        <select class="p-selWellCol"><option value="">— select —</option></select>
      </div>

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
      <div class="style-curves-container" id="styleCurves-${cardId}"></div>
      <div class="style-section">
        <div class="style-section-title">Layout</div>
        <div class="style-row">
          <span class="style-label">Gridlines</span>
          <label class="style-check"><input type="checkbox" class="s-grid-x" checked> Horizontal</label>
          <label class="style-check"><input type="checkbox" class="s-grid-y" checked> Vertical</label>
        </div>
        <div class="style-row">
          <span class="style-label">Section Header</span>
          <input type="color" class="s-header-color" value="#e2e8f0" title="Color">
          <input type="range" class="s-header-fsize" min="0.8" max="4.0" step="0.1" value="1.8" title="Size">
          <span class="s-header-fsize-val style-range-val">1.8</span>
          <label class="style-check"><input type="checkbox" class="s-header-bold"> Bold</label>
          <select class="s-header-align">
            <option value="left">Left</option>
            <option value="center">Center</option>
            <option value="right">Right</option>
          </select>
        </div>
      </div>
    </div>

    <div class="chart-area" style="display:none;">

      <div class="chart-toolbar">

        <button onclick="toggleStylePanel('${cardId}')" title="Customize Style">🎨 Style</button>

        <button onclick="saveCardAsTemplate('${cardId}')" title="Save this card as a DCA template">💾 Template</button>

        <button onclick="downloadChart('${cardId}','png')" title="Download PNG">PNG</button>

        <button onclick="downloadChart('${cardId}','jpg')" title="Download JPG">JPG</button>

        <button onclick="downloadChart('${cardId}','pdf')" title="Download PDF">PDF</button>

        <button class="reset-zoom-btn" id="resetZoom-${cardId}" onclick="resetZoom('${cardId}')">Reset Zoom</button>

        <button class="reset-all-btn" id="resetAll-${cardId}" onclick="resetAll('${cardId}')">⟲ Reset All</button>

      </div>

      <div class="mini-chart" id="chart-${cardId}"></div>

      <div class="formula-display" id="formula-${cardId}" style="display:none;"></div>

      <div class="param-display" id="params-${cardId}"></div>

      <div class="qi-box" id="qiBox-${cardId}" style="display:none;"></div>

      <div class="qi-input-panel" id="qiInputPanel-${cardId}" style="display:none;">
        <span class="qi-input-label">Set Qi:</span>
        <input type="number" id="qiValueInput-${cardId}" class="qi-input-field" placeholder="Value" step="any" title="Qi value">
        <span class="qi-input-label">at</span>
        <input type="date" id="qiDateInput-${cardId}" class="qi-input-field qi-date-field" title="Anchor date (leave empty for start)">
        <button class="qi-input-btn" onclick="applyQiFromInput('${cardId}')" title="Apply Qi">Apply</button>
      </div>

      <div class="dca-stats-summary" id="dcaStats-${cardId}" style="display:none;"></div>

      <div class="curve-summary-wrap" id="curveSummaryWrap-${cardId}" style="display:none;">
        <div class="curve-summary-title">Curve Summary</div>
        <div class="curve-summary-table-wrap" id="curveSummary-${cardId}"></div>
      </div>

      <div class="multi-fit-panel" id="multiFitPanel-${cardId}" style="display:none;"></div>

      <div class="excl-hint" id="exclHint-${cardId}">Click scatter points to exclude them from curve fitting</div>

      <div id="forecastTableSection-${cardId}" style="display:none;">
        <div class="data-table-toolbar">
          <div style="display:flex;align-items:center;justify-content:space-between;">
            <span class="dt-title">📊 Data Table</span>
            <div style="display:flex;align-items:center;gap:6px;">
              <span class="dt-row-count" id="dtRowCount-${cardId}"></span>
              <button onclick="downloadTableCSV('${cardId}')" title="Download as CSV">↓ CSV</button>
            </div>
          </div>
          <div class="dt-filter-row">
            <input class="dt-filter-input" type="text" placeholder="🔍 Filter well..." oninput="filterTable('${cardId}')" id="dtFilterWell-${cardId}">
            <select class="dt-filter-select" onchange="filterTable('${cardId}')" id="dtFilterSection-${cardId}">
              <option value="">All sections</option>
              <option value="Actual">Actual</option>
              <option value="Forecast">Forecast</option>
            </select>
            <button class="dt-filter-clear" onclick="clearTableFilter('${cardId}')" title="Clear filters">✕</button>
          </div>
        </div>
        <div class="table-wrap" id="forecastTable-${cardId}" style="max-height:250px; border:1px solid var(--border);"></div>
      </div>

    </div>

  `;

  // Create wrapper and external section header
  const cardWrap = document.createElement('div');
  cardWrap.className = 'card-wrap';

  const headerDiv = document.createElement('div');
  headerDiv.className = 'plot-card-header' + (presetHeader ? ' has-content' : '');

  const headerInput = document.createElement('input');
  headerInput.type = 'text';
  headerInput.className = 'plot-card-header-input p-header';
  headerInput.placeholder = 'Click to add section header…';
  headerInput.value = presetHeader || '';
  headerDiv.appendChild(headerInput);

  // Store reference on card element so existing querySelector('.p-header') calls still work
  card._headerInput = headerInput;

  // Toggle has-content class as user types
  headerInput.addEventListener('input', () => {
    cardHeaders[cardId] = headerInput.value;
    headerDiv.classList.toggle('has-content', headerInput.value.trim().length > 0);
  });

  cardWrap.appendChild(headerDiv);
  cardWrap.appendChild(card);
  container.appendChild(cardWrap);

  // Build initial curve style sections (will show defaults until data is loaded)
  rebuildCurveStyleSections(cardId);

  // ─── Populate per-card column selectors ───
  populateCardColumnSelectors(cardId, presetSelX, presetSelY, presetSelWellCol);

  // Wire well-column change → fetch wells for this card
  const cardSelWellCol = card.querySelector('.p-selWellCol');
  if (cardSelWellCol) {
    cardSelWellCol.addEventListener('change', async () => {
      const wellCol = cardSelWellCol.value;
      if (!wellCol) {
        populateWellPicker(cardId, []);
        return;
      }
      try {
        const res = await fetch(`/api/wells?well_col=${encodeURIComponent(wellCol)}`);
        const data = await res.json();
        const wells = data.wells || [];
        // Store wells on the card element for reference
        card._cardWells = wells;
        populateWellPicker(cardId, [], wells);
      } catch (e) {
        console.warn('Failed to fetch wells for card', cardId, e);
      }
      if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();
    });
  }
  const cardSelX = card.querySelector('.p-selX');
  if (cardSelX) cardSelX.addEventListener('change', () => { if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave(); });
  const cardSelY = card.querySelector('.p-selY');
  if (cardSelY) cardSelY.addEventListener('change', () => { if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave(); });

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

    const headerEl = card._headerInput || card.querySelector('.p-header');
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

  // headerInput wired above when creating the external header element



  return cardId;

}



function getDefaultStyles() {
  return { plotTheme: 'classic', actualColor: '#3b82f6', actualSymbol: 'circle', actualSize: 10, fittedColor: '#f59e0b', fittedStyle: 'solid', fittedWidth: 2, fittedMarkers: true, fittedSymbol: 'triangle', fittedSymbolSize: 14, forecastColor: '#22c55e', forecastStyle: 'dashed', forecastWidth: 3, forecastMarkers: true, forecastLabels: false, forecastSymbol: 'triangle', forecastSymbolSize: 14, p10Color: '#22c55e', p10Style: 'solid', p10Line: true, p10Marker: true, p10Labels: false, p90Color: '#ef4444', p90Style: 'solid', p90Line: true, p90Marker: true, p90Labels: false, gridX: true, gridY: true, headerFontSize: 1.8, headerColor: document.documentElement.getAttribute('data-theme') === 'light' ? '#0f172a' : '#e2e8f0', headerFontWeight: 'normal', headerTextAlign: 'left' };
}

/* Build per-curve style HTML sections inside the style panel.
   Called after cardLastData is populated so we know the well names. */
function rebuildCurveStyleSections(cardId) {
  const container = document.getElementById('styleCurves-' + cardId);
  if (!container) return;

  const card = document.getElementById(cardId);
  const data = cardLastData[cardId];
  const wells = data && data.wells ? data.wells : [];
  const isSingle = wells.length <= 1;
  const defaults = getDefaultStyles();
  const stored = cardStyles[cardId] || defaults;
  const curveStyles = stored.curveStyles || {};
  const mfStyles = stored.multiFitStyles || {};
  const palette = getPlotThemePalette(stored.plotTheme || 'classic');
  const multiFits = cardMultiFits[cardId] || [];

  /* Helper: escape well name for use in data attribute */
  const esc = (s) => (s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;');

  const sel = (options, selected) => options.map(o => '<option value="' + o.v + '"' + (o.v === selected ? ' selected' : '') + '>' + o.l + '</option>').join('');
  const symOpts = [{v:'circle',l:'● Circle'},{v:'diamond',l:'◆ Diamond'},{v:'rect',l:'■ Square'},{v:'triangle',l:'▲ Triangle'}];
  const lineOpts = [{v:'solid',l:'Solid'},{v:'dashed',l:'Dashed'},{v:'dotted',l:'Dotted'}];
  const chk = (v) => v ? ' checked' : '';

  /* Build list of all available curves for the selector */
  const curveList = []; /* { key, label, dotColor } */
  const wellNames = [];
  if (wells.length === 0) {
    wellNames.push('default');
  } else {
    wells.forEach((w, idx) => wellNames.push(w.well || ('Well ' + (idx + 1))));
  }
  const mainModel = data?.model || 'exponential';
  wellNames.forEach((wn, idx) => {
    const cs = curveStyles[wn] || {};
    const defColor = (!isSingle) ? (palette[idx % palette.length]) : (cs.fittedColor || stored.fittedColor || defaults.fittedColor);
    const dotColor = cs.fittedColor || defColor;
    const label = (isSingle ? wn : wn) + ' (' + mainModel + ')';
    curveList.push({ key: 'well:' + wn, label: label, dotColor: dotColor });
  });
  multiFits.forEach(mf => {
    const ms = mfStyles['mf_' + mf.id] || {};
    const dotColor = ms.color || mf.color || '#8b5cf6';
    curveList.push({ key: 'mf:' + mf.id, label: 'Curve #' + mf.id + ' (' + mf.model + ')', dotColor: dotColor });
  });

  /* Determine active key */
  let activeKey = cardActiveCurveStyle[cardId];
  if (!activeKey || !curveList.find(c => c.key === activeKey)) {
    activeKey = curveList.length > 0 ? curveList[0].key : null;
  }
  cardActiveCurveStyle[cardId] = activeKey;

  /* ---- Build selector dropdown ---- */
  let html = '';
  if (curveList.length > 1) {
    html += '<div class="style-curve-selector"><select class="sc-curve-select">';
    curveList.forEach(c => {
      html += '<option value="' + esc(c.key) + '"' + (c.key === activeKey ? ' selected' : '') + '>' + c.label + '</option>';
    });
    html += '</select></div>';
  }

  /* ---- Build well sections (hidden unless active) ---- */
  const buildCurveSection = (wellName, wIdx) => {
    const cs = curveStyles[wellName] || {};
    const isMulti = !isSingle;
    const defActualColor = isMulti ? (palette[wIdx % palette.length]) : (cs.actualColor || stored.actualColor || defaults.actualColor);
    const defFittedColor = isMulti ? (palette[wIdx % palette.length]) : (cs.fittedColor || stored.fittedColor || defaults.fittedColor);
    const defForecastColor = isMulti ? (palette[wIdx % palette.length]) : (cs.forecastColor || stored.forecastColor || defaults.forecastColor);
    const actualColor = cs.actualColor || defActualColor;
    const actualSymbol = cs.actualSymbol || stored.actualSymbol || defaults.actualSymbol;
    const actualSize = cs.actualSize != null ? cs.actualSize : (stored.actualSize != null ? stored.actualSize : defaults.actualSize);
    const fittedColor = cs.fittedColor || defFittedColor;
    const fittedStyle = cs.fittedStyle || stored.fittedStyle || defaults.fittedStyle;
    const fittedWidth = cs.fittedWidth != null ? cs.fittedWidth : (stored.fittedWidth != null ? stored.fittedWidth : defaults.fittedWidth);
    const fittedMarkers = cs.fittedMarkers != null ? cs.fittedMarkers : (stored.fittedMarkers != null ? stored.fittedMarkers : defaults.fittedMarkers);
    const fittedSymbol = cs.fittedSymbol || stored.fittedSymbol || defaults.fittedSymbol;
    const fittedSymbolSize = cs.fittedSymbolSize != null ? cs.fittedSymbolSize : (stored.fittedSymbolSize != null ? stored.fittedSymbolSize : defaults.fittedSymbolSize);
    const forecastColor = cs.forecastColor || defForecastColor;
    const forecastStyle = cs.forecastStyle || stored.forecastStyle || defaults.forecastStyle;
    const forecastWidth = cs.forecastWidth != null ? cs.forecastWidth : (stored.forecastWidth != null ? stored.forecastWidth : defaults.forecastWidth);
    const forecastMarkers = cs.forecastMarkers != null ? cs.forecastMarkers : (stored.forecastMarkers != null ? stored.forecastMarkers : defaults.forecastMarkers);
    const forecastLabels = cs.forecastLabels != null ? cs.forecastLabels : (stored.forecastLabels != null ? stored.forecastLabels : defaults.forecastLabels);
    const forecastSymbol = cs.forecastSymbol || stored.forecastSymbol || defaults.forecastSymbol;
    const forecastSymbolSize = cs.forecastSymbolSize != null ? cs.forecastSymbolSize : (stored.forecastSymbolSize != null ? stored.forecastSymbolSize : defaults.forecastSymbolSize);
    const p10Color = cs.p10Color || stored.p10Color || defaults.p10Color;
    const p10Line = cs.p10Line != null ? cs.p10Line : (stored.p10Line != null ? stored.p10Line : defaults.p10Line);
    const p10Marker = cs.p10Marker != null ? cs.p10Marker : (stored.p10Marker != null ? stored.p10Marker : defaults.p10Marker);
    const p10Labels = cs.p10Labels != null ? cs.p10Labels : (stored.p10Labels != null ? stored.p10Labels : defaults.p10Labels);
    const p10Style = cs.p10Style || stored.p10Style || defaults.p10Style;
    const p90Color = cs.p90Color || stored.p90Color || defaults.p90Color;
    const p90Line = cs.p90Line != null ? cs.p90Line : (stored.p90Line != null ? stored.p90Line : defaults.p90Line);
    const p90Marker = cs.p90Marker != null ? cs.p90Marker : (stored.p90Marker != null ? stored.p90Marker : defaults.p90Marker);
    const p90Labels = cs.p90Labels != null ? cs.p90Labels : (stored.p90Labels != null ? stored.p90Labels : defaults.p90Labels);
    const p90Style = cs.p90Style || stored.p90Style || defaults.p90Style;

    const sectionKey = 'well:' + wellName;
    const isActive = sectionKey === activeKey;
    const dw = 'data-well="' + esc(wellName) + '"';
    const prefix = isSingle ? '' : wellName + ' ';
    const seriesName = prefix + 'Fitted';

    html += '<div class="style-section style-curve-section" ' + dw + ' data-curve-key="' + esc(sectionKey) + '" data-series-name="' + esc(seriesName) + '"' + (isActive ? '' : ' style="display:none"') + '>';

    /* Model selector */
    const mainModelOpts = [{v:'exponential',l:'Exponential'},{v:'hyperbolic',l:'Hyperbolic'},{v:'harmonic',l:'Harmonic'}];
    html += '<div class="style-row"><span class="style-label">Model</span>'
      + '<select class="sc-main-model" ' + dw + '>' + sel(mainModelOpts, mainModel) + '</select>'
      + '</div>';

    /* Actual Points */
    html += '<div class="style-row"><span class="style-label">Actual Pts</span>'
      + '<input type="color" class="sc-actual-color" ' + dw + ' value="' + actualColor + '">'
      + '<select class="sc-actual-symbol" ' + dw + '>' + sel(symOpts, actualSymbol) + '</select>'
      + '<input type="range" class="sc-actual-size" ' + dw + ' min="2" max="20" value="' + actualSize + '" title="Size">'
      + '<span class="style-range-val">' + actualSize + '</span>'
      + '</div>';

    /* Fitted Curve */
    html += '<div class="style-row"><span class="style-label">Fitted Curve</span>'
      + '<input type="color" class="sc-fitted-color" ' + dw + ' value="' + fittedColor + '">'
      + '<select class="sc-fitted-style" ' + dw + '>' + sel(lineOpts, fittedStyle) + '</select>'
      + '<input type="range" class="sc-fitted-width" ' + dw + ' min="1" max="6" value="' + fittedWidth + '" title="Width">'
      + '<span class="style-range-val">' + fittedWidth + '</span>'
      + '</div>';

    /* Fitted Markers */
    html += '<div class="style-row"><span class="style-label">Fitted Markers</span>'
      + '<label class="style-check"><input type="checkbox" class="sc-fitted-markers" ' + dw + chk(fittedMarkers) + '> Show</label>'
      + '<select class="sc-fitted-symbol" ' + dw + '>' + sel(symOpts, fittedSymbol) + '</select>'
      + '<input type="range" class="sc-fitted-msize" ' + dw + ' min="2" max="25" value="' + fittedSymbolSize + '" title="Size">'
      + '<span class="style-range-val">' + fittedSymbolSize + '</span>'
      + '</div>';

    /* Forecast */
    html += '<div class="style-row"><span class="style-label">Forecast</span>'
      + '<input type="color" class="sc-forecast-color" ' + dw + ' value="' + forecastColor + '">'
      + '<select class="sc-forecast-style" ' + dw + '>' + sel(lineOpts, forecastStyle) + '</select>'
      + '<input type="range" class="sc-forecast-width" ' + dw + ' min="1" max="6" value="' + forecastWidth + '" title="Width">'
      + '<span class="style-range-val">' + forecastWidth + '</span>'
      + '</div>';

    /* Forecast Markers */
    html += '<div class="style-row"><span class="style-label">Fcst Markers</span>'
      + '<label class="style-check"><input type="checkbox" class="sc-forecast-markers" ' + dw + chk(forecastMarkers) + '> Show</label>'
      + '<label class="style-check"><input type="checkbox" class="sc-forecast-labels" ' + dw + chk(forecastLabels) + '> Labels</label>'
      + '<select class="sc-forecast-symbol" ' + dw + '>' + sel(symOpts, forecastSymbol) + '</select>'
      + '<input type="range" class="sc-forecast-msize" ' + dw + ' min="2" max="20" value="' + forecastSymbolSize + '" title="Size">'
      + '<span class="style-range-val">' + forecastSymbolSize + '</span>'
      + '</div>';

    /* P10/P90 */
    html += '<div class="style-row"><span class="style-label">P10 Curve</span>'
      + '<input type="color" class="sc-p10-color" ' + dw + ' value="' + p10Color + '">'
      + '<label class="style-check"><input type="checkbox" class="sc-p10-line" ' + dw + chk(p10Line) + '> Line</label>'
      + '<label class="style-check"><input type="checkbox" class="sc-p10-marker" ' + dw + chk(p10Marker) + '> Markers</label>'
      + '<label class="style-check"><input type="checkbox" class="sc-p10-labels" ' + dw + chk(p10Labels) + '> Labels</label>'
      + '<select class="sc-p10-style" ' + dw + '>' + sel(lineOpts, p10Style) + '</select>'
      + '</div>';

    html += '<div class="style-row"><span class="style-label">P90 Curve</span>'
      + '<input type="color" class="sc-p90-color" ' + dw + ' value="' + p90Color + '">'
      + '<label class="style-check"><input type="checkbox" class="sc-p90-line" ' + dw + chk(p90Line) + '> Line</label>'
      + '<label class="style-check"><input type="checkbox" class="sc-p90-marker" ' + dw + chk(p90Marker) + '> Markers</label>'
      + '<label class="style-check"><input type="checkbox" class="sc-p90-labels" ' + dw + chk(p90Labels) + '> Labels</label>'
      + '<select class="sc-p90-style" ' + dw + '>' + sel(lineOpts, p90Style) + '</select>'
      + '</div>';

    html += '</div>'; /* close section */
  };

  wellNames.forEach((wn, idx) => buildCurveSection(wn, idx));

  /* ---- Multi-fit curve style sections (with P10/P90) ---- */
  multiFits.forEach(mf => {
    const mfKey = 'mf_' + mf.id;
    const ms = mfStyles[mfKey] || {};
    const mfColor = ms.color || mf.color || '#8b5cf6';
    const mfLineStyle = ms.lineStyle || 'solid';
    const mfWidth = ms.lineWidth != null ? ms.lineWidth : 2.5;
    const mfP10Color = ms.p10Color || mfColor;
    const mfP10Line = ms.p10Line != null ? ms.p10Line : true;
    const mfP10Marker = ms.p10Marker || false;
    const mfP10Labels = ms.p10Labels || false;
    const mfP10Style = ms.p10Style || 'dotted';
    const mfP90Color = ms.p90Color || mfColor;
    const mfP90Line = ms.p90Line != null ? ms.p90Line : true;
    const mfP90Marker = ms.p90Marker || false;
    const mfP90Labels = ms.p90Labels || false;
    const mfP90Style = ms.p90Style || 'dotted';
    const mfSeriesName = 'Curve #' + mf.id + ' (' + mf.model + ')';
    const sectionKey = 'mf:' + mf.id;
    const isActive = sectionKey === activeKey;

    html += '<div class="style-section style-curve-section style-mf-section" data-mf-id="' + mf.id + '" data-curve-key="' + esc(sectionKey) + '" data-series-name="' + esc(mfSeriesName) + '"' + (isActive ? '' : ' style="display:none"') + '>';

    /* Model selector */
    const modelOpts2 = [{v:'exponential',l:'Exponential'},{v:'hyperbolic',l:'Hyperbolic'},{v:'harmonic',l:'Harmonic'}];
    html += '<div class="style-row"><span class="style-label">Model</span>'
      + '<select class="sc-mf-model" data-mf-id="' + mf.id + '">' + sel(modelOpts2, mf.model) + '</select>'
      + '</div>';

    html += '<div class="style-row"><span class="style-label">Color</span>'
      + '<input type="color" class="sc-mf-color" data-mf-id="' + mf.id + '" value="' + mfColor + '">'
      + '</div>';

    html += '<div class="style-row"><span class="style-label">Line Style</span>'
      + '<select class="sc-mf-line-style" data-mf-id="' + mf.id + '">' + sel(lineOpts, mfLineStyle) + '</select>'
      + '<input type="range" class="sc-mf-line-width" data-mf-id="' + mf.id + '" min="1" max="6" value="' + mfWidth + '" title="Width">'
      + '<span class="style-range-val">' + mfWidth + '</span>'
      + '</div>';

    const mfMarkers = ms.showMarkers || false;
    const mfMarkerSymbol = ms.markerSymbol || 'circle';
    const mfMarkerSize = ms.markerSize != null ? ms.markerSize : 8;
    const mfShowLabels = ms.showLabels || false;
    html += '<div class="style-row"><span class="style-label">Markers</span>'
      + '<label class="style-check"><input type="checkbox" class="sc-mf-markers" data-mf-id="' + mf.id + '"' + chk(mfMarkers) + '> Show</label>'
      + '<label class="style-check"><input type="checkbox" class="sc-mf-labels" data-mf-id="' + mf.id + '"' + chk(mfShowLabels) + '> Labels</label>'
      + '<select class="sc-mf-symbol" data-mf-id="' + mf.id + '">' + sel(symOpts, mfMarkerSymbol) + '</select>'
      + '<input type="range" class="sc-mf-msize" data-mf-id="' + mf.id + '" min="2" max="25" value="' + mfMarkerSize + '" title="Size">'
      + '<span class="style-range-val">' + mfMarkerSize + '</span>'
      + '</div>';
      + '<input type="color" class="sc-mf-color" data-mf-id="' + mf.id + '" value="' + mfColor + '">'
      + '</div>';

    html += '<div class="style-row"><span class="style-label">Line Style</span>'
      + '<select class="sc-mf-line-style" data-mf-id="' + mf.id + '">' + sel(lineOpts, mfLineStyle) + '</select>'
      + '<input type="range" class="sc-mf-line-width" data-mf-id="' + mf.id + '" min="1" max="6" value="' + mfWidth + '" title="Width">'
      + '<span class="style-range-val">' + mfWidth + '</span>'
      + '</div>';

    html += '<div class="style-row"><span class="style-label">P10 Curve</span>'
      + '<input type="color" class="sc-mf-p10-color" data-mf-id="' + mf.id + '" value="' + mfP10Color + '">'
      + '<label class="style-check"><input type="checkbox" class="sc-mf-p10-line" data-mf-id="' + mf.id + '"' + chk(mfP10Line) + '> Line</label>'
      + '<label class="style-check"><input type="checkbox" class="sc-mf-p10-marker" data-mf-id="' + mf.id + '"' + chk(mfP10Marker) + '> Markers</label>'
      + '<label class="style-check"><input type="checkbox" class="sc-mf-p10-labels" data-mf-id="' + mf.id + '"' + chk(mfP10Labels) + '> Labels</label>'
      + '<select class="sc-mf-p10-style" data-mf-id="' + mf.id + '">' + sel(lineOpts, mfP10Style) + '</select>'
      + '</div>';

    html += '<div class="style-row"><span class="style-label">P90 Curve</span>'
      + '<input type="color" class="sc-mf-p90-color" data-mf-id="' + mf.id + '" value="' + mfP90Color + '">'
      + '<label class="style-check"><input type="checkbox" class="sc-mf-p90-line" data-mf-id="' + mf.id + '"' + chk(mfP90Line) + '> Line</label>'
      + '<label class="style-check"><input type="checkbox" class="sc-mf-p90-marker" data-mf-id="' + mf.id + '"' + chk(mfP90Marker) + '> Markers</label>'
      + '<label class="style-check"><input type="checkbox" class="sc-mf-p90-labels" data-mf-id="' + mf.id + '"' + chk(mfP90Labels) + '> Labels</label>'
      + '<select class="sc-mf-p90-style" data-mf-id="' + mf.id + '">' + sel(lineOpts, mfP90Style) + '</select>'
      + '</div>';

    html += '</div>';
  });

  container.innerHTML = html;

  /* ---- Wire up curve selector dropdown ---- */
  const selectEl = container.querySelector('.sc-curve-select');
  if (selectEl) {
    selectEl.addEventListener('change', () => {
      const newKey = selectEl.value;
      cardActiveCurveStyle[cardId] = newKey;
      container.querySelectorAll('.style-curve-section').forEach(sec => {
        sec.style.display = sec.getAttribute('data-curve-key') === newKey ? '' : 'none';
      });
    });
  }

  /* ---- Wire up live re-render for per-well controls ---- */
  const liveStyleRerender = () => {
    const st = readCardStyles(cardId);
    cardStyles[cardId] = st;
    const headerEl = card._headerInput || card.querySelector('.p-header');
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

  container.querySelectorAll('input[type="range"]').forEach(r => {
    const valSpan = r.nextElementSibling;
    r.addEventListener('input', () => {
      if (valSpan) valSpan.textContent = r.value;
      liveStyleRerender();
    });
  });
  container.querySelectorAll('input[type="color"], select:not(.sc-curve-select):not(.sc-mf-model):not(.sc-main-model), input[type="checkbox"]').forEach(el => {
    el.addEventListener('change', liveStyleRerender);
  });

  /* Wire up model selectors for multi-fit curves (re-fit, not just style) */
  container.querySelectorAll('.sc-mf-model').forEach(sel => {
    sel.addEventListener('change', () => {
      const mfId = sel.getAttribute('data-mf-id');
      changeMultiFitModel(cardId, mfId, sel.value);
    });
  });

  /* Wire up model selector for main fit curve (re-fit with new model) */
  container.querySelectorAll('.sc-main-model').forEach(sel => {
    sel.addEventListener('change', () => {
      changeMainFitModel(cardId, sel.value);
    });
  });
}

/* Switch the visible style section to the curve matching seriesName */
function highlightStyleSection(cardId, seriesName) {
  const card = document.getElementById(cardId);
  if (!card) return;

  /* Open style panel if closed */
  const panel = document.getElementById('style-' + cardId);
  if (panel && !panel.classList.contains('show')) panel.classList.add('show');

  const container = document.getElementById('styleCurves-' + cardId);
  if (!container) return;

  /* Resolve seriesName to a data-curve-key */
  let targetKey = null;

  /* Direct match by data-series-name */
  container.querySelectorAll('.style-curve-section').forEach(sec => {
    if (sec.getAttribute('data-series-name') === seriesName) {
      targetKey = sec.getAttribute('data-curve-key');
    }
  });

  /* For P10/P90 or Actual: map back to parent section */
  if (!targetKey) {
    const cleaned = seriesName.replace(/ P10.*$| P90.*$/, '').replace(/ Actual$/, ' Fitted').replace(/^Actual$/, 'Fitted');
    container.querySelectorAll('.style-curve-section').forEach(sec => {
      if (sec.getAttribute('data-series-name') === cleaned) {
        targetKey = sec.getAttribute('data-curve-key');
      }
    });
  }
  /* Multi-fit P10/P90: "Curve #1 (exp) P10" → "Curve #1 (exp)" */
  if (!targetKey) {
    const mfCleaned = seriesName.replace(/ P10$| P90$/, '');
    container.querySelectorAll('.style-curve-section').forEach(sec => {
      if (sec.getAttribute('data-series-name') === mfCleaned) {
        targetKey = sec.getAttribute('data-curve-key');
      }
    });
  }

  if (!targetKey) return;

  /* Switch active section */
  cardActiveCurveStyle[cardId] = targetKey;
  container.querySelectorAll('.style-curve-section').forEach(sec => {
    sec.style.display = sec.getAttribute('data-curve-key') === targetKey ? '' : 'none';
  });

  /* Update selector dropdown if present */
  const selectEl = container.querySelector('.sc-curve-select');
  if (selectEl) selectEl.value = targetKey;

  /* Flash highlight */
  const targetSection = container.querySelector('.style-curve-section[data-curve-key="' + targetKey + '"]');
  if (targetSection) {
    targetSection.classList.remove('style-section-highlight');
    void targetSection.offsetWidth; /* force reflow */
    targetSection.classList.add('style-section-highlight');
    targetSection.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    setTimeout(() => targetSection.classList.remove('style-section-highlight'), 2000);
  }
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
  const palette = preset.palette || [preset.actualColor];
  const sections = card.querySelectorAll('.style-curve-section[data-well]');
  const isSingle = sections.length <= 1;
  sections.forEach((sec, idx) => {
    const color = isSingle ? preset.actualColor : palette[idx % palette.length];
    const q = (cls) => sec.querySelector('.' + cls);
    if (q('sc-actual-color'))   q('sc-actual-color').value = color;
    if (q('sc-fitted-color'))   q('sc-fitted-color').value = isSingle ? preset.fittedColor : color;
    if (q('sc-forecast-color')) q('sc-forecast-color').value = isSingle ? preset.forecastColor : color;
    if (q('sc-p10-color'))      q('sc-p10-color').value = preset.p10Color;
    if (q('sc-p90-color'))      q('sc-p90-color').value = preset.p90Color;
  });
}



function readCardStyles(cardId) {

  const card = document.getElementById(cardId);

  if (!card) return getDefaultStyles();

  /* Read per-curve styles from dynamic sections */
  const curveStyles = {};
  const curveSections = card.querySelectorAll('.style-curve-section');
  curveSections.forEach(sec => {
    const wellName = sec.getAttribute('data-well');
    if (!wellName) return;
    const q = (cls) => sec.querySelector('.' + cls);
    curveStyles[wellName] = {
      actualColor: q('sc-actual-color')?.value || '#3b82f6',
      actualSymbol: q('sc-actual-symbol')?.value || 'circle',
      actualSize: parseInt(q('sc-actual-size')?.value || '10'),
      fittedColor: q('sc-fitted-color')?.value || '#f59e0b',
      fittedStyle: q('sc-fitted-style')?.value || 'solid',
      fittedWidth: parseInt(q('sc-fitted-width')?.value || '2'),
      fittedMarkers: q('sc-fitted-markers')?.checked || false,
      fittedSymbol: q('sc-fitted-symbol')?.value || 'triangle',
      fittedSymbolSize: parseInt(q('sc-fitted-msize')?.value || '14'),
      forecastColor: q('sc-forecast-color')?.value || '#22c55e',
      forecastStyle: q('sc-forecast-style')?.value || 'dashed',
      forecastWidth: parseInt(q('sc-forecast-width')?.value || '3'),
      forecastMarkers: q('sc-forecast-markers')?.checked || false,
      forecastLabels: q('sc-forecast-labels')?.checked || false,
      forecastSymbol: q('sc-forecast-symbol')?.value || 'triangle',
      forecastSymbolSize: parseInt(q('sc-forecast-msize')?.value || '14'),
      p10Color: q('sc-p10-color')?.value || '#22c55e',
      p10Line: q('sc-p10-line')?.checked !== false,
      p10Marker: q('sc-p10-marker')?.checked || false,
      p10Labels: q('sc-p10-labels')?.checked || false,
      p10Style: q('sc-p10-style')?.value || 'solid',
      p90Color: q('sc-p90-color')?.value || '#ef4444',
      p90Line: q('sc-p90-line')?.checked !== false,
      p90Marker: q('sc-p90-marker')?.checked || false,
      p90Labels: q('sc-p90-labels')?.checked || false,
      p90Style: q('sc-p90-style')?.value || 'solid',
    };
  });

  /* Read multi-fit curve styles from dynamic sections */
  const multiFitStyles = {};
  card.querySelectorAll('.style-mf-section').forEach(sec => {
    const mfId = sec.getAttribute('data-mf-id');
    if (!mfId) return;
    const q = (cls) => sec.querySelector('.' + cls);
    multiFitStyles['mf_' + mfId] = {
      color: q('sc-mf-color')?.value || '#8b5cf6',
      lineStyle: q('sc-mf-line-style')?.value || 'solid',
      lineWidth: parseFloat(q('sc-mf-line-width')?.value || '2.5'),
      showMarkers: q('sc-mf-markers')?.checked || false,
      markerSymbol: q('sc-mf-symbol')?.value || 'circle',
      markerSize: parseInt(q('sc-mf-msize')?.value || '8'),
      showLabels: q('sc-mf-labels')?.checked || false,
      p10Color: q('sc-mf-p10-color')?.value || '#8b5cf6',
      p10Line: q('sc-mf-p10-line')?.checked !== false,
      p10Marker: q('sc-mf-p10-marker')?.checked || false,
      p10Labels: q('sc-mf-p10-labels')?.checked || false,
      p10Style: q('sc-mf-p10-style')?.value || 'dotted',
      p90Color: q('sc-mf-p90-color')?.value || '#8b5cf6',
      p90Line: q('sc-mf-p90-line')?.checked !== false,
      p90Marker: q('sc-mf-p90-marker')?.checked || false,
      p90Labels: q('sc-mf-p90-labels')?.checked || false,
      p90Style: q('sc-mf-p90-style')?.value || 'dotted',
    };
  });

  /* First curve provides backward-compatible top-level defaults */
  const firstWell = Object.keys(curveStyles)[0];
  const first = firstWell ? curveStyles[firstWell] : {};

  return {
    plotTheme: card.querySelector('.s-plot-theme')?.value || 'classic',
    curveStyles: curveStyles,
    multiFitStyles: multiFitStyles,

    actualColor: first.actualColor || '#3b82f6',
    actualSymbol: first.actualSymbol || 'circle',
    actualSize: first.actualSize != null ? first.actualSize : 10,
    fittedColor: first.fittedColor || '#f59e0b',
    fittedStyle: first.fittedStyle || 'solid',
    fittedWidth: first.fittedWidth != null ? first.fittedWidth : 2,
    fittedMarkers: first.fittedMarkers || false,
    fittedSymbol: first.fittedSymbol || 'triangle',
    fittedSymbolSize: first.fittedSymbolSize != null ? first.fittedSymbolSize : 14,
    forecastColor: first.forecastColor || '#22c55e',
    forecastStyle: first.forecastStyle || 'dashed',
    forecastWidth: first.forecastWidth != null ? first.forecastWidth : 3,
    forecastMarkers: first.forecastMarkers || false,
    forecastLabels: first.forecastLabels || false,
    forecastSymbol: first.forecastSymbol || 'triangle',
    forecastSymbolSize: first.forecastSymbolSize != null ? first.forecastSymbolSize : 14,
    p10Color: first.p10Color || '#22c55e',
    p10Line: first.p10Line !== false,
    p10Marker: first.p10Marker || false,
    p10Labels: first.p10Labels || false,
    p10Style: first.p10Style || 'solid',
    p90Color: first.p90Color || '#ef4444',
    p90Line: first.p90Line !== false,
    p90Marker: first.p90Marker || false,
    p90Labels: first.p90Labels || false,
    p90Style: first.p90Style || 'solid',

    gridX: card.querySelector('.s-grid-x')?.checked !== false,

    gridY: card.querySelector('.s-grid-y')?.checked !== false,

    headerFontSize: parseFloat(card.querySelector('.s-header-fsize')?.value || '1.8'),
    headerColor: card.querySelector('.s-header-color')?.value || (document.documentElement.getAttribute('data-theme') === 'light' ? '#0f172a' : '#e2e8f0'),
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

  /* Store curveStyles and multiFitStyles so rebuildCurveStyleSections can pick them up */
  if (merged.curveStyles || merged.multiFitStyles) {
    const existing = cardStyles[cardId] || {};
    if (merged.curveStyles) existing.curveStyles = merged.curveStyles;
    if (merged.multiFitStyles) existing.multiFitStyles = merged.multiFitStyles;
    cardStyles[cardId] = { ...merged, ...existing, curveStyles: merged.curveStyles || existing.curveStyles, multiFitStyles: merged.multiFitStyles || existing.multiFitStyles };
  }

  /* Rebuild dynamic curve sections (they will read from cardStyles[cardId].curveStyles) */
  rebuildCurveStyleSections(cardId);

  const gx = card.querySelector('.s-grid-x'); if (gx) gx.checked = merged.gridX !== false;

  const gy = card.querySelector('.s-grid-y'); if (gy) gy.checked = merged.gridY !== false;

  s('.s-header-fsize', merged.headerFontSize || 1.8);
  s('.s-header-color', merged.headerColor || (document.documentElement.getAttribute('data-theme') === 'light' ? '#0f172a' : '#e2e8f0'));
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

  delete cardLogScale[id]; delete cardLogScaleX[id]; delete cardPctChange[id]; delete cardAxisLabels[id]; delete cardAxisPositions[id];

  delete cardMultiFits[id]; delete cardHiddenSeries[id];

  const el = document.getElementById(id);

  if (el) {
    const wrap = el.closest('.card-wrap');
    if (wrap) wrap.remove(); else el.remove();
  }

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

  // If there are pending Ctrl-selected points, auto-include them (remove from exclusions)
  // so that "select more points → click Plot" naturally adds them to the fit
  const pendingCtrl = cardCtrlSelected[cardId];
  if (pendingCtrl && pendingCtrl.size > 0) {
    if (!cardExclusions[cardId]) cardExclusions[cardId] = new Set();
    pendingCtrl.forEach(function (idx) { cardExclusions[cardId].delete(idx); });
    clearCtrlSelectionHighlights(cardId);
    hideMultiPointMenu();
  }
  // Get column selections from the card's own selectors
  const xVal = card.querySelector('.p-selX')?.value || '';
  const yVal = card.querySelector('.p-selY')?.value || '';
  const wellCol = card.querySelector('.p-selWellCol')?.value || '';
  if (!xVal || !yVal || !wellCol) { alert('Please select X Axis, Y Axis, and Well Column on this card.'); return; }

  const btn = card.querySelector('.btn');

  btn.innerHTML = '<span class="loader"></span>';

  btn.disabled = true;

  /* Save existing Qi anchor (x-display, y) pairs so they can be re-inserted
     into the fresh server data after the DCA fetch completes. */
  const savedAnchors = [];
  const prevWell = cardLastData[cardId]?.wells?.[0];
  if (prevWell && prevWell.qi_anchor_indices && prevWell.qi_anchor_indices.length > 0) {
    for (const i of prevWell.qi_anchor_indices) {
      if (i < prevWell.x.length) {
        savedAnchors.push({ x: prevWell.x[i], y: prevWell.y_actual[i] });
      }
    }
  }

  const excl = cardExclusions[cardId] || new Set();

  const exclStr = [...excl].join(',');

  try {

    const url = `/api/dca?x=${enc(xVal)}&y=${enc(yVal)}&well_col=${enc(wellCol)}&wells=${enc(well)}&model=${enc(model)}&forecast_months=${months}&exclude_indices=${enc(exclStr)}&combine=${combine}&combine_func=${enc(combineAgg)}`;

    const res = await fetch(url);

    const data = await res.json();

    if (!res.ok) throw new Error(data.detail || 'Error');

    cardLastData[cardId] = data;

    rebuildCurveStyleSections(cardId);
    cardStyles[cardId] = readCardStyles(cardId);

    /* Re-insert saved Qi anchor points into fresh data, then refit */
    if (savedAnchors.length > 0 && data.wells && data.wells.length === 1) {
      const w = data.wells[0];
      const isDate = w.is_date || false;

      for (const anchor of savedAnchors) {
        const xCoord = isDate ? parseDateStr(anchor.x) : anchor.x;
        const tNew = xToT(w, xCoord);

        /* Insert in sorted t order */
        let insertIdx = w.t.length;
        for (let i = 0; i < w.t.length; i++) {
          if (tNew < w.t[i]) { insertIdx = i; break; }
        }
        w.t.splice(insertIdx, 0, tNew);
        w.y_actual.splice(insertIdx, 0, anchor.y);
        w.x.splice(insertIdx, 0, anchor.x);
        if (w.y_fitted) w.y_fitted.splice(insertIdx, 0, null);

        /* Shift exclusion indices */
        const curExcl = cardExclusions[cardId] || new Set();
        const shiftedExcl = new Set();
        for (const idx of curExcl) { shiftedExcl.add(idx >= insertIdx ? idx + 1 : idx); }
        cardExclusions[cardId] = shiftedExcl;
        w.excluded_indices = [...shiftedExcl].sort((a, b) => a - b);

        /* Shift and record anchor indices */
        const anchorSet = new Set();
        for (const idx of (w.qi_anchor_indices || [])) {
          anchorSet.add(idx >= insertIdx ? idx + 1 : idx);
        }
        anchorSet.add(insertIdx);
        w.qi_anchor_indices = [...anchorSet];
      }

      await refitCurrentData(cardId);
    } else {
      renderSingleChart(cardId, data, months);
    }

  } catch (err) { alert(err.message); }

  finally { btn.innerHTML = 'Plot'; btn.disabled = false; }

}



function toggleExclusion(cardId, index) {

  const _w0 = cardLastData[cardId]?.wells?.[0];
  /* Qi anchor points are immune — clicking them does nothing */
  if (_w0 && (_w0.qi_anchor_indices || []).includes(index)) return;

  if (!cardExclusions[cardId]) cardExclusions[cardId] = new Set();

  const s = cardExclusions[cardId];

  if (s.has(index)) s.delete(index); else s.add(index);

  saveZoomState(cardId);

  if (_w0 && _w0.qi_anchor_indices && _w0.qi_anchor_indices.length > 0) {
    refitCurrentData(cardId);
  } else {
    runSingleDCA(cardId);
  }

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
    if (opts) {
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
        if (idx !== undefined && idx !== null) {
          const nativeEvt = params.event && params.event.event;
          if (nativeEvt) nativeEvt.stopPropagation();
          const ex = nativeEvt ? nativeEvt.clientX : (params.event ? params.event.offsetX || 300 : 300);
          const ey = nativeEvt ? nativeEvt.clientY : (params.event ? params.event.offsetY || 200 : 200);
          showPointMenu(cardId, idx, ex, ey);
        }
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
    setupCtrlClickSelection(cardId, fullChart, chartDiv);
    setupAxisHoverTooltip(cardId, chartDiv); // cardId allows axis hover to read same global axis styles

    // Resize observer to handle window resize while modal is open
    const resizeObserver = new ResizeObserver(() => fullChart.resize());
    resizeObserver.observe(chartDiv);

    // Clean up observer when modal is removed
    const overlay = document.querySelector('.modal-overlay');
    if (overlay) {
      const originalRemove = overlay.remove.bind(overlay);
      overlay.remove = function () {
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

function toggleLabelCollapse(cardId, type) {
  if (!cardLabelCollapseStates[cardId]) cardLabelCollapseStates[cardId] = {};
  cardLabelCollapseStates[cardId][type] = !cardLabelCollapseStates[cardId][type];

  // Update both mini and full charts if they exist
  const mChart = chartInstances[cardId];
  if (mChart) updatePctChangeGraphic(cardId, mChart);
  const fChart = chartInstances['fullChart-' + cardId];
  if (fChart) updatePctChangeGraphic(cardId, fChart);
}

/* Logic to generate specialized % change graphic elements */
function getPctChangeGraphic(cardId) {
  const showPct = cardPctChange[cardId] || false;
  if (!showPct) return [];
  const data = cardLastData[cardId];
  if (!data || !data.wells || data.wells.length === 0) return [];
  const pw = data.wells[0];
  const isLight = document.documentElement.getAttribute('data-theme') === 'light';
  const isDate = pw.is_date || false;
  const colStates = cardLabelCollapseStates[cardId] || {};

  let graphicElems = [];
  let pctTopOffset = 55;

  const pushPctGraphic = (type, labelPrefix, fVal, lVal, fDate, lDate, ovrCol) => {
    const isCollapsed = colStates[type] || false;
    const diff = lVal - fVal;
    const pct = fVal !== 0 ? ((diff / Math.abs(fVal)) * 100) : 0;
    const sign = diff >= 0 ? '+' : '';
    const toggleIcon = isCollapsed ? '[+] ' : '[-] ';

    let pctText;
    if (isCollapsed) {
      pctText = `${toggleIcon}${labelPrefix}\u0394 ${sign}${diff.toFixed(2)} (${sign}${pct.toFixed(1)}%)`;
    } else {
      pctText = `${toggleIcon}${labelPrefix}[${fDate}] ${fVal.toFixed(2)} \u2192 [${lDate}] ${lVal.toFixed(2)}  |  \u0394 ${sign}${diff.toFixed(2)} (${sign}${pct.toFixed(1)}%)`;
    }

    const color = ovrCol || (diff >= 0 ? '#22c55e' : '#ef4444');
    const borderColor = ovrCol || (diff >= 0 ? 'rgba(34,197,94,.4)' : 'rgba(239,68,68,.4)');

    graphicElems.push({
      type: 'text', right: 40, top: pctTopOffset, z: 100,
      cursor: 'pointer',
      onclick: () => toggleLabelCollapse(cardId, type),
      style: {
        text: pctText, fontSize: 11.2, fontWeight: 'bold',
        fill: color,
        backgroundColor: isLight ? 'rgba(255,255,255,.92)' : 'rgba(34,37,51,.92)',
        borderColor: borderColor,
        borderWidth: 1.2, padding: [6, 12, 6, 12], borderRadius: 4,
        shadowBlur: 6, shadowColor: 'rgba(0,0,0,0.15)', shadowOffsetX: 1, shadowOffsetY: 2
      },
      emphasis: {
        style: {
          shadowBlur: 10,
          backgroundColor: isLight ? '#fff' : '#2d3142'
        }
      }
    });
    pctTopOffset += isCollapsed ? 30 : 35;
  };

  /* Actual data */
  const validIndices = [];
  pw.y_actual.forEach((v, i) => { if (v != null) validIndices.push(i); });
  if (validIndices.length >= 2) {
    const firstIdx = validIndices[0], lastIdx = validIndices[validIndices.length - 1];
    pushPctGraphic('actual', '', pw.y_actual[firstIdx], pw.y_actual[lastIdx], pw.x[firstIdx], pw.x[lastIdx]);
  }

  /* P10 / P50 / P90 curves */
  if (pw.params && pw.y_fitted) {
    const ps = (cardPCurveState[cardId] || {})['main'];
    const st = readCardStyles(cardId) || {};

    const addPCurvePct = (type, name, diVal, color) => {
      const pts = buildPCurveData(pw, data, diVal, isDate, null);
      const validPts = pts.filter(p => p[1] != null && isFinite(p[1]));
      if (validPts.length >= 2) {
        const fPt = validPts[0], lPt = validPts[validPts.length - 1];
        const fDateStr = isDate ? formatDateTs(fPt[0]) : fPt[0];
        const lDateStr = isDate ? formatDateTs(lPt[0]) : lPt[0];
        pushPctGraphic(type, name + ': ', fPt[1], lPt[1], fDateStr, lDateStr, color);
      }
    };

    if (ps && ps.enabled) addPCurvePct('p10', 'P10', ps.p10Di, st.p10Color || '#22c55e');
    addPCurvePct('p50', 'P50', Math.abs(pw.params.di), st.fittedColor || '#f59e0b');
    if (ps && ps.enabled) addPCurvePct('p90', 'P90', ps.p90Di, st.p90Color || '#ef4444');
  }
  return graphicElems;
}

/* Live-update the % change / diff graphic elements on the chart */
function updatePctChangeGraphic(cardId, myChart) {
  const graphicElems = getPctChangeGraphic(cardId);
  myChart.setOption({ graphic: graphicElems }, false);
}



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



/* Toggle P10/P90 curves for a SPECIFIC curve (main or multi-fit) */
function toggleCurvePCurves(cardId, cType, cId) {
  const data = cardLastData[cardId];
  if (!data || !data.wells || !data.wells[0]) return;
  const w = data.wells[0];

  const curveKey = cType === 'main' ? 'main' : 'mf_' + cId;

  let baseDi = 0.01;
  if (cType === 'main') {
    if (w.params && w.params.di) baseDi = Math.abs(w.params.di);
  } else {
    const fits = cardMultiFits[cardId] || [];
    const mf = fits.find(f => f.id == cId);
    if (mf && mf.params && mf.params.di) {
      baseDi = Math.abs(mf.params.di);
    }
  }

  if (!cardPCurveState[cardId]) cardPCurveState[cardId] = {};

  if (!cardPCurveState[cardId][curveKey]) {
    cardPCurveState[cardId][curveKey] = {
      enabled: false,
      p10Di: baseDi * 0.7,
      p90Di: baseDi * 1.5,
    };
  }

  const ps = cardPCurveState[cardId][curveKey];
  ps.enabled = !ps.enabled;

  if (cardLastData[cardId]) {
    saveZoomState(cardId);
    renderSingleChart(cardId, cardLastData[cardId], document.getElementById(cardId)?.querySelector('.p-forecast')?.value || 0);
  }
  updateMultiFitPanel(cardId);
}

function setupPCurveDragHandles(cardId, myChart) {
  const data = cardLastData[cardId];
  if (!data || !data.wells || data.wells.length === 0) return;
  const w = data.wells[0];
  if (!w.params || !w.t) return;

  const isDate = w.is_date || false;
  const zr = myChart.getZr();
  let dragging = null;
  let anchorT = 0;
  let dragOffsetY = 0;
  let anchorXPx = 0;

  function hitTestSeries(px, py) {
    const THRESHOLD = 10;
    const opt = myChart.getOption();
    if (!opt || !opt.series) { console.log('[HitTest] No option/series'); return null; }

    const isSingle = data.wells.length === 1;
    const prefix = isSingle ? '' : (w.well || '') + ' ';
    const fittedName = prefix + 'Fitted';

    for (let si = 0; si < opt.series.length; si++) {
      const s = opt.series[si];
      if (s.type !== 'line') continue;
      const sData = s.data;
      if (!sData || sData.length === 0) continue;

      let hitType = null, which = null, curveKey = null, mfId = null;

      if (s.id) {
        const parts = s.id.split('|');
        if (parts.length >= 2 && (parts[0] === 'p10' || parts[0] === 'p90')) {
          hitType = 'pcurve';
          which = parts[0];
          curveKey = parts[1];
        }
      }
      if (!hitType && s.name === fittedName) {
        hitType = 'fitted';
        curveKey = 'main';
      }
      if (!hitType && s.name && s.name.startsWith('Curve #')) {
        const m = s.name.match(/^Curve #(\d+)\s/);
        if (m) {
          hitType = 'multifit';
          mfId = parseInt(m[1]);
          curveKey = 'mf_' + mfId;
        }
      }
      if (!hitType) continue;

      console.log('[HitTest] Checking series:', s.name, 'type:', hitType, 'dataLen:', sData.length);
      let closestPixDist = Infinity;
      let prevPt = null;
      for (let di = 0; di < sData.length; di++) {
        if (!Array.isArray(sData[di]) || sData[di][1] == null) { prevPt = null; continue; }
        const ptPx = myChart.convertToPixel('grid', [sData[di][0], sData[di][1]]);
        if (!ptPx) { console.log('[HitTest]  convertToPixel returned null for di=', di, 'data=', sData[di]); prevPt = null; continue; }
        const dxPt = Math.abs(ptPx[0] - px), dyPt = Math.abs(ptPx[1] - py);
        const ptDist = Math.hypot(dxPt, dyPt);
        if (ptDist < closestPixDist) closestPixDist = ptDist;
        if (dxPt < THRESHOLD * 3 && dyPt < THRESHOLD) {
          return { type: hitType, which, curveKey, mfId, seriesIdx: si, dataIdx: di };
        }
        if (prevPt) {
          const dist = pointToSegmentDist(px, py, prevPt[0], prevPt[1], ptPx[0], ptPx[1]);
          if (dist < THRESHOLD) return { type: hitType, which, curveKey, mfId, seriesIdx: si, dataIdx: di };
        }
        prevPt = ptPx;
      }
      console.log('[HitTest]  closestPixDist for', s.name, '=', closestPixDist.toFixed(1));
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

  function getTAtPixelX(pxX) {
    const dp = myChart.convertFromPixel('grid', [pxX, 0]);
    if (!dp) return null;

    if (!isDate) {
      const dataX = dp[0];
      if (w.t.length === 0) return null;
      const approxT = dataX - w.x[0] + w.t[0];
      return Math.max(0, approxT);
    }

    const tsVal = dp[0];
    const xtPairs = [];
    for (let i = 0; i < w.x.length; i++) {
      if (w.t[i] == null) continue;
      const ts = parseDateStr(w.x[i]);
      if (Number.isFinite(ts)) xtPairs.push([ts, w.t[i]]);
    }
    if (w.forecast && w.forecast.x && w.forecast.t) {
      for (let i = 0; i < w.forecast.x.length; i++) {
        if (w.forecast.t[i] == null) continue;
        const ts = parseDateStr(w.forecast.x[i]);
        if (Number.isFinite(ts)) xtPairs.push([ts, w.forecast.t[i]]);
      }
    }
    if (xtPairs.length === 0) return null;

    xtPairs.sort((a, b) => a[0] - b[0]);
    if (tsVal <= xtPairs[0][0]) return xtPairs[0][1];
    if (tsVal >= xtPairs[xtPairs.length - 1][0]) return xtPairs[xtPairs.length - 1][1];

    for (let i = 1; i < xtPairs.length; i++) {
      const prev = xtPairs[i - 1];
      const next = xtPairs[i];
      if (tsVal > next[0]) continue;
      if (next[0] === prev[0]) return next[1];
      const frac = (tsVal - prev[0]) / (next[0] - prev[0]);
      return prev[1] + frac * (next[1] - prev[1]);
    }

    return xtPairs[xtPairs.length - 1][1];
  }

  function interpolateCurvePxY(seriesIdx, px) {
    const opt = myChart.getOption();
    const s = opt && opt.series && opt.series[seriesIdx];
    if (!s || !s.data) return null;
    let bestCurvePxY = null;
    let prevPx = null;
    let closestDist = Infinity, closestPxY = null;
    for (let di = 0; di < s.data.length; di++) {
      if (!Array.isArray(s.data[di]) || s.data[di][1] == null) { prevPx = null; continue; }
      const curPx = myChart.convertToPixel('grid', [s.data[di][0], s.data[di][1]]);
      if (!curPx) { prevPx = null; continue; }
      const xDist = Math.abs(curPx[0] - px);
      if (xDist < closestDist) { closestDist = xDist; closestPxY = curPx[1]; }
      if (xDist < 1) { bestCurvePxY = curPx[1]; break; }
      if (prevPx) {
        const minX = Math.min(prevPx[0], curPx[0]);
        const maxX = Math.max(prevPx[0], curPx[0]);
        if (px >= minX && px <= maxX && maxX > minX) {
          const frac = (px - prevPx[0]) / (curPx[0] - prevPx[0]);
          bestCurvePxY = prevPx[1] + frac * (curPx[1] - prevPx[1]);
          break;
        }
      }
      prevPx = curPx;
    }
    return bestCurvePxY != null ? bestCurvePxY : closestPxY;
  }

  function onMouseDown(e) {
    const px = e.offsetX, py = e.offsetY;
    console.log('[PCurveDrag] mousedown at px=', px, 'py=', py);
    const hit = hitTestSeries(px, py);
    console.log('[PCurveDrag] hitTestSeries result:', hit);
    if (hit) {
      dragging = hit;
      anchorXPx = px;
      const tVal = getTAtPixelX(px);
      anchorT = (tVal != null && tVal > 0) ? tVal : 1;

      const curvePxY = interpolateCurvePxY(hit.seriesIdx, px);
      dragOffsetY = curvePxY != null ? (py - curvePxY) : 0;

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
    const adjustedPy = py - dragOffsetY;
    const dp = myChart.convertFromPixel('grid', [anchorXPx || e.offsetX, adjustedPy]);
    const newDataY = dp ? dp[1] : 0;
    if (newDataY <= 0) return;

    if (dragging.type === 'pcurve') {
      const allPs = cardPCurveState[cardId] || {};
      const ps = allPs[dragging.curveKey];
      if (!ps) return;

      let targetQi, targetB, targetModel;
      if (dragging.curveKey === 'main') {
        targetQi = w.params.qi; targetB = w.params.b; targetModel = data.model || 'exponential';
      } else {
        const mfId = parseInt(dragging.curveKey.replace('mf_', ''));
        const fits = cardMultiFits[cardId] || [];
        const mf = fits.find(f => f.id === mfId);
        if (!mf || !mf.params) return;
        targetQi = mf.params.qi; targetB = mf.params.b; targetModel = mf.model;
      }

      const newDi = solveForDi(targetModel, targetQi, targetB, anchorT, newDataY);
      const diKey = dragging.which === 'p10' ? 'p10Di' : 'p90Di';
      ps[diKey] = newDi;
      updatePCurveSeries(cardId, myChart, dragging.curveKey);

    } else if (dragging.type === 'fitted') {
      const targetModel = data.model || 'exponential';
      const newDi = solveForDi(targetModel, w.params.qi, w.params.b, anchorT, newDataY);
      w.params.di = newDi;

      if (w.y_fitted) {
        for (let i = 0; i < w.t.length; i++) {
          if (w.y_fitted[i] != null) {
            w.y_fitted[i] = evalDeclineModel(targetModel, w.t[i], w.params);
          }
        }
      }
      if (w.forecast && w.forecast.t && w.forecast.y) {
        for (let i = 0; i < w.forecast.t.length; i++) {
          w.forecast.y[i] = evalDeclineModel(targetModel, w.forecast.t[i], w.params);
        }
      }

      const opt = myChart.getOption();
      const isSingle = data.wells.length === 1;
      const prefix = isSingle ? '' : (w.well || '') + ' ';
      const fittedName = prefix + 'Fitted';
      for (let si = 0; si < opt.series.length; si++) {
        const s = opt.series[si];
        if (s.name === fittedName && s.type === 'line') {
          let combinedData = [];
          if (w.y_fitted) {
            combinedData = w.x.map((xv, i) => [isDate ? parseDateStr(xv) : xv, w.y_fitted[i]]).filter(p => p[1] != null);
          }
          if (w.forecast && w.forecast.x) {
            w.forecast.x.forEach((xv, i) => combinedData.push([isDate ? parseDateStr(xv) : xv, w.forecast.y[i]]));
          }
          s.data = combinedData;
          break;
        }
      }
      const ps = (cardPCurveState[cardId] || {})['main'];
      if (ps && ps.enabled) {
        for (let si = 0; si < opt.series.length; si++) {
          const s = opt.series[si];
          if (!s.id) continue;
          const parts = s.id.split('|');
          if (parts.length >= 2 && parts[1] === 'main' && (parts[0] === 'p10' || parts[0] === 'p90')) {
            const diVal = parts[0] === 'p10' ? ps.p10Di : ps.p90Di;
            s.data = buildPCurveData(w, data, diVal, isDate, null);
          }
        }
      }
      myChart.setOption({ series: opt.series }, false);

    } else if (dragging.type === 'multifit') {
      const fits = cardMultiFits[cardId] || [];
      const mf = fits.find(f => f.id === dragging.mfId);
      if (!mf || !mf.params) return;

      const newDi = solveForDi(mf.model, mf.params.qi, mf.params.b, anchorT, newDataY);
      mf.params.di = newDi;

      if (mf.indices && w.t) {
        const idxMin = Math.min(...mf.indices);
        const idxMax = Math.max(...mf.indices);
        mf.fittedData = [];
        for (let i = idxMin; i <= Math.min(idxMax, w.t.length - 1); i++) {
          const yVal = evalDeclineModel(mf.model, w.t[i], mf.params);
          mf.fittedData.push([isDate ? parseDateStr(w.x[i]) : w.x[i], yVal]);
        }
      }

      const opt = myChart.getOption();
      const mfName = 'Curve #' + mf.id + ' (' + mf.model + ')';
      for (let si = 0; si < opt.series.length; si++) {
        const s = opt.series[si];
        if (s.name === mfName && s.type === 'line' && !s.id) {
          let mfCombined = mf.fittedData ? [...mf.fittedData] : [];
          const _fcMonths = parseFloat(document.getElementById(cardId)?.querySelector('.p-forecast')?.value || 0);
          const mfTMax = mf.indices && mf.indices.length > 0 && w.t
            ? Math.max(...mf.indices.filter(i => i < w.t.length).map(i => w.t[i]))
            : null;
          if (_fcMonths && mfTMax != null) {
            const MS_PER_DAY = 86400000;
            const firstDateMs = isDate ? parseDateStr(w.x[0]) : 0;
            const tOffset = w.t[0] || 0;
            const x0 = w.x[0] || 0;
            const nM = Math.round(_fcMonths);
            for (let i = 1; i <= nM; i++) {
              const tVal = mfTMax + 30.4375 * i;
              const yVal = evalDeclineModel(mf.model, tVal, mf.params);
              if (isDate) {
                mfCombined.push([firstDateMs + (tVal - tOffset) * MS_PER_DAY, yVal]);
              } else {
                mfCombined.push([tVal - tOffset + x0, yVal]);
              }
            }
          }
          s.data = mfCombined;
          break;
        }
      }
      const mfKey = 'mf_' + mf.id;
      const mfPs = (cardPCurveState[cardId] || {})[mfKey];
      if (mfPs && mfPs.enabled) {
        for (let si = 0; si < opt.series.length; si++) {
          const s = opt.series[si];
          if (!s.id) continue;
          const parts = s.id.split('|');
          if (parts.length >= 2 && parts[1] === mfKey && (parts[0] === 'p10' || parts[0] === 'p90')) {
            const diVal = parts[0] === 'p10' ? mfPs.p10Di : mfPs.p90Di;
            const pParams = Object.assign({}, mf.params, { di: diVal });
            const pData = [];
            const idxMin = mf.indices ? Math.min(...mf.indices) : 0;
            const idxMax = mf.indices ? Math.max(...mf.indices) : w.t.length - 1;
            for (let i = idxMin; i <= Math.min(idxMax, w.t.length - 1); i++) {
              pData.push([isDate ? parseDateStr(w.x[i]) : w.x[i], evalDeclineModel(mf.model, w.t[i], pParams)]);
            }
            s.data = pData;
          }
        }
      }
      myChart.setOption({ series: opt.series }, false);
    }

    e.event && e.event.preventDefault && e.event.preventDefault();
  }

  function onMouseUp() {
    if (dragging) {
      if (dragging.type === 'fitted' || dragging.type === 'multifit') {
        saveZoomState(cardId);
        renderSingleChart(cardId, cardLastData[cardId], document.getElementById(cardId)?.querySelector('.p-forecast')?.value || 0);
      }
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

  let firstFit = -1, lastFit = -1;
  for (let i = 0; i < (w.y_fitted || []).length; i++) {
    if (w.y_fitted[i] != null) { if (firstFit === -1) firstFit = i; lastFit = i; }
  }

  const pts = [];
  for (let i = 0; i < w.t.length; i++) {
    if (w.y_fitted && w.y_fitted[i] != null) {
      const xVal = isDate ? parseDateStr(w.x[i]) : w.x[i];
      pts.push([xVal, evalDeclineModel(model, w.t[i], pParams)]);
    }
  }

  if (hasForecast && w.forecast.t) {
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

function updatePCurveSeries(cardId, myChart, curveKeyOverride) {
  const allPs = cardPCurveState[cardId] || {};
  const data = cardLastData[cardId];
  if (!data || !data.wells) return;
  const w = data.wells[0];
  if (!w.y_fitted || !w.params || !w.t) return;

  const st = readCardStyles(cardId);
  const isDate = w.is_date || false;

  const opt = myChart.getOption();
  const seriesOpt = opt.series;
  let p10Ys = [], p90Ys = []; // for DcaStats info

  /* Update each P10/P90 series currently configured in option */
  for (let si = 0; si < seriesOpt.length; si++) {
    const s = seriesOpt[si];
    if (!s.id) continue;

    const parts = s.id.split('|');
    if (parts.length < 2 || (parts[0] !== 'p10' && parts[0] !== 'p90')) continue;

    const which = parts[0];
    const curveKey = parts[1];
    if (curveKeyOverride && curveKey !== curveKeyOverride) continue;

    const ps = allPs[curveKey];
    if (!ps || !ps.enabled) continue;

    let lineObj = {};
    let itemObj = {};
    let targetName = '';
    let pData = [];
    const diVal = which === 'p10' ? ps.p10Di : ps.p90Di;
    const diRound = Math.round(diVal * 1e6) / 1e6;

    if (curveKey === 'main') {
      const isSingle = data.wells.length === 1;
      const prefix = isSingle ? '' : w.well + ' ';
      targetName = prefix + (which === 'p10' ? 'P10' : 'P90') + ' (Di=' + diRound + ')';
      pData = buildPCurveData(w, data, diVal, isDate, null);

      const pColor = which === 'p10' ? (st.p10Color || '#22c55e') : (st.p90Color || '#ef4444');
      const showLine = which === 'p10' ? (st.p10Line !== false) : (st.p90Line !== false);
      const lineStyle = which === 'p10' ? (st.p10Style || 'solid') : (st.p90Style || 'solid');

      lineObj = { color: pColor, width: showLine ? 1.5 : 0, type: lineStyle };
      itemObj = { color: pColor };
      s.showSymbol = which === 'p10' ? (st.p10Marker || false) : (st.p90Marker || false);

      if (which === 'p10') p10Ys = pData.map(pt => pt[1]).filter(v => v != null && isFinite(v));
      if (which === 'p90') p90Ys = pData.map(pt => pt[1]).filter(v => v != null && isFinite(v));

    } else {
      const mfId = parseInt(curveKey.replace('mf_', ''));
      const mf = (cardMultiFits[cardId] || []).find(f => f.id === mfId);
      if (!mf || !mf.params) continue;

      targetName = 'Curve #' + mf.id + ' (' + mf.model + ') ' + (which === 'p10' ? 'P10' : 'P90');

      const pParams = Object.assign({}, mf.params, { di: diVal });
      const idxMin = mf.indices ? Math.min(...mf.indices) : 0;
      const idxMax = mf.indices ? Math.max(...mf.indices) : w.t.length - 1;
      for (let i = idxMin; i <= Math.min(idxMax, w.t.length - 1); i++) {
        const yVal = evalDeclineModel(mf.model, w.t[i], pParams);
        pData.push([isDate ? parseDateStr(w.x[i]) : w.x[i], yVal]);
      }
      /* Extend dynamically: mfTMax + current forecastMonths */
      const _pFcMonths = parseFloat(document.getElementById(cardId)?.querySelector('.p-forecast')?.value || 0);
      const mfTMaxP = mf.indices && mf.indices.length > 0
        ? Math.max(...mf.indices.filter(i => i < w.t.length).map(i => w.t[i]))
        : null;
      if (_pFcMonths && mfTMaxP != null) {
        const MS_PER_DAY = 86400000;
        const firstDateMs = isDate ? parseDateStr(w.x[0]) : 0;
        const tOffset = w.t[0] || 0;
        const x0 = w.x[0] || 0;
        const nM = Math.round(_pFcMonths);
        for (let fi = 1; fi <= nM; fi++) {
          const tVal = mfTMaxP + 30.4375 * fi;
          const yVal = evalDeclineModel(mf.model, tVal, pParams);
          if (isDate) {
            pData.push([firstDateMs + (tVal - tOffset) * MS_PER_DAY, yVal]);
          } else {
            pData.push([tVal - tOffset + x0, yVal]);
          }
        }
      }

      const mfStylesObj = st.multiFitStyles || {};
      const ms = mfStylesObj[curveKey] || {};
      const mfBaseColor = ms.color || mf.color;
      const pColor = which === 'p10' ? (ms.p10Color || mfBaseColor) : (ms.p90Color || mfBaseColor);
      const pStyle = which === 'p10' ? (ms.p10Style || 'dotted') : (ms.p90Style || 'dotted');

      lineObj = { color: pColor, width: 1.5, type: pStyle };
      itemObj = { color: pColor, opacity: 0.6 };
      s.showSymbol = which === 'p10' ? (ms.p10Marker || false) : (ms.p90Marker || false);
      const mfLabelKey = which === 'p10' ? 'p10Labels' : 'p90Labels';
      s.label = ms[mfLabelKey] ? { show: true, position: 'top', formatter: (p) => { let v = Array.isArray(p.data) ? p.data[1] : p.data; return (typeof v === 'number') ? v.toFixed(1) : ''; }, fontSize: 9 } : { show: false };
    }

    s.name = targetName;
    s.data = pData;
    s.lineStyle = lineObj;
    s.itemStyle = itemObj;
  }

  myChart.setOption({ series: seriesOpt }, false);
  updatePctChangeGraphic(cardId, myChart);

  /* Update P10/P90 stats in dcaStatsDiv IF ONLY main curve was updated (or everything) */
  if (!curveKeyOverride || curveKeyOverride === 'main') {
    const dcaStatsDivU = document.getElementById('dcaStats-' + cardId);
    if (dcaStatsDivU && p10Ys.length > 0 && p90Ys.length > 0) {
      function updateStatItems(prefix, vals) {
        if (vals.length < 2) return;
        const first = vals[0], last = vals[vals.length - 1];
        const diff = last - first;
        const pct = first !== 0 ? ((diff / Math.abs(first)) * 100) : 0;
        const sign = diff >= 0 ? '+' : '';
        const cls = diff >= 0 ? 'positive' : 'negative';
        dcaStatsDivU.querySelectorAll('.dca-stat-item').forEach(el => {
          const lbl = el.querySelector('.dca-stat-label');
          const val = el.querySelector('.dca-stat-value');
          if (!lbl || !val) return;
          if (lbl.textContent === prefix + ' First') { val.textContent = first.toFixed(2); }
          else if (lbl.textContent === prefix + ' Last') { val.textContent = last.toFixed(2); }
          else if (lbl.textContent === prefix + ' Δ') {
            val.textContent = sign + diff.toFixed(2);
            val.className = 'dca-stat-value ' + cls;
          } else if (lbl.textContent === prefix + ' %') {
            val.textContent = sign + pct.toFixed(1) + '%';
            val.className = 'dca-stat-value ' + cls;
          }
        });
      }
      updateStatItems('P10', p10Ys);
      updateStatItems('P90', p90Ys);
    }
  }
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

        /* Rebuild combined fitted + forecast data */

        let combinedData = isDate

          ? w.x.map((xv, i) => [parseDateStr(xv), w.y_fitted[i]]).filter(p => p[1] != null)

          : w.x.map((xv, i) => [xv, w.y_fitted[i]]).filter(p => p[1] != null);

        if (w.forecast && w.forecast.x) {

          for (let i = 0; i < w.forecast.x.length; i++) {

            const xv = isDate ? parseDateStr(w.forecast.x[i]) : w.forecast.x[i];

            combinedData.push([xv, w.forecast.y[i]]);

          }

        }

        seriesOpt[si].data = combinedData;

      }

    }

    myChart.setOption({ series: seriesOpt }, false);

    /* Update P10/P90 curves if enabled (they depend on qi) */

    updatePCurveSeries(cardId, myChart, 'main');

    /* Live-update the % change graphic labels (P50 changed) */
    updatePctChangeGraphic(cardId, myChart);

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

        updatePCurveSeries(cardId, otherChart, 'main');

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

    const multiFits = cardMultiFits[cardId] || [];

    const st = cardStyles[cardId] || {};

    const mfStylesObj = st.multiFitStyles || {};

    const mainColor = (st.curveStyles && st.curveStyles[w.well || 'default'] || {}).fittedColor || st.fittedColor || '#f59e0b';

    const buildEq = (model, params, color) => {

      const mn = (model || '').toLowerCase();

      let fitted = '';

      if (mn === 'exponential') fitted = `q(t) = ${fmt(p.qi)} &middot; e<sup>&minus;${fmt(params.di)}&middot;t</sup>`;

      else if (mn === 'hyperbolic') fitted = `q(t) = ${fmt(params.qi)} / (1 + ${fmt(params.b)}&middot;${fmt(params.di)}&middot;t)<sup>1/${fmt(params.b)}</sup>`;

      else if (mn === 'harmonic') fitted = `q(t) = ${fmt(params.qi)} / (1 + ${fmt(params.di)}&middot;t)`;

      return fitted ? `<span class="formula-fitted" style="color:${color}">${fitted}</span>` : '';

    };

    let formulaHtml = '';

    const wName = data?.wells?.[0]?.well || 'Fitted';
    const label = multiFits.length > 0 ? wName : '';

    if (label) formulaHtml += `<span class="formula-label" style="color:${mainColor}">&#9679; ${label} (${data.model})</span>`;

    formulaHtml += buildEq(modelName, p, mainColor);

    multiFits.forEach(mf => {

      if (!mf.params) return;

      const ms = mfStylesObj['mf_' + mf.id] || {};

      const mfColor = ms.color || mf.color || '#8b5cf6';

      formulaHtml += `<span class="formula-label" style="color:${mfColor}">&#9679; Curve #${mf.id} (${mf.model})</span>`;

      formulaHtml += buildEq(mf.model, mf.params, mfColor);

    });

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

  /* Update Qi box */
  const qiBoxDiv = document.getElementById('qiBox-' + cardId);
  const qiInputPanel = document.getElementById('qiInputPanel-' + cardId);
  const isDate = w.is_date || false;

  if (qiBoxDiv && Number.isFinite(Number(p.qi))) {
    qiBoxDiv.innerHTML = `<span class="qi-box-label">Qi</span><span class="qi-box-value">${Number(p.qi).toFixed(2)}</span>`;
    qiBoxDiv.style.display = 'inline-flex';
    if (qiInputPanel) {
      qiInputPanel.style.display = 'flex';
      /* Pre-fill current Qi value */
      const qiValInput = document.getElementById('qiValueInput-' + cardId);
      if (qiValInput && !qiValInput.value) qiValInput.value = Number(p.qi).toFixed(2);

      /* Set correct input type for date vs numeric */
      const qiDateInput = document.getElementById('qiDateInput-' + cardId);
      if (qiDateInput) {
        if (isDate) {
          qiDateInput.type = 'date';
          qiDateInput.placeholder = 'YYYY-MM-DD';
        } else {
          qiDateInput.type = 'number';
          qiDateInput.placeholder = 'X Value';
        }
      }
    }
  } else if (qiBoxDiv) {
    qiBoxDiv.style.display = 'none';
    if (qiInputPanel) qiInputPanel.style.display = 'none';
  }

  renderCurveSummaryTable(cardId, data);

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

      if (cardAxisAutoFit[cardId]) cardAxisAutoFit[cardId].y = false;

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

      if (cardAxisAutoFit[cardId]) cardAxisAutoFit[cardId].x = false;

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

  const _mfForExcl = cardMultiFits[cardId] || [];
  const _mfUsedIdx = new Set();
  _mfForExcl.forEach(mf => (mf.indices || []).forEach(i => _mfUsedIdx.add(i)));
  const _visibleExclCount = [...excluded].filter(i => !_mfUsedIdx.has(i)).length;

  cardExclusions[cardId] = excluded;

  exclHint.textContent = _visibleExclCount > 0 ? `${_visibleExclCount} point(s) excluded — click to toggle` : 'Click scatter points to exclude / include';



  let pHtml = `<span>Model: <strong>${data.model}</strong></span>`;

  if (firstW.params) { for (const [k, v] of Object.entries(firstW.params)) pHtml += `<span>${k}: <strong>${v}</strong></span>`; }

  if (allWellsData.length > 1) pHtml += `<span>Wells: <strong>${allWellsData.length}</strong></span>`;

  paramDiv.innerHTML = pHtml;



  /* --- Formula display below chart --- */

  const formulaDiv = document.getElementById('formula-' + cardId);

  if (formulaDiv) {

    const fmt = (v) => typeof v === 'number' ? (Number.isInteger(v) ? v.toString() : v.toFixed(4)) : v;

    const buildEqHtml = (model, params, color) => {
      const mn = (model || '').toLowerCase();
      let fitted = '';
      if (mn === 'exponential') {
        fitted = `q(t) = ${fmt(params.qi)} &middot; e<sup>&minus;${fmt(params.di)}&middot;t</sup>`;
      } else if (mn === 'hyperbolic') {
        fitted = `q(t) = ${fmt(params.qi)} / (1 + ${fmt(params.b)}&middot;${fmt(params.di)}&middot;t)<sup>1/${fmt(params.b)}</sup>`;
      } else if (mn === 'harmonic') {
        fitted = `q(t) = ${fmt(params.qi)} / (1 + ${fmt(params.di)}&middot;t)`;
      }
      return fitted ? `<span class="formula-fitted" style="color:${color}">${fitted}</span>` : '';
    };

    let formulaHtml = '';
    const multiFits = cardMultiFits[cardId] || [];
    const st = cardStyles[cardId] || {};
    const mfStylesObj = st.multiFitStyles || {};

    /* Main fit equation */
    if (firstW.equation && firstW.params) {
      const modelName = (data.model || '').toLowerCase();
      const mainColor = (st.curveStyles && st.curveStyles[firstW.well || 'default'] || {}).fittedColor || st.fittedColor || '#f59e0b';
      const wName = firstW.well || 'Fitted';
      const label = multiFits.length > 0 ? wName : '';
      if (label) formulaHtml += `<span class="formula-label" style="color:${mainColor}">&#9679; ${label} (${data.model})</span>`;
      formulaHtml += buildEqHtml(modelName, firstW.params, mainColor);
    }

    /* Multi-fit curve equations */
    multiFits.forEach(mf => {
      if (!mf.params) return;
      const ms = mfStylesObj['mf_' + mf.id] || {};
      const mfColor = ms.color || mf.color || '#8b5cf6';
      formulaHtml += `<span class="formula-label" style="color:${mfColor}">&#9679; Curve #${mf.id} (${mf.model})</span>`;
      formulaHtml += buildEqHtml(mf.model, mf.params, mfColor);
    });

    if (formulaHtml) {
      formulaDiv.innerHTML = formulaHtml;
      formulaDiv.style.display = '';
    } else {
      formulaDiv.style.display = 'none';
    }

  }

  /* --- Qi box --- */
  const qiBoxDiv = document.getElementById('qiBox-' + cardId);
  if (qiBoxDiv && firstW.params && Number.isFinite(Number(firstW.params.qi))) {
    const qiValue = Number(firstW.params.qi);
    qiBoxDiv.innerHTML = `<span class="qi-box-label">Qi</span><span class="qi-box-value">${qiValue.toFixed(2)}</span>`;
    qiBoxDiv.style.display = 'inline-flex';
  } else if (qiBoxDiv) {
    qiBoxDiv.style.display = 'none';
  }



  /* Deprecated: old mini stats table is superseded by the curve summary table. */
  const dcaStatsDiv = document.getElementById('dcaStats-' + cardId);
  if (dcaStatsDiv) {
    dcaStatsDiv.innerHTML = '';
    dcaStatsDiv.style.display = 'none';
  }



  const st = readCardStyles(cardId);

  renderCurveSummaryTable(cardId, data);

  const customTitle = card.querySelector('.p-title')?.value || '';



  if (chartInstances[cardId]) chartInstances[cardId].dispose();

  const isLight = document.documentElement.getAttribute('data-theme') === 'light';

  const myChart = echarts.init(chartDiv, isLight ? null : 'dark');

  chartInstances[cardId] = myChart;



  const isDate = firstW.is_date || false;

  const xAxisType = isDate ? 'time' : 'value';

  const series = [];

  const pointLabelFormatter = function (params) {
    let val = Array.isArray(params.data) ? params.data[1] : params.data;
    if (val == null) return '';
    return typeof val === 'number' ? val.toFixed(1) : val;
  };

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

  const multiFitsForPoints = cardMultiFits[cardId] || [];
  const mfIndexMeta = multiFitsForPoints.map(mf => ({
    id: mf.id,
    color: mf.color,
    indexSet: new Set(mf.indices || []),
  }));
  const anyMfIncluded = new Set();
  mfIndexMeta.forEach(m => m.indexSet.forEach(i => anyMfIncluded.add(i)));



  allWellsData.forEach((w, wIdx) => {

    /* Per-curve styles: look up by well name, fall back to top-level st */
    const cs = (st.curveStyles && st.curveStyles[w.well]) || st;

    const wColor = cs.actualColor || st.actualColor;

    const wFitColor = cs.fittedColor || st.fittedColor;

    const wFcColor = cs.forecastColor || st.forecastColor;

    const wActualSymbol = cs.actualSymbol || st.actualSymbol;
    const wActualSize = cs.actualSize != null ? cs.actualSize : st.actualSize;
    const wFittedStyle = cs.fittedStyle || st.fittedStyle;
    const wFittedWidth = cs.fittedWidth != null ? cs.fittedWidth : st.fittedWidth;
    const wFittedMarkers = cs.fittedMarkers != null ? cs.fittedMarkers : st.fittedMarkers;
    const wFittedSymbol = cs.fittedSymbol || st.fittedSymbol;
    const wFittedSymbolSize = cs.fittedSymbolSize != null ? cs.fittedSymbolSize : st.fittedSymbolSize;

    const prefix = isSingle ? '' : w.well + ' ';

    const actualLen = w.x.length;

    const hasForecast = w.forecast && w.forecast.x && w.forecast.x.length > 0;



    if (isDate) {

      const anchorSet = new Set(w.qi_anchor_indices || []);

      const incD = [], exclD = [], anchorD = [];
      const mfBuckets = {};
      if (isSingle && wIdx === 0) mfIndexMeta.forEach(m => { mfBuckets[m.id] = []; });

      for (let i = 0; i < actualLen; i++) {

        const ts = parseDateStr(w.x[i]);

        const pt = [ts, w.y_actual[i], i];

        if (isSingle && wIdx === 0) {
          mfIndexMeta.forEach(m => {
            if (m.indexSet.has(i)) mfBuckets[m.id].push(pt);
          });
        }

        if (isSingle && excluded.has(i) && !anyMfIncluded.has(i)) exclD.push(pt);

        else if (isSingle && anchorSet.has(i)) anchorD.push(pt);

        else incD.push(pt);

      }

      series.push({ name: prefix + 'Actual', type: 'scatter', symbolSize: wActualSize, symbol: wActualSymbol, itemStyle: { color: wColor }, data: incD });

      if (isSingle && exclD.length > 0) series.push({ name: 'Excluded', type: 'scatter', symbolSize: wActualSize, symbol: 'diamond', itemStyle: { color: '#ef4444', opacity: 0.5 }, data: exclD });

      if (isSingle && anchorD.length > 0) series.push({ name: 'Qi Anchor', type: 'scatter', symbolSize: wActualSize + 6, symbol: 'pin', itemStyle: { color: '#f97316', borderColor: '#fff', borderWidth: 1.5 }, data: anchorD, z: 10 });

      if (isSingle && wIdx === 0) {
        mfIndexMeta.forEach(m => {
          const pts = mfBuckets[m.id] || [];
          if (pts.length > 0) {
            series.push({
              name: '_mfinc_' + m.id,
              type: 'scatter',
              symbolSize: Math.max(wActualSize - 1, 6),
              symbol: 'circle',
              z: 6,
              itemStyle: { color: m.color, borderColor: '#ffffff', borderWidth: 1.2 },
              data: pts,
              tooltip: { show: false },
            });
          }
        });
      }

      if (w.y_fitted || hasForecast) {

        /* Build combined fitted + forecast data as a single series */
        let combinedData = [];
        if (w.y_fitted) {
          combinedData = w.x.map((xv, i) => [parseDateStr(xv), w.y_fitted[i]]).filter(p => p[1] != null);
        }
        if (hasForecast) {
          /* Append forecast points (bridge is automatic since fitted ends where forecast begins) */
          w.forecast.x.forEach((xv, i) => combinedData.push([parseDateStr(xv), w.forecast.y[i]]));
        }

        series.push({
          name: prefix + 'Fitted',
          type: 'line',
          showSymbol: wFittedMarkers,
          symbol: wFittedSymbol,
          symbolSize: wFittedSymbolSize,
          smooth: false,
          lineStyle: { color: wFitColor, width: wFittedWidth, type: wFittedStyle },
          itemStyle: { color: wFitColor },
          label: wFittedMarkers ? { show: true, position: 'top', formatter: pointLabelFormatter, fontSize: 9, color: isLight ? '#475569' : '#e2e8f0' } : { show: false },
          data: combinedData
        });

      }

    } else {

      const anchorSet2 = new Set(w.qi_anchor_indices || []);

      const incD = [], exclD = [], anchorD2 = [];
      const mfBuckets = {};
      if (isSingle && wIdx === 0) mfIndexMeta.forEach(m => { mfBuckets[m.id] = []; });

      for (let i = 0; i < actualLen; i++) {
        const pt = [w.x[i], w.y_actual[i], i];
        if (isSingle && wIdx === 0) {
          mfIndexMeta.forEach(m => {
            if (m.indexSet.has(i)) mfBuckets[m.id].push(pt);
          });
        }
        if (isSingle && excluded.has(i) && !anyMfIncluded.has(i)) exclD.push(pt);
        else if (isSingle && anchorSet2.has(i)) anchorD2.push(pt);
        else incD.push(pt);
      }

      series.push({ name: prefix + 'Actual', type: 'scatter', symbolSize: wActualSize, symbol: wActualSymbol, itemStyle: { color: wColor }, data: incD });

      if (isSingle && exclD.length > 0) series.push({ name: 'Excluded', type: 'scatter', symbolSize: wActualSize, symbol: 'diamond', itemStyle: { color: '#ef4444', opacity: 0.5 }, data: exclD });

      if (isSingle && anchorD2.length > 0) series.push({ name: 'Qi Anchor', type: 'scatter', symbolSize: wActualSize + 6, symbol: 'pin', itemStyle: { color: '#f97316', borderColor: '#fff', borderWidth: 1.5 }, data: anchorD2, z: 10 });

      if (isSingle && wIdx === 0) {
        mfIndexMeta.forEach(m => {
          const pts = mfBuckets[m.id] || [];
          if (pts.length > 0) {
            series.push({
              name: '_mfinc_' + m.id,
              type: 'scatter',
              symbolSize: Math.max(wActualSize - 1, 6),
              symbol: 'circle',
              z: 6,
              itemStyle: { color: m.color, borderColor: '#ffffff', borderWidth: 1.2 },
              data: pts,
              tooltip: { show: false },
            });
          }
        });
      }

      if (w.y_fitted || hasForecast) {
        /* Build combined fitted + forecast data as a single series */
        let combinedData = [];
        if (w.y_fitted) {
          combinedData = w.x.map((xv, i) => [xv, w.y_fitted[i]]).filter(p => p[1] != null);
        }
        if (hasForecast) {
          w.forecast.x.forEach((xv, i) => combinedData.push([xv, w.forecast.y[i]]));
        }

        series.push({
          name: prefix + 'Fitted',
          type: 'line',
          showSymbol: wFittedMarkers,
          symbol: wFittedSymbol,
          symbolSize: wFittedSymbolSize,
          smooth: false,
          lineStyle: { color: wFitColor, width: wFittedWidth, type: wFittedStyle },
          itemStyle: { color: wFitColor },
          label: wFittedMarkers ? { show: true, position: 'top', formatter: pointLabelFormatter, fontSize: 9, color: isLight ? '#475569' : '#e2e8f0' } : { show: false },
          data: combinedData
        });
      }

    }



    /* ---- P10 / P90 decline curves (physics-based, drag recalculates D) ---- */

    const ps = (cardPCurveState[cardId] || {})['main'];

    if (ps && ps.enabled && w.y_fitted && w.params && w.t) {

      const P10_COLOR = cs.p10Color || st.p10Color || '#22c55e', P90_COLOR = cs.p90Color || st.p90Color || '#ef4444';

      const p10ShowLine = (cs.p10Line != null ? cs.p10Line : st.p10Line) !== false, p10ShowMarker = (cs.p10Marker != null ? cs.p10Marker : st.p10Marker) || false;
      const p90ShowLine = (cs.p90Line != null ? cs.p90Line : st.p90Line) !== false, p90ShowMarker = (cs.p90Marker != null ? cs.p90Marker : st.p90Marker) || false;
      const p10DiRound = Math.round(ps.p10Di * 1e6) / 1e6;
      const p90DiRound = Math.round(ps.p90Di * 1e6) / 1e6;

      const p10Data = buildPCurveData(w, data, ps.p10Di, isDate, null);
      const p90Data = buildPCurveData(w, data, ps.p90Di, isDate, null);

      const wP10Labels = cs.p10Labels != null ? cs.p10Labels : st.p10Labels;
      const wP10Style = cs.p10Style || st.p10Style || 'solid';
      const wP90Labels = cs.p90Labels != null ? cs.p90Labels : st.p90Labels;
      const wP90Style = cs.p90Style || st.p90Style || 'solid';

      series.push({
        id: 'p10|main',
        name: prefix + 'P10 (Di=' + p10DiRound + ')', type: 'line', z: 1, showSymbol: p10ShowMarker, symbol: 'circle', symbolSize: 6, smooth: false,
        lineStyle: { color: P10_COLOR, width: p10ShowLine ? 1.5 : 0, type: wP10Style }, itemStyle: { color: P10_COLOR },
        label: wP10Labels ? { show: true, position: 'top', formatter: pointLabelFormatter, fontSize: 9, color: isLight ? '#475569' : '#e2e8f0' } : { show: false },
        data: p10Data
      });

      series.push({
        id: 'p90|main',
        name: prefix + 'P90 (Di=' + p90DiRound + ')', type: 'line', z: 1, showSymbol: p90ShowMarker, symbol: 'circle', symbolSize: 6, smooth: false,
        lineStyle: { color: P90_COLOR, width: p90ShowLine ? 1.5 : 0, type: wP90Style }, itemStyle: { color: P90_COLOR },
        label: wP90Labels ? { show: true, position: 'top', formatter: pointLabelFormatter, fontSize: 9, color: isLight ? '#475569' : '#e2e8f0' } : { show: false },
        data: p90Data
      });

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

        label: { show: true, formatter: ul.name, color: ul.color, fontSize: 11, position: ul.type === 'h' ? 'insideStartTop' : 'insideEndTop' },

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

          color: ann.color || (isLight ? '#0f172a' : '#f8fafc'),

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

            return pointLabelFormatter(params);

          },

          fontSize: 9, color: isLight ? '#475569' : '#e2e8f0'

        };

      }

    });

  }



  /* ---- Multi-fit additional curves ---- */
  const multiFits = cardMultiFits[cardId];
  const _fcMonths = parseFloat(forecastMonths) || 0;
  if (multiFits && multiFits.length > 0) {
    const mfStylesObj = st.multiFitStyles || {};
    multiFits.forEach((mf, mfIdx) => {
      const mfKey = 'mf_' + mf.id;
      const ms = mfStylesObj[mfKey] || {};
      const mfColor = ms.color || mf.color;
      const mfLineStyle = ms.lineStyle || 'solid';
      const mfLineWidth = ms.lineWidth != null ? ms.lineWidth : 2.5;
      const mfName = 'Curve #' + mf.id + ' (' + mf.model + ')';

      /* Latest t value among included indices */
      const mfW0 = allWellsData[0];
      const mfTMax = mf.indices && mf.indices.length > 0 && mfW0
        ? Math.max(...mf.indices.filter(i => i < mfW0.t.length).map(i => mfW0.t[i]))
        : (mf.fittedData && mf.fittedData.length > 0 ? null : null);

      /* Build dynamic forecast for this curve based on current forecastMonths */
      const buildMfForecast = () => {
        if (!_fcMonths || !mf.params || mfTMax == null) return [];
        const MS_PER_DAY = 86400000;
        const firstDateMs = isDate ? parseDateStr(mfW0.x[0]) : 0;
        const tOffset = mfW0 ? (mfW0.t[0] || 0) : 0;
        const x0 = mfW0 ? (mfW0.x[0] || 0) : 0;
        const pts = [];
        const nMonths = Math.round(_fcMonths);
        for (let i = 1; i <= nMonths; i++) {
          const tVal = mfTMax + 30.4375 * i;
          const yVal = evalDeclineModel(mf.model, tVal, mf.params);
          if (isDate) {
            pts.push([firstDateMs + (tVal - tOffset) * MS_PER_DAY, yVal]);
          } else {
            pts.push([tVal - tOffset + x0, yVal]);
          }
        }
        return pts;
      };

      /* Combine fitted + dynamic forecast */
      let mfCombined = [];
      if (mf.fittedData && mf.fittedData.length > 0) mfCombined = [...mf.fittedData];
      const dynForecast = buildMfForecast();
      if (dynForecast.length > 0) mfCombined = mfCombined.concat(dynForecast);
      if (mfCombined.length > 0) {
        series.push({
          name: mfName,
          type: 'line',
          showSymbol: ms.showMarkers || false,
          symbol: ms.markerSymbol || 'circle',
          symbolSize: ms.markerSize != null ? ms.markerSize : 8,
          smooth: false,
          lineStyle: { color: mfColor, width: mfLineWidth, type: mfLineStyle },
          itemStyle: { color: mfColor },
          label: ms.showLabels ? { show: true, position: 'top', formatter: pointLabelFormatter, fontSize: 9, color: isLight ? '#475569' : '#e2e8f0' } : { show: false },
          data: mfCombined,
          z: 5,
        });
      }

      /* P10/P90 for this multi-fit curve (if enabled on the card) */
      const mfPs = (cardPCurveState[cardId] || {})['mf_' + mf.id];
      if (mfPs && mfPs.enabled && mf.params) {
        const mfW = allWellsData[0];
        if (mfW && mfW.t) {
          const mfP10Di = mfPs.p10Di;
          const mfP90Di = mfPs.p90Di;
          const makeMultiFitPCurve = (diVal) => {
            const pParams = Object.assign({}, mf.params, { di: diVal });
            const pData = [];
            /* Evaluate over selected indices range */
            const idxMin = mf.indices ? Math.min(...mf.indices) : 0;
            const idxMax = mf.indices ? Math.max(...mf.indices) : mfW.t.length - 1;
            for (let i = idxMin; i <= Math.min(idxMax, mfW.t.length - 1); i++) {
              const yVal = evalDeclineModel(mf.model, mfW.t[i], pParams);
              if (isDate) {
                pData.push([parseDateStr(mfW.x[i]), yVal]);
              } else {
                pData.push([mfW.x[i], yVal]);
              }
            }
            /* Extend dynamically: mfTMax + forecastMonths months */
            if (_fcMonths && mfTMax != null) {
              const MS_PER_DAY = 86400000;
              const firstDateMs = isDate ? parseDateStr(mfW.x[0]) : 0;
              const tOffset = mfW.t[0] || 0;
              const x0 = mfW.x[0] || 0;
              const nM = Math.round(_fcMonths);
              for (let i = 1; i <= nM; i++) {
                const tVal = mfTMax + 30.4375 * i;
                const yVal = evalDeclineModel(mf.model, tVal, pParams);
                if (isDate) {
                  pData.push([firstDateMs + (tVal - tOffset) * MS_PER_DAY, yVal]);
                } else {
                  pData.push([tVal - tOffset + x0, yVal]);
                }
              }
            }
            return pData;
          };
          const mfP10Data = makeMultiFitPCurve(mfP10Di);
          const mfP90Data = makeMultiFitPCurve(mfP90Di);
          const mfP10Color = ms.p10Color || mfColor;
          const mfP10Style = ms.p10Style || 'dotted';
          const mfP90Color = ms.p90Color || mfColor;
          const mfP90Style = ms.p90Style || 'dotted';
          if (mfP10Data.length > 0) {
            series.push({
              id: 'p10|mf_' + mf.id,
              name: mfName + ' P10',
              type: 'line', z: 1, showSymbol: ms.p10Marker || false, symbol: 'circle', symbolSize: 6, smooth: false,
              lineStyle: { color: mfP10Color, width: 1.5, type: mfP10Style },
              itemStyle: { color: mfP10Color, opacity: 0.6 },
              label: ms.p10Labels ? { show: true, position: 'top', formatter: pointLabelFormatter, fontSize: 9, color: isLight ? '#475569' : '#e2e8f0' } : { show: false },
              data: mfP10Data,
            });
          }
          if (mfP90Data.length > 0) {
            series.push({
              id: 'p90|mf_' + mf.id,
              name: mfName + ' P90',
              type: 'line', z: 1, showSymbol: ms.p90Marker || false, symbol: 'circle', symbolSize: 6, smooth: false,
              lineStyle: { color: mfP90Color, width: 1.5, type: mfP90Style },
              itemStyle: { color: mfP90Color, opacity: 0.6 },
              label: ms.p90Labels ? { show: true, position: 'top', formatter: pointLabelFormatter, fontSize: 9, color: isLight ? '#475569' : '#e2e8f0' } : { show: false },
              data: mfP90Data,
            });
          }
        }
      }
    });
  }



  /* ---- Filter manually-hidden series ---- */
  const _hiddenSet = cardHiddenSeries[cardId];
  if (_hiddenSet && _hiddenSet.size > 0) {
    const kept = series.filter(s => {
      if (s.name.startsWith('_') || s.name.endsWith('_bridge')) return true;
      return !_hiddenSet.has(s.name);
    });
    series.length = 0;
    kept.forEach(s => series.push(s));
  }

  const legendData = series.filter(s => !s.name.endsWith('_bridge') && !s.name.startsWith('_')).map(s => s.name);

  const axC = isLight ? '#94a3b8' : '#333750'; // Darker axis lines for light theme

  const spC = isLight ? '#d1d5db' : '#262a3a'; // Darker grid lines for light theme

  const lbC = isLight ? '#475569' : '#94a3b8'; // Darker labels for light theme

  const ttC = isLight ? '#020617' : '#e2e8f0'; // Almost black title for light theme



  const chartTitle = customTitle || (isSingle ? firstW.well : allWellsData.map(w => w.well).join(', '));

  const customLabels = cardAxisLabels[cardId] || {};

  const useLogScale = cardLogScale[cardId] || false;
  const useLogScaleX = (cardLogScaleX[cardId] || false) && !isDate; // X log only for numeric axes



  /* --- % Change & Difference graphic --- */

  const graphicElems = getPctChangeGraphic(cardId);


  const axPos = cardAxisPositions[cardId] || { x: 'bottom', y: 'left' };

  const gridLeft = axPos.y === 'right' ? 30 : 80;
  const gridRight = axPos.y === 'right' ? 80 : 30;
  const gridTop = axPos.x === 'top' ? 70 : 50;
  const gridBottom = axPos.x === 'top' ? (isDate ? 40 : 30) : (isDate ? 65 : 35);
  const dzBottom = axPos.x === 'top' ? (isDate ? 8 : 4) : (isDate ? 8 : 4);

  const option = {

    backgroundColor: 'transparent',

    graphic: graphicElems,

    title: {

      text: chartTitle,

      left: 'center',

      textStyle: { color: ttC, fontSize: 15, fontWeight: 'bold' }

    },

    tooltip: {
      trigger: 'axis', axisPointer: { type: 'cross', lineStyle: { color: 'var(--text-dim, #999)', type: 'dashed' } }, formatter: function (params) {
        if (!Array.isArray(params)) params = params ? [params] : [];
        if (params.length === 0) return '';
        
        let header = '';
        const firstParam = params[0];
        const xVal = Array.isArray(firstParam.data) ? firstParam.data[0] : firstParam.axisValue;
        header = isDate ? '<b>' + formatDateTs(xVal) + '</b>' : '<b>' + xVal + '</b>';
        
        params.forEach(p => {
          if (p && !p.seriesName.endsWith('_bridge') && !p.seriesName.startsWith('_')) {
            let val = Array.isArray(p.data) ? p.data[1] : p.data;
            if (val != null) {
              header += '<br/>' + p.marker + ' ' + p.seriesName + ': <b>' + (typeof val === 'number' ? val.toFixed(2) : val) + '</b>';
            }
          }
        });
        
        return header;
      }
    },

    legend: { data: legendData, top: 4, left: 60, textStyle: { color: lbC, fontSize: 11 } },

    grid: { left: gridLeft, right: gridRight, top: gridTop, bottom: gridBottom },

    xAxis: {

      type: useLogScaleX ? 'log' : xAxisType,

      position: axPos.x,

      min: (useLogScaleX && !(cardAxisAutoFit[cardId] && cardAxisAutoFit[cardId].x)) ? 0.01 : (isDate ? dateMin : undefined),

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
        formatter: isDate ? function (val) { return formatDateTs(val); } : undefined
      }

    },

    yAxis: {

      type: useLogScale ? 'log' : 'value',

      position: axPos.y,

      name: customLabels.y || data.y_label,

      nameTextStyle: { color: lbC, fontSize: 13, fontWeight: 'bold' },

      splitLine: { show: st.gridX !== false, lineStyle: { color: spC, type: 'dashed' } },

      axisLine: { lineStyle: { color: axC } },
      axisLabel: {
        color: lbC,
        formatter: function (val) {
          if (!Number.isFinite(val)) return '';
          const absVal = Math.abs(val);
          if (Math.abs(val) >= 1000000) {
            return (val / 1000000).toFixed(1) + 'M';
          } else if (Math.abs(val) >= 1000) {
            return (val / 1000).toFixed(1) + 'k';
          }
          if (absVal >= 1) return Number(val.toFixed(2)).toString();
          if (absVal >= 0.01) return Number(val.toFixed(4)).toString();
          return Number(val.toPrecision(3)).toString();
        }
      },
      min: (useLogScale && !(cardAxisAutoFit[cardId] && cardAxisAutoFit[cardId].y)) ? 0.01 : undefined
    },


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

      /* Qi anchor click → show anchor popup menu */
      if (params.seriesName === 'Qi Anchor') {
        const idx = params.data && params.data[2];
        if (idx !== undefined && idx !== null) {
          /* Stop the native click from bubbling to document (which would
             immediately trigger hideAnchorPointMenu). */
          const nativeEvt = params.event && params.event.event;
          if (nativeEvt) nativeEvt.stopPropagation();
          /* Use viewport-relative coordinates (position:fixed menu needs clientX/Y) */
          const ex = nativeEvt ? nativeEvt.clientX : (params.event ? params.event.clientX || 300 : 300);
          const ey = nativeEvt ? nativeEvt.clientY : (params.event ? params.event.clientY || 200 : 200);
          showAnchorPointMenu(cardId, idx, ex, ey);
        }
        return;
      }

      if (!params.seriesName.endsWith('Actual') && params.seriesName !== 'Excluded') {
        /* Click on a fitted/forecast/multi-fit line → open its style section */
        if (params.seriesName && !params.seriesName.startsWith('_')) {
          highlightStyleSection(cardId, params.seriesName);
        }
        return;
      }

      let idx;

      idx = params.data && params.data[2];

      if (idx !== undefined && idx !== null) {
        const nativeEvt = params.event && params.event.event;
        if (nativeEvt) nativeEvt.stopPropagation();
        const ex = nativeEvt ? nativeEvt.clientX : (params.event ? params.event.offsetX || 300 : 300);
        const ey = nativeEvt ? nativeEvt.clientY : (params.event ? params.event.offsetY || 200 : 200);
        showPointMenu(cardId, idx, ex, ey);
      }

    });

  } else {

    /* Multi-well mode: handle annotation clicks + style section highlight */

    myChart.on('click', function (params) {

      if (params.componentType === 'markPoint' && params.name && params.name.startsWith('ann_')) {

        const annId = parseInt(params.name.replace('ann_', ''));

        const anns = cardAnnotations[cardId] || [];

        const idx = anns.findIndex(a => a.id === annId);

        if (idx >= 0) showAnnotationEditor(cardId, idx, params.event);

        return;

      }

      /* Click on any series → open its style section */
      if (params.seriesName && !params.seriesName.startsWith('_')) {
        highlightStyleSection(cardId, params.seriesName);
      }

    });

  }



  /* Setup legend click → color picker for user lines */

  setupLegendColorPicker(cardId, myChart);

  /* Setup legend right-click → remove series context menu */
  myChart.on('contextmenu', function (params) {
    if (params.componentType !== 'legend') return;
    const nativeEvt = params.event && params.event.event;
    if (nativeEvt) nativeEvt.preventDefault();
    const rect = chartDiv.getBoundingClientRect();
    const cx = nativeEvt ? nativeEvt.clientX : (rect.left + (params.event ? params.event.offsetX || 0 : 0));
    const cy = nativeEvt ? nativeEvt.clientY : (rect.top + (params.event ? params.event.offsetY || 0 : 0));
    showLegendContextMenu(cardId, params.name, cx, cy);
  });

  /* Update restore-hidden button visibility */
  updateRestoreHiddenBtn(cardId);


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
  setupCtrlClickSelection(cardId, myChart, chartDiv);
  setupAxisHoverTooltip(cardId, chartDiv);

  /* Update multi-fit panel */
  updateMultiFitPanel(cardId);

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

function renderCurveSummaryTable(cardId, data) {
  const wrap = document.getElementById('curveSummaryWrap-' + cardId);
  const host = document.getElementById('curveSummary-' + cardId);
  if (!wrap || !host) return;

  const wells = (data && data.wells) ? data.wells : [];
  const multiFits = cardMultiFits[cardId] || [];
  const hiddenSet = cardHiddenSeries[cardId] || new Set();

  const fmtNum = (v, digits = 4) => {
    if (!Number.isFinite(v)) return '—';
    return Number(v).toFixed(digits);
  };

  const fmtShort = (v, digits = 4) => {
    if (!Number.isFinite(v)) return '—';
    return Number(v).toFixed(digits).replace(/\.0+$/, '').replace(/(\.\d*?)0+$/, '$1');
  };

  const buildFormula = (model, params, fallbackEquation) => {
    if (!params || !Number.isFinite(params.qi) || !Number.isFinite(params.di)) return fallbackEquation || '—';
    if (model === 'exponential') {
      return 'q(t)=' + fmtShort(params.qi) + '*exp(-' + fmtShort(params.di, 6) + '*t)';
    }
    if (model === 'hyperbolic') {
      const b = Number.isFinite(params.b) ? params.b : 0.5;
      return 'q(t)=' + fmtShort(params.qi) + '/(1+' + fmtShort(b) + '*' + fmtShort(params.di, 6) + '*t)^(1/' + fmtShort(b) + ')';
    }
    if (model === 'harmonic') {
      return 'q(t)=' + fmtShort(params.qi) + '/(1+' + fmtShort(params.di, 6) + '*t)';
    }
    return fallbackEquation || '—';
  };

  const getStats = (arr) => {
    const vals = (arr || []).filter(v => Number.isFinite(v));
    if (vals.length < 2) return null;
    const first = vals[0];
    const last = vals[vals.length - 1];
    const delta = last - first;
    const pct = first !== 0 ? (delta / Math.abs(first)) * 100 : 0;
    return {
      first,
      last,
      delta,
      pct,
      sign: delta >= 0 ? '+' : '',
      cls: delta >= 0 ? 'positive' : 'negative',
    };
  };

  const rows = [];

  wells.forEach((w) => {
    const prefix = wells.length === 1 ? (w.well || 'Fitted') : w.well;
    const fittedName = wells.length === 1 ? 'Fitted' : (w.well + ' Fitted');
    if (hiddenSet.has(fittedName)) return;

    const fittedVals = (w.y_fitted || []).filter(v => Number.isFinite(v));
    const forecastVals = (w.forecast && w.forecast.y) ? w.forecast.y.filter(v => Number.isFinite(v)) : [];
    const seriesStats = getStats(fittedVals.concat(forecastVals));
    if (!seriesStats) return;

    const fcStats = getStats(forecastVals);
    const model = data.model || '—';
    const baseParams = w.params || {};
    rows.push({
      curve: prefix,
      model,
      formula: buildFormula(model, baseParams, w.equation),
      qi: Number.isFinite(baseParams.qi) ? baseParams.qi : null,
      di: Number.isFinite(baseParams.di) ? baseParams.di : null,
      forecastPoints: forecastVals.length,
      fcStats,
      seriesStats,
    });

    const mainPs = (cardPCurveState[cardId] || {}).main;
    if (mainPs && mainPs.enabled) {
      const isDate = w.is_date || false;
      const p10Di = mainPs.p10Di;
      const p90Di = mainPs.p90Di;
      const p10Name = (wells.length === 1 ? '' : (w.well + ' ')) + 'P10 (Di=' + (Math.round(p10Di * 1e6) / 1e6) + ')';
      const p90Name = (wells.length === 1 ? '' : (w.well + ' ')) + 'P90 (Di=' + (Math.round(p90Di * 1e6) / 1e6) + ')';

      if (!hiddenSet.has(p10Name)) {
        const p10Params = { ...baseParams, di: p10Di };
        const p10Pts = buildPCurveData(w, data, p10Di, isDate, null);
        const p10Vals = p10Pts.map(p => p[1]).filter(v => Number.isFinite(v));
        const p10SeriesStats = getStats(p10Vals);
        if (p10SeriesStats) {
          const fcCount = (w.forecast && w.forecast.y) ? w.forecast.y.length : 0;
          const p10FcVals = fcCount > 0 ? p10Vals.slice(-fcCount) : [];
          rows.push({
            curve: wells.length === 1 ? 'P10' : (w.well + ' P10'),
            model,
            formula: buildFormula(model, p10Params, ''),
            qi: Number.isFinite(p10Params.qi) ? p10Params.qi : null,
            di: Number.isFinite(p10Params.di) ? p10Params.di : null,
            forecastPoints: p10FcVals.length,
            fcStats: getStats(p10FcVals),
            seriesStats: p10SeriesStats,
          });
        }
      }

      if (!hiddenSet.has(p90Name)) {
        const p90Params = { ...baseParams, di: p90Di };
        const p90Pts = buildPCurveData(w, data, p90Di, isDate, null);
        const p90Vals = p90Pts.map(p => p[1]).filter(v => Number.isFinite(v));
        const p90SeriesStats = getStats(p90Vals);
        if (p90SeriesStats) {
          const fcCount = (w.forecast && w.forecast.y) ? w.forecast.y.length : 0;
          const p90FcVals = fcCount > 0 ? p90Vals.slice(-fcCount) : [];
          rows.push({
            curve: wells.length === 1 ? 'P90' : (w.well + ' P90'),
            model,
            formula: buildFormula(model, p90Params, ''),
            qi: Number.isFinite(p90Params.qi) ? p90Params.qi : null,
            di: Number.isFinite(p90Params.di) ? p90Params.di : null,
            forecastPoints: p90FcVals.length,
            fcStats: getStats(p90FcVals),
            seriesStats: p90SeriesStats,
          });
        }
      }
    }
  });

  multiFits.forEach((mf) => {
    const name = 'Curve #' + mf.id + ' (' + mf.model + ')';
    if (hiddenSet.has(name)) return;

    const fittedVals = (mf.fittedData || []).map(p => Array.isArray(p) ? p[1] : null).filter(v => Number.isFinite(v));
    let forecastVals = (mf.forecastData || []).map(p => Array.isArray(p) ? p[1] : null).filter(v => Number.isFinite(v));

    if (fittedVals.length > 0 && forecastVals.length > 0) {
      const lastFit = fittedVals[fittedVals.length - 1];
      if (Math.abs(forecastVals[0] - lastFit) < 1e-12) forecastVals = forecastVals.slice(1);
    }

    const seriesStats = getStats(fittedVals.concat(forecastVals));
    if (!seriesStats) return;

    rows.push({
      curve: 'Curve #' + mf.id,
      model: mf.model || '—',
      formula: buildFormula(mf.model, mf.params || {}, mf.equation || ''),
      qi: mf.params && Number.isFinite(mf.params.qi) ? mf.params.qi : null,
      di: mf.params && Number.isFinite(mf.params.di) ? mf.params.di : null,
      forecastPoints: forecastVals.length,
      fcStats: getStats(forecastVals),
      seriesStats,
    });

    const mfPs = (cardPCurveState[cardId] || {})['mf_' + mf.id];
    if (mfPs && mfPs.enabled && mf.params && wells[0] && wells[0].t) {
      const w0 = wells[0];
      const idxMin = mf.indices ? Math.min(...mf.indices) : 0;
      const idxMax = mf.indices ? Math.max(...mf.indices) : w0.t.length - 1;

      const buildMfVals = (diVal) => {
        const pParams = { ...mf.params, di: diVal };
        const vals = [];
        for (let i = idxMin; i <= Math.min(idxMax, w0.t.length - 1); i++) {
          const y = evalDeclineModel(mf.model, w0.t[i], pParams);
          if (Number.isFinite(y)) vals.push(y);
        }
        return vals;
      };

      const p10Name = name + ' P10';
      if (!hiddenSet.has(p10Name)) {
        const p10Vals = buildMfVals(mfPs.p10Di);
        const p10Stats = getStats(p10Vals);
        if (p10Stats) {
          rows.push({
            curve: 'Curve #' + mf.id + ' P10',
            model: mf.model || '—',
            formula: buildFormula(mf.model, { ...mf.params, di: mfPs.p10Di }, ''),
            qi: Number.isFinite(mf.params.qi) ? mf.params.qi : null,
            di: Number.isFinite(mfPs.p10Di) ? mfPs.p10Di : null,
            forecastPoints: 0,
            fcStats: null,
            seriesStats: p10Stats,
          });
        }
      }

      const p90Name = name + ' P90';
      if (!hiddenSet.has(p90Name)) {
        const p90Vals = buildMfVals(mfPs.p90Di);
        const p90Stats = getStats(p90Vals);
        if (p90Stats) {
          rows.push({
            curve: 'Curve #' + mf.id + ' P90',
            model: mf.model || '—',
            formula: buildFormula(mf.model, { ...mf.params, di: mfPs.p90Di }, ''),
            qi: Number.isFinite(mf.params.qi) ? mf.params.qi : null,
            di: Number.isFinite(mfPs.p90Di) ? mfPs.p90Di : null,
            forecastPoints: 0,
            fcStats: null,
            seriesStats: p90Stats,
          });
        }
      }
    }
  });

  if (rows.length === 0) {
    wrap.style.display = 'none';
    host.innerHTML = '';
    return;
  }

  const htmlRows = rows.map((r) => {
    const fc = r.fcStats;
    const ss = r.seriesStats;
    const fcCls = fc ? fc.cls : '';
    return '<tr>'
      + '<td class="curve-summary-left">' + r.curve + '</td>'
      + '<td>' + r.model + '</td>'
      + '<td class="curve-summary-formula">' + (r.formula || '—') + '</td>'
      + '<td>' + (r.qi == null ? '—' : fmtNum(r.qi, 4)) + '</td>'
      + '<td>' + (r.di == null ? '—' : fmtNum(r.di, 6)) + '</td>'
      + '<td>' + r.forecastPoints + '</td>'
      + '<td>' + (fc ? fmtNum(fc.first, 2) : '—') + '</td>'
      + '<td>' + (fc ? fmtNum(fc.last, 2) : '—') + '</td>'
      + '<td class="' + fcCls + '">' + (fc ? (fc.sign + fmtNum(fc.delta, 2)) : '—') + '</td>'
      + '<td class="' + fcCls + '">' + (fc ? (fc.sign + fmtNum(fc.pct, 1) + '%') : '—') + '</td>'
      + '<td>' + fmtNum(ss.first, 2) + '</td>'
      + '<td>' + fmtNum(ss.last, 2) + '</td>'
      + '<td class="' + ss.cls + '">' + ss.sign + fmtNum(ss.delta, 2) + '</td>'
      + '<td class="' + ss.cls + '">' + ss.sign + fmtNum(ss.pct, 1) + '%</td>'
      + '</tr>';
  }).join('');

  host.innerHTML = '<table class="curve-summary-table">'
    + '<thead><tr>'
    + '<th class="curve-summary-left">Curve</th>'
    + '<th>Model</th>'
    + '<th class="curve-summary-formula">Formula</th>'
    + '<th>Qi</th>'
    + '<th>Di</th>'
    + '<th>Fcst Pts</th>'
    + '<th>Fcst First</th>'
    + '<th>Fcst Last</th>'
    + '<th>Fcst Δ</th>'
    + '<th>Fcst %</th>'
    + '<th>Series First</th>'
    + '<th>Series Last</th>'
    + '<th>Series Δ</th>'
    + '<th>Series %</th>'
    + '</tr></thead><tbody>' + htmlRows + '</tbody></table>';

  wrap.style.display = 'block';
}

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

    // Only start box-selection when the click is strictly inside the grid area.
    // containPixel returns false for axis regions, so axis-pan drags are not captured.
    if (!chart.containPixel('grid', [offsetX, offsetY])) return;

    _activeSelection = { cardId, chart, chartDiv, startX: offsetX, startY: offsetY, overlay: null, ctrlHeld: !!e.ctrlKey };

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

  // If Ctrl was held when starting the drag, add points in box to multi-select
  if (sel.ctrlHeld || e.ctrlKey) {
    if (sel.overlay) sel.overlay.remove();
    addBoxPointsToCtrlSelection(sel.cardId, sel.chart, selRect);
    showMultiPointMenu(e.clientX, e.clientY, sel.cardId);
    return;
  }

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

    else if (action === 'addfit') addCurveFitFromSelection(cardId, chart, selRect);

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

  /* Manual zoom overrides "Fit to frame" auto-fit */
  if (cardAxisAutoFit[cardId]) {
    if (mode === 'box' || mode === 'x') cardAxisAutoFit[cardId].x = false;
    if (mode === 'box' || mode === 'y') cardAxisAutoFit[cardId].y = false;
  }

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

  delete cardAxisAutoFit[cardId];

  setResetZoomButtonsVisible(cardId, false);

}



function resetAll(cardId) {

  cardExclusions[cardId] = new Set();

  delete cardZoomState[cardId];

  delete cardAxisAutoFit[cardId];

  delete cardMultiFits[cardId]; delete cardHiddenSeries[cardId];

  setResetZoomButtonsVisible(cardId, false);

  const ra = document.getElementById('resetAll-' + cardId); if (ra) ra.classList.remove('show');

  updateMultiFitPanel(cardId);

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



  const _anchorsFSO = new Set(w.qi_anchor_indices || []);

  const newExcl = new Set();

  for (let i = 0; i < w.x.length; i++) {

    if (_anchorsFSO.has(i)) continue; // Qi anchor points are never excluded

    let xVal, yVal = w.y_actual[i];

    if (isDate) { xVal = parseDateStr(w.x[i]); } else { xVal = w.x[i]; }

    if (xVal < xMin || xVal > xMax || yVal < yMin || yVal > yMax) {

      newExcl.add(i);

    }

  }

  cardExclusions[cardId] = newExcl;

  saveZoomState(cardId);

  if (w.qi_anchor_indices && w.qi_anchor_indices.length > 0) {
    refitCurrentData(cardId);
  } else {
    runSingleDCA(cardId);
  }

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
  const _anchorsES = new Set(w.qi_anchor_indices || []);

  for (let i = 0; i < w.x.length; i++) {

    if (_anchorsES.has(i)) continue; // Qi anchor points are never excluded

    let xVal, yVal = w.y_actual[i];

    if (isDate) { xVal = parseDateStr(w.x[i]); } else { xVal = w.x[i]; }

    if (xVal >= xMin && xVal <= xMax && yVal >= yMin && yVal <= yMax) {

      existing.add(i);

    }

  }

  saveZoomState(cardId);

  if (w.qi_anchor_indices && w.qi_anchor_indices.length > 0) {
    refitCurrentData(cardId);
  } else {
    runSingleDCA(cardId);
  }

}



/* ====================================================================

   Multi-Curve Fitting — Add additional decline curves to different
   groups of selected data on the same chart

   ==================================================================== */

/* Add a curve fit from a box-selection rectangle */
async function addCurveFitFromSelection(cardId, chart, selRect) {
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

  // Collect indices of points inside the selection
  const indices = [];
  for (let i = 0; i < w.x.length; i++) {
    let xVal, yVal = w.y_actual[i];
    if (yVal == null) continue;
    if (isDate) { xVal = parseDateStr(w.x[i]); } else { xVal = w.x[i]; }
    if (xVal >= xMin && xVal <= xMax && yVal >= yMin && yVal <= yMax) {
      indices.push(i);
    }
  }

  if (indices.length < 3) {
    showToast('Need at least 3 data points to fit a curve', 'warning');
    return;
  }

  await _performMultiFit(cardId, indices);
}

/* Add a curve fit from Ctrl+click multi-selected points */
async function addCurveFitFromMultiSelect(cardId, selectedSet) {
  const lastData = cardLastData[cardId];
  if (!lastData || !lastData.wells || !lastData.wells[0]) return;

  const indices = [...selectedSet].sort((a, b) => a - b);
  if (indices.length < 3) {
    showToast('Need at least 3 data points to fit a curve', 'warning');
    return;
  }

  await _performMultiFit(cardId, indices);
}

/* Core: fit a curve to a subset of points and add it to the multi-fit list */
async function _performMultiFit(cardId, indices, modelOverride) {
  const lastData = cardLastData[cardId];
  const w = lastData.wells[0];
  const isDate = w.is_date || false;

  // Get model — use explicit override if given, else fall back to card selector
  const card = document.getElementById(cardId);
  const model = modelOverride || card?.querySelector('.p-model')?.value || lastData.model || 'exponential';
  const forecastMonths = parseFloat(card?.querySelector('.p-forecast')?.value || 0);

  // Build t and y arrays for the selected indices
  const tFit = [], yFit = [];
  for (const i of indices) {
    if (i < w.t.length && w.y_actual[i] != null) {
      tFit.push(w.t[i]);
      yFit.push(w.y_actual[i]);
    }
  }

  if (tFit.length < 3) {
    showToast('Need at least 3 valid data points', 'warning');
    return;
  }

  try {
    const res = await fetch('/api/fit_inline', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ t: tFit, y: yFit, model, forecast_months: forecastMonths }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      showToast(err.detail || 'Curve fitting failed', 'error');
      return;
    }
    const fit = await res.json();

    // Build fitted data series: evaluate fitted curve only at actual data point dates
    const fittedData = [];
    for (let i = 0; i < tFit.length; i++) {
      const tVal = tFit[i];
      const yVal = evalDeclineModel(model, tVal, fit.params);
      if (isDate) {
        const MS_PER_DAY = 86400000;
        const firstDateMs = parseDateStr(w.x[0]);
        const tOffset = w.t[0] || 0;
        const ms = firstDateMs + (tVal - tOffset) * MS_PER_DAY;
        fittedData.push([ms, yVal]);
      } else {
        const x0 = w.x[0] || 0, t0 = w.t[0] || 0;
        fittedData.push([tVal - t0 + x0, yVal]);
      }
    }

    // Build forecast data
    let forecastData = [];
    if (fit.forecast_t && fit.forecast_y && fit.forecast_t.length > 0) {
      for (let i = 0; i < fit.forecast_t.length; i++) {
        if (isDate) {
          const MS_PER_DAY = 86400000;
          const firstDateMs = parseDateStr(w.x[0]);
          const tOffset = w.t[0] || 0;
          const ms = firstDateMs + (fit.forecast_t[i] - tOffset) * MS_PER_DAY;
          forecastData.push([ms, fit.forecast_y[i]]);
        } else {
          const x0 = w.x[0] || 0, t0 = w.t[0] || 0;
          forecastData.push([fit.forecast_t[i] - t0 + x0, fit.forecast_y[i]]);
        }
      }
      // Bridge from last fitted point to first forecast point
      if (fittedData.length > 0) {
        forecastData.unshift(fittedData[fittedData.length - 1]);
      }
    }

    // Assign a color
    if (!cardMultiFits[cardId]) cardMultiFits[cardId] = [];
    const colorIdx = cardMultiFits[cardId].length % MULTI_FIT_COLORS.length;

    const curveFit = {
      id: _multiFitNextId++,
      model: model,
      params: fit.params,
      equation: fit.equation || '',
      indices: indices,
      color: MULTI_FIT_COLORS[colorIdx],
      fittedData: fittedData,
      forecastData: forecastData,
      forecast_t: fit.forecast_t || [],
    };
    cardMultiFits[cardId].push(curveFit);

    // Rebuild style sections to include the new multi-fit curve
    rebuildCurveStyleSections(cardId);

    // Re-render chart and update management panel
    saveZoomState(cardId);
    renderSingleChart(cardId, lastData, forecastMonths);
    updateMultiFitPanel(cardId);

    const fmt = (v) => typeof v === 'number' ? v.toFixed(4) : v;
    showToast(`Curve #${curveFit.id} added (${model}, qi=${fmt(fit.params.qi)})`, 'success', 3000);

    if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();
  } catch (e) {
    showToast('Add curve fit failed: ' + e.message, 'error');
  }
}

/* Remove a specific curve */
/* Re-fit a multi-fit curve with a different model */
async function changeMultiFitModel(cardId, mfId, newModel) {
  const fits = cardMultiFits[cardId];
  if (!fits) return;
  const mf = fits.find(f => f.id === parseInt(mfId));
  if (!mf) return;
  const indices = mf.indices;
  if (!indices || indices.length < 3) {
    showToast('Need at least 3 data points to re-fit', 'warning');
    return;
  }

  const lastData = cardLastData[cardId];
  const w = lastData?.wells?.[0];
  if (!w) return;
  const isDate = w.is_date || false;
  const card = document.getElementById(cardId);
  const forecastMonths = parseFloat(card?.querySelector('.p-forecast')?.value || 0);

  const tFit = [], yFit = [];
  for (const i of indices) {
    if (i < w.t.length && w.y_actual[i] != null) {
      tFit.push(w.t[i]);
      yFit.push(w.y_actual[i]);
    }
  }
  if (tFit.length < 3) { showToast('Not enough valid data points', 'warning'); return; }

  try {
    const res = await fetch('/api/fit_inline', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ t: tFit, y: yFit, model: newModel, forecast_months: forecastMonths }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      showToast(err.detail || 'Re-fit failed', 'error');
      return;
    }
    const fit = await res.json();

    /* Rebuild fitted data */
    const tMin = Math.min(...tFit), tMax = Math.max(...tFit);
    const fittedData = [];
    const nPts = Math.max(100, indices.length * 2);
    const step = (tMax - tMin) / (nPts - 1);
    for (let i = 0; i < nPts; i++) {
      const tVal = tMin + step * i;
      const yVal = evalDeclineModel(newModel, tVal, fit.params);
      if (isDate) {
        const MS_PER_DAY = 86400000;
        const firstDateMs = parseDateStr(w.x[0]);
        const tOffset = w.t[0] || 0;
        fittedData.push([firstDateMs + (tVal - tOffset) * MS_PER_DAY, yVal]);
      } else {
        const x0 = w.x[0] || 0, t0 = w.t[0] || 0;
        fittedData.push([tVal - t0 + x0, yVal]);
      }
    }

    /* Update the curveFit object in-place */
    mf.model = newModel;
    mf.params = fit.params;
    mf.equation = fit.equation || '';
    mf.fittedData = fittedData;
    mf.forecast_t = fit.forecast_t || [];

    rebuildCurveStyleSections(cardId);
    saveZoomState(cardId);
    renderSingleChart(cardId, lastData, forecastMonths);
    updateMultiFitPanel(cardId);

    const fmt = (v) => typeof v === 'number' ? v.toFixed(4) : v;
    showToast('Curve #' + mf.id + ' re-fit as ' + newModel + ' (qi=' + fmt(fit.params.qi) + ')', 'success', 3000);
    if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();
  } catch (e) {
    showToast('Re-fit failed: ' + e.message, 'error');
  }
}

/* Re-fit the main curve with a different model */
async function changeMainFitModel(cardId, newModel) {
  const card = document.getElementById(cardId);
  if (!card) return;
  /* Update the model selector dropdown to the new model */
  const modelSel = card.querySelector('.p-model');
  if (modelSel) modelSel.value = newModel;
  /* Trigger a re-fit by calling runSingleDCA */
  await runSingleDCA(cardId);
}

/* Dispatcher: change model for any curve type */
function changeCurveModel(cardId, type, id, newModel) {
  if (type === 'main') {
    changeMainFitModel(cardId, newModel);
  } else {
    changeMultiFitModel(cardId, id, newModel);
  }
}

function removeCurve(cardId, type, id) {
  if (type === 'multi') {
    const fits = cardMultiFits[cardId];
    if (fits) {
      const idx = fits.findIndex(f => f.id === parseInt(id));
      if (idx >= 0) fits.splice(idx, 1);
      if (fits.length === 0) delete cardMultiFits[cardId];
    }
    if (cardPCurveState[cardId]) {
      delete cardPCurveState[cardId]['mf_' + id];
    }
  } else if (type === 'main') {
    const data = cardLastData[cardId];
    if (data && data.wells) {
      data.wells.forEach(w => {
        delete w.y_fitted;
        delete w.params;
        delete w.forecast;
        delete w.qi_anchor_indices;
      });
    }
    if (cardPCurveState[cardId]) delete cardPCurveState[cardId]['main'];
  }

  rebuildCurveStyleSections(cardId);
  const data = cardLastData[cardId];
  const card = document.getElementById(cardId);
  const forecastMonths = parseFloat(card?.querySelector('.p-forecast')?.value || 0);
  if (data) {
    saveZoomState(cardId);
    renderSingleChart(cardId, data, forecastMonths);
  }
  updateMultiFitPanel(cardId);
  if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();
}

/* Clear all multi-fit curves */
function clearAllMultiFits(cardId) {
  delete cardMultiFits[cardId];
  if (cardPCurveState[cardId]) {
    for (const key of Object.keys(cardPCurveState[cardId])) {
      if (key.startsWith('mf_')) delete cardPCurveState[cardId][key];
    }
  }
  rebuildCurveStyleSections(cardId);
  const data = cardLastData[cardId];
  const card = document.getElementById(cardId);
  const forecastMonths = parseFloat(card?.querySelector('.p-forecast')?.value || 0);
  if (data) {
    saveZoomState(cardId);
    renderSingleChart(cardId, data, forecastMonths);
  }
  updateMultiFitPanel(cardId);
  if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();
}

/* Update the multi-fit management panel — shows ALL curves (main + additional) */
function updateMultiFitPanel(cardId) {
  const panel = document.getElementById('multiFitPanel-' + cardId);
  if (!panel) return;

  const fmt = (v) => typeof v === 'number' ? (Number.isInteger(v) ? v.toString() : v.toFixed(4)) : v;
  const fits = cardMultiFits[cardId] || [];
  const lastData = cardLastData[cardId];
  const allWells = lastData?.wells || [];
  const hiddenSet = cardHiddenSeries[cardId] || new Set();
  const pcs = cardPCurveState[cardId] || {};

  /* Collect all curve entries */
  const allCurves = [];

  /* Main fitted curves (one per well) */
  allWells.forEach((w, wIdx) => {
    const isSingle = allWells.length === 1;
    const prefix = isSingle ? '' : w.well + ' ';
    const fittedName = prefix + 'Fitted';
    if (w.y_fitted || (w.forecast && w.forecast.x && w.forecast.x.length > 0)) {
      const st = cardStyles[cardId] || getDefaultStyles();
      const wellColors = getPlotThemePalette(st.plotTheme);
      const cs = (st.curveStyles && st.curveStyles[w.well]) || {};
      const fitColor = isSingle ? (cs.fittedColor || st.fittedColor) : (cs.fittedColor || wellColors[wIdx % wellColors.length]);
      const params = w.params || {};
      const model = lastData?.model || 'exponential';

      /* Sub-series names for this main curve (fitted + P10 + P90) */
      const ps = pcs['main'];
      const subSeries = [fittedName];
      if (ps && ps.enabled && w.params && w.t) {
        const p10DiRound = Math.round(ps.p10Di * 1e6) / 1e6;
        const p90DiRound = Math.round(ps.p90Di * 1e6) / 1e6;
        subSeries.push(prefix + 'P10 (Di=' + p10DiRound + ')');
        subSeries.push(prefix + 'P90 (Di=' + p90DiRound + ')');
      }
      const isHidden = subSeries.some(n => hiddenSet.has(n));
      allCurves.push({
        type: 'main',
        id: '',
        name: fittedName,
        label: (isSingle ? (w.well || 'Fitted') : w.well) + ' (' + model + ')',
        model: model,
        params: params,
        color: fitColor,
        subSeries: subSeries,
        isHidden: isHidden,
      });
    }
  });

  /* Additional multi-fit curves */
  const mfStylesObj = (cardStyles[cardId] || {}).multiFitStyles || {};
  fits.forEach(f => {
    const mfName = 'Curve #' + f.id + ' (' + f.model + ')';
    const ms = mfStylesObj['mf_' + f.id] || {};
    const mfDisplayColor = ms.color || f.color;
    const ps = pcs['mf_' + f.id];
    const subSeries = [mfName];
    if (ps && ps.enabled && f.params) {
      subSeries.push(mfName + ' P10');
      subSeries.push(mfName + ' P90');
    }
    const isHidden = subSeries.some(n => hiddenSet.has(n));
    allCurves.push({
      type: 'multi',
      id: f.id,
      name: mfName,
      label: 'Curve #' + f.id,
      model: f.model,
      params: f.params,
      color: mfDisplayColor,
      subSeries: subSeries,
      isHidden: isHidden,
    });
  });

  if (allCurves.length === 0) {
    panel.style.display = 'none';
    panel.innerHTML = '';
    return;
  }

  let html = '<div class="mf-header"><span class="mf-title">All Curves (' + allCurves.length + ')</span>'
    + '<button class="mf-clear-all" onclick="clearAllCurvesFromPanel(\'' + cardId + '\')" title="Remove all curves from plot">Clear All</button></div>';

  allCurves.forEach(c => {
    const paramStr = Object.entries(c.params).map(([k, v]) => k + '=' + fmt(v)).join(', ');
    const hiddenCls = c.isHidden ? ' mf-item-hidden' : '';
    const eyeIcon = c.isHidden ? '👁️‍🗨️' : '';
    let actionsHtml = '';

    if (c.isHidden) {
      actionsHtml = '<button class="mf-restore" onclick="restoreCurveInPanel(\'' + cardId + '\', ' + JSON.stringify(c.subSeries).replace(/'/g, "\\'").replace(/"/g, '&quot;') + ')" title="Show this curve">Show</button>';
    } else {
      /* P10/P90 toggle per curve */
      const cKey = c.type === 'main' ? 'main' : 'mf_' + c.id;
      const isPEnabled = pcs[cKey] && pcs[cKey].enabled;
      const pColor = isPEnabled ? 'color: var(--accent); border-color: var(--accent); font-weight: 500;' : '';
      actionsHtml += '<button class="mf-pcurve" style="' + pColor + '" onclick="toggleCurvePCurves(\'' + cardId + '\', \'' + c.type + '\', \'' + c.id + '\')" title="Toggle P10/P90">± P10/P90</button>';

      actionsHtml += '<button class="mf-remove" onclick="removeCurve(\'' + cardId + '\', \'' + c.type + '\', \'' + c.id + '\')" title="Delete this curve permanently">&times;</button>';
      actionsHtml += '<button class="mf-hide" onclick="hideCurveFromPanel(\'' + cardId + '\', ' + JSON.stringify(c.subSeries).replace(/'/g, "\\'").replace(/"/g, '&quot;') + ')" title="Hide this curve from plot">Hide</button>';
    }

    /* Model selector dropdown for this curve */
    const modelOpts = ['exponential', 'hyperbolic', 'harmonic'];
    let modelSelectHtml = '<select class="mf-model-select" data-type="' + c.type + '" data-id="' + c.id + '" onchange="changeCurveModel(\'' + cardId + '\', \'' + c.type + '\', \'' + c.id + '\', this.value)">';
    modelOpts.forEach(m => {
      modelSelectHtml += '<option value="' + m + '"' + (m === c.model ? ' selected' : '') + '>' + m.charAt(0).toUpperCase() + m.slice(1) + '</option>';
    });
    modelSelectHtml += '</select>';

    html += '<div class="mf-item' + hiddenCls + '">'
      + '<span class="mf-color-dot" style="background:' + c.color + ';"></span>'
      + '<span class="mf-label">' + eyeIcon + c.label + '</span>'
      + modelSelectHtml
      + '<span class="mf-params">' + paramStr + '</span>'
      + actionsHtml
      + '</div>';
  });

  panel.innerHTML = html;
  panel.style.display = 'block';
}

/* Hide a curve (and its sub-series like P10/P90) from the plot */
function hideCurveFromPanel(cardId, subSeriesJson) {
  const subSeries = typeof subSeriesJson === 'string' ? JSON.parse(subSeriesJson) : subSeriesJson;
  if (!cardHiddenSeries[cardId]) cardHiddenSeries[cardId] = new Set();
  subSeries.forEach(n => cardHiddenSeries[cardId].add(n));

  const card = document.getElementById(cardId);
  const forecastMonths = parseFloat(card?.querySelector('.p-forecast')?.value || 0);
  const lastData = cardLastData[cardId];
  if (lastData) {
    saveZoomState(cardId);
    renderSingleChart(cardId, lastData, forecastMonths);
  }
  if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();
}

/* Restore (show) a hidden curve back on the plot */
function restoreCurveInPanel(cardId, subSeriesJson) {
  const subSeries = typeof subSeriesJson === 'string' ? JSON.parse(subSeriesJson) : subSeriesJson;
  const hidden = cardHiddenSeries[cardId];
  if (hidden) {
    subSeries.forEach(n => hidden.delete(n));
    if (hidden.size === 0) delete cardHiddenSeries[cardId];
  }

  const card = document.getElementById(cardId);
  const forecastMonths = parseFloat(card?.querySelector('.p-forecast')?.value || 0);
  const lastData = cardLastData[cardId];
  if (lastData) {
    saveZoomState(cardId);
    renderSingleChart(cardId, lastData, forecastMonths);
  }
  if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();
}

/* Clear all curves (wipe main, delete multi-fit) */
function clearAllCurvesFromPanel(cardId) {
  /* Delete all multi-fit curves */
  delete cardMultiFits[cardId];
  delete cardPCurveState[cardId];
  
  /* Perm-delete the main fitted curve */
  const lastData = cardLastData[cardId];
  if (lastData && lastData.wells) {
    lastData.wells.forEach(w => {
      delete w.y_fitted;
      delete w.params;
      delete w.forecast;
      delete w.qi_anchor_indices;
    });
  }

  rebuildCurveStyleSections(cardId);
  const card = document.getElementById(cardId);
  const forecastMonths = parseFloat(card?.querySelector('.p-forecast')?.value || 0);
  if (lastData) {
    saveZoomState(cardId);
    renderSingleChart(cardId, lastData, forecastMonths);
  }
  updateMultiFitPanel(cardId);
  if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();
}



/* ====================================================================

   Legend Right-Click → Remove / Restore Series

   ==================================================================== */

let _legendMenuCardId = null;
let _legendMenuSeriesName = null;

/* Show the floating legend context menu */
function showLegendContextMenu(cardId, seriesName, clientX, clientY) {
  const SKIP = ['Excluded', 'Qi Anchor'];
  if (SKIP.includes(seriesName)) return;

  _legendMenuCardId = cardId;
  _legendMenuSeriesName = seriesName;

  const menu = document.getElementById('legendContextMenu');
  if (!menu) return;

  const label = menu.querySelector('.lcm-series-name');
  if (label) label.textContent = seriesName.length > 38 ? seriesName.slice(0, 35) + '\u2026' : seriesName;

  /* Position: keep inside viewport */
  const menuW = 200, menuH = 80;
  const left = Math.min(clientX, window.innerWidth - menuW - 8);
  const top = Math.min(clientY, window.innerHeight - menuH - 8);
  menu.style.left = left + 'px';
  menu.style.top = top + 'px';
  menu.style.display = 'block';

  setTimeout(() => {
    document.addEventListener('mousedown', _hideLegendMenuOutside, { once: true, capture: true });
  }, 10);
}

function _hideLegendMenuOutside(e) {
  const menu = document.getElementById('legendContextMenu');
  if (menu && !menu.contains(e.target)) hideLegendMenu();
}

function hideLegendMenu() {
  const menu = document.getElementById('legendContextMenu');
  if (menu) menu.style.display = 'none';
  _legendMenuCardId = null;
  _legendMenuSeriesName = null;
}

/* Called by the "Remove" button in the legend context menu */
function legendMenuRemove() {
  if (_legendMenuCardId && _legendMenuSeriesName) {
    removeLegendSeries(_legendMenuCardId, _legendMenuSeriesName);
  }
  hideLegendMenu();
}

/* Remove (or permanently delete) a series by legend name */
function removeLegendSeries(cardId, seriesName) {
  /* Multi-fit curves: extract id and call proper removal */
  const mfMatch = seriesName.match(/^Curve #(\d+) \(/);
  if (mfMatch) {
    removeCurve(cardId, 'multi', parseInt(mfMatch[1]));
    return;
  }
  if (seriesName.endsWith('Fitted')) {
    removeCurve(cardId, 'main', '');
    return;
  }

  /* Everything else: add to hidden set */
  if (!cardHiddenSeries[cardId]) cardHiddenSeries[cardId] = new Set();
  cardHiddenSeries[cardId].add(seriesName);

  const card = document.getElementById(cardId);
  const forecastMonths = parseFloat(card?.querySelector('.p-forecast')?.value || 0);
  const lastData = cardLastData[cardId];
  if (lastData) {
    saveZoomState(cardId);
    renderSingleChart(cardId, lastData, forecastMonths);
  }
  if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();
}

/* Restore all hidden series for a card */
function restoreHiddenSeries(cardId) {
  delete cardHiddenSeries[cardId];
  const card = document.getElementById(cardId);
  const forecastMonths = parseFloat(card?.querySelector('.p-forecast')?.value || 0);
  const lastData = cardLastData[cardId];
  if (lastData) {
    saveZoomState(cardId);
    renderSingleChart(cardId, lastData, forecastMonths);
  }
  if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();
}

/* Show/hide the "Restore Hidden" button in the card toolbar */
function updateRestoreHiddenBtn(cardId) {
  const toolbar = document.querySelector('#' + cardId + ' .chart-toolbar');
  if (!toolbar) return;
  let btn = toolbar.querySelector('.restore-hidden-btn');
  const hasHidden = cardHiddenSeries[cardId] && cardHiddenSeries[cardId].size > 0;
  if (hasHidden && !btn) {
    btn = document.createElement('button');
    btn.className = 'restore-hidden-btn';
    btn.title = 'Restore all hidden series';
    btn.textContent = '\uD83D\uDC41 Restore Hidden';
    btn.onclick = () => restoreHiddenSeries(cardId);
    toolbar.appendChild(btn);
  } else if (!hasHidden && btn) {
    btn.remove();
  }
}


/* ====================================================================

   DCA Templates – Save / Load / Delete / Apply

   ==================================================================== */

const dcaTemplates = [];           // Array of template objects
let _dcaSidebarOpen = true;        // sidebar visible by default

/* Collect full template state from a card */
function _collectTemplateFromCard(cardId) {
  const card = document.getElementById(cardId);
  if (!card) return null;

  // Gather selected wells
  const wells = [];
  card.querySelectorAll('.well-picker-list input[type="checkbox"]:checked').forEach(cb => {
    wells.push(cb.value);
  });

  const lastData = cardLastData[cardId] || null;
  const w = lastData?.wells?.[0];

  // Build a concise summary
  const model = card.querySelector('.p-model')?.value || 'exponential';
  const equation = lastData?.equation || '';
  const params = lastData?.params || {};

  return {
    id: 'tpl-' + Date.now() + '-' + Math.floor(Math.random() * 10000),
    name: card.querySelector('.p-title')?.value || (wells.length ? wells[0] : 'Untitled'),
    createdAt: Date.now(),
    // Card configuration
    wells: wells,
    selX: card.querySelector('.p-selX')?.value || '',
    selY: card.querySelector('.p-selY')?.value || '',
    selWellCol: card.querySelector('.p-selWellCol')?.value || '',
    model: model,
    forecastMonths: card.querySelector('.p-forecast')?.value || '0',
    title: card.querySelector('.p-title')?.value || '',
    header: (card._headerInput || card.querySelector('.p-header'))?.value || '',
    combine: card.querySelector('.p-combine')?.checked || false,
    combineAgg: card.querySelector('.p-combine-agg')?.value || 'sum',
    // Fit results
    equation: equation,
    params: params,
    // State
    styles: cardStyles[cardId] ? JSON.parse(JSON.stringify(cardStyles[cardId])) : null,
    exclusions: cardExclusions[cardId] ? [...cardExclusions[cardId]] : [],
    pCurveState: cardPCurveState[cardId] ? JSON.parse(JSON.stringify(cardPCurveState[cardId])) : null,
    multiFits: cardMultiFits[cardId] ? JSON.parse(JSON.stringify(cardMultiFits[cardId])) : null,
    userLines: cardUserLines[cardId] ? JSON.parse(JSON.stringify(cardUserLines[cardId])) : [],
    annotations: cardAnnotations[cardId] ? JSON.parse(JSON.stringify(cardAnnotations[cardId])) : [],
    valueLabels: cardValueLabels[cardId] || 'none',
    logScale: cardLogScale[cardId] || false,
    logScaleX: cardLogScaleX[cardId] || false,
    axisLabels: cardAxisLabels[cardId] ? JSON.parse(JSON.stringify(cardAxisLabels[cardId])) : null,
    axisPositions: cardAxisPositions[cardId] ? JSON.parse(JSON.stringify(cardAxisPositions[cardId])) : null,
    // DCA response data (for offline restoring)
    lastData: lastData ? JSON.parse(JSON.stringify(lastData)) : null,
  };
}

/* Save a card as a DCA template */
function saveCardAsTemplate(cardId) {
  const lastData = cardLastData[cardId];
  if (!lastData) {
    showToast('Plot the card first before saving as a template', 'warning');
    return;
  }

  const tpl = _collectTemplateFromCard(cardId);
  if (!tpl) return;

  dcaTemplates.push(tpl);
  _saveDcaTemplatesToIDB();
  renderTemplateSidebar();

  // Make sure sidebar is visible
  if (!_dcaSidebarOpen) toggleDcaSidebar();

  showToast('Template "' + tpl.name + '" saved', 'success', 3000);
}

/* Delete a template */
function deleteTemplate(tplId) {
  const idx = dcaTemplates.findIndex(t => t.id === tplId);
  if (idx < 0) return;
  const name = dcaTemplates[idx].name;
  dcaTemplates.splice(idx, 1);
  _saveDcaTemplatesToIDB();
  renderTemplateSidebar();
  showToast('Template "' + name + '" deleted', 'info', 2500);
}

/* Rename a template */
function renameTemplate(tplId, newName) {
  const tpl = dcaTemplates.find(t => t.id === tplId);
  if (!tpl) return;
  tpl.name = newName || tpl.name;
  _saveDcaTemplatesToIDB();
}

/* Apply a template – creates a new card and restores the saved state */
function applyTemplate(tplId) {
  const tpl = dcaTemplates.find(t => t.id === tplId);
  if (!tpl) return;

  // Create a new card with preset values from the template
  const cardId = addPlotCard(
    tpl.wells,
    tpl.model,
    tpl.forecastMonths,
    tpl.title,
    tpl.combine,
    tpl.header,
    tpl.combineAgg,
    tpl.selX,
    tpl.selY,
    tpl.selWellCol
  );

  if (!cardId) return;

  // Restore state
  if (tpl.styles) cardStyles[cardId] = JSON.parse(JSON.stringify(tpl.styles));
  if (tpl.exclusions && tpl.exclusions.length) cardExclusions[cardId] = new Set(tpl.exclusions);
  if (tpl.pCurveState) cardPCurveState[cardId] = JSON.parse(JSON.stringify(tpl.pCurveState));
  if (tpl.multiFits) cardMultiFits[cardId] = JSON.parse(JSON.stringify(tpl.multiFits));
  if (tpl.userLines) cardUserLines[cardId] = JSON.parse(JSON.stringify(tpl.userLines));
  if (tpl.annotations) cardAnnotations[cardId] = JSON.parse(JSON.stringify(tpl.annotations));
  if (tpl.valueLabels) cardValueLabels[cardId] = tpl.valueLabels;
  if (tpl.logScale) cardLogScale[cardId] = tpl.logScale;
  if (tpl.logScaleX) cardLogScaleX[cardId] = tpl.logScaleX;
  if (tpl.axisLabels) cardAxisLabels[cardId] = JSON.parse(JSON.stringify(tpl.axisLabels));
  if (tpl.axisPositions) cardAxisPositions[cardId] = JSON.parse(JSON.stringify(tpl.axisPositions));

  // Restore styles to UI
  if (tpl.styles) {
    applyStylesToCard(cardId, tpl.styles);
  }

  // If we have stored DCA data, restore and render directly
  if (tpl.lastData) {
    cardLastData[cardId] = JSON.parse(JSON.stringify(tpl.lastData));
    rebuildCurveStyleSections(cardId);
    const card = document.getElementById(cardId);
    if (card) {
      card.querySelector('.chart-area').style.display = 'block';
    }
    renderSingleChart(cardId, cardLastData[cardId], tpl.forecastMonths);
    updateMultiFitPanel(cardId);
    showToast('Template "' + tpl.name + '" applied', 'success', 3000);
  } else {
    // No cached data – run the DCA
    runSingleDCA(cardId);
    showToast('Template "' + tpl.name + '" applied – running DCA...', 'info', 3000);
  }

  if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();
}

/* Toggle sidebar visibility */
function toggleDcaSidebar() {
  const sidebar = document.getElementById('dcaSidebar');
  const toggle = document.getElementById('dcaSidebarToggle');
  if (!sidebar) return;

  _dcaSidebarOpen = !_dcaSidebarOpen;
  sidebar.classList.toggle('collapsed', !_dcaSidebarOpen);
  if (toggle) toggle.classList.toggle('hidden', _dcaSidebarOpen);
}

/* Render the template sidebar contents */
function renderTemplateSidebar() {
  const container = document.getElementById('dcaSidebarContent');
  if (!container) return;

  if (dcaTemplates.length === 0) {
    container.innerHTML = '<div class="dca-tpl-empty">No templates saved yet.<br><span style="font-size:.72rem;opacity:.6;">Use the 💾 button on any card to save a template.</span></div>';
    return;
  }

  const fmt = (v) => typeof v === 'number' ? (Number.isInteger(v) ? v.toString() : v.toFixed(4)) : v;
  const formatDate = (ts) => {
    const d = new Date(ts);
    return d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  };

  let html = '';
  dcaTemplates.forEach(tpl => {
    const wellNames = (tpl.wells || []).join(', ') || 'No well';
    const paramStr = tpl.params ? Object.entries(tpl.params).map(([k, v]) => k + '=' + fmt(v)).join(', ') : '';
    const hasPCurve = tpl.pCurveState && tpl.pCurveState.enabled;
    const multiFitCount = (tpl.multiFits || []).length;

    html += '<div class="dca-tpl-item" data-tpl-id="' + tpl.id + '">';
    html += '  <div class="dca-tpl-date">' + formatDate(tpl.createdAt) + '</div>';
    html += '  <div class="dca-tpl-name">';
    html += '    <input type="text" class="dca-tpl-name-input" value="' + (tpl.name || '').replace(/"/g, '&quot;') + '" '
      + 'onchange="renameTemplate(\'' + tpl.id + '\', this.value)" '
      + 'title="Click to rename template">';
    html += '  </div>';
    html += '  <div class="dca-tpl-meta">';
    html += '    <span class="dca-tpl-tag model">' + (tpl.model || 'exponential') + '</span>';
    html += '    <span class="dca-tpl-tag well">' + wellNames + '</span>';
    if (hasPCurve) html += '    <span class="dca-tpl-tag pcurve">P10/P90</span>';
    if (multiFitCount > 0) html += '    <span class="dca-tpl-tag">+' + multiFitCount + ' curves</span>';
    if (parseInt(tpl.forecastMonths) > 0) html += '    <span class="dca-tpl-tag">' + tpl.forecastMonths + 'mo forecast</span>';
    html += '  </div>';
    if (tpl.equation) {
      html += '  <div class="dca-tpl-equation" title="' + tpl.equation.replace(/"/g, '&quot;') + '">' + tpl.equation + '</div>';
    }
    if (paramStr) {
      html += '  <div class="dca-tpl-params">' + paramStr + '</div>';
    }
    html += '  <div class="dca-tpl-actions">';
    html += '    <button class="tpl-apply-btn" onclick="applyTemplate(\'' + tpl.id + '\')" title="Apply this template as a new card">▶ Apply</button>';
    html += '    <button class="tpl-delete-btn" onclick="deleteTemplate(\'' + tpl.id + '\')" title="Delete this template">🗑 Delete</button>';
    html += '  </div>';
    html += '</div>';
  });

  container.innerHTML = html;
}

/* ── Template IDB Persistence ── */

async function _saveDcaTemplatesToIDB() {
  try {
    const db = await openIDB();
    const tx = db.transaction(IDB_WS_STORE, 'readwrite');
    const store = tx.objectStore(IDB_WS_STORE);
    store.put({ id: 'dcaTemplates', data: dcaTemplates });
    await new Promise((resolve, reject) => {
      tx.oncomplete = resolve;
      tx.onerror = () => reject(tx.error);
    });
  } catch (e) {
    console.warn('Failed to save DCA templates to IDB:', e);
  }
}

async function _loadDcaTemplatesFromIDB() {
  try {
    const db = await openIDB();
    const tx = db.transaction(IDB_WS_STORE, 'readonly');
    const store = tx.objectStore(IDB_WS_STORE);
    const result = await new Promise((resolve, reject) => {
      const r = store.get('dcaTemplates');
      r.onsuccess = () => resolve(r.result);
      r.onerror = () => reject(r.error);
    });
    if (result && Array.isArray(result.data)) {
      dcaTemplates.length = 0;
      result.data.forEach(t => dcaTemplates.push(t));
      renderTemplateSidebar();
    }
  } catch (e) {
    console.warn('Failed to load DCA templates from IDB:', e);
  }
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

  const isLogX = cardLogScaleX[cardId] || false;
  const logXItem = menu.querySelector('[data-action="log-scale-x"]');
  if (logXItem) logXItem.innerHTML = '<span class="ccm-icon">㏒</span> ' + (isLogX ? '✓ X-Axis Log Scale' : '　X-Axis Log Scale');

  const showPct = cardPctChange[cardId] || false;

  const pctItem = menu.querySelector('[data-action="pct-change"]');

  if (pctItem) pctItem.innerHTML = '<span class="ccm-icon">Δ</span> ' + (showPct ? '✓ Show % Change & Diff' : '　Show % Change & Diff');

  const labelPos = cardValueLabels[cardId] || 'none';

  menu.querySelectorAll('.ccm-label-opt').forEach(el => {

    const pos = el.dataset.pos;

    el.textContent = (pos === labelPos ? '✓ ' : '　') + pos.charAt(0).toUpperCase() + pos.slice(1);

  });

  menu.style.left = e.clientX + 'px'; menu.style.top = e.clientY + 'px';
  _ctxMenuClientX = e.clientX; _ctxMenuClientY = e.clientY;

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

/* ====================================================================
   Anchor Point Popup Menu
   ==================================================================== */

function showAnchorPointMenu(cardId, idx, clientX, clientY) {
  _anchorMenuCardId = cardId;
  _anchorMenuIdx = idx;
  _anchorMenuJustOpened = true;
  setTimeout(() => { _anchorMenuJustOpened = false; }, 50);

  const excl = cardExclusions[cardId] || new Set();
  const label = document.getElementById('anchorExclLabel');
  if (label) label.textContent = excl.has(idx) ? 'Include in fitting' : 'Exclude from fitting';

  const menu = document.getElementById('anchorPointMenu');
  menu.style.left = clientX + 'px';
  menu.style.top = clientY + 'px';
  menu.style.display = 'block';

  requestAnimationFrame(() => {
    const mr = menu.getBoundingClientRect();
    if (mr.right > window.innerWidth) menu.style.left = (clientX - mr.width) + 'px';
    if (mr.bottom > window.innerHeight) menu.style.top = (clientY - mr.height) + 'px';
  });
}

function hideAnchorPointMenu() {
  const m = document.getElementById('anchorPointMenu');
  if (m) m.style.display = 'none';
}

document.addEventListener('click', function (e) {
  if (_anchorMenuJustOpened) return;
  if (!e.target.closest('#anchorPointMenu')) hideAnchorPointMenu();
});

/* Toggle a Qi anchor point in/out of the exclusion set (it stays visible on the chart) */
function anchorToggleExclude() {
  const cardId = _anchorMenuCardId, idx = _anchorMenuIdx;
  hideAnchorPointMenu();
  if (cardId == null || idx == null) return;

  if (!cardExclusions[cardId]) cardExclusions[cardId] = new Set();
  const s = cardExclusions[cardId];
  if (s.has(idx)) s.delete(idx); else s.add(idx);

  saveZoomState(cardId);
  refitCurrentData(cardId);
}

/* Remove a Qi anchor data point from the well arrays entirely */
async function anchorRemovePoint() {
  const cardId = _anchorMenuCardId, idx = _anchorMenuIdx;
  hideAnchorPointMenu();
  if (cardId == null || idx == null) return;

  const data = cardLastData[cardId];
  if (!data || !data.wells || !data.wells[0]) return;
  const w = data.wells[0];

  /* 1. Remove from all data arrays */
  w.t.splice(idx, 1);
  w.x.splice(idx, 1);
  w.y_actual.splice(idx, 1);
  if (w.y_fitted) w.y_fitted.splice(idx, 1);

  /* 2. Update exclusions: remove idx, shift higher indices down by 1 */
  const excl = cardExclusions[cardId] || new Set();
  const newExcl = new Set();
  for (const i of excl) {
    if (i === idx) continue;
    newExcl.add(i > idx ? i - 1 : i);
  }
  cardExclusions[cardId] = newExcl;
  w.excluded_indices = [...newExcl].sort((a, b) => a - b);

  /* 3. Update anchor indices: remove idx, shift higher down by 1 */
  const newAnchors = [];
  for (const i of (w.qi_anchor_indices || [])) {
    if (i === idx) continue;
    newAnchors.push(i > idx ? i - 1 : i);
  }
  w.qi_anchor_indices = newAnchors;

  saveZoomState(cardId);

  if (newAnchors.length > 0) {
    /* Still have anchors — use in-memory refit */
    await refitCurrentData(cardId);
  } else {
    /* No more anchors — normal server-side DCA */
    runSingleDCA(cardId);
  }
}

/* Wire up anchor menu buttons (script runs after DOM, no need for DOMContentLoaded) */
(function () {
  const exclBtn = document.getElementById('anchorExclBtn');
  const removeBtn = document.getElementById('anchorRemoveBtn');
  if (exclBtn) exclBtn.addEventListener('click', anchorToggleExclude);
  if (removeBtn) removeBtn.addEventListener('click', anchorRemovePoint);
})();

/* ---- Regular data-point popup menu ---- */

function showPointMenu(cardId, idx, clientX, clientY) {
  _pointMenuCardId = cardId;
  _pointMenuIdx = idx;
  _pointMenuJustOpened = true;
  setTimeout(() => { _pointMenuJustOpened = false; }, 50);

  const excl = cardExclusions[cardId] || new Set();
  const label = document.getElementById('pointMenuExclLabel');
  if (label) label.textContent = excl.has(idx) ? 'Include in fitting' : 'Exclude from fitting';

  const menu = document.getElementById('pointMenu');
  menu.style.left = clientX + 'px';
  menu.style.top = clientY + 'px';
  menu.style.display = 'block';

  requestAnimationFrame(() => {
    const mr = menu.getBoundingClientRect();
    if (mr.right > window.innerWidth) menu.style.left = (clientX - mr.width) + 'px';
    if (mr.bottom > window.innerHeight) menu.style.top = (clientY - mr.height) + 'px';
  });
}

function hidePointMenu() {
  const m = document.getElementById('pointMenu');
  if (m) m.style.display = 'none';
}

document.addEventListener('click', function (e) {
  if (_pointMenuJustOpened) return;
  if (!e.target.closest('#pointMenu')) hidePointMenu();
});

function pointMenuToggleExclude() {
  const cardId = _pointMenuCardId, idx = _pointMenuIdx;
  hidePointMenu();
  if (cardId == null || idx == null) return;
  saveZoomState(cardId);
  toggleExclusion(cardId, idx);
}

/* Wire up point menu button */
(function () {
  const btn = document.getElementById('pointMenuExclBtn');
  if (btn) btn.addEventListener('click', pointMenuToggleExclude);
})();

/* ====================================================================
   Ctrl+click multi-point selection
   ==================================================================== */

/* Setup a raw DOM click listener on the chart div to detect Ctrl+click.
   This bypasses ECharts' event system entirely, which may suppress clicks
   when modifier keys are held. We manually hit-test scatter points. */
function setupCtrlClickSelection(cardId, chart, chartDiv) {
  function handler(e) {
    if (!e.ctrlKey) return; // only handle Ctrl+click
    const data = cardLastData[cardId];
    if (!data || !data.wells || data.wells.length !== 1) return;
    const w = data.wells[0];
    if (!w.x || !w.y_actual) return;

    const rect = chartDiv.getBoundingClientRect();
    const px = e.clientX - rect.left;
    const py = e.clientY - rect.top;

    // Must be inside the grid area
    if (!chart.containPixel('grid', [px, py])) return;

    const isDate = w.is_date || false;
    const THRESHOLD = 18; // pixel distance threshold
    let bestIdx = -1, bestDist = Infinity;

    for (let i = 0; i < w.x.length; i++) {
      if (w.y_actual[i] == null) continue;
      const xVal = isDate ? parseDateStr(w.x[i]) : w.x[i];
      const yVal = w.y_actual[i];
      const ptPx = chart.convertToPixel('grid', [xVal, yVal]);
      if (!ptPx || isNaN(ptPx[0]) || isNaN(ptPx[1])) continue;
      const dist = Math.hypot(ptPx[0] - px, ptPx[1] - py);
      if (dist < bestDist) { bestDist = dist; bestIdx = i; }
    }

    if (bestIdx >= 0 && bestDist <= THRESHOLD) {
      e.stopPropagation();
      e.preventDefault();
      toggleCtrlSelection(cardId, bestIdx, chart);
      showMultiPointMenu(e.clientX, e.clientY, cardId);
    }
  }

  // Clean up previous listener if chart was re-rendered
  if (chartDiv.__ctrlClickHandler) {
    chartDiv.removeEventListener('click', chartDiv.__ctrlClickHandler, true);
  }
  chartDiv.__ctrlClickHandler = handler;
  chartDiv.addEventListener('click', handler, true); // capture phase
}

function toggleCtrlSelection(cardId, idx, chart) {
  if (!cardCtrlSelected[cardId]) cardCtrlSelected[cardId] = new Set();
  const s = cardCtrlSelected[cardId];
  if (s.has(idx)) s.delete(idx); else s.add(idx);
  updateCtrlSelectionHighlight(cardId, chart);
}

/* Add all data points inside a pixel rectangle to the Ctrl multi-select set */
function addBoxPointsToCtrlSelection(cardId, chart, selRect) {
  const data = cardLastData[cardId];
  if (!data || !data.wells || data.wells.length !== 1) return;
  const w = data.wells[0];
  if (!w.x || !w.y_actual) return;
  const isDate = w.is_date || false;

  if (!cardCtrlSelected[cardId]) cardCtrlSelected[cardId] = new Set();
  const sel = cardCtrlSelected[cardId];

  for (let i = 0; i < w.x.length; i++) {
    if (w.y_actual[i] == null) continue;
    const xVal = isDate ? parseDateStr(w.x[i]) : w.x[i];
    const yVal = w.y_actual[i];
    const ptPx = chart.convertToPixel('grid', [xVal, yVal]);
    if (!ptPx || isNaN(ptPx[0]) || isNaN(ptPx[1])) continue;
    if (ptPx[0] >= selRect.x1 && ptPx[0] <= selRect.x2 &&
      ptPx[1] >= selRect.y1 && ptPx[1] <= selRect.y2) {
      sel.add(i);
    }
  }

  updateCtrlSelectionHighlight(cardId, chart);
}

function updateCtrlSelectionHighlight(cardId, chart) {
  const selected = cardCtrlSelected[cardId];
  const data = cardLastData[cardId];
  if (!data || !data.wells || data.wells.length === 0) return;
  const w = data.wells[0];
  const isDate = w.is_date || false;

  // Get or create the highlight overlay canvas
  const chartDom = chart.getDom();
  let overlay = chartDom.querySelector('.ctrl-select-overlay');
  if (!overlay) {
    overlay = document.createElement('canvas');
    overlay.className = 'ctrl-select-overlay';
    overlay.style.cssText = 'position:absolute;top:0;left:0;width:100%;height:100%;pointer-events:none;z-index:100;';
    chartDom.style.position = 'relative';
    chartDom.appendChild(overlay);
  }
  overlay.width = chartDom.clientWidth;
  overlay.height = chartDom.clientHeight;
  const ctx = overlay.getContext('2d');
  ctx.clearRect(0, 0, overlay.width, overlay.height);

  if (!selected || selected.size === 0) return;

  selected.forEach(function (idx) {
    if (idx >= w.x.length) return;
    const xVal = isDate ? parseDateStr(w.x[idx]) : w.x[idx];
    const yVal = w.y_actual[idx];
    if (yVal == null) return;
    const pxPt = chart.convertToPixel('grid', [xVal, yVal]);
    if (!pxPt || isNaN(pxPt[0]) || isNaN(pxPt[1])) return;
    // Outer ring
    ctx.beginPath();
    ctx.arc(pxPt[0], pxPt[1], 11, 0, Math.PI * 2);
    ctx.fillStyle = 'rgba(255,215,0,0.22)';
    ctx.fill();
    ctx.strokeStyle = '#FFD700';
    ctx.lineWidth = 2.5;
    ctx.stroke();
  });
}

function clearCtrlSelectionHighlights(cardId) {
  if (cardCtrlSelected[cardId]) cardCtrlSelected[cardId].clear();
  // Clear overlay canvases
  [cardId, 'fullChart-' + cardId].forEach(function (id) {
    const ch = chartInstances[id];
    if (!ch) return;
    const ov = ch.getDom().querySelector('.ctrl-select-overlay');
    if (ov) { const c = ov.getContext('2d'); c.clearRect(0, 0, ov.width, ov.height); }
  });
}

function showMultiPointMenu(clientX, clientY, cardId) {
  _multiPointMenuJustOpened = true;
  setTimeout(function () { _multiPointMenuJustOpened = false; }, 50);

  const count = (cardCtrlSelected[cardId] || new Set()).size;
  const header = document.getElementById('multiPointMenuCount');
  if (header) header.textContent = count + ' point' + (count !== 1 ? 's' : '') + ' selected';

  const menu = document.getElementById('multiPointMenu');
  menu.dataset.cardId = cardId;
  menu.style.left = clientX + 'px';
  menu.style.top = clientY + 'px';
  menu.style.display = 'block';

  requestAnimationFrame(function () {
    const mr = menu.getBoundingClientRect();
    if (mr.right > window.innerWidth) menu.style.left = (clientX - mr.width) + 'px';
    if (mr.bottom > window.innerHeight) menu.style.top = (clientY - mr.height) + 'px';
  });
}

function hideMultiPointMenu() {
  const m = document.getElementById('multiPointMenu');
  if (m) m.style.display = 'none';
}

document.addEventListener('click', function (e) {
  if (_multiPointMenuJustOpened) return;
  if (!e.target.closest('#multiPointMenu')) hideMultiPointMenu();
});

function multiPointMenuApply(action) {
  const menu = document.getElementById('multiPointMenu');
  const cardId = menu && menu.dataset.cardId;
  hideMultiPointMenu();
  if (!cardId) return;

  const selected = cardCtrlSelected[cardId] || new Set();
  if (selected.size === 0) return;

  if (!cardExclusions[cardId]) cardExclusions[cardId] = new Set();
  const excl = cardExclusions[cardId];
  const w0 = cardLastData[cardId] && cardLastData[cardId].wells && cardLastData[cardId].wells[0];
  const anchorIndices = (w0 && w0.qi_anchor_indices) ? w0.qi_anchor_indices : [];

  if (action === 'exclude') {
    // Add selected points to exclusion set
    selected.forEach(function (idx) {
      if (anchorIndices.includes(idx)) return;
      excl.add(idx);
    });
  } else if (action === 'include') {
    // Remove selected points from exclusion set (add them back to fitting)
    selected.forEach(function (idx) {
      excl.delete(idx);
    });
  } else if (action === 'fitonly') {
    // Fit ONLY these selected points — exclude everything else
    const totalLen = w0 ? w0.x.length : 0;
    excl.clear();
    for (let i = 0; i < totalLen; i++) {
      if (!selected.has(i) && !anchorIndices.includes(i)) {
        excl.add(i);
      }
    }
  } else if (action === 'addfit') {
    // Add a new curve fit to the selected points only
    addCurveFitFromMultiSelect(cardId, selected);
    clearCtrlSelectionHighlights(cardId);
    return; // addCurveFitFromMultiSelect handles everything
  }

  clearCtrlSelectionHighlights(cardId);
  saveZoomState(cardId);
  if (w0 && w0.qi_anchor_indices && w0.qi_anchor_indices.length > 0) {
    refitCurrentData(cardId);
  } else {
    runSingleDCA(cardId);
  }
}

/* Wire up multi-point menu buttons */
(function () {
  const exclBtn = document.getElementById('multiPointMenuExclBtn');
  if (exclBtn) exclBtn.addEventListener('click', function () { multiPointMenuApply('exclude'); });

  const inclBtn = document.getElementById('multiPointMenuInclBtn');
  if (inclBtn) inclBtn.addEventListener('click', function () { multiPointMenuApply('include'); });

  const fitOnlyBtn = document.getElementById('multiPointMenuFitOnlyBtn');
  if (fitOnlyBtn) fitOnlyBtn.addEventListener('click', function () { multiPointMenuApply('fitonly'); });

  const addFitBtn = document.getElementById('multiPointMenuAddFitBtn');
  if (addFitBtn) addFitBtn.addEventListener('click', function () { multiPointMenuApply('addfit'); });

  const clearBtn = document.getElementById('multiPointMenuClearBtn');
  if (clearBtn) clearBtn.addEventListener('click', function () {
    const menu = document.getElementById('multiPointMenu');
    const cardId = menu && menu.dataset.cardId;
    hideMultiPointMenu();
    if (cardId) clearCtrlSelectionHighlights(cardId);
  });
})();

/* --- Set Qi at any point --- */

/* Convert an X coordinate (date timestamp or numeric) to a t value for the decline model */
function xToT(w, xVal) {
  const isDate = w.is_date || false;
  if (isDate) {
    /* Calculate t directly from date difference rather than snapping to the
       nearest existing data point.  t is in days from the first date (t[0]=0). */
    const MS_PER_DAY = 86400000;
    const firstDateMs = parseDateStr(w.x[0]);
    return (xVal - firstDateMs) / MS_PER_DAY + (w.t[0] || 0);
  } else {
    /* Numeric axis: t ≈ xVal - x[0] + t[0] */
    if (w.t.length === 0) return 0;
    return Math.max(0, xVal - w.x[0] + w.t[0]);
  }
}

/* Core logic: add (xClick, yClick) as a new data point, then re-fit the
   decline model to ALL data (including the new point) via /api/fit_inline.
   The old applyQiAtT that only tweaked qi is replaced by a true re-fit. */
async function addPointAndRefit(cardId, tNew, yNew, xDisplayNew) {
  const data = cardLastData[cardId];
  if (!data || !data.wells || data.wells.length !== 1) return;
  const w = data.wells[0];
  if (!w.y_fitted || !w.params || !w.t) return;

  const model = data.model || 'exponential';
  const isDate = w.is_date || false;

  /* ---- 1. Insert the new point into the well data arrays in sorted order ---- */
  let insertIdx = w.t.length; // default: append at end
  for (let i = 0; i < w.t.length; i++) {
    if (tNew < w.t[i]) { insertIdx = i; break; }
  }

  w.t.splice(insertIdx, 0, tNew);
  w.y_actual.splice(insertIdx, 0, yNew);
  w.x.splice(insertIdx, 0, xDisplayNew);
  // y_fitted will be rebuilt after fitting
  w.y_fitted.splice(insertIdx, 0, null);

  /* Adjust excluded indices: shift indices >= insertIdx up by 1 */
  const excl = cardExclusions[cardId] || new Set();
  const shifted = new Set();
  for (const idx of excl) {
    shifted.add(idx >= insertIdx ? idx + 1 : idx);
  }
  cardExclusions[cardId] = shifted;
  w.excluded_indices = [...shifted].sort((a, b) => a - b);

  /* Track anchor (Qi-set) indices — shift existing ones, then add the new one */
  const anchorSet = new Set();
  for (const idx of (w.qi_anchor_indices || [])) {
    anchorSet.add(idx >= insertIdx ? idx + 1 : idx);
  }
  anchorSet.add(insertIdx);
  w.qi_anchor_indices = [...anchorSet];

  /* ---- 2. Build (t, y) arrays for fitting (excluding excluded points) ---- */
  const tFit = [], yFit = [];
  for (let i = 0; i < w.t.length; i++) {
    if (!shifted.has(i)) {
      tFit.push(w.t[i]);
      yFit.push(w.y_actual[i]);
    }
  }

  if (tFit.length < 3) {
    showToast('Not enough data points to fit', 'warning');
    return;
  }

  /* ---- 3. Call /api/fit_inline to re-fit the model ---- */
  const card = document.getElementById(cardId);
  const forecastMonths = parseFloat(card?.querySelector('.p-forecast')?.value || 0);

  try {
    const res = await fetch('/api/fit_inline', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        t: tFit,
        y: yFit,
        model: model,
        forecast_months: forecastMonths,
      }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      showToast(err.detail || 'Re-fit failed', 'error');
      return;
    }
    const fit = await res.json();

    /* ---- 4. Update well data with new fit results ---- */
    w.params = fit.params;
    w.equation = fit.equation || '';

    /* Rebuild full fitted array (including excluded-nulls) using new params */
    const func = (t) => evalDeclineModel(model, t, w.params);
    // Determine first included index
    const nonExcl = [];
    for (let i = 0; i < w.t.length; i++) {
      if (!shifted.has(i)) nonExcl.push(i);
    }
    const firstIncluded = nonExcl.length > 0 ? nonExcl[0] : 0;
    for (let i = 0; i < w.t.length; i++) {
      if (i < firstIncluded || shifted.has(i)) {
        w.y_fitted[i] = null;
      } else {
        w.y_fitted[i] = func(w.t[i]);
      }
    }

    /* Rebuild forecast */
    if (fit.forecast_t && fit.forecast_y) {
      const fX = [];
      if (isDate) {
        const MS_PER_DAY = 86400000;
        const firstDateMs = parseDateStr(w.x[0]);
        // t was built as (date - firstDate)/86400 so firstDateMs corresponds to t[0]
        // but t[0] might not be 0 if data was shifted, so use relation:
        // dateMs = firstDateMs + (t_forecast - t[0]) * MS_PER_DAY
        const tOffset = w.t[0] || 0;
        for (let i = 0; i < fit.forecast_t.length; i++) {
          const ms = firstDateMs + (fit.forecast_t[i] - tOffset) * MS_PER_DAY;
          fX.push(formatDateTs(ms));
        }
      } else {
        const x0 = w.x[0] || 0;
        const t0 = w.t[0] || 0;
        for (let i = 0; i < fit.forecast_t.length; i++) {
          fX.push(fit.forecast_t[i] - t0 + x0);
        }
      }
      w.forecast = {
        x: fX,
        y: fit.forecast_y,
        t: fit.forecast_t,
      };
    }

    /* ---- 5. Re-render charts ---- */
    saveZoomState(cardId);
    renderSingleChart(cardId, data, forecastMonths);

    const fullId = 'fullChart-' + cardId;
    if (chartInstances[fullId]) {
      const fullChart = chartInstances[fullId];
      const origOpts = chartInstances[cardId]?.getOption();
      if (origOpts) {
        fullChart.setOption(origOpts, true);
      }
    }

    /* Persist the updated state to IDB */
    if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();

  } catch (e) {
    showToast('Re-fit request failed: ' + e.message, 'error');
  }
}

/* Re-fit the current in-memory data via /api/fit_inline without a server round-trip.
   Used when exclusions change on a card that has Qi anchor points, so those anchor
   points (which only exist in cardLastData, not in the server dataframe) are preserved. */
async function refitCurrentData(cardId) {
  const data = cardLastData[cardId];
  if (!data || !data.wells || data.wells.length !== 1) { return runSingleDCA(cardId); }
  const w = data.wells[0];
  if (!w.y_actual || !w.t || !w.params) { return runSingleDCA(cardId); }

  const model = data.model || 'exponential';
  const isDate = w.is_date || false;
  const shifted = cardExclusions[cardId] || new Set();
  w.excluded_indices = [...shifted].sort((a, b) => a - b);

  const tFit = [], yFit = [];
  for (let i = 0; i < w.t.length; i++) {
    if (!shifted.has(i)) { tFit.push(w.t[i]); yFit.push(w.y_actual[i]); }
  }

  if (tFit.length < 3) { showToast('Not enough data points to fit', 'warning'); return; }

  const card = document.getElementById(cardId);
  const forecastMonths = parseFloat(card?.querySelector('.p-forecast')?.value || 0);

  try {
    const res = await fetch('/api/fit_inline', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ t: tFit, y: yFit, model, forecast_months: forecastMonths }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      showToast(err.detail || 'Re-fit failed', 'error');
      return;
    }
    const fit = await res.json();

    w.params = fit.params;
    w.equation = fit.equation || '';

    const func = (t) => evalDeclineModel(model, t, w.params);
    const nonExcl = [];
    for (let i = 0; i < w.t.length; i++) { if (!shifted.has(i)) nonExcl.push(i); }
    const firstIncluded = nonExcl.length > 0 ? nonExcl[0] : 0;
    for (let i = 0; i < w.t.length; i++) {
      w.y_fitted[i] = (i < firstIncluded || shifted.has(i)) ? null : func(w.t[i]);
    }

    if (fit.forecast_t && fit.forecast_y) {
      const fX = [];
      if (isDate) {
        const MS_PER_DAY = 86400000;
        const firstDateMs = parseDateStr(w.x[0]);
        const tOffset = w.t[0] || 0;
        for (let i = 0; i < fit.forecast_t.length; i++) {
          fX.push(formatDateTs(firstDateMs + (fit.forecast_t[i] - tOffset) * MS_PER_DAY));
        }
      } else {
        const x0 = w.x[0] || 0, t0 = w.t[0] || 0;
        for (let i = 0; i < fit.forecast_t.length; i++) { fX.push(fit.forecast_t[i] - t0 + x0); }
      }
      w.forecast = { x: fX, y: fit.forecast_y, t: fit.forecast_t };
    }

    saveZoomState(cardId);
    renderSingleChart(cardId, data, forecastMonths);
    if (typeof _debouncedAutoSave === 'function') _debouncedAutoSave();
  } catch (e) {
    showToast('Re-fit failed: ' + e.message, 'error');
  }
}

/* Context menu handler: add the clicked point as data and re-fit all curves */
function setQiAtPoint(cardId) {
  const coord = _ctxMenuCoord;
  if (!coord) return;

  const data = cardLastData[cardId];
  if (!data || !data.wells || data.wells.length !== 1) {
    showToast('Set Qi is only available for single-well charts', 'warning');
    return;
  }
  const w = data.wells[0];
  if (!w.y_fitted || !w.params || !w.t) {
    showToast('No fitted curve to adjust', 'warning');
    return;
  }

  const qiValue = coord.y;
  if (!qiValue || qiValue <= 0) {
    showToast('Qi must be a positive value', 'warning');
    return;
  }

  /* Convert clicked X to t value and display string */
  const tAnchor = xToT(w, coord.x);
  const isDate = w.is_date || false;
  let xDisplay;
  if (isDate) {
    xDisplay = coord.xLabel || formatDateTs(coord.x);
  } else {
    xDisplay = coord.x;
  }

  addPointAndRefit(cardId, tAnchor, qiValue, xDisplay);

  const qAtClick = qiValue.toFixed(2);
  const dateLabel = coord.xLabel || '';
  showToast(`Point added: q(${dateLabel}) = ${qAtClick} — re-fitting…`, 'info', 3000);
}

/* Input widget handler: add a Qi point from the card's input controls and re-fit */
function applyQiFromInput(cardId) {
  const data = cardLastData[cardId];
  if (!data || !data.wells || data.wells.length !== 1) {
    showToast('Set Qi is only available for single-well charts', 'warning');
    return;
  }
  const w = data.wells[0];
  if (!w.y_fitted || !w.params || !w.t) {
    showToast('No fitted curve to adjust', 'warning');
    return;
  }

  const qiInput = document.getElementById('qiValueInput-' + cardId);
  const dateInput = document.getElementById('qiDateInput-' + cardId);
  if (!qiInput) return;

  const qiValue = parseFloat(qiInput.value);
  if (!qiValue || qiValue <= 0 || !isFinite(qiValue)) {
    showToast('Enter a valid positive Qi value', 'warning');
    return;
  }

  const isDate = w.is_date || false;
  let tAnchor = 0;
  let xDisplay;

  if (isDate && dateInput && dateInput.value) {
    const parts = dateInput.value.split('-');
    if (parts.length === 3) {
      const ts = new Date(+parts[0], +parts[1] - 1, +parts[2]).getTime();
      tAnchor = xToT(w, ts);
      xDisplay = formatDateTs(ts);
    } else {
      xDisplay = w.x[0];
    }
  } else if (!isDate && dateInput && dateInput.value) {
    const numX = parseFloat(dateInput.value);
    if (isFinite(numX)) {
      tAnchor = xToT(w, numX);
      xDisplay = numX;
    } else {
      xDisplay = w.x[0];
    }
  } else {
    /* No date provided — anchor at first point */
    xDisplay = isDate ? w.x[0] : (w.x[0] || 0);
  }

  addPointAndRefit(cardId, tAnchor, qiValue, xDisplay);
  showToast(`Point added: ${qiValue.toFixed(2)} at ${dateInput ? dateInput.value || 'start' : 'start'} — re-fitting…`, 'info', 3000);
}


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

    else if (action === 'log-scale-x') toggleLogScaleX(_ctxMenuCardId);

    else if (action === 'set-qi') setQiAtPoint(_ctxMenuCardId);

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

function addAnnotationPrompt(cardId, coord, mouseEvt) {

  const annCoord = coord || _ctxMenuCoord;

  if (!annCoord) return;

  const cx = mouseEvt ? mouseEvt.clientX : _ctxMenuClientX || window.innerWidth / 2;
  const cy = mouseEvt ? mouseEvt.clientY : _ctxMenuClientY || window.innerHeight / 2;

  const input = document.getElementById('annotInlineInput');
  input.value = '';
  input.style.left = cx + 'px';
  input.style.top = cy + 'px';
  input.style.display = 'block';
  input._cardId = cardId;
  input._annCoord = annCoord;
  input.focus();

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

/* Inline annotation input handlers */
(function () {
  const inp = document.getElementById('annotInlineInput');
  if (!inp) return;

  function saveAnnotation() {
    const text = inp.value.trim();
    const cardId = inp._cardId;
    const annCoord = inp._annCoord;
    inp.style.display = 'none';
    inp._cardId = null; inp._annCoord = null;
    if (!text || !cardId || !annCoord) return;
    if (!cardAnnotations[cardId]) cardAnnotations[cardId] = [];
    cardAnnotations[cardId].push({
      id: Date.now() + Math.floor(Math.random() * 1000),
      x: annCoord.x, y: annCoord.y,
      xLabel: annCoord.xLabel, xType: annCoord.xType,
      text, fontSize: 12,
      color: document.documentElement.getAttribute('data-theme') === 'light' ? '#0f172a' : '#e2e8f0'
    });
    reRenderChart(cardId);
  }

  inp.addEventListener('keydown', function (e) {
    if (e.key === 'Enter') { e.preventDefault(); saveAnnotation(); }
    if (e.key === 'Escape') {
      inp.style.display = 'none';
      inp._cardId = null; inp._annCoord = null;
    }
  });

  inp.addEventListener('blur', function () {
    // Small delay so click-away doesn't prevent Enter from firing
    setTimeout(() => {
      if (inp.style.display !== 'none') saveAnnotation();
    }, 150);
  });
})();

/* Close popups on outside click */

document.addEventListener('mousedown', function (e) {

  if (!e.target.closest('#lineColorPopup')) document.getElementById('lineColorPopup').style.display = 'none';

  if (!e.target.closest('#annotationPopup') && !e.target.closest('.mini-chart, .modal-full-view-chart')) document.getElementById('annotationPopup').style.display = 'none';

  if (e.target.id !== 'annotInlineInput') { const ai = document.getElementById('annotInlineInput'); if (ai && ai.style.display !== 'none') { ai.blur(); } }

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

/* --- Feature 5b: Log Scale X-axis (numeric only) --- */

function toggleLogScaleX(cardId) {
  cardLogScaleX[cardId] = !cardLogScaleX[cardId];
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

  if (!chart.containPixel('grid', [px, py])) return;

  const opt = chart.getOption();
  const xType = opt?.xAxis?.[0]?.type;
  let dataCoord;
  if (xType === 'category') dataCoord = chart.convertFromPixel({ seriesIndex: 0 }, [px, py]);
  else dataCoord = chart.convertFromPixel('grid', [px, py]);
  if (!dataCoord) return;
  const catIdx = Math.round(dataCoord[0]);
  const annCoord = {
    x: dataCoord[0],
    y: dataCoord[1],
    xType: xType,
    xLabel: xType === 'category' ? (opt.xAxis[0].data[catIdx] || catIdx) : (xType === 'time' ? formatDateTs(dataCoord[0]) : dataCoord[0])
  };
  addAnnotationPrompt(cardId, annCoord, e);

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
  const opt = chart.getOption() || {};
  const axisOpt = axis === 'x' ? ((opt.xAxis && opt.xAxis[0]) || {}) : ((opt.yAxis && opt.yAxis[0]) || {});
  const isLogAxis = axisOpt && axisOpt.type === 'log';

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

  let mn, mx;
  if (isLogAxis) {
    const safeMin = Math.max(Number(curMin), 1e-12);
    const safeMax = Math.max(Number(curMax), safeMin * 1.000001);
    const logMin = Math.log10(safeMin);
    const logMax = Math.log10(safeMax);
    const logCenter = (logMin + logMax) / 2;
    const logSpan = Math.max((logMax - logMin) * factor, 1e-9);
    mn = Math.pow(10, logCenter - logSpan / 2);
    mx = Math.pow(10, logCenter + logSpan / 2);
  } else {
    const range = curMax - curMin;
    const center = (curMin + curMax) / 2;
    const newRange = range * factor;
    mn = center - newRange / 2;
    mx = center + newRange / 2;
  }

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
  if (axis === 'x') { cardZoomState[cardId].xMin = mn; cardZoomState[cardId].xMax = mx; if (cardAxisAutoFit[cardId]) cardAxisAutoFit[cardId].x = false; }
  else { cardZoomState[cardId].yMin = mn; cardZoomState[cardId].yMax = mx; if (cardAxisAutoFit[cardId]) cardAxisAutoFit[cardId].y = false; }
  setResetZoomButtonsVisible(cardId, true);
}

/* --- Axis toolbar: fit single axis to data --- */
function _axisFit(cardId, axis, chartKey) {
  const activeKey = chartKey || cardId;
  const chart = chartInstances[activeKey], origOpt = cardOptions[cardId];
  if (!chart || !origOpt) return;
  const setObj = {};
  if (axis === 'x') {
    /* For log-scale axes the stored min is a sentinel (0.01) to avoid log(0).
       Pass null so ECharts auto-fits to the actual data range instead. */
    const xIsLog = (origOpt.xAxis && origOpt.xAxis.type === 'log');
    setObj.xAxis = {
      min: xIsLog ? null : (origOpt.xAxis.min || null),
      max: xIsLog ? null : (origOpt.xAxis.max || null)
    };
    if (cardZoomState[cardId]) { delete cardZoomState[cardId].xMin; delete cardZoomState[cardId].xMax; }
    if (!cardAxisAutoFit[cardId]) cardAxisAutoFit[cardId] = {};
    cardAxisAutoFit[cardId].x = true;
  } else {
    /* Same sentinel issue for the Y axis in log mode. */
    const yIsLog = (origOpt.yAxis && origOpt.yAxis.type === 'log');
    setObj.yAxis = {
      min: yIsLog ? null : (origOpt.yAxis.min || null),
      max: yIsLog ? null : (origOpt.yAxis.max || null)
    };
    if (cardZoomState[cardId]) { delete cardZoomState[cardId].yMin; delete cardZoomState[cardId].yMax; }
    if (!cardAxisAutoFit[cardId]) cardAxisAutoFit[cardId] = {};
    cardAxisAutoFit[cardId].y = true;
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
        case 'zoom-in': _axisZoom(cardId, axis, 0.8, chartKey); break;
        case 'zoom-out': _axisZoom(cardId, axis, 1.25, chartKey); break;
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
    if (axis === 'x') { cardZoomState[cardId].xMin = minVal; cardZoomState[cardId].xMax = maxVal; if (cardAxisAutoFit[cardId]) cardAxisAutoFit[cardId].x = false; }
    else { cardZoomState[cardId].yMin = minVal; cardZoomState[cardId].yMax = maxVal; if (cardAxisAutoFit[cardId]) cardAxisAutoFit[cardId].y = false; }
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

    if (data.derived_columns) derivedColumnNames = data.derived_columns.map(d => d.name || d);

    renderEditorTable(data);

    renderEditorPagination(data);

    renderDerivedColumnsPanel();

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

    const isDerived = derivedColumnNames.includes(c);
    const derivedCls = isDerived ? ' th-derived' : '';
    const derivedBadge = isDerived ? '<span class="derived-badge">ƒ</span>' : '';
    html += `<th class="th-sortable${derivedCls}" onclick="handleHeaderStart(event, '${c}', 'editor')" title="${c}${isDerived ? ' (calculated)' : ''}">

      ${derivedBadge}${c} ${filterIcon} ${sortIcon}

      <span class="th-menu" onclick="event.stopPropagation(); showHeaderMenu(event, '${c}', 'editor')">⋮</span>

    </th>`;

  });

  html += '</tr></thead><tbody>';

  data.rows.forEach((r, idx) => {

    const absRow = startRow + idx;

    html += `<tr><td style="color:var(--text-dim)">${absRow}</td>`;

    data.columns.forEach(c => {

      const isDerived = derivedColumnNames.includes(c);
      const cls = isDerived ? ' class="td-derived"' : '';
      html += `<td${cls} contenteditable="true" data-row="${absRow}" data-col="${c}" onblur="handleCellEdit(this)">${r[c] ?? ''}</td>`;

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



/* ====================================================================
   Formula Composer – Calculated Fields (v2)
   ==================================================================== */

let _fcParts = [];            // [{type:'col'|'fn'|'op'|'num'|'text', value:string}]
let _fcPreviewTimer = null;
let _fcAutoComplete = [];     // dropdown suggestions
let _fcAcIndex = -1;          // highlighted autocomplete index
let _fcFnSearch = '';

// ── Function catalogue organised by category ──
const FC_FUNCTIONS = {
  Math: ['ABS', 'ROUND', 'SQRT', 'LOG', 'LOG10', 'POW', 'MOD', 'CEIL', 'FLOOR', 'EXP'],
  Statistical: ['SUM', 'AVG', 'MIN', 'MAX', 'STD', 'VAR', 'MEDIAN', 'COUNT'],
  Logical: ['IF', 'AND', 'OR', 'NOT', 'ISNULL', 'FILLNA'],
};
const FC_ALL_FNS = Object.values(FC_FUNCTIONS).flat();
const FC_FN_DESCRIPTIONS = {
  ABS: 'Absolute value', ROUND: 'Round to N digits', SQRT: 'Square root',
  LOG: 'Natural logarithm', LOG10: 'Base-10 log', POW: 'Raise to power',
  MOD: 'Modulo / remainder', CEIL: 'Round up', FLOOR: 'Round down', EXP: 'e^x',
  SUM: 'Sum of column', AVG: 'Average / mean', MIN: 'Minimum value',
  MAX: 'Maximum value', STD: 'Standard deviation', VAR: 'Variance',
  MEDIAN: 'Median value', COUNT: 'Count non-null',
  IF: 'Conditional: IF(cond, then, else)', AND: 'Logical AND',
  OR: 'Logical OR', NOT: 'Logical NOT', ISNULL: 'Check for null',
  FILLNA: 'Replace null with value',
};

/* ── Open the Composer ── */
function showFormulaBuilder(editFormula, editName) {
  _fcParts = [];
  _fcAutoComplete = [];
  _fcAcIndex = -1;
  _fcFnSearch = '';

  const mc = document.getElementById('modalContainer');

  // Column chips for sidebar
  const colChips = (uploadedColumns || []).map(c => {
    const isNum = (numericColumns || []).includes(c);
    return `<div class="fc-col-chip${isNum ? ' fc-num' : ''}" data-col="${c}"
              onclick="fcInsertColumn('${c.replace(/'/g, "\\'")}')">
              <span class="fc-col-dot ${isNum ? 'num' : 'txt'}"></span>
              <span class="fc-col-name">${c}</span>
              <span class="fc-col-badge">${isNum ? 'Num' : 'Txt'}</span>
            </div>`;
  }).join('');

  // Function list grouped
  let fnGroupsHtml = '';
  for (const [cat, fns] of Object.entries(FC_FUNCTIONS)) {
    const items = fns.map(f =>
      `<div class="fc-fn-row" data-fn="${f}" onclick="fcInsertFunction('${f}')">
         <span class="fc-fn-name">${f}</span>
         <span class="fc-fn-desc">${FC_FN_DESCRIPTIONS[f] || ''}</span>
       </div>`
    ).join('');
    fnGroupsHtml += `<div class="fc-fn-group">
      <div class="fc-fn-cat">${cat}</div>${items}</div>`;
  }

  mc.innerHTML = `
    <div class="modal-overlay fc-overlay" onclick="if(event.target===this)this.remove()">
      <div class="modal fc-modal">

        <!-- Title bar -->
        <div class="fc-titlebar">
          <div class="fc-titlebar-left">
            <span class="fc-icon">ƒ</span>
            <h3>Formula Composer</h3>
          </div>
          <button class="fc-close" onclick="this.closest('.modal-overlay').remove()" title="Close">
            <svg width="14" height="14" viewBox="0 0 14 14"><path d="M1 1l12 12M13 1L1 13" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>
          </button>
        </div>

        <!-- Two-column body -->
        <div class="fc-workspace">

          <!-- LEFT panel ─ columns & functions -->
          <div class="fc-panel">
            <div class="fc-panel-tabs">
              <button class="fc-ptab active" data-tab="cols" onclick="fcSwitchTab('cols')">Columns</button>
              <button class="fc-ptab" data-tab="fns"  onclick="fcSwitchTab('fns')">Functions</button>
            </div>

            <!-- Columns tab -->
            <div class="fc-tab-body" id="fcTabCols">
              <input type="text" class="fc-panel-search" id="fcColSearch"
                     placeholder="Search columns…" oninput="fcFilterCols(this.value)" autocomplete="off">
              <div class="fc-col-list" id="fcColList">${colChips}</div>
            </div>

            <!-- Functions tab -->
            <div class="fc-tab-body" id="fcTabFns" style="display:none">
              <input type="text" class="fc-panel-search" id="fcFnSearch"
                     placeholder="Search functions…" oninput="fcFilterFns(this.value)" autocomplete="off">
              <div class="fc-fn-list" id="fcFnList">${fnGroupsHtml}</div>
            </div>

            <!-- Operators strip -->
            <div class="fc-ops-strip">
              <button onclick="fcInsertOp('+')">+</button>
              <button onclick="fcInsertOp('−')">−</button>
              <button onclick="fcInsertOp('*')">×</button>
              <button onclick="fcInsertOp('/')">÷</button>
              <button onclick="fcInsertOp('**')">^</button>
              <button onclick="fcInsertOp('(')">(</button>
              <button onclick="fcInsertOp(')')">)</button>
            </div>
          </div>

          <!-- RIGHT panel ─ editor + preview -->
          <div class="fc-editor-panel">

            <!-- Column name -->
            <div class="fc-field">
              <label>Column Name</label>
              <input type="text" id="fcColName" value="${editName || ''}"
                     placeholder="e.g. total_production" autocomplete="off" spellcheck="false">
            </div>

            <!-- Tokenised formula editor -->
            <div class="fc-field" style="flex:1;display:flex;flex-direction:column;min-height:0">
              <label>Formula <span class="fc-hint">Type or click to build</span></label>
              <div class="fc-token-editor" id="fcTokenEditor" onclick="document.getElementById('fcInput').focus()">
                <div class="fc-tokens" id="fcTokens"></div>
                <input type="text" id="fcInput" class="fc-input" autocomplete="off" spellcheck="false"
                       placeholder="Start typing…"
                       oninput="fcOnInput(this)" onkeydown="fcOnKeydown(event)">
                <div class="fc-autocomplete" id="fcAutocomplete"></div>
              </div>
              <div class="fc-formula-raw" id="fcFormulaRaw"></div>
            </div>

            <!-- Live sample result -->
            <div class="fc-live-result">
              <div class="fc-live-header">
                <span class="fc-live-dot"></span>
                <span>Live Sample Result</span>
                <span class="fc-live-badge" id="fcLiveStatus">Waiting</span>
              </div>
              <div class="fc-live-body" id="fcLiveBody">
                <span class="fc-live-empty">Compose a formula to see live results</span>
              </div>
              <div class="fc-error" id="fcError"></div>
            </div>
          </div>
        </div>

        <!-- Footer -->
        <div class="fc-footer">
          <button class="btn btn-outline btn-sm" onclick="this.closest('.modal-overlay').remove()">Cancel</button>
          <button class="btn btn-sm fc-save-btn" id="fcSaveBtn" onclick="fcSave()">Create Column</button>
        </div>
      </div>
    </div>`;

  if (editFormula) fcParseExisting(editFormula);
  setTimeout(() => document.getElementById('fcColName')?.focus(), 80);
}

/* ── Panel tab switching ── */
function fcSwitchTab(tab) {
  document.querySelectorAll('.fc-ptab').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  document.getElementById('fcTabCols').style.display = tab === 'cols' ? '' : 'none';
  document.getElementById('fcTabFns').style.display = tab === 'fns' ? '' : 'none';
}

/* ── Column / function search ── */
function fcFilterCols(q) {
  const lc = q.toLowerCase();
  document.querySelectorAll('#fcColList .fc-col-chip').forEach(el => {
    el.style.display = el.dataset.col.toLowerCase().includes(lc) ? '' : 'none';
  });
}
function fcFilterFns(q) {
  const lc = q.toLowerCase();
  document.querySelectorAll('#fcFnList .fc-fn-row').forEach(el => {
    el.style.display = el.dataset.fn.toLowerCase().includes(lc) ? '' : 'none';
  });
  // hide empty group headers
  document.querySelectorAll('#fcFnList .fc-fn-group').forEach(g => {
    const visible = g.querySelectorAll('.fc-fn-row:not([style*="display: none"])').length;
    g.style.display = visible ? '' : 'none';
  });
}

/* ── Token insertion ── */
function fcInsertColumn(col) {
  _fcParts.push({ type: 'col', value: col });
  _fcClearInput();
  fcRender();
}

function fcInsertFunction(fn) {
  _fcParts.push({ type: 'fn', value: fn });
  _fcParts.push({ type: 'op', value: '(' });
  _fcClearInput();
  fcRender();
  // Focus input so cursor is inside parens
  setTimeout(() => document.getElementById('fcInput')?.focus(), 30);
}

function fcInsertOp(op) {
  // Normalise display chars to real operators for the hidden input
  const map = { '−': '-', '×': '*', '÷': '/' };
  _fcParts.push({ type: 'op', value: map[op] || op });
  _fcClearInput();
  fcRender();
}

function _fcClearInput() {
  const inp = document.getElementById('fcInput');
  if (inp) inp.value = '';
  _fcAutoComplete = [];
  _fcAcIndex = -1;
  fcHideAc();
}

function fcRemoveToken(idx) {
  _fcParts.splice(idx, 1);
  fcRender();
}

/* ── Autocomplete while typing ── */
function fcOnInput(inp) {
  const raw = inp.value;
  if (!raw) { fcHideAc(); return; }
  const q = raw.toLowerCase();

  // Build suggestions: columns + functions
  let suggestions = [];
  uploadedColumns.forEach(c => {
    if (c.toLowerCase().includes(q)) suggestions.push({ type: 'col', value: c, label: c });
  });
  FC_ALL_FNS.forEach(f => {
    if (f.toLowerCase().includes(q)) suggestions.push({ type: 'fn', value: f, label: f + '()' });
  });
  if (suggestions.length === 0) { fcHideAc(); return; }
  suggestions = suggestions.slice(0, 8);

  _fcAutoComplete = suggestions;
  _fcAcIndex = 0;
  fcShowAc();
}

function fcShowAc() {
  const el = document.getElementById('fcAutocomplete');
  if (!el || _fcAutoComplete.length === 0) { fcHideAc(); return; }
  el.innerHTML = _fcAutoComplete.map((s, i) => {
    const active = i === _fcAcIndex ? ' fc-ac-active' : '';
    const icon = s.type === 'col' ? '<span class="fc-ac-icon col">⊞</span>'
      : '<span class="fc-ac-icon fn">ƒ</span>';
    return `<div class="fc-ac-item${active}" data-i="${i}"
              onmousedown="fcPickAc(${i})">${icon}<span>${s.label}</span></div>`;
  }).join('');
  el.style.display = 'block';
}

function fcHideAc() {
  const el = document.getElementById('fcAutocomplete');
  if (el) { el.style.display = 'none'; el.innerHTML = ''; }
  _fcAutoComplete = [];
  _fcAcIndex = -1;
}

function fcPickAc(i) {
  const s = _fcAutoComplete[i];
  if (!s) return;
  if (s.type === 'col') fcInsertColumn(s.value);
  else fcInsertFunction(s.value);
  fcHideAc();
}

/* ── Keyboard handling ── */
function fcOnKeydown(e) {
  const inp = e.target;

  // Autocomplete navigation
  if (_fcAutoComplete.length > 0) {
    if (e.key === 'ArrowDown') { e.preventDefault(); _fcAcIndex = Math.min(_fcAcIndex + 1, _fcAutoComplete.length - 1); fcShowAc(); return; }
    if (e.key === 'ArrowUp') { e.preventDefault(); _fcAcIndex = Math.max(_fcAcIndex - 1, 0); fcShowAc(); return; }
    if (e.key === 'Enter' || e.key === 'Tab') { e.preventDefault(); fcPickAc(_fcAcIndex); return; }
    if (e.key === 'Escape') { fcHideAc(); return; }
  }

  // Backspace on empty → remove last token
  if (e.key === 'Backspace' && inp.value === '') {
    e.preventDefault();
    if (_fcParts.length) { _fcParts.pop(); fcRender(); }
    return;
  }

  // Space  → commit literal number / operator
  if (e.key === ' ' || (e.key === 'Enter' && _fcAutoComplete.length === 0)) {
    const val = inp.value.trim();
    if (val) {
      if (!isNaN(val) && val !== '') {
        _fcParts.push({ type: 'num', value: val });
      } else if (['+', '-', '*', '/', '**', '(', ')'].includes(val)) {
        _fcParts.push({ type: 'op', value: val });
      } else {
        const match = uploadedColumns.find(c => c.toLowerCase() === val.toLowerCase());
        if (match) _fcParts.push({ type: 'col', value: match });
        else _fcParts.push({ type: 'text', value: val });
      }
      inp.value = '';
      fcHideAc();
      fcRender();
    }
    if (e.key === 'Enter') e.preventDefault();
  }
}

/* ── Render tokens + raw formula + trigger live preview ── */
function fcRender() {
  const tokensEl = document.getElementById('fcTokens');
  const rawEl = document.getElementById('fcFormulaRaw');
  if (!tokensEl) return;

  const displayMap = { '+': '+', '-': '−', '*': '×', '/': '÷', '**': '^' };

  let html = '';
  _fcParts.forEach((p, i) => {
    let cls = 'fc-pill fc-pill-' + p.type;
    let label = p.value;
    if (p.type === 'col') label = p.value;          // just name, no brackets
    if (p.type === 'op') label = displayMap[p.value] || p.value;
    html += `<span class="${cls}" onclick="fcRemoveToken(${i})" title="Click to remove">
               ${label}<i class="fc-x">×</i>
             </span>`;
  });
  tokensEl.innerHTML = html;

  const formula = fcBuildFormulaBackend();
  if (rawEl) rawEl.innerHTML = formula
    ? `<code>${fcBuildFormulaDisplay()}</code>`
    : '<span class="fc-raw-empty">Your formula will appear here</span>';

  // Live preview (debounced – instant feel)
  fcSchedulePreview();
}

/* ── Build formula strings ── */
function fcBuildFormulaDisplay() {
  return _fcParts.map(p => {
    if (p.type === 'col') return `[${p.value}]`;
    if (p.type === 'fn') return p.value;
    return p.value;
  }).join(' ');
}

function fcBuildFormulaBackend() {
  let parts = [];
  let i = 0;
  while (i < _fcParts.length) {
    const p = _fcParts[i];
    if (p.type === 'fn' && ['SUM', 'AVG', 'MIN', 'MAX', 'STD', 'VAR', 'MEDIAN', 'COUNT'].includes(p.value)) {
      const fn = p.value;
      let j = i + 1;
      while (j < _fcParts.length && _fcParts[j].value !== '(') j++;
      j++; // skip '('
      let depth = 1, inner = [];
      while (j < _fcParts.length && depth > 0) {
        if (_fcParts[j].value === '(') depth++;
        else if (_fcParts[j].value === ')') { depth--; if (depth === 0) break; }
        inner.push(_fcParts[j]);
        j++;
      }
      const colExpr = inner.map(ip => ip.type === 'col' ? '`' + ip.value + '`' : ip.value).join(' ');
      const aggMap = { SUM: 'sum', AVG: 'mean', MIN: 'min', MAX: 'max', STD: 'std', VAR: 'var', MEDIAN: 'median', COUNT: 'count' };
      parts.push(`${colExpr}.${aggMap[fn]}()`);
      i = j + 1;
      continue;
    }
    if (p.type === 'col') parts.push('`' + p.value + '`');
    else if (p.type === 'fn') {
      const m = {
        ABS: 'abs', ROUND: 'round', SQRT: 'sqrt', LOG: 'log', LOG10: 'log10', POW: 'pow',
        MOD: 'mod', CEIL: 'ceil', FLOOR: 'floor', EXP: 'exp',
        IF: 'where', AND: 'and', OR: 'or', NOT: 'not', ISNULL: 'isnull', FILLNA: 'fillna'
      };
      parts.push((m[p.value] || p.value.toLowerCase()));
    }
    else parts.push(p.value);
    i++;
  }
  return parts.join(' ').replace(/\s+/g, ' ').trim();
}

/* ── Live preview (auto-fires 350ms after last edit) ── */
function fcSchedulePreview() {
  clearTimeout(_fcPreviewTimer);
  _fcPreviewTimer = setTimeout(fcRunPreview, 350);
}

async function fcRunPreview() {
  const body = document.getElementById('fcLiveBody');
  const badge = document.getElementById('fcLiveStatus');
  const errDiv = document.getElementById('fcError');
  if (!body) return;

  const formula = fcBuildFormulaBackend();
  if (!formula) {
    body.innerHTML = '<span class="fc-live-empty">Compose a formula to see live results</span>';
    badge.textContent = 'Waiting';
    badge.className = 'fc-live-badge';
    errDiv.textContent = ''; errDiv.style.display = 'none';
    return;
  }

  badge.textContent = 'Computing…';
  badge.className = 'fc-live-badge computing';
  errDiv.textContent = ''; errDiv.style.display = 'none';

  try {
    const res = await fetch('/api/data/formula_preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ formula })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Formula error');

    const vals = data.preview || [];
    let html = '<table class="fc-result-table"><thead><tr><th>Row</th><th>Result</th></tr></thead><tbody>';
    vals.forEach((v, i) => {
      const fmt = (typeof v === 'number') ? v.toLocaleString(undefined, { maximumFractionDigits: 6 }) : String(v);
      html += `<tr><td>${i + 1}</td><td class="fc-result-val">${fmt}</td></tr>`;
    });
    html += '</tbody></table>';
    body.innerHTML = html;

    badge.textContent = 'Valid ✓';
    badge.className = 'fc-live-badge valid';
  } catch (e) {
    body.innerHTML = '<span class="fc-live-empty">—</span>';
    badge.textContent = 'Error';
    badge.className = 'fc-live-badge err';
    errDiv.textContent = e.message;
    errDiv.style.display = 'block';
  }
}

/* ── Save column ── */
async function fcSave() {
  const name = document.getElementById('fcColName')?.value.trim();
  const formula = fcBuildFormulaBackend();
  const errDiv = document.getElementById('fcError');

  if (!name) { _fcShowErr(errDiv, 'Please enter a column name.'); document.getElementById('fcColName')?.focus(); return; }
  if (!formula) { _fcShowErr(errDiv, 'Build a formula first.'); return; }

  const btn = document.getElementById('fcSaveBtn');
  btn.disabled = true; btn.textContent = 'Creating…';

  try {
    const res = await fetch('/api/data/add_column', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, formula })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Error');

    uploadedColumns = data.columns;
    numericColumns = data.numeric_columns;
    derivedColumnNames = (data.derived_columns || []).map(d => d.name || d);
    populateSelectors();
    previewState.columns = data.columns;
    renderPreview();
    loadEditorPage(editorPage);
    renderDerivedColumnsPanel();

    document.querySelector('.modal-overlay')?.remove();
    showNotification(`Calculated field "${name}" created`);
  } catch (e) {
    _fcShowErr(errDiv, e.message);
    btn.disabled = false; btn.textContent = 'Create Column';
  }
}

function _fcShowErr(el, msg) {
  if (!el) return;
  el.textContent = msg;
  el.style.display = 'block';
}

/* ── Re-parse existing formula ── */
function fcParseExisting(formula) {
  const tokens = formula.match(/`[^`]+`|[a-zA-Z_]\w*\(\)|[+\-*/()^]|\d+\.?\d*/g) || [];
  tokens.forEach(t => {
    if (t.startsWith('`') && t.endsWith('`')) _fcParts.push({ type: 'col', value: t.slice(1, -1) });
    else if (t.endsWith('()')) {
      const m = {
        'sum()': 'SUM', 'mean()': 'AVG', 'min()': 'MIN', 'max()': 'MAX', 'abs()': 'ABS', 'sqrt()': 'SQRT',
        'std()': 'STD', 'var()': 'VAR', 'median()': 'MEDIAN', 'count()': 'COUNT'
      };
      _fcParts.push({ type: 'fn', value: m[t.toLowerCase()] || t.slice(0, -2).toUpperCase() });
    } else if (['+', '-', '*', '/', '**', '(', ')'].includes(t)) _fcParts.push({ type: 'op', value: t });
    else if (!isNaN(t)) _fcParts.push({ type: 'num', value: t });
    else _fcParts.push({ type: 'text', value: t });
  });
  fcRender();
}

// Backward-compat aliases
function showAddColumnModal() { showFormulaBuilder(); }
async function doAddColumn() { fcSave(); }


/* ── Calculated Columns Panel ── */
function renderDerivedColumnsPanel() {
  const panel = document.getElementById('derivedColumnsPanel');
  if (!panel) return;

  if (!derivedColumnNames.length) {
    panel.style.display = 'none';
    return;
  }

  panel.style.display = 'block';

  // Fetch full info (with formulas)
  fetch('/api/derived_columns')
    .then(r => r.json())
    .then(data => {
      const cols = data.columns || [];
      if (!cols.length) { panel.style.display = 'none'; return; }

      let html = '<div class="dc-panel-header"><span class="dc-panel-icon">ƒ</span> Calculated Columns <span class="dc-panel-count">' + cols.length + '</span></div>';
      html += '<div class="dc-panel-list">';
      cols.forEach(d => {
        html += `<div class="dc-panel-item">
          <div class="dc-panel-item-info">
            <span class="dc-panel-item-name">${d.name}</span>
            <span class="dc-panel-item-formula">${d.formula}</span>
          </div>
          <button class="dc-panel-item-del" title="Delete column" onclick="doDeleteColumnByName('${d.name.replace(/'/g, "\\'")}')">×</button>
        </div>`;
      });
      html += '</div>';
      panel.innerHTML = html;
    })
    .catch(() => { panel.style.display = 'none'; });
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

  /* Re-use the same comprehensive state collector that IDB auto-save uses */
  const state = _collectWorkspaceState();

  /* Also embed cardLastData so fitted curves, Qi anchors, P10/P90, etc. survive */
  const cardData = {};
  state.cards.forEach(c => {
    if (cardLastData[c.cardId]) {
      cardData[c.cardId] = cardLastData[c.cardId];
    }
  });
  state.cardLastData = cardData;

  /* Include DCA templates in the workspace file */
  state.dcaTemplates = dcaTemplates;

  /* Fetch derived column formulas from the server so they survive export/import */
  const _finishSave = (derivedCols) => {
    if (derivedCols && derivedCols.length) state.derivedColumns = derivedCols;
    const currentPage = pages.find(p => p.id === activePageId);
    const blob = new Blob([JSON.stringify(state)], { type: 'application/json' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `workspace_${currentPage ? currentPage.name.replace(/\s+/g, '_') : 'page'}.dcapro`;
    a.click();
    showNotification('Workspace saved to file');
  };

  fetch('/api/derived_columns')
    .then(r => r.ok ? r.json() : { columns: [] })
    .then(d => _finishSave(d.columns || []))
    .catch(() => _finishSave(null));
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

      /* Embedded chart data (new format) */
      const fileCardData = state.cardLastData || {};

      // Clear existing cards and pages
      document.querySelectorAll('.plot-card').forEach(card => {
        if (chartInstances[card.id]) { chartInstances[card.id].dispose(); delete chartInstances[card.id]; }
      });
      document.querySelectorAll('.card-wrap').forEach(w => w.remove());
      document.querySelectorAll('.plot-card').forEach(card => card.remove());
      document.querySelectorAll('.page-columns-card').forEach(el => el.remove());

      pages.length = 0;

      // Support old "workspaces" format for backward compatibility

      const savedPages = state.pages || state.workspaces || [];

      if (savedPages.length > 0) {

        for (const pg of savedPages) {
          pages.push({ id: pg.id, name: pg.name });
        }

      } else {

        const legacyId = 'page-legacy';
        pages.push({ id: legacyId, name: 'Page 1' });

      }

      activePageId = state.activePageId || state.activeWsId || pages[0].id;
      if (!pages.some(p => p.id === activePageId)) activePageId = pages[0].id;

      // Restore theme if saved
      if (state.theme) {
        document.documentElement.setAttribute('data-theme', state.theme);
        const tb = document.getElementById('themeToggle');
        if (tb) tb.textContent = state.theme === 'light' ? '\u2600\uFE0F Light' : '\uD83C\uDF19 Dark';
      }

      renderPageTabs();

      // Check if server has data
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
        await _restoreImportTab();

        /* Re-apply derived columns (calculated fields) if stored in the file */
        if (state.derivedColumns && state.derivedColumns.length) {
          for (const dc of state.derivedColumns) {
            try {
              await fetch('/api/data/add_column', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: dc.name, formula: dc.formula }),
              });
            } catch (e) { console.warn('Could not restore derived column:', dc.name, e); }
          }
          /* Refresh column lists after adding derived columns */
          try {
            const res = await fetch('/api/columns');
            if (res.ok) {
              const colData = await res.json();
              uploadedColumns = colData.columns || [];
              numericColumns = colData.numeric_columns || [];
            }
          } catch (e) { /* ignore */ }
        }
      } else if (state.uploadedColumns) {
        uploadedColumns = state.uploadedColumns;
        numericColumns = state.numericColumns || [];
        allWells = state.allWells || [];
        populateSelectors();
      }

      // Recreate cards with full state

      for (const cs of (state.cards || [])) {

        const savedPage = activePageId;

        activePageId = cs.pageId || cs.page || cs.workspace || pages[0].id;

        // Determine per-card columns (backward compat: fall back to page/global)
        const cardSelX = cs.selX || state.selX || '';
        const cardSelY = cs.selY || state.selY || '';
        const cardSelWellCol = cs.selWellCol || state.selWellCol || '';

        // Fetch wells for this card's well column
        if (cardSelWellCol && serverHasData) {
          try {
            const res = await fetch(`/api/wells?well_col=${encodeURIComponent(cardSelWellCol)}`);
            const data = await res.json();
            allWells = data.wells || [];
          } catch (e) { allWells = state.allWells || []; }
        } else {
          allWells = state.allWells || [];
        }

        const newId = addPlotCard(cs.wells || cs.well, cs.model, cs.forecast, cs.title, cs.combine || false, cs.header || '', cs.combineAgg || 'sum', cardSelX, cardSelY, cardSelWellCol);

        const cardEl = document.getElementById(newId);
        if (cardEl) cardEl.dataset.page = cs.pageId || cs.page || activePageId;

        /* Restore all per-card state */
        if (cs.exclusions?.length) cardExclusions[newId] = new Set(cs.exclusions);
        if (cs.userLines?.length) cardUserLines[newId] = cs.userLines;
        if (cs.annotations?.length) cardAnnotations[newId] = cs.annotations;
        if (cs.valueLabels && cs.valueLabels !== 'none') cardValueLabels[newId] = cs.valueLabels;
        if (cs.logScale) cardLogScale[newId] = true;
        if (cs.logScaleX) cardLogScaleX[newId] = true;
        if (cs.pctChange) cardPctChange[newId] = true;
        if (cs.axisLabels) cardAxisLabels[newId] = cs.axisLabels;
        if (cs.axisPositions) cardAxisPositions[newId] = cs.axisPositions;
        if (cs.headers) cardHeaders[newId] = cs.headers;
        if (cs.zoomState || cs.zoom) cardZoomState[newId] = cs.zoomState || cs.zoom;
        if (cs.pCurveState) cardPCurveState[newId] = cs.pCurveState;
        if (cs.multiFits && cs.multiFits.length > 0) {
          cardMultiFits[newId] = cs.multiFits;
          cs.multiFits.forEach(mf => { if (mf.id >= _multiFitNextId) _multiFitNextId = mf.id + 1; });
        }
        if (cs.hiddenSeries && cs.hiddenSeries.length > 0) cardHiddenSeries[newId] = new Set(cs.hiddenSeries);

        if (cs.styles) {
          const mergedStyles = { ...getDefaultStyles(), ...cs.styles };
          cardStyles[newId] = mergedStyles;
          applyStylesToCard(newId, mergedStyles);
          const headerEl = cardEl?._headerInput || cardEl?.querySelector('.p-header');
          if (headerEl && mergedStyles.headerFontSize) {
            headerEl.style.fontSize = mergedStyles.headerFontSize + 'rem';
            headerEl.style.color = mergedStyles.headerColor || '';
          }
        }

        /* Restore chart data from file if available, otherwise re-run DCA */
        const savedData = fileCardData[cs.cardId];
        if (savedData) {
          cardLastData[newId] = savedData;
          rebuildCurveStyleSections(newId);
          try {
            renderSingleChart(newId, savedData, cs.forecast);
          } catch (e) {
            console.warn('Failed to render saved chart for', newId, e);
            if (serverHasData) await runSingleDCA(newId);
          }
        } else if (serverHasData) {
          await runSingleDCA(newId);
        }

        activePageId = savedPage;
      }

      switchPage(activePageId);

      /* Restore DCA templates from file */
      if (state.dcaTemplates && Array.isArray(state.dcaTemplates)) {
        dcaTemplates.length = 0;
        state.dcaTemplates.forEach(t => dcaTemplates.push(t));
        _saveDcaTemplatesToIDB();
        renderTemplateSidebar();
      }

      showNotification('Workspace loaded');

      // Switch to DCA tab

      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));

      document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));

      document.querySelector('[data-tab="dca"]').classList.add('active');

      document.getElementById('tab-dca').classList.add('active');

      /* Trigger auto-save so the loaded state is persisted to IDB */
      _debouncedAutoSave();

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



function populateWellPicker(cardId, selectedWells, customWellsList) {

  const wp = document.getElementById('wp-' + cardId);

  if (!wp) return;

  const list = wp.querySelector('.well-picker-list');

  list.innerHTML = '';

  const wells = customWellsList || allWells;

  wells.forEach(w => {

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

  // Temporarily show off-screen to measure
  menu.style.left = '-9999px';
  menu.style.top = '-9999px';
  menu.classList.add('show');

  const mw = menu.offsetWidth;
  const mh = menu.offsetHeight;
  const vw = window.innerWidth;
  const vh = window.innerHeight;

  let x = e.clientX;
  let y = e.clientY;

  // Prevent overflow on right
  if (x + mw > vw - 8) x = vw - mw - 8;
  // Prevent overflow on bottom
  if (y + mh > vh - 8) y = vh - mh - 8;
  // Prevent negative
  if (x < 4) x = 4;
  if (y < 4) y = 4;

  menu.style.left = x + 'px';
  menu.style.top = y + 'px';

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
      if (confirm(`Remove column "${col}" from the dataset? This action cannot be undone.`)) {
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
    derivedColumnNames = (data.derived_columns || []).map(d => d.name || d);
    populateSelectors();

    // Sync preview state columns
    previewState.columns = data.columns;

    // Reset sort/filter if the deleted column was being used
    if (previewState.sortCol === col) previewState.sortCol = null;
    if (previewState.filterCol === col) {
      previewState.filterCol = null;
      previewState.filterVal = '';
    }

    // Refresh the preview view completely
    renderPreview();

    loadEditorPage(editorPage);
    renderDerivedColumnsPanel();
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

  const filterState = cardTableFilter[cardId] || { well: '', section: '' };

  // Apply filters

  const wellQ = (filterState.well || '').trim().toLowerCase();

  const sectionQ = filterState.section || '';

  const filtered = rows.filter(r => {

    if (wellQ && !String(r.well).toLowerCase().includes(wellQ)) return false;

    if (sectionQ && r.section !== sectionQ) return false;

    return true;

  });

  // Update row count badge

  const countEl = document.getElementById('dtRowCount-' + cardId);

  if (countEl) {

    const isFiltered = wellQ || sectionQ;

    countEl.textContent = isFiltered ? `${filtered.length} / ${rows.length} rows` : `${rows.length} rows`;

    countEl.style.color = isFiltered ? 'var(--accent)' : 'var(--text-muted)';

  }

  // Sort rows if a sort column is set

  const sorted = [...filtered];

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

function filterTable(cardId) {

  const wellEl = document.getElementById('dtFilterWell-' + cardId);

  const secEl = document.getElementById('dtFilterSection-' + cardId);

  cardTableFilter[cardId] = {

    well: wellEl ? wellEl.value : '',

    section: secEl ? secEl.value : ''

  };

  renderSortableTable(cardId);

}

function clearTableFilter(cardId) {

  const wellEl = document.getElementById('dtFilterWell-' + cardId);

  const secEl = document.getElementById('dtFilterSection-' + cardId);

  if (wellEl) wellEl.value = '';

  if (secEl) secEl.value = '';

  cardTableFilter[cardId] = { well: '', section: '' };

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
              label: Object.assign({}, (s.markLine || {}).label, { formatter: line.name, color: line.color, position: line.type === 'h' ? 'insideStartTop' : 'insideEndTop' }),
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
          label: Object.assign({}, (s.markLine || {}).label, { formatter: line.name, color: line.color, position: line.type === 'h' ? 'insideStartTop' : 'insideEndTop' }),
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
      selX: card.querySelector('.p-selX')?.value || '',
      selY: card.querySelector('.p-selY')?.value || '',
      selWellCol: card.querySelector('.p-selWellCol')?.value || '',
      model: card.querySelector('.p-model')?.value || 'exponential',
      forecast: card.querySelector('.p-forecast')?.value || '0',
      title: card.querySelector('.p-title')?.value || '',
      header: (card._headerInput || card.querySelector('.p-header'))?.value || '',
      combine: card.querySelector('.p-combine')?.checked || false,
      combineAgg: card.querySelector('.p-combine-agg')?.value || 'sum',
      styles: cardStyles[cid] || null,
      exclusions: cardExclusions[cid] ? [...cardExclusions[cid]] : [],
      userLines: cardUserLines[cid] || [],
      annotations: cardAnnotations[cid] || [],
      valueLabels: cardValueLabels[cid] || 'none',
      logScale: cardLogScale[cid] || false,
      logScaleX: cardLogScaleX[cid] || false,
      pctChange: cardPctChange[cid] || false,
      axisLabels: cardAxisLabels[cid] || null,
      axisPositions: cardAxisPositions[cid] || null,
      headers: cardHeaders[cid] || '',
      zoomState: cardZoomState[cid] || null,
      pCurveState: cardPCurveState[cid] || null,
      multiFits: cardMultiFits[cid] || null,
      hiddenSeries: cardHiddenSeries[cid] ? [...cardHiddenSeries[cid]] : [],
      hasPlot: (card.querySelector('.chart-area')?.style.display !== 'none'),
    });
  });

  return {
    version: 2,
    savedAt: Date.now(),
    selX: '',
    selY: '',
    selWellCol: '',
    uploadedColumns: uploadedColumns,
    numericColumns: numericColumns,
    allWells: allWells,
    activeDatasetId: activeDatasetId,
    pages: pages.map(p => {
      return { id: p.id, name: p.name };
    }),
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
    const hasCards = state && state.cards && state.cards.length > 0;

    // Check if the server still has data loaded (always, even without saved cards)
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
    } else if (hasCards) {
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

    if (!hasCards) {
      console.log('No saved cards found.');
      return serverHasData;  // still return true if we restored the data tab
    }

    // Restore theme
    if (state.theme) {
      document.documentElement.setAttribute('data-theme', state.theme);
      const tb = document.getElementById('themeToggle');
      if (tb) tb.textContent = state.theme === 'light' ? '\u2600\uFE0F Light' : '\uD83C\uDF19 Dark';
    }

    // Remove existing page column UIs
    document.querySelectorAll('.page-columns-card').forEach(el => el.remove());

    // Restore pages (columns are now per-card, not per-page)
    pages.length = 0;
    const savedPages = state.pages || [];
    if (savedPages.length > 0) {
      for (const p of savedPages) {
        pages.push({ id: p.id, name: p.name });
      }
    }
    if (pages.length === 0) {
      const defId = 'page-default';
      pages.push({ id: defId, name: 'Page 1' });
    }
    // Ensure activePageId matches an existing page
    const candidateId = state.activePageId || pages[0].id;
    activePageId = pages.some(p => p.id === candidateId) ? candidateId : pages[0].id;
    renderPageTabs();

    // Remove existing cards
    document.querySelectorAll('.plot-card').forEach(card => {
      if (chartInstances[card.id]) { chartInstances[card.id].dispose(); delete chartInstances[card.id]; }
    });
    document.querySelectorAll('.card-wrap').forEach(w => w.remove());
    document.querySelectorAll('.plot-card').forEach(card => card.remove());

    // Restore cards with per-card column selections
    for (const c of state.cards) {
      const cardPageId = c.pageId || activePageId;
      const savedActive = activePageId;
      activePageId = cardPageId;

      // Determine per-card column selections (backward compat: fall back to page/global columns)
      const cardSelX = c.selX || (state.pages?.find(p => p.id === cardPageId)?.selX) || state.selX || '';
      const cardSelY = c.selY || (state.pages?.find(p => p.id === cardPageId)?.selY) || state.selY || '';
      const cardSelWellCol = c.selWellCol || (state.pages?.find(p => p.id === cardPageId)?.selWellCol) || state.selWellCol || '';

      // Check if we should skip this card (only if it was an empty/non-plotted config card)
      if (c.hasPlot === false) {
        continue;
      }

      // Fetch wells for this card's well column if needed
      if (cardSelWellCol && serverHasData) {
        try {
          const res = await fetch(`/api/wells?well_col=${encodeURIComponent(cardSelWellCol)}`);
          const data = await res.json();
          allWells = data.wells || [];
        } catch (e) { allWells = state.allWells || []; }
      } else {
        allWells = state.allWells || [];
      }

      const cardId = addPlotCard(c.wells, c.model, c.forecast, c.title, c.combine, c.header, c.combineAgg || 'sum', cardSelX, cardSelY, cardSelWellCol);

      const cardEl = document.getElementById(cardId);
      if (cardEl) cardEl.dataset.page = cardPageId;
      activePageId = savedActive;

      if (c.exclusions?.length) cardExclusions[cardId] = new Set(c.exclusions);
      if (c.userLines?.length) cardUserLines[cardId] = c.userLines;
      if (c.annotations?.length) cardAnnotations[cardId] = c.annotations;
      if (c.valueLabels && c.valueLabels !== 'none') cardValueLabels[cardId] = c.valueLabels;
      if (c.logScale) cardLogScale[cardId] = true;
      if (c.logScaleX) cardLogScaleX[cardId] = true;
      if (c.pctChange) cardPctChange[cardId] = true;
      if (c.axisLabels) cardAxisLabels[cardId] = c.axisLabels;
      if (c.axisPositions) cardAxisPositions[cardId] = c.axisPositions;
      if (c.headers) cardHeaders[cardId] = c.headers;
      if (c.zoomState) cardZoomState[cardId] = c.zoomState;
      if (c.pCurveState) cardPCurveState[cardId] = c.pCurveState;
      if (c.multiFits && c.multiFits.length > 0) {
        cardMultiFits[cardId] = c.multiFits;
        // Ensure _multiFitNextId stays above any restored IDs
        c.multiFits.forEach(mf => { if (mf.id >= _multiFitNextId) _multiFitNextId = mf.id + 1; });
      }
      if (c.hiddenSeries && c.hiddenSeries.length > 0) cardHiddenSeries[cardId] = new Set(c.hiddenSeries);

      if (c.styles) {
        const mergedStyles = { ...getDefaultStyles(), ...c.styles };
        cardStyles[cardId] = mergedStyles;
        applyStylesToCard(cardId, mergedStyles);
        const headerEl = cardEl?._headerInput || cardEl?.querySelector('.p-header');
        if (headerEl && mergedStyles.headerFontSize) {
          headerEl.style.fontSize = mergedStyles.headerFontSize + 'rem';
          headerEl.style.color = mergedStyles.headerColor || '';
        }
      }

      const savedData = cardData[c.cardId];
      if (savedData && serverHasData) {
        cardLastData[cardId] = savedData;
        rebuildCurveStyleSections(cardId);
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

// Per-card column selectors auto-save is handled in addPlotCard change handlers

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
    // Load DCA templates from IDB
    try {
      await _loadDcaTemplatesFromIDB();
    } catch (e) {
      console.warn('Failed to load DCA templates:', e);
    }
  }, 500);
});

