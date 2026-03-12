import io
import json
import os
import shutil
import time
import uuid
import threading
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.optimize import curve_fit
from fastapi import FastAPI, File, UploadFile, HTTPException, Query, Body
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="DCA Pro – Decline Curve Analysis")
app.mount("/static", StaticFiles(directory="static"), name="static")

# ---------------------------------------------------------------------------
# Storage & Dataset Registry
# ---------------------------------------------------------------------------
STORAGE_DIR = Path(__file__).parent / "data" / "storage"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

_datasets: dict = {}   # dataset_id -> {status, filename, suffix, raw_path, parquet_path, error, progress, ...}
_active_dataset_id: Optional[str] = None
_worker_pool = ThreadPoolExecutor(max_workers=2)

# Version control: dataset_id -> list of version dicts (newest last)
# Each version: {version, parquet_path, timestamp, rows, columns, status}
_versions: dict = {}
MAX_VERSIONS = 50  # keep last N versions per dataset
_active_version: int = 0   # which version is currently loaded (0 = latest)

# Derived columns / transformations registry (for pipeline replay)
# dataset_id -> [{"name": "col", "formula": "expr"}, ...]
_derived_columns: dict = {}

# ---------------------------------------------------------------------------
# In-memory store (kept for backward compat with existing endpoints)
# ---------------------------------------------------------------------------
_current_df: Optional[pd.DataFrame] = None
_current_filename: str = ""
_date_columns: list = []   # columns auto-detected as dates
_current_file_bytes: bytes = b""
_current_file_suffix: str = ""
_file_disk_path: Optional[str] = None   # path on disk for auto-reload
_file_last_modified: float = 0
_last_import_timestamp: Optional[str] = None  # ISO timestamp of last import

_META_FILE = STORAGE_DIR / "_meta.json"


def _save_active_meta():
    """Persist the active dataset metadata so the server can reload it on restart."""
    meta = {
        "active_dataset_id": _active_dataset_id,
        "filename": _current_filename,
        "date_columns": _date_columns,
        "last_import_timestamp": _last_import_timestamp,
        "file_disk_path": _file_disk_path,
        "active_version": _active_version,
        "derived_columns": _derived_columns,
    }
    try:
        _META_FILE.write_text(json.dumps(meta, indent=2))
    except Exception as e:
        print(f"Warning: Could not save active meta: {e}")


def _load_active_dataset_on_startup():
    """Reload the last active dataset from disk when the server starts."""
    global _current_df, _current_filename, _date_columns
    global _current_file_bytes, _current_file_suffix, _file_disk_path
    global _active_dataset_id, _last_import_timestamp, _file_last_modified
    global _active_version, _derived_columns

    if not _META_FILE.exists():
        return

    try:
        meta = json.loads(_META_FILE.read_text())
    except Exception:
        return

    ds_id = meta.get("active_dataset_id")
    if not ds_id:
        return

    ds_dir = STORAGE_DIR / ds_id
    parquet_path = ds_dir / "data.parquet"
    if not parquet_path.exists():
        return

    # Find the raw file to recover bytes and suffix
    raw_files = [f for f in ds_dir.iterdir() if f.name.startswith("raw")]
    suffix = ".csv"
    raw_bytes = b""
    if raw_files:
        suffix = raw_files[0].suffix
        raw_bytes = raw_files[0].read_bytes()

    try:
        table = pq.read_table(str(parquet_path))
        df = table.to_pandas()

        # Re-detect date columns from meta
        date_columns = meta.get("date_columns", [])
        for col in date_columns:
            if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors='coerce')

        _current_df = df
        _current_filename = meta.get("filename", "restored.csv")
        _date_columns = date_columns
        _current_file_bytes = raw_bytes
        _current_file_suffix = suffix
        _active_dataset_id = ds_id
        _last_import_timestamp = meta.get("last_import_timestamp")
        _active_version = meta.get("active_version", 0)
        _file_disk_path = meta.get("file_disk_path")
        if _file_disk_path and Path(_file_disk_path).exists():
            _file_last_modified = Path(_file_disk_path).stat().st_mtime
        else:
            _file_last_modified = 0

        # Restore derived column formulas from meta
        saved_derived = meta.get("derived_columns", {})
        if saved_derived:
            _derived_columns.update(saved_derived)

        # Rebuild _datasets registry entry
        _datasets[ds_id] = {
            "status": "ready",
            "filename": _current_filename,
            "suffix": suffix,
            "file_size": len(raw_bytes),
            "raw_path": str(raw_files[0]) if raw_files else "",
            "parquet_path": str(parquet_path),
            "error": None,
            "progress": 100,
            "bytes_received": len(raw_bytes),
            "rows": len(df),
            "columns": list(df.columns),
            "numeric_columns": list(df.select_dtypes(include="number").columns),
            "date_columns": date_columns,
        }

        # Rebuild version entries from existing version files
        ver_files = sorted(ds_dir.glob("data_v*.parquet"))
        _versions[ds_id] = []
        for vf in ver_files:
            try:
                ver_num = int(vf.stem.replace("data_v", ""))
                vt = pq.read_table(str(vf))
                _versions[ds_id].append({
                    "version": ver_num,
                    "parquet_path": str(vf),
                    "timestamp": datetime.fromtimestamp(vf.stat().st_mtime, tz=timezone.utc).isoformat(),
                    "rows": vt.num_rows,
                    "columns": vt.column_names,
                    "status": "ok",
                })
            except Exception:
                pass

        print(f"Restored dataset '{_current_filename}' ({len(df)} rows) from {ds_id}")

        # Cleanup orphan folders from previous sessions
        for child in STORAGE_DIR.iterdir():
            if child.is_dir() and child.name != ds_id:
                try:
                    shutil.rmtree(child)
                    print(f"  Cleaned up orphan folder: {child.name}")
                except Exception:
                    pass

    except Exception as e:
        print(f"Warning: Failed to restore dataset on startup: {e}")


# Run on module load
_load_active_dataset_on_startup()


def _persist_current_df():
    """Write the current in-memory DataFrame back to the active dataset's parquet file."""
    if _current_df is None or not _active_dataset_id:
        return
    ds_dir = STORAGE_DIR / _active_dataset_id
    parquet_path = ds_dir / "data.parquet"
    if not ds_dir.exists():
        return
    try:
        export_df = _current_df.copy()
        for col in _date_columns:
            if col in export_df.columns and pd.api.types.is_datetime64_any_dtype(export_df[col]):
                export_df[col] = export_df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna("")
        table = pa.Table.from_pandas(export_df, preserve_index=False)
        pq.write_table(table, str(parquet_path), compression='snappy')
    except Exception as e:
        print(f"Warning: Could not persist DataFrame to parquet: {e}")


def _parse_data(raw_bytes: bytes, suffix: str, header_row: int = None):
    """Parse raw file bytes and auto-detect date columns (dayfirst=True)."""
    if header_row is None:
        try:
            if suffix == ".csv":
                df_temp = pd.read_csv(io.BytesIO(raw_bytes), header=None, nrows=30)
            elif suffix in (".xlsx", ".xls"):
                df_temp = pd.read_excel(io.BytesIO(raw_bytes), engine="openpyxl", header=None, nrows=30)
            else:
                raise HTTPException(status_code=400, detail="Unsupported file type.")
            
            best_row = 0
            max_valid = 0
            for i in range(len(df_temp)):
                valid_count = df_temp.iloc[i].notna().sum()
                if valid_count > max_valid:
                    max_valid = valid_count
                    best_row = i
            header_row = best_row
        except Exception:
            header_row = 0
            
    if suffix == ".csv":
        df = pd.read_csv(io.BytesIO(raw_bytes), header=header_row)
    elif suffix in (".xlsx", ".xls"):
        df = pd.read_excel(io.BytesIO(raw_bytes), engine="openpyxl", header=header_row)
    else:
        raise HTTPException(status_code=400, detail="Unsupported file type.")
    
    # Ensure all column names are strings
    df.columns = df.columns.astype(str)

    detected_dates = []
    for col in df.columns:
        if df[col].dtype == "object":
            sample = df[col].dropna().head(20)
            if len(sample) == 0:
                continue
            first_val = str(sample.iloc[0])
            if any(sep in first_val for sep in ['.', '/', '-']) and len(first_val) >= 6:
                try:
                    parsed = pd.to_datetime(sample, dayfirst=True, errors='coerce')
                    if parsed.notna().sum() >= len(sample) * 0.8:
                        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
                        detected_dates.append(col)
                except Exception:
                    pass

    return df, detected_dates


def _build_upload_response():
    """Build the standard JSON response with data summary."""
    preview_df = _current_df.head(100).copy()
    for col in _date_columns:
        if col in preview_df.columns and pd.api.types.is_datetime64_any_dtype(preview_df[col]):
            preview_df[col] = preview_df[col].dt.strftime('%d.%m.%Y').fillna("")
    return JSONResponse({
        "filename": _current_filename,
        "rows": len(_current_df),
        "columns": list(_current_df.columns),
        "numeric_columns": list(_current_df.select_dtypes(include="number").columns),
        "date_columns": _date_columns,
        "preview": preview_df.fillna("").to_dict(orient="records"),
        "has_disk_path": _file_disk_path is not None,
        "last_import": _last_import_timestamp,
        "dataset_id": _active_dataset_id,
        "version": _get_current_version_number(),
        "derived_columns": _derived_columns.get(_active_dataset_id, []) if _active_dataset_id else [],
    })


def _get_current_version_number():
    """Return the current version number for the active dataset."""
    if not _active_dataset_id or _active_dataset_id not in _versions:
        return 0
    if _active_version > 0:
        return _active_version
    return len(_versions[_active_dataset_id])


def _save_version_snapshot(dataset_id: str, df: pd.DataFrame, date_columns: list):
    """Save a versioned copy of the Parquet file and record metadata."""
    global _active_version
    ds = _datasets.get(dataset_id)
    if not ds:
        return

    ds_dir = STORAGE_DIR / dataset_id
    ver_num = len(_versions.get(dataset_id, [])) + 1
    ts = datetime.now(timezone.utc).isoformat()

    # Copy current parquet to versioned file
    ver_parquet = ds_dir / f"data_v{ver_num}.parquet"
    current_parquet = ds_dir / "data.parquet"
    if current_parquet.exists():
        shutil.copy2(str(current_parquet), str(ver_parquet))
    else:
        # Write fresh
        export_df = df.copy()
        for col in date_columns:
            if col in export_df.columns and pd.api.types.is_datetime64_any_dtype(export_df[col]):
                export_df[col] = export_df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna("")
        table = pa.Table.from_pandas(export_df, preserve_index=False)
        pq.write_table(table, str(ver_parquet), compression='snappy')

    ver_entry = {
        "version": ver_num,
        "parquet_path": str(ver_parquet),
        "timestamp": ts,
        "rows": len(df),
        "columns": list(df.columns),
        "status": "ok",
    }

    if dataset_id not in _versions:
        _versions[dataset_id] = []
    _versions[dataset_id].append(ver_entry)
    _active_version = ver_num  # new snapshot is always the current version

    # Prune old versions
    while len(_versions[dataset_id]) > MAX_VERSIONS:
        old = _versions[dataset_id].pop(0)
        old_path = Path(old["parquet_path"])
        if old_path.exists():
            try:
                old_path.unlink()
            except Exception:
                pass

    return ver_entry


def _replay_derived_columns(dataset_id: str, df: pd.DataFrame):
    """Re-apply all registered derived columns to a new DataFrame.
    Returns (df, errors) where errors is a list of failed column names."""
    errors = []
    derivations = _derived_columns.get(dataset_id, [])
    for d in derivations:
        try:
            df[d["name"]] = df.eval(d["formula"])
            if pd.api.types.is_numeric_dtype(df[d["name"]]):
                df[d["name"]] = df[d["name"]].round(4)
        except Exception as e:
            errors.append({"name": d["name"], "error": str(e)})
    return df, errors


# ---------------------------------------------------------------------------
# Decline-curve models
# ---------------------------------------------------------------------------
def _exponential(t, qi, di):
    """q(t) = qi * exp(-di * t)"""
    return qi * np.exp(-di * t)


def _hyperbolic(t, qi, di, b):
    """q(t) = qi / (1 + b*di*t)^(1/b)"""
    denom = 1.0 + b * di * t
    # guard against negative base before fractional power
    denom = np.maximum(denom, 1e-12)
    return qi / np.power(denom, 1.0 / b)


def _harmonic(t, qi, di):
    """q(t) = qi / (1 + di*t)  (hyperbolic with b=1)"""
    return qi / (1.0 + di * t)


_MODELS = {
    "exponential": (_exponential, ["qi", "di"],      [100, 0.01],  (0, [1e8, 10]), "q(t) = {qi} * exp(-{di} * t)"),
    "hyperbolic":  (_hyperbolic,  ["qi", "di", "b"], [100, 0.01, 0.5], (0, [1e8, 10, 2]), "q(t) = {qi} / (1 + {b}*{di}*t)^(1/{b})"),
    "harmonic":    (_harmonic,    ["qi", "di"],      [100, 0.01],  (0, [1e8, 10]), "q(t) = {qi} / (1 + {di}*t)"),
}


def _fit_decline(t: np.ndarray, q: np.ndarray, model_name: str):
    """Fit a decline curve. Returns params_dict (empty dict if failed)."""
    func, param_names, p0, bounds, eq_fmt = _MODELS[model_name]

    # Smart initial guess: qi ≈ max production
    p0 = list(p0)
    p0[0] = float(np.nanmax(q)) if np.nanmax(q) > 0 else 100.0

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            popt, _ = curve_fit(func, t, q, p0=p0, bounds=bounds, maxfev=10000)
        except Exception:
            return {}

    def safe_float(v):
        if np.isfinite(v):
            return round(float(v), 12)
        return 0.0

    result = {name: safe_float(val) for name, val in zip(param_names, popt)}
    
    try:
        y_pred = func(t, *popt)
        ss_res = np.sum((q - y_pred) ** 2)
        ss_tot = np.sum((q - np.mean(q)) ** 2)
        
        # For nearly flat horizontal lines with tiny variance (std < 2% of mean), 
        # standard centered R^2 naturally drives to 0.0 even on perfect fits.
        # Fall back to uncentered R^2 calculation for highly flat clustered data.
        mean_q = float(np.mean(q))
        if mean_q > 0 and (np.var(q) / (mean_q ** 2)) < 4e-4:
            ss_tot_unc = np.sum(q ** 2)
            if ss_tot_unc > 1e-12:
                result["r2"] = max(0.0, safe_float(1 - (ss_res / ss_tot_unc)))
            else:
                result["r2"] = 0.0
        else:
            if ss_tot > 1e-12:
                r2_val = 1 - (ss_res / ss_tot)
                result["r2"] = max(0.0, safe_float(r2_val))
            else:
                if ss_res <= 1e-12:
                    result["r2"] = 1.0
                else:
                    result["r2"] = 0.0

    except Exception:
        result["r2"] = 0.0

    return result


# ---------------------------------------------------------------------------
# Chunked Upload System
# ---------------------------------------------------------------------------
CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB default; frontend can send smaller


class UploadInitRequest(BaseModel):
    filename: str
    file_size: int
    header_row: Optional[int] = None


def _background_parse_and_convert(dataset_id: str):
    """Background worker: parse uploaded file → Parquet + populate DataFrame."""
    global _current_df, _current_filename, _date_columns
    global _current_file_bytes, _current_file_suffix, _file_disk_path, _file_last_modified
    global _active_dataset_id, _last_import_timestamp

    ds = _datasets.get(dataset_id)
    if not ds:
        return
    try:
        ds["status"] = "processing"
        ds["progress"] = 10

        raw_path = Path(ds["raw_path"])
        suffix = ds["suffix"]
        raw_bytes = raw_path.read_bytes()

        ds["progress"] = 30

        # Parse into DataFrame
        header_row = ds.get("header_row")
        df, detected_dates = _parse_data(raw_bytes, suffix, header_row=header_row)
        ds["progress"] = 60

        # Convert to Parquet
        parquet_path = raw_path.parent / "data.parquet"
        # For parquet, convert date columns to string to avoid issues
        export_df = df.copy()
        for col in detected_dates:
            if col in export_df.columns and pd.api.types.is_datetime64_any_dtype(export_df[col]):
                export_df[col] = export_df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna("")
        table = pa.Table.from_pandas(export_df, preserve_index=False)
        pq.write_table(table, str(parquet_path), compression='snappy')
        ds["parquet_path"] = str(parquet_path)
        ds["progress"] = 80

        # Replay derived columns (pipeline replay)
        replay_errors = []
        if dataset_id in _derived_columns and _derived_columns[dataset_id]:
            df, replay_errors = _replay_derived_columns(dataset_id, df)
            ds["replay_errors"] = replay_errors

        ds["progress"] = 90

        # Set as active dataset
        _current_df = df
        _current_filename = ds["filename"]
        _date_columns = detected_dates
        _current_file_bytes = raw_bytes
        _current_file_suffix = suffix
        _active_dataset_id = dataset_id
        _last_import_timestamp = datetime.now(timezone.utc).isoformat()

        # Check disk path
        disk_path = Path.cwd() / ds["filename"]
        if disk_path.exists():
            _file_disk_path = str(disk_path)
            _file_last_modified = disk_path.stat().st_mtime
        else:
            _file_disk_path = None
            _file_last_modified = 0

        # Save version snapshot
        _save_version_snapshot(dataset_id, df, detected_dates)

        # Persist active dataset meta for restart recovery
        _save_active_meta()

        ds["status"] = "ready"
        ds["progress"] = 100
        ds["rows"] = len(df)
        ds["columns"] = list(df.columns)
        ds["numeric_columns"] = list(df.select_dtypes(include="number").columns)
        ds["date_columns"] = detected_dates

    except Exception as e:
        ds["status"] = "error"
        ds["error"] = str(e)
        ds["progress"] = 0


def _cleanup_old_datasets(exclude_id: str):
    """Remove data folders for datasets other than exclude_id to prevent disk bloat."""
    global _datasets
    keys_to_remove = []
    
    for ds_id in _datasets:
        if ds_id != exclude_id:
            ds_dir = STORAGE_DIR / ds_id
            if ds_dir.exists() and ds_dir.is_dir():
                try:
                    shutil.rmtree(ds_dir)
                except Exception as e:
                    print(f"Warning: Failed to cleanup dataset {ds_id}: {e}")
            keys_to_remove.append(ds_id)
            
    for ds_id in keys_to_remove:
        del _datasets[ds_id]

    # Also clean orphaned folders on disk not in _datasets
    if STORAGE_DIR.exists():
        for child in STORAGE_DIR.iterdir():
            if child.is_dir() and child.name != exclude_id and child.name not in _datasets:
                try:
                    shutil.rmtree(child)
                except Exception:
                    pass


@app.post("/api/upload/init")
async def upload_init(req: UploadInitRequest):
    """Initialize a chunked upload. Returns a unique dataset_id."""
    dataset_id = uuid.uuid4().hex[:12]
    ds_dir = STORAGE_DIR / dataset_id
    ds_dir.mkdir(parents=True, exist_ok=True)

    suffix = Path(req.filename).suffix.lower()
    raw_path = ds_dir / f"raw{suffix}"

    _datasets[dataset_id] = {
        "status": "uploading",
        "filename": req.filename,
        "suffix": suffix,
        "file_size": req.file_size,
        "header_row": req.header_row,
        "raw_path": str(raw_path),
        "parquet_path": None,
        "error": None,
        "progress": 0,
        "bytes_received": 0,
        "rows": 0,
        "columns": [],
        "numeric_columns": [],
        "date_columns": [],
    }

    # Create/truncate raw file
    raw_path.write_bytes(b"")

    _cleanup_old_datasets(exclude_id=dataset_id)

    return {"dataset_id": dataset_id, "chunk_size": CHUNK_SIZE}


@app.post("/api/upload/chunk")
async def upload_chunk(
    dataset_id: str = Query(...),
    chunk_index: int = Query(...),
    file: UploadFile = File(...),
):
    """Receive a single chunk and append it to the raw file."""
    ds = _datasets.get(dataset_id)
    if not ds:
        raise HTTPException(404, "Dataset not found.")
    if ds["status"] != "uploading":
        raise HTTPException(400, "Upload already finalized or failed.")

    chunk_bytes = await file.read()
    raw_path = Path(ds["raw_path"])

    with open(raw_path, "ab") as f:
        f.write(chunk_bytes)

    ds["bytes_received"] += len(chunk_bytes)
    if ds["file_size"] > 0:
        ds["progress"] = min(95, int(ds["bytes_received"] / ds["file_size"] * 100))

    return {
        "ok": True,
        "chunk_index": chunk_index,
        "bytes_received": ds["bytes_received"],
        "progress": ds["progress"],
    }


@app.post("/api/upload/finalize")
async def upload_finalize(dataset_id: str = Query(...)):
    """Signal that all chunks are uploaded. Triggers background parsing."""
    ds = _datasets.get(dataset_id)
    if not ds:
        raise HTTPException(404, "Dataset not found.")
    if ds["status"] != "uploading":
        raise HTTPException(400, "Upload not in uploading state.")

    ds["status"] = "processing"
    ds["progress"] = 5

    # Submit to background worker
    _worker_pool.submit(_background_parse_and_convert, dataset_id)

    return {"dataset_id": dataset_id, "status": "processing"}


@app.get("/api/dataset/{dataset_id}/status")
async def dataset_status(dataset_id: str):
    """Poll processing status for a dataset."""
    ds = _datasets.get(dataset_id)
    if not ds:
        raise HTTPException(404, "Dataset not found.")

    result = {
        "dataset_id": dataset_id,
        "status": ds["status"],
        "progress": ds["progress"],
        "filename": ds["filename"],
        "error": ds["error"],
    }

    # When ready, include the full upload response
    if ds["status"] == "ready":
        result.update({
            "rows": ds["rows"],
            "columns": ds["columns"],
            "numeric_columns": ds["numeric_columns"],
            "date_columns": ds["date_columns"],
            "has_disk_path": _file_disk_path is not None,
            "preview": _build_preview_list(),
            "last_import": _last_import_timestamp,
            "version": _get_current_version_number(),
            "replay_errors": ds.get("replay_errors", []),
        })

    return result


def _build_preview_list():
    """Return first 100 rows as dicts for the frontend preview."""
    if _current_df is None:
        return []
    preview_df = _current_df.head(100).copy()
    for col in _date_columns:
        if col in preview_df.columns and pd.api.types.is_datetime64_any_dtype(preview_df[col]):
            preview_df[col] = preview_df[col].dt.strftime('%d.%m.%Y').fillna("")
    return preview_df.fillna("").to_dict(orient="records")


# Legacy single-shot upload (still works for small files / backward compat)
@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    global _current_df, _current_filename, _date_columns
    global _current_file_bytes, _current_file_suffix, _file_disk_path, _file_last_modified
    global _active_dataset_id, _last_import_timestamp

    raw_bytes = file.file.read()
    suffix = Path(file.filename).suffix.lower()
    _current_file_bytes = raw_bytes
    _current_file_suffix = suffix
    _current_filename = file.filename

    _current_df, _date_columns = _parse_data(raw_bytes, suffix)

    # Also save to storage + convert to Parquet
    dataset_id = uuid.uuid4().hex[:12]
    ds_dir = STORAGE_DIR / dataset_id
    ds_dir.mkdir(parents=True, exist_ok=True)
    raw_path = ds_dir / f"raw{suffix}"
    raw_path.write_bytes(raw_bytes)
    parquet_path = ds_dir / "data.parquet"
    export_df = _current_df.copy()
    for col in _date_columns:
        if col in export_df.columns and pd.api.types.is_datetime64_any_dtype(export_df[col]):
            export_df[col] = export_df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna("")
    table = pa.Table.from_pandas(export_df, preserve_index=False)
    pq.write_table(table, str(parquet_path), compression='snappy')
    _active_dataset_id = dataset_id
    _last_import_timestamp = datetime.now(timezone.utc).isoformat()

    # Register dataset entry for version control
    _datasets[dataset_id] = {
        "status": "ready",
        "filename": file.filename,
        "suffix": suffix,
        "file_size": len(raw_bytes),
        "raw_path": str(raw_path),
        "parquet_path": str(parquet_path),
        "error": None,
        "progress": 100,
        "bytes_received": len(raw_bytes),
        "rows": len(_current_df),
        "columns": list(_current_df.columns),
        "numeric_columns": list(_current_df.select_dtypes(include="number").columns),
        "date_columns": _date_columns,
    }

    # Save version snapshot
    _save_version_snapshot(dataset_id, _current_df, _date_columns)

    _cleanup_old_datasets(exclude_id=dataset_id)

    # Persist active dataset meta for restart recovery
    _save_active_meta()

    # Check if file exists on disk for auto-reload
    disk_path = Path.cwd() / file.filename
    if disk_path.exists():
        _file_disk_path = str(disk_path)
        _file_last_modified = disk_path.stat().st_mtime
    else:
        _file_disk_path = None
        _file_last_modified = 0

    return _build_upload_response()


@app.get("/api/columns")
async def get_columns():
    if _current_df is None:
        raise HTTPException(status_code=404, detail="No dataset loaded yet.")
    columns = list(_current_df.columns)
    numeric_columns = list(_current_df.select_dtypes(include="number").columns)
    return {"columns": columns, "numeric_columns": numeric_columns}


@app.get("/api/current")
async def get_current_dataset():
    """Return the full current dataset info (same shape as upload response).
    Used by the frontend to restore the Import Data tab on page reload."""
    if _current_df is None:
        raise HTTPException(status_code=404, detail="No dataset loaded yet.")
    return _build_upload_response()


@app.get("/api/wells")
async def get_wells(well_col: str):
    """Return unique well names from the specified column."""
    if _current_df is None:
        raise HTTPException(status_code=404, detail="No dataset loaded yet.")
    if well_col not in _current_df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{well_col}' not found.")
    wells = sorted(_current_df[well_col].dropna().unique().astype(str).tolist())
    return {"wells": wells}


@app.get("/api/dca")
async def decline_curve_analysis(
    x: str,
    y: str,
    well_col: str,
    wells: str = Query(..., description="Comma-separated well names"),
    model: str = Query("exponential", description="exponential|hyperbolic|harmonic"),
    forecast_months: float = Query(0, description="Months to forecast"),
    exclude_indices: str = Query("", description="Comma-separated indices to exclude from fitting"),
    combine: bool = Query(False, description="If true, sum y-values of selected wells by time period"),
    combine_func: str = Query("sum", description="Combine aggregation: sum|mean|median|min|max"),
    group_col: str = Query("", description="Optional grouping column. When set, each 'well' value is a group value from this column; rows matching the group value are aggregated by x."),
    plot_only: bool = Query(False, description="If true, return raw x/y scatter data without fitting a curve"),
):
    """
    Perform Decline Curve Analysis.
    Returns actual production data + fitted decline curves per well + forecast.
    If combine=true, aggregates y-values of all selected wells grouped by the x column
    and returns a single combined "well" for DCA.
    If plot_only=true, only returns raw x/y data without fitting.
    """
    if _current_df is None:
        raise HTTPException(status_code=404, detail="No dataset loaded yet.")
    if model not in _MODELS:
        raise HTTPException(status_code=400, detail=f"Unknown model '{model}'.")

    for col in [x, y, well_col]:
        if col not in _current_df.columns:
            raise HTTPException(status_code=400, detail=f"Column '{col}' not found.")

    # Validate optional group column
    group_col_name = (group_col or "").strip()
    if group_col_name and group_col_name not in _current_df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{group_col_name}' not found.")

    well_list = [w.strip() for w in wells.split(",") if w.strip()]

    # Parse forecast months
    try:
        f_months = float(forecast_months)
    except ValueError:
        f_months = 0.0

    # Check if x column is already a datetime (parsed at upload time)
    is_date = pd.api.types.is_datetime64_any_dtype(_current_df[x])

    agg_map = {
        "sum": "sum",
        "mean": "mean",
        "avg": "mean",
        "median": "median",
        "min": "min",
        "max": "max",
    }
    combine_func_norm = (combine_func or "sum").strip().lower()
    agg_func = agg_map.get(combine_func_norm)
    if agg_func is None:
        raise HTTPException(status_code=400, detail="Invalid combine_func. Use sum|mean|median|min|max")

    # ---- Combine mode: aggregate y-values across selected wells by time ----
    cols_to_extract = list(dict.fromkeys([x, y]))

    if combine and len(well_list) > 1:
        masks = _current_df[well_col].astype(str).isin(well_list)
        combined = _current_df.loc[masks, cols_to_extract].dropna().copy()
        combined[y] = pd.to_numeric(combined[y], errors='coerce').fillna(0.0)
        combined = combined.groupby(x, sort=True)[y].agg(agg_func).reset_index()
        combined = combined.sort_values(x).reset_index(drop=True)
        # Treat as a single "well" named after all combined wells
        combined_name = ' + '.join(well_list)
        well_list = [combined_name]
        # Temporarily inject combined data — we'll use a flag
        _combined_override = combined
    else:
        _combined_override = None

    result = []
    for well_name in well_list:
        if _combined_override is not None:
            subset = _combined_override.copy()
        elif group_col_name:
            # Group-by mode: filter rows where group_col matches the selected value,
            # then aggregate y by x using the chosen aggregation function.
            mask = _current_df[group_col_name].astype(str) == well_name
            grp = _current_df.loc[mask, cols_to_extract].dropna().copy()
            grp[y] = pd.to_numeric(grp[y], errors='coerce').fillna(0.0)
            subset = grp.groupby(x, sort=True)[y].agg(agg_func).reset_index()
        else:
            mask = _current_df[well_col].astype(str) == well_name
            subset = _current_df.loc[mask, cols_to_extract].dropna().copy()

        # Sort by x column
        subset = subset.sort_values(x).reset_index(drop=True)

        if len(subset) < 3:
            continue

        # Parse exclude indices (indices in sorted order)
        excl = set()
        if exclude_indices:
            excl = {int(i) for i in exclude_indices.split(",") if i.strip().isdigit()}

        fit_mask = np.array([i not in excl for i in range(len(subset))])

        # Build numeric t for curve fitting based on FIRST INCLUDED point
        if is_date:
            x_dates = subset[x]
            x_min_val = x_dates[fit_mask].min() if np.any(fit_mask) else x_dates.min()
            t = (x_dates - x_min_val).dt.total_seconds().values / 86400.0
            # Display strings in DD.MM.YYYY format
            x_display = x_dates.dt.strftime('%d.%m.%Y').tolist()
        else:
            x_numeric = pd.to_numeric(subset[x], errors='coerce').fillna(0.0).values.astype(float)
            x_min_numeric = x_numeric[fit_mask].min() if np.any(fit_mask) else x_numeric.min()
            t = x_numeric - x_min_numeric
            x_display = x_numeric.tolist()
            x_min_val = None

        y_vals = pd.to_numeric(subset[y], errors='coerce').fillna(0.0).values.astype(float)

        # --- Plot-only mode: return raw data without curve fitting ---
        if plot_only:
            entry = {
                "well": well_name,
                "x": x_display,
                "t": t.tolist(),
                "y_actual": y_vals.tolist(),
                "y_fitted": None,
                "forecast": {},
                "params": {},
                "equation": "",
                "is_date": is_date,
                "excluded_indices": sorted(excl),
            }
            result.append(entry)
            continue

        t_fit = t[fit_mask]
        y_fit = y_vals[fit_mask]

        # Fit the model on non-excluded data
        if len(t_fit) >= 3:
            params = _fit_decline(t_fit, y_fit, model)
        else:
            params = {}

        # Generate fitted values only for the non-excluded range
        fitted = None
        equation = ""
        if params:
            func = _MODELS[model][0]
            param_names = _MODELS[model][1]
            eq_fmt = _MODELS[model][4]
            p_values = [params[n] for n in param_names]
            fitted_arr = func(t, *p_values)
            fitted_arr = np.where(np.isfinite(fitted_arr), fitted_arr, np.nan)
            # Null-out fitted values for excluded points before the fitted region
            # so the fitted line only appears from the first included point onward
            if excl:
                non_excl_indices = sorted(set(range(len(t))) - excl)
                if non_excl_indices:
                    first_included = non_excl_indices[0]
                    for ei in range(len(fitted_arr)):
                        if ei < first_included:
                            fitted_arr[ei] = np.nan
                        elif ei in excl:
                            fitted_arr[ei] = np.nan
            fitted = [None if np.isnan(v) else v for v in fitted_arr]
            
            # Format equation string
            try:
                equation = eq_fmt.format(**params)
            except Exception:
                equation = ""

        # Forecast — monthly intervals (starting 1 month after last INCLUDED data)
        # Initialize as empty dict so w.forecast.x checks works safely
        forecast_data = {}
        if params and f_months > 0:
            func = _MODELS[model][0]
            # Use the last *included* point as forecast origin (not the last overall point)
            if excl:
                non_excl_idx = sorted(set(range(len(t))) - excl)
                last_t = t[non_excl_idx[-1]] if non_excl_idx else t[-1]
            else:
                last_t = t[-1]
            n_months = int(f_months)
            # Roughly 30.44 days per month for basic forecast stepping
            t_forecast = np.array([last_t + 30.4375 * (i + 1) for i in range(n_months)])
            
            p_vals = [params[n] for n in _MODELS[model][1]]
            q_forecast = func(t_forecast, *p_vals)
            q_forecast = np.nan_to_num(q_forecast, nan=0.0, posinf=0.0, neginf=0.0)

            if is_date:
                # Convert back to timestamp strings
                # t was (date - x_min_val).days
                # So new_date = x_min_val + t_forecast
                start_date = pd.to_datetime(x_min_val)
                forecast_dates = [start_date + pd.Timedelta(days=v) for v in t_forecast]
                x_fore_display = [d.strftime('%d.%m.%Y') for d in forecast_dates]
            else:
                x_fore_display = (t_forecast + x_min_numeric).tolist()

            forecast_data = {
                "x": x_fore_display,
                "y": q_forecast.tolist(),
                "t": t_forecast.tolist(),
            }

        entry = {
            "well": well_name,
            "x": x_display,
            "t": t.tolist(),
            "y_actual": y_vals.tolist(),
            "y_fitted": fitted,
            "forecast": forecast_data,
            "params": params,
            "equation": equation,
            "is_date": is_date,
            "excluded_indices": sorted(excl),
        }
        result.append(entry)

    return {
        "x_label": x,
        "y_label": y,
        "model": model,
        "wells": result,
    }


# ---------------------------------------------------------------------------
# Inline re-fit endpoint (used when user adds an anchor point client-side)
# ---------------------------------------------------------------------------
class FitInlineRequest(BaseModel):
    t: List[float]
    y: List[float]
    model: str
    forecast_months: float = 0.0


@app.post("/api/fit_inline")
async def fit_inline(req: FitInlineRequest):
    """Re-fit a decline model to caller-supplied (t, y) arrays.

    Returns fitted params, fitted y values, equation string, and optional
    forecast arrays – everything the front-end needs to refresh a chart
    without a full /api/dca round-trip.
    """
    if req.model not in _MODELS:
        raise HTTPException(status_code=400, detail=f"Unknown model '{req.model}'.")
    t_arr = np.array(req.t, dtype=float)
    y_arr = np.array(req.y, dtype=float)
    if len(t_arr) < 3 or len(t_arr) != len(y_arr):
        raise HTTPException(status_code=400, detail="Need at least 3 matching t/y values.")

    params = _fit_decline(t_arr, y_arr, req.model)
    if not params:
        raise HTTPException(status_code=422, detail="Curve fitting failed.")

    func = _MODELS[req.model][0]
    param_names = _MODELS[req.model][1]
    eq_fmt = _MODELS[req.model][4]

    p_vals = [params[n] for n in param_names]
    fitted = func(t_arr, *p_vals)
    fitted = [None if not np.isfinite(v) else float(v) for v in fitted]

    try:
        equation = eq_fmt.format(**params)
    except Exception:
        equation = ""

    result: dict = {
        "params": params,
        "y_fitted": fitted,
        "equation": equation,
    }

    # Forecast
    if req.forecast_months and req.forecast_months > 0:
        last_t = float(t_arr[-1])
        n_months = int(req.forecast_months)
        t_forecast = np.array([last_t + 30.4375 * (i + 1) for i in range(n_months)])
        q_forecast = func(t_forecast, *p_vals)
        q_forecast = np.where(np.isfinite(q_forecast), q_forecast, 0.0)
        result["forecast_t"] = t_forecast.tolist()
        result["forecast_y"] = q_forecast.tolist()

    return result


# ---------------------------------------------------------------------------
# Data editing & reload endpoints
# ---------------------------------------------------------------------------
class CellUpdate(BaseModel):
    row: int
    column: str
    value: str


class NewColumn(BaseModel):
    name: str
    formula: str


@app.get("/api/reload")
async def reload_from_disk():
    """Re-read the file from disk if available, otherwise re-parse stored bytes.
    Saves a new version and replays derived columns."""
    global _current_df, _date_columns, _file_last_modified, _last_import_timestamp
    if _current_df is None:
        raise HTTPException(404, "No dataset loaded.")
    if _file_disk_path and Path(_file_disk_path).exists():
        raw = Path(_file_disk_path).read_bytes()
        _current_df, _date_columns = _parse_data(raw, _current_file_suffix)
        _file_last_modified = Path(_file_disk_path).stat().st_mtime
    else:
        _current_df, _date_columns = _parse_data(_current_file_bytes, _current_file_suffix)

    _last_import_timestamp = datetime.now(timezone.utc).isoformat()

    # Update Parquet + version snapshot
    if _active_dataset_id:
        ds_dir = STORAGE_DIR / _active_dataset_id
        parquet_path = ds_dir / "data.parquet"
        export_df = _current_df.copy()
        for col in _date_columns:
            if col in export_df.columns and pd.api.types.is_datetime64_any_dtype(export_df[col]):
                export_df[col] = export_df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna("")
        table = pa.Table.from_pandas(export_df, preserve_index=False)
        pq.write_table(table, str(parquet_path), compression='snappy')
        _save_version_snapshot(_active_dataset_id, _current_df, _date_columns)

        # Replay derived columns
        if _active_dataset_id in _derived_columns and _derived_columns[_active_dataset_id]:
            _current_df, replay_errors = _replay_derived_columns(_active_dataset_id, _current_df)
        else:
            replay_errors = []

        # Update dataset registry
        ds = _datasets.get(_active_dataset_id)
        if ds:
            ds["rows"] = len(_current_df)
            ds["columns"] = list(_current_df.columns)
            ds["numeric_columns"] = list(_current_df.select_dtypes(include="number").columns)
            ds["date_columns"] = _date_columns

    return _build_upload_response()


@app.post("/api/sync/upload")
async def sync_upload(file: UploadFile = File(...)):
    """Receive a re-synced file from the browser (File System Access API).
    Creates a new version, replays pipeline, returns updated data."""
    global _current_df, _current_filename, _date_columns
    global _current_file_bytes, _current_file_suffix, _last_import_timestamp

    if not _active_dataset_id:
        raise HTTPException(400, "No active dataset to sync.")

    raw_bytes = file.file.read()
    suffix = Path(file.filename).suffix.lower()

    try:
        df, detected_dates = _parse_data(raw_bytes, suffix)
    except Exception as e:
        raise HTTPException(400, f"Failed to parse file: {e}")

    # Update stored raw file
    ds = _datasets.get(_active_dataset_id)
    if ds:
        raw_path = Path(ds["raw_path"])
        raw_path.write_bytes(raw_bytes)

    # Write new Parquet
    ds_dir = STORAGE_DIR / _active_dataset_id
    parquet_path = ds_dir / "data.parquet"
    export_df = df.copy()
    for col in detected_dates:
        if col in export_df.columns and pd.api.types.is_datetime64_any_dtype(export_df[col]):
            export_df[col] = export_df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna("")
    table = pa.Table.from_pandas(export_df, preserve_index=False)
    pq.write_table(table, str(parquet_path), compression='snappy')

    # Replay derived columns
    replay_errors = []
    if _active_dataset_id in _derived_columns and _derived_columns[_active_dataset_id]:
        df, replay_errors = _replay_derived_columns(_active_dataset_id, df)

    # Save version snapshot
    _save_version_snapshot(_active_dataset_id, df, detected_dates)

    # Update globals
    _current_df = df
    _current_filename = file.filename
    _date_columns = detected_dates
    _current_file_bytes = raw_bytes
    _current_file_suffix = suffix
    _last_import_timestamp = datetime.now(timezone.utc).isoformat()

    # Update dataset registry
    if ds:
        ds["rows"] = len(df)
        ds["columns"] = list(df.columns)
        ds["numeric_columns"] = list(df.select_dtypes(include="number").columns)
        ds["date_columns"] = detected_dates
        ds["replay_errors"] = replay_errors

    resp = {
        "filename": _current_filename,
        "rows": len(_current_df),
        "columns": list(_current_df.columns),
        "numeric_columns": list(_current_df.select_dtypes(include="number").columns),
        "date_columns": _date_columns,
        "has_disk_path": _file_disk_path is not None,
        "last_import": _last_import_timestamp,
        "dataset_id": _active_dataset_id,
        "version": _get_current_version_number(),
        "replay_errors": replay_errors,
    }
    return resp


@app.get("/api/versions")
async def list_versions():
    """List all stored versions for the active dataset."""
    if not _active_dataset_id:
        raise HTTPException(404, "No active dataset.")
    versions = _versions.get(_active_dataset_id, [])
    return {
        "dataset_id": _active_dataset_id,
        "versions": [
            {"version": v["version"], "timestamp": v["timestamp"],
             "rows": v["rows"], "columns": len(v["columns"]), "status": v["status"]}
            for v in versions
        ],
        "current_version": _get_current_version_number(),
    }


@app.post("/api/versions/rollback")
async def rollback_version(version: int = Query(...)):
    """Rollback to a specific version by re-loading its Parquet snapshot."""
    global _current_df, _date_columns, _last_import_timestamp, _active_version
    if not _active_dataset_id:
        raise HTTPException(404, "No active dataset.")
    versions = _versions.get(_active_dataset_id, [])
    target = None
    for v in versions:
        if v["version"] == version:
            target = v
            break
    if not target:
        raise HTTPException(404, f"Version {version} not found.")

    parquet_path = Path(target["parquet_path"])
    if not parquet_path.exists():
        raise HTTPException(404, "Version Parquet file missing.")

    # Read back the versioned Parquet
    table = pq.read_table(str(parquet_path))
    df = table.to_pandas()

    # Preserve known date columns from the dataset registry or current state,
    # then re-parse them (parquet may store dates as strings).
    ds = _datasets.get(_active_dataset_id)
    known_dates = list(_date_columns) if _date_columns else []
    if not known_dates and ds:
        known_dates = list(ds.get("date_columns", []))

    restored_dates = []
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            restored_dates.append(col)
        elif col in known_dates:
            # Re-parse string dates from parquet
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            restored_dates.append(col)

    _current_df = df
    _date_columns = restored_dates
    _last_import_timestamp = datetime.now(timezone.utc).isoformat()
    _active_version = version  # track which version is now current

    # Copy this version's parquet to become the current data.parquet
    ds_dir = STORAGE_DIR / _active_dataset_id
    shutil.copy2(str(parquet_path), str(ds_dir / "data.parquet"))

    # Re-persist the in-memory df (with proper date handling)
    _persist_current_df()

    # Update dataset registry
    if ds:
        ds["rows"] = len(df)
        ds["columns"] = list(df.columns)
        ds["numeric_columns"] = list(df.select_dtypes(include="number").columns)
        ds["date_columns"] = _date_columns

    # Persist meta so the rollback survives server restart
    _save_active_meta()

    return {
        "ok": True,
        "rolled_back_to": version,
        "rows": len(df),
        "columns": list(df.columns),
        "numeric_columns": list(df.select_dtypes(include="number").columns),
        "date_columns": _date_columns,
        "last_import": _last_import_timestamp,
        "filename": _current_filename,
        "dataset_id": _active_dataset_id,
        "has_disk_path": _file_disk_path is not None,
        "version": _get_current_version_number(),
    }


@app.get("/api/derived_columns")
async def get_derived_columns():
    """Return the pipeline of derived columns for the active dataset."""
    if not _active_dataset_id:
        return {"columns": []}
    return {"columns": _derived_columns.get(_active_dataset_id, [])}


@app.get("/api/file_status")
async def file_status():
    """Check if the file on disk has been modified since last load."""
    if not _file_disk_path or not Path(_file_disk_path).exists():
        return {"has_disk_path": False, "modified": False}
    current_mtime = Path(_file_disk_path).stat().st_mtime
    return {
        "has_disk_path": True,
        "modified": current_mtime > _file_last_modified,
        "disk_mtime": current_mtime,
    }


@app.get("/api/data")
async def get_data(
    page: int = Query(1),
    page_size: int = Query(50),
    sort_col: Optional[str] = None,
    sort_asc: bool = True,
    filter_col: Optional[str] = None,
    filter_val: Optional[str] = None
):
    """Get paginated data for the editor with sorting and filtering."""
    if _current_df is None:
        raise HTTPException(404, "No dataset loaded.")
    
    df_view = _current_df.copy()

    # Filtering
    if filter_col and filter_val and filter_col in df_view.columns:
        # Simple string contains filter, case-insensitive
        mask = df_view[filter_col].astype(str).str.contains(filter_val, case=False, na=False)
        df_view = df_view[mask]

    # Sorting
    if sort_col and sort_col in df_view.columns:
        df_view = df_view.sort_values(by=sort_col, ascending=sort_asc)

    total = len(df_view)
    start = (page - 1) * page_size
    end = min(start + page_size, total)
    chunk = df_view.iloc[start:end].copy()

    for col in _date_columns:
        if col in chunk.columns and pd.api.types.is_datetime64_any_dtype(chunk[col]):
            chunk[col] = chunk[col].dt.strftime('%d.%m.%Y').fillna("")
            
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, (total + page_size - 1) // page_size),
        "columns": list(_current_df.columns),
        "rows": chunk.fillna("").to_dict(orient="records"),
        "derived_columns": _derived_columns.get(_active_dataset_id, []) if _active_dataset_id else [],
    }


@app.post("/api/data/update")
async def update_cell(update: CellUpdate):
    """Update a single cell value."""
    global _current_df
    if _current_df is None:
        raise HTTPException(404, "No dataset loaded.")
    if update.column not in _current_df.columns:
        raise HTTPException(400, f"Column '{update.column}' not found.")
    if update.row < 0 or update.row >= len(_current_df):
        raise HTTPException(400, f"Row {update.row} out of range.")
    col = update.column
    val = update.value
    if pd.api.types.is_numeric_dtype(_current_df[col]):
        try:
            val = float(val)
        except ValueError:
            pass
    elif pd.api.types.is_datetime64_any_dtype(_current_df[col]):
        try:
            val = pd.to_datetime(val, dayfirst=True)
        except Exception:
            pass
    _current_df.at[update.row, col] = val
    _persist_current_df()
    return {"ok": True}


class FormulaPreview(BaseModel):
    formula: str

@app.post("/api/data/formula_preview")
async def preview_formula(body: FormulaPreview):
    """Preview a formula on the first 5 rows without saving."""
    if _current_df is None:
        raise HTTPException(404, "No dataset loaded.")
    try:
        # Evaluate on the full DF to ensure aggregations (SUM, MEAN, etc.) are correct
        full_result = _current_df.eval(body.formula)
        
        df_head = _current_df.head(5).copy()
        
        if isinstance(full_result, pd.Series):
            df_head['RESULT'] = full_result.iloc[:5]
        elif np.isscalar(full_result):
            df_head['RESULT'] = full_result
        else:
            # Handle other types (like numpy arrays)
            try:
                df_head['RESULT'] = full_result[:5]
            except Exception:
                df_head['RESULT'] = full_result

        for col in _date_columns:
            if col in df_head.columns and pd.api.types.is_datetime64_any_dtype(df_head[col]):
                df_head[col] = df_head[col].dt.strftime('%d.%m.%Y').fillna("")

        # Handle NaNs and return as list of dicts
        preview_data = df_head.fillna("").to_dict(orient="records")
        return {"ok": True, "preview": preview_data}
    except Exception as e:
        raise HTTPException(400, f"Formula error: {e}")

@app.post("/api/data/add_column")
async def add_computed_column(col: NewColumn):
    """Add a computed column using a pandas-eval expression.
    Also registers it in the derived-column pipeline for replay."""
    global _current_df
    if _current_df is None:
        raise HTTPException(404, "No dataset loaded.")
    if col.name in _current_df.columns:
        raise HTTPException(400, f"Column '{col.name}' already exists.")
    try:
        _current_df[col.name] = _current_df.eval(col.formula)
    except Exception as e:
        raise HTTPException(400, f"Formula error: {e}")

    # Round numeric results to 4 decimal places for readability
    if pd.api.types.is_numeric_dtype(_current_df[col.name]):
        _current_df[col.name] = _current_df[col.name].round(4)

    # Register in pipeline for replay
    if _active_dataset_id:
        if _active_dataset_id not in _derived_columns:
            _derived_columns[_active_dataset_id] = []
        # Avoid duplicates
        existing_names = {d["name"] for d in _derived_columns[_active_dataset_id]}
        if col.name not in existing_names:
            _derived_columns[_active_dataset_id].append({"name": col.name, "formula": col.formula})

    _persist_current_df()
    _save_active_meta()
    return {
        "ok": True,
        "columns": list(_current_df.columns),
        "numeric_columns": list(_current_df.select_dtypes(include="number").columns),
        "derived_columns": _derived_columns.get(_active_dataset_id, []) if _active_dataset_id else [],
    }


@app.delete("/api/data/column")
async def delete_column(column: str = Query(...)):
    """Delete a column. Also removes it from the derived-column pipeline."""
    global _current_df, _date_columns
    if _current_df is None:
        raise HTTPException(404, "No dataset loaded.")
    if column not in _current_df.columns:
        raise HTTPException(400, f"Column '{column}' not found.")
    _current_df.drop(columns=[column], inplace=True)
    if column in _date_columns:
        _date_columns.remove(column)
    # Remove from derived pipeline
    if _active_dataset_id and _active_dataset_id in _derived_columns:
        _derived_columns[_active_dataset_id] = [
            d for d in _derived_columns[_active_dataset_id] if d["name"] != column
        ]
    _persist_current_df()
    _save_active_meta()
    return {
        "ok": True,
        "columns": list(_current_df.columns),
        "numeric_columns": list(_current_df.select_dtypes(include="number").columns),
        "derived_columns": _derived_columns.get(_active_dataset_id, []) if _active_dataset_id else [],
    }


@app.get("/api/data/export")
async def export_data():
    """Export the current DataFrame as CSV text."""
    if _current_df is None:
        raise HTTPException(404, "No dataset loaded.")
    buf = io.StringIO()
    export_df = _current_df.copy()
    for col in _date_columns:
        if col in export_df.columns and pd.api.types.is_datetime64_any_dtype(export_df[col]):
            export_df[col] = export_df[col].dt.strftime('%d.%m.%Y').fillna("")
    export_df.to_csv(buf, index=False)
    return JSONResponse({"csv": buf.getvalue()})


# ---------------------------------------------------------------------------
# Virtual Scroll: on-demand row fetching for Data Preview
# ---------------------------------------------------------------------------
@app.get("/api/preview/rows")
async def preview_rows(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    sort_col: Optional[str] = None,
    sort_asc: bool = True,
    filter_col: Optional[str] = None,
    filter_val: Optional[str] = None,
):
    """Return a slice of rows for virtual-scroll preview.

    The frontend requests rows by *offset* (absolute row index) so it can
    map a scrollbar position directly to a data window.
    """
    if _current_df is None:
        raise HTTPException(404, "No dataset loaded.")

    df_view = _current_df

    # Filtering (create a view, avoid full copy for perf)
    if filter_col and filter_val and filter_col in df_view.columns:
        mask = df_view[filter_col].astype(str).str.contains(filter_val, case=False, na=False)
        df_view = df_view[mask]

    # Sorting
    if sort_col and sort_col in df_view.columns:
        df_view = df_view.sort_values(by=sort_col, ascending=sort_asc)

    total = len(df_view)
    start = min(offset, total)
    end = min(start + limit, total)
    chunk = df_view.iloc[start:end].copy()

    for col in _date_columns:
        if col in chunk.columns and pd.api.types.is_datetime64_any_dtype(chunk[col]):
            chunk[col] = chunk[col].dt.strftime('%d.%m.%Y').fillna("")

    return {
        "total": total,
        "offset": start,
        "limit": limit,
        "columns": list(_current_df.columns),
        "rows": chunk.fillna("").to_dict(orient="records"),
        "derived_columns": _derived_columns.get(_active_dataset_id, []) if _active_dataset_id else [],
    }


# ---------------------------------------------------------------------------
# Columnar Summary / Stats  (computed from Parquet when available)
# ---------------------------------------------------------------------------
@app.get("/api/preview/stats")
async def preview_stats():
    """Return per-column statistics: type, count, nulls, min, max, mean,
    std, and a 20-bin histogram for numeric columns."""
    if _current_df is None:
        raise HTTPException(404, "No dataset loaded.")

    stats = []
    for col in _current_df.columns:
        s = _current_df[col]
        total = len(s)
        null_count = int(s.isna().sum())
        unique_count = int(s.nunique())

        entry = {
            "column": col,
            "dtype": str(s.dtype),
            "total": total,
            "non_null": total - null_count,
            "null_count": null_count,
            "unique": unique_count,
        }

        if pd.api.types.is_numeric_dtype(s):
            clean = s.dropna()
            entry["type"] = "numeric"
            entry["min"] = _safe_json(clean.min()) if len(clean) else None
            entry["max"] = _safe_json(clean.max()) if len(clean) else None
            entry["mean"] = _safe_json(clean.mean()) if len(clean) else None
            entry["std"] = _safe_json(clean.std()) if len(clean) else None
            entry["median"] = _safe_json(clean.median()) if len(clean) else None
            entry["p25"] = _safe_json(clean.quantile(0.25)) if len(clean) else None
            entry["p75"] = _safe_json(clean.quantile(0.75)) if len(clean) else None

            # Histogram (20 bins)
            if len(clean) >= 2:
                try:
                    counts, edges = np.histogram(clean.values, bins=20)
                    entry["histogram"] = {
                        "counts": counts.tolist(),
                        "edges": [round(float(e), 6) for e in edges],
                    }
                except Exception:
                    entry["histogram"] = None
            else:
                entry["histogram"] = None
        elif pd.api.types.is_datetime64_any_dtype(s):
            clean = s.dropna()
            entry["type"] = "datetime"
            entry["min"] = str(clean.min()) if len(clean) else None
            entry["max"] = str(clean.max()) if len(clean) else None
            entry["histogram"] = None
        else:
            entry["type"] = "string"
            # Top 10 most frequent values
            if len(s.dropna()) > 0:
                top = s.value_counts().head(10)
                entry["top_values"] = [{"value": str(k), "count": int(v)} for k, v in top.items()]
            else:
                entry["top_values"] = []
            entry["histogram"] = None

        stats.append(entry)

    return {"columns": stats, "total_rows": len(_current_df)}


def _safe_json(v):
    """Convert numpy numeric to JSON-safe float."""
    if v is None:
        return None
    f = float(v)
    if not np.isfinite(f):
        return None
    return round(f, 6)


# ---------------------------------------------------------------------------
# Serve the single-page frontend
# ---------------------------------------------------------------------------
FRONTEND_PATH = Path(__file__).parent / "templates" / "index.html"


@app.get("/", response_class=HTMLResponse)
async def index():
    return FRONTEND_PATH.read_text(encoding="utf-8")
