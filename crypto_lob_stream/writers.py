import json
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

MAX_RETRY_ATTEMPTS = 3


# ── Local writer ──────────────────────────────────────────────────────────────

def write_local(records, schema, output_dir, prefix, exchange, asset, ts_str):
    """Write records to a local Parquet file.

    Path: {output_dir}/{prefix}/{exchange}/{asset}/{ts_str}.parquet
    Creates directories as needed.

    Refuses to silently overwrite an existing file at that exact path --
    this should never happen with a correctly unique ts_str, but "should
    never happen" is exactly what the previous hour-only ts_str bug also
    looked like right up until it silently destroyed most of a hundred+
    million rows across the project's whole history. If it ever does
    happen (clock issue, a future regression, anything), this logs loudly
    and disambiguates instead of destroying data quietly.
    """
    if not records:
        return True

    out_path = Path(output_dir) / prefix / exchange / asset / f"{ts_str}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        logger.error(
            f"COLLISION: {out_path} already exists and would have been "
            f"silently overwritten. This should never happen with a "
            f"correctly unique ts_str -- writing to a disambiguated path "
            f"instead so nothing is lost, but this is worth investigating."
        )
        n = 1
        while out_path.exists():
            out_path = out_path.with_name(f"{ts_str}_dup{n}.parquet")
            n += 1

    try:
        table = pa.Table.from_pylist(records, schema=schema)
        pq.write_table(table, str(out_path), compression="snappy")
        logger.info(f"Wrote {len(records):,} records -> {out_path}")
        return True
    except Exception as e:
        logger.error(f"Local write failed for {out_path}: {e}")
        return False


# ── GCS writer ────────────────────────────────────────────────────────────────

def write_gcs(records, schema, bucket_name, prefix, exchange, asset, ts_str, fallback_dir=None):
    """Upload records to GCS with retry and optional local fallback.

    Blob path: {prefix}/{exchange}/{asset}/{ts_str}.parquet
    Records are never silently dropped: if all GCS retries fail and a
    fallback_dir is provided, the Parquet file is saved there instead.

    Uploads with if_generation_match=0, meaning the write only succeeds
    if no object already exists at that exact blob path -- this should
    always be true with a correctly unique ts_str, but makes the
    overwrite-safety guarantee atomic and free (the same billed
    operation either way) rather than requiring a separate existence
    check beforehand, which would both cost an extra operation per flush
    and leave a race window between checking and writing. If the
    precondition fails (something's already there), this logs loudly and
    retries once with a disambiguated path instead of silently
    overwriting.
    """
    if not records:
        return True

    blob_path = f"{prefix}/{exchange}/{asset}/{ts_str}.parquet"
    tmp_path = None

    try:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        tmp_path = tmp.name
        tmp.close()
        table = pa.Table.from_pylist(records, schema=schema)
        pq.write_table(table, tmp_path, compression="snappy")
    except Exception as e:
        logger.error(f"Serialisation failed for {prefix}/{exchange}/{asset}: {e}")
        _emergency_json_fallback(records, fallback_dir, prefix, exchange, asset, ts_str)
        return False

    # Lazy import -- users who use local output only don't need google-cloud-storage
    try:
        from google.cloud import storage as gcs
    except ImportError:
        raise ImportError(
            "google-cloud-storage is required for GCS output. "
            "Install it with: pip install crypto-lob-stream[gcs]"
        )

    def _is_precondition_failure(exc) -> bool:
        # google-cloud-storage raises google.api_core.exceptions.PreconditionFailed
        # (HTTP 412) when if_generation_match's condition isn't met. Checked
        # defensively by status code/message rather than importing the
        # exact exception class, since different google-cloud-storage
        # versions have moved this around before.
        code = getattr(exc, "code", None) or getattr(exc, "status_code", None)
        return code == 412 or "412" in str(exc) or "conditionNotMet" in str(exc)

    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        try:
            client = gcs.Client()
            client.bucket(bucket_name).blob(blob_path).upload_from_filename(
                tmp_path, if_generation_match=0,
            )
            logger.info(f"Uploaded {len(records):,} records -> gs://{bucket_name}/{blob_path}")
            os.remove(tmp_path)
            return True
        except Exception as e:
            if _is_precondition_failure(e):
                logger.error(
                    f"COLLISION: gs://{bucket_name}/{blob_path} already exists "
                    f"and would have been silently overwritten. This should "
                    f"never happen with a correctly unique ts_str -- uploading "
                    f"to a disambiguated path instead, but this is worth "
                    f"investigating."
                )
                blob_path = f"{prefix}/{exchange}/{asset}/{ts_str}_dup.parquet"
                continue
            logger.warning(f"GCS attempt {attempt}/{MAX_RETRY_ATTEMPTS} failed for {blob_path}: {e}")
            if attempt < MAX_RETRY_ATTEMPTS:
                time.sleep(2 ** attempt)

    logger.error(f"All GCS retries failed for {blob_path}.")
    if fallback_dir:
        fallback_path = Path(fallback_dir) / f"{prefix}_{exchange}_{asset}_{ts_str}.parquet"
        fallback_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy(tmp_path, fallback_path)
            logger.info(f"Fallback saved: {fallback_path}")
        except Exception as fe:
            logger.error(f"Local fallback also failed: {fe}")

    if tmp_path and os.path.exists(tmp_path):
        os.remove(tmp_path)

    return False


def _emergency_json_fallback(records, fallback_dir, prefix, exchange, asset, ts_str):
    if not fallback_dir:
        return
    try:
        path = Path(fallback_dir) / f"{prefix}_{exchange}_{asset}_{ts_str}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(records, f)
        logger.info(f"Emergency JSON fallback saved: {path}")
    except Exception as e:
        logger.error(f"Emergency JSON fallback failed: {e}")


def retry_gcs_fallbacks(fallback_dir, bucket_name):
    """On startup, attempt to re-upload any Parquet files in fallback_dir."""
    fallback_dir = Path(fallback_dir)
    files = list(fallback_dir.glob("*.parquet"))
    if not files:
        return
    logger.info(f"Found {len(files)} fallback file(s). Attempting re-upload...")
    try:
        from google.cloud import storage as gcs
    except ImportError:
        logger.warning("google-cloud-storage not installed; skipping fallback re-upload.")
        return
    for f in files:
        try:
            client = gcs.Client()
            # Fallback filenames are "{prefix}_{exchange}_{asset}_{ts_str}.parquet".
            # asset/ts_str themselves never contain "_" in current schemas, so a
            # max-3-split is safe and reverses write_gcs's naming exactly.
            parts = f.stem.split("_", 3)
            if len(parts) == 4:
                prefix, exchange, asset, ts_str = parts
                blob_path = f"{prefix}/{exchange}/{asset}/{ts_str}.parquet"
                client.bucket(bucket_name).blob(blob_path).upload_from_filename(str(f))
                logger.info(f"Re-uploaded: {blob_path}")
                f.unlink()
        except Exception as e:
            logger.error(f"Re-upload of {f.name} failed: {e}")