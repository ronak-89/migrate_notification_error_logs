#!/usr/bin/env python3
"""
One-time migration: move FCM error rows from notification_logs to notification_log_errors.

Standalone script: run on any server with DB_* and MONGO_* env vars. Resumable via MongoDB
checkpoint: on failure or SIGINT/SIGTERM, re-run to continue from last batch.

  python migrate_notification_log_errors.py --dry-run
  python migrate_notification_log_errors.py

Options:
  --dry-run      Only count rows that would be moved; do not migrate; no checkpoint.
  --batch-size   Number of rows per batch (default 50000 or BATCH_SIZE env).
  --reason-only  Match by reason only, any status 1/2/3 (default).
  --all-failures Move all status=2 rows regardless of reason.
"""
import argparse
import logging
import os
import signal
import sys
import time
from typing import Optional

# Script dir on path for utils
_script_dir = os.path.dirname(os.path.abspath(__file__))
if _script_dir not in sys.path:
    sys.path.insert(0, _script_dir)

from dotenv import load_dotenv
load_dotenv(os.path.join(_script_dir, ".env"))

from utils.db import get_db_connection  # noqa: E402
from utils.checkpoint import (  # noqa: E402
    load_checkpoint,
    save_checkpoint,
    close_checkpoint_client,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Exact reason strings treated as FCM errors
FCM_ERROR_REASONS_EXACT = (
    "NotRegistered",
    "Requested entity was not found.",
    "Failed to send FCM notification",
    "Internal error encountered.",
    "No recipients defined",
    "Request contains an invalid argument.",
    "The service is currently unavailable.",
    "sent",
    "registration-token-not-registered",
    "UNREGISTERED",
)

# LIKE patterns for FCM errors
FCM_ERROR_REASONS_LIKE = (
    "Invalid value at%",
    "Visibility check was unavailable%",
)

BATCH_SIZE_DEFAULT = 50_000

MONGO_COLLECTION = os.getenv("MONGO_CHECKPOINT_COLLECTION", "notification_log_errors_migration")
CHECKPOINT_ID = "migrate_notification_log_errors"

# Set by main() for signal handler
_shutdown_requested = False


def _handle_signal(_signum, _frame):
    global _shutdown_requested
    _shutdown_requested = True
    logger.info("Shutdown requested; will save checkpoint and exit after current batch")


def build_where_clause(reason_only: bool) -> tuple[str, tuple]:
    """Return SQL WHERE fragment and params for error rows."""
    if reason_only:
        exact_placeholders = ", ".join(["%s"] * len(FCM_ERROR_REASONS_EXACT))
        like_conditions = " OR ".join(["reason LIKE %s"] * len(FCM_ERROR_REASONS_LIKE))
        exact_sql = f"reason IN ({exact_placeholders})"
        like_sql = f"({like_conditions})" if FCM_ERROR_REASONS_LIKE else "FALSE"
        params = list(FCM_ERROR_REASONS_EXACT) + list(FCM_ERROR_REASONS_LIKE)
        return f"({exact_sql} OR {like_sql})", tuple(params)
    return "status = 2", ()


def count_eligible(conn, reason_only: bool) -> int:
    """Return number of rows that would be migrated."""
    where_sql, params = build_where_clause(reason_only)
    sql = f"SELECT COUNT(*) AS n FROM public.notification_logs WHERE {where_sql}"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return row[0]


def migrate_batch(
    conn,
    batch_size: int,
    reason_only: bool,
    last_created_at: Optional[str],
    last_id: Optional[str],
) -> tuple[int, Optional[str], Optional[str]]:
    """
    Copy one batch to notification_log_errors and delete from notification_logs.
    Uses cursor (last_created_at, last_id) to resume. Returns (moved_count, last_created_at, last_id).
    """
    where_sql, params = build_where_clause(reason_only)
    if last_created_at is not None and last_id is not None:
        where_sql = f"{where_sql} AND (created_at > %s OR (created_at = %s AND id::text > %s))"
        params = (*params, last_created_at, last_created_at, last_id)
    order_limit = "ORDER BY created_at, id LIMIT %s"
    params = (*params, batch_size)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, created_at FROM public.notification_logs
            WHERE {where_sql}
            {order_limit}
            """,
            params,
        )
        rows = cur.fetchall()

    if not rows:
        return 0, last_created_at, last_id

    ids = [r[0] for r in rows]
    last_row = rows[-1]
    new_last_id = str(last_row[0])
    new_last_created_at = last_row[1]
    if new_last_created_at is not None and hasattr(new_last_created_at, "isoformat"):
        new_last_created_at = new_last_created_at.isoformat()
    else:
        new_last_created_at = str(new_last_created_at)

    id_placeholders = ",".join(["%s"] * len(ids))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO public.notification_log_errors (
                user_id, title, description, is_read, status, created_at,
                extra, fcm_token, message_id, reason, device_id, notification_id
            )
            SELECT
                user_id, title, description, is_read, status, created_at,
                extra, fcm_token, message_id, reason, device_id, notification_id
            FROM public.notification_logs
            WHERE id IN ({id_placeholders})
            """,
            tuple(ids),
        )
        cur.execute(
            f"DELETE FROM public.notification_logs WHERE id IN ({id_placeholders})",
            tuple(ids),
        )
    conn.commit()
    return len(ids), new_last_created_at, new_last_id


def main():
    global _shutdown_requested
    parser = argparse.ArgumentParser(description="Move FCM error rows to notification_log_errors")
    parser.add_argument("--dry-run", action="store_true", help="Only count rows, do not migrate")
    parser.add_argument(
        "--from-start",
        action="store_true",
        help="Ignore checkpoint and process all eligible rows from the beginning",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Rows per batch (default from BATCH_SIZE env, or %s)" % BATCH_SIZE_DEFAULT,
    )
    parser.add_argument(
        "--all-failures",
        action="store_true",
        help="Move all status=2 rows; default is match by reason only (any status)",
    )
    args = parser.parse_args()
    if args.batch_size is not None:
        batch_size = args.batch_size
    else:
        batch_size = int(os.getenv("BATCH_SIZE", str(BATCH_SIZE_DEFAULT)))

    sleep_between_batches = float(os.getenv("SLEEP_BETWEEN_BATCHES", "2") or "2")

    reason_only = not args.all_failures

    logger.info(
        "Starting migration script (reason_only=%s, batch_size=%s, sleep_between_batches=%s)",
        reason_only,
        batch_size,
        sleep_between_batches,
    )

    with get_db_connection() as conn:
        total = count_eligible(conn, reason_only)
        logger.info("Eligible rows to migrate: %s", total)

        if args.dry_run:
            logger.info("Dry run: no changes made")
            return

    # Checkpoint: load so we can resume
    default_checkpoint = {
        "last_created_at": None,
        "last_id": None,
        "total_moved": 0,
        "reason_only": 1 if reason_only else 0,
    }
    try:
        cp = load_checkpoint(MONGO_COLLECTION, CHECKPOINT_ID, default_checkpoint)
    except Exception as e:
        logger.warning("Could not load checkpoint (will start from beginning): %s", e)
        cp = default_checkpoint

    if args.from_start:
        cp = default_checkpoint
        logger.info("--from-start: ignoring checkpoint")
    elif cp.get("reason_only", 1) != (1 if reason_only else 0):
        logger.info("Checkpoint reason_only differs; starting from beginning")
        cp = default_checkpoint

    # Normalize cursor from MongoDB (may return datetime/other types)
    last_created_at = cp.get("last_created_at")
    last_id = cp.get("last_id")
    if last_created_at is not None and not isinstance(last_created_at, str):
        last_created_at = last_created_at.isoformat() if hasattr(last_created_at, "isoformat") else str(last_created_at)
    if last_id is not None and not isinstance(last_id, str):
        last_id = str(last_id)
    total_moved = int(cp.get("total_moved") or 0)
    if last_created_at is not None or last_id is not None:
        logger.info("Resuming from checkpoint: total_moved so far=%s", total_moved)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        with get_db_connection() as conn:
            while not _shutdown_requested:
                n, last_created_at, last_id = migrate_batch(
                    conn, batch_size, reason_only, last_created_at, last_id
                )
                if n == 0:
                    break
                total_moved += n
                logger.info("Moved batch of %s rows (total so far: %s)", n, total_moved)
                save_checkpoint(
                    MONGO_COLLECTION,
                    CHECKPOINT_ID,
                    {
                        "last_created_at": last_created_at,
                        "last_id": last_id,
                        "total_moved": total_moved,
                        "reason_only": 1 if reason_only else 0,
                    },
                )
                if _shutdown_requested:
                    logger.info("Checkpoint saved; exiting")
                    break
                if sleep_between_batches > 0 and not _shutdown_requested:
                    logger.info("Sleeping for %s seconds before next batch", sleep_between_batches)
                    time.sleep(sleep_between_batches)
    finally:
        close_checkpoint_client()

    logger.info("Migration complete. Total rows moved: %s", total_moved)


if __name__ == "__main__":
    main()
