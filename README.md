# Notification Log Errors Migration

**Standalone one-time migration**: move FCM error rows from `public.notification_logs` to `public.notification_log_errors`. No monorepo dependency; runs on any server with PostgreSQL and MongoDB. **Resumable**: progress is saved to MongoDB after each batch; on failure or SIGINT/SIGTERM, re-run to continue from where you left off.

## Overview

- **`migrate_notification_log_errors.py`** — Selects error rows (by known FCM error reasons or by `status = 2`), copies them to `notification_log_errors`, then deletes from `notification_logs`. Batched; optional dry-run.

- **Default (reason-only):** Migrates rows whose `reason` matches known FCM error strings (e.g. `NotRegistered`, `UNREGISTERED`, `Invalid value at%`, etc.), any status 1/2/3.

- **`--all-failures`:** Migrates all rows with `status = 2` regardless of reason.

**Prerequisite:** Table `public.notification_log_errors` must exist (same columns as `notification_logs` for the migrated fields). DDL lives in the main repo: `apps/notification_service/docs/notification_log_errors_table.sql`.

## Structure

```
notification-log-errors-migration/
├── migrate_notification_log_errors.py   # Main script (cursor-based + MongoDB checkpoint)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
├── .env.example
├── .gitignore
└── utils/
    ├── __init__.py
    ├── db.py
    └── checkpoint.py
```

## Prerequisites

1. **Environment** — Copy `.env.example` to `.env` and set:

      **Required:**

   - `DB_HOST`, `DB_DATABASE`, `DB_USER`, `DB_PASSWORD`, `DB_PORT` — PostgreSQL for `notification_logs`
   - `MONGO_URI` — MongoDB for checkpoint (resumability)

   **Optional:** `MONGO_DB_NAME` (default: `checkpoint_db`), `MONGO_CHECKPOINT_COLLECTION` (default: `notification_log_errors_migration`), `DB_CONNECTION_TIMEOUT` (default: 30).

2. **Table** — Ensure `public.notification_log_errors` is created before running.

## Usage

### Dry run (count only)

```bash
python migrate_notification_log_errors.py --dry-run
```

### Run migration (reason-only, default)

```bash
python migrate_notification_log_errors.py
```

### Migrate all status=2 rows

```bash
python migrate_notification_log_errors.py --all-failures
```

### Options

- `--dry-run` — Count eligible rows only; no copy/delete.
- `--batch-size N` — Rows per batch (default: 50000).
- `--all-failures` — Match `status = 2` only; default is match by known FCM error reasons.

## Deploy to GitHub

1. **Create a repo** on GitHub (e.g. `migrate_notification_error_logs`).

2. **Push this project** (from your machine):

   ```bash
   cd /path/to/migrate_notification_error_logs
   git remote add origin https://github.com/YOUR_USERNAME/migrate_notification_error_logs.git
   git add .
   git commit -m "Initial commit: notification log errors migration"
   git branch -M main
   git push -u origin main
   ```

   `.env` is in `.gitignore` — do **not** commit it. Set env vars on the machine where you run the script (see below).

3. **On the server (or any machine) where you want to run the migration:**

   ```bash
   git clone https://github.com/YOUR_USERNAME/migrate_notification_error_logs.git
   cd migrate_notification_error_logs
   cp .env.example .env
   # Edit .env with DB_* and MONGO_* values
   ```

   Then run the script using one of the options below.

## Deployment (standalone server)

### With Docker Compose

**Build the image and run with default command** (runs the migration once, then container exits):

```bash
cp .env.example .env
# Edit .env
docker-compose up --build -d
```

**Run with custom args** — use `docker-compose run` to override the command (one-off container, then removed with `--rm`):

```bash
# Dry run (count only)
docker-compose run --rm migrate-notification-log-errors python migrate_notification_log_errors.py --dry-run

# Migration with custom batch size
docker-compose run --rm migrate-notification-log-errors python migrate_notification_log_errors.py --batch-size 10000

# Migrate all status=2 rows
docker-compose run --rm migrate-notification-log-errors python migrate_notification_log_errors.py --all-failures

# Full migration (default reason-only, batch size from BATCH_SIZE env)
docker-compose run --rm migrate-notification-log-errors python migrate_notification_log_errors.py
```

### With Docker

```bash
docker build -t notification-log-errors-migration .
docker run --rm --env-file .env notification-log-errors-migration
```

### Local (no Docker)

```bash
pip install -r requirements.txt
# Set env (e.g. export $(cat .env | xargs) or rely on .env via dotenv)
python migrate_notification_log_errors.py --dry-run
python migrate_notification_log_errors.py
```

## Checkpoints (MongoDB)

- **Collection:** `MONGO_CHECKPOINT_COLLECTION` (default: `notification_log_errors_migration`)
- **Document `_id`:** `migrate_notification_log_errors`
- **Stored:** `last_created_at`, `last_id` (cursor for next batch), `total_moved`, `reason_only`, `last_updated`

On failure or Ctrl+C / SIGTERM, the script saves the current cursor and exits. Re-run with the same flags to resume.

## Reference

- Same layout and checkpoint pattern as **`inventory-fields-backfill/`** in the monorepo.
- Error table DDL: `apps/notification_service/docs/notification_log_errors_table.sql` (in main repo).
