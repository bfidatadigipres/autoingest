# autoingest

## Getting started

### Installing dependencies

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

You can run Dagster in two modes: **development** (single command, ideal for local work and testing) or **production** (persistent daemon + webserver, survivable across reboots).

#### Development mode

One terminal, one process. Sensors, schedules, and the Dagit UI all run together:

```bash
dagster dev -h 0.0.0.0 -p 3000
```

Stopping the process stops everything. Not suitable for production — sensors will not run when the terminal closes.

#### Production mode (recommended for live deployment)

Two long-running processes, typically managed as systemd services so they survive reboots and can be monitored independently.

**Process 1 — Daemon:** Runs sensors (`watch_folder`, `validation_folder`), schedules, and the run queue. Must be running continuously.

```bash
DAGSTER_HOME=/opt/dagster/home dagster-daemon run
```

**Process 2 — Webserver:** Serves the Dagit UI.

```bash
DAGSTER_HOME=/opt/dagster/home dagster-webserver -h 0.0.0.0 -p 3000
```

**As systemd services** (survive reboots, start on boot):

Create `/etc/systemd/system/dagster-daemon.service`:

```ini
[Unit]
Description=Dagster Daemon
After=network.target

[Service]
Type=simple
User=your-username
WorkingDirectory=/home/your-username/autoingest
Environment=DAGSTER_HOME=/opt/dagster/home
Environment=PATH=/home/your-username/autoingest/.venv/bin:/usr/bin
ExecStart=/home/your-username/autoingest/.venv/bin/dagster-daemon run
Restart=always

[Install]
WantedBy=multi-user.target
```

And `/etc/systemd/system/dagster-webserver.service`:

```ini
[Unit]
Description=Dagster Webserver
After=network.target

[Service]
Type=simple
User=your-username
WorkingDirectory=/home/your-username/autoingest
Environment=DAGSTER_HOME=/opt/dagster/home
Environment=PATH=/home/your-username/autoingest/.venv/bin:/usr/bin
ExecStart=/home/your-username/autoingest/.venv/bin/dagster-webserver -h 0.0.0.0 -p 3000
Restart=always

[Install]
WantedBy=multi-user.target
```

Then enable and start both:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now dagster-daemon dagster-webserver
```

Check status with `systemctl status dagster-daemon dagster-webserver`.

> **Note:** The daemon and webserver read configuration from `DAGSTER_HOME` (e.g. `/opt/dagster/home/dagster.yaml` and `workspace.yaml`). Copy your config files there before starting:
> ```bash
> sudo mkdir -p /opt/dagster/home
> sudo cp dagster.yaml workspace.yaml /opt/dagster/home/
> ```
> Once running in production, you can safely remove `dagster.yaml` from the project folder — having it in both locations produces a warning that the copy in the project folder is being ignored. The same applies to `workspace.yaml`.

### Starting Celery Workers (for distributed transcoding)

On each worker server, install the project and run:

```bash
pip install -e ".[dev]"
source .venv/bin/activate
export CELERY_BROKER_URL=redis://:password@redis-server:6379/0
export CELERY_RESULT_BACKEND=redis://:password@redis-server:6379/1
dagster-celery worker start -A dagster_celery.app -q checksum,encoding -c 1
```

Workers need the same Redis credentials as the control server. Queue mapping:

| Queue | Ops | Concurrency guidance |
|---|---|---|
| `checksum` | `generate_checksum` | High — I/O bound, deploy ~20 workers per node |
| `encoding` | `encode_proxy_mp4`, `generate_images` | Low — CPU-bound (one FFmpeg per worker slot) |

For 80-thread servers running 40 concurrent encodes, per-node examples:
```bash
# checksum workers (high throughput)
dagster-celery worker start -A dagster_celery.app -q checksum -c 20

# encoding workers (1 FFmpeg per slot)
dagster-celery worker start -A dagster_celery.app -q encoding -c 40
```

If your Redis password contains special characters, URL-encode it first:

```bash
python3 -c "import urllib.parse; print(urllib.parse.quote_plus('password'))"
```

Redis must be bound to `0.0.0.0` and firewalls must allow TCP 6379 from both the control server and all workers.

### Pipeline viewer

The project includes a Flask-based file viewer at `autoingest/app/` that shows the `file_catalogue` table with auto-refresh, status badges, and per-row action buttons (⚠ Actions column) to:
- **↻ Re-ingest** — reset a file to `No Status` and move it back to the watch folder for re-processing
- **↺ Validator reset** — return a file to pre-verification state (`File cleared for ingest`), keeping `bp_job_id` but clearing tape checksums/version ID
- **✕ Delete** — remove the database row entirely

Also hosts the KLC viewer on the same port under `/klc`.

```bash
pip install flask
source .venv/bin/activate
export WORKFLOW_PG_HOST=...
export WORKFLOW_PG_USERNAME=...
export WORKFLOW_PG_PASSWORD=...
export WORKFLOW_PG_DB=...
export CONFLUENCE_URL=https://your-confluence.example.com       # optional
export SERVICE_DESK_URL=https://your-servicedesk.example.com      # optional
export KLC_HELP_URL=https://your-confluence.example.com/display   # optional — KLC guidance links
python -m autoingest.app
```

Opens on `http://localhost:5050` (and `/klc` for the KLC viewer). Pages auto-refresh every 30 seconds (a 5-minute meta-refresh also applies to `/klc`).

### KLC File Progress Viewer

A read-only Flask Blueprint at `/klc` designed for KLC colleagues to review file progress. Features:

- **9-column table** — File Name, Status, Error, Storage, Size in GB, Media Type, MD5 checksum, File format, Last updated
- **Search** by file name, status, or error message (debounced 350ms)
- **Storage filter** — dropdown for qnap_01 through qnap_11 paths (prefix-matched to support subpaths)
- **Error filter** — show files with errors, without errors, or all
- **Error tooltips** — hover over ⚠ to see full error text with matched guidance from a built-in pattern lookup; the error text itself is a clickable link to the relevant Confluence guidance page (opens in new tab)
- **Dark theme** — `#444` page background, `#666` brand bar with BFI logo, white bold title, yellow Refresh button, Service Desk link
- **No write operations** — read-only viewer, no refresh/delete buttons

Env vars: `SERVICE_DESK_URL` (enables Service Desk button), `KLC_HELP_URL` (base URL for error guidance links).

### Pipeline Dashboard (Streamlit)

A Streamlit-based monitoring dashboard at `autoingest/dashboard/` with 5 tabs:

| Tab | Contents |
|---|---|
| Overview | 6 metric cards (files today, completed, errored, GB processed, avg encode, avg total), status distribution bar chart, source pie chart, recent activity table |
| Performance | Encode time histogram, stage timing bar chart, timing summary table, top-20 slowest encodes |
| Throughput | Files + GB per hour (7 days), per day (30 days), total ingest latency box plot |
| Errors | Error distribution bar chart, files-with-errors table (100 most recent) |
| File Lookup | Search by file name → file details, all pipeline runs with run IDs, per-stage timing bar chart, raw pipeline_events JSON dump |

```bash
pip install streamlit plotly pandas
source .venv/bin/activate
export WORKFLOW_PG_HOST=...
export WORKFLOW_PG_USERNAME=...
export WORKFLOW_PG_PASSWORD=...
export WORKFLOW_PG_DB=...
export DASHBOARD_REFRESH=60          # optional — auto-refresh seconds (default 60)
export DASHBOARD_MAX_ROWS=500        # optional — max rows in data tables (default 500)
streamlit run autoingest/dashboard/app.py --server.port 8501
```

Opens on `http://localhost:8501`. The File Lookup tab is especially useful for investigating problem files — it collates all run IDs and per-stage timings for any file name into a single view.

For production, run as a systemd service (see the Daemon / Webserver examples above — same pattern, different `ExecStart`).

### Running tests

```bash
source .venv/bin/activate
pytest
```

See [`tests/README.md`](tests/README.md) for detailed test documentation.

## Forking & dependencies

This project was built for the BFI National Archive's digital preservation pipeline. If you are forking it for your own use, the following dependencies will require adaptation or replacement.

### Proprietary / hard-to-replace services

| Dependency | Location in code | What it does | Workaround |
|---|---|---|---|
| **Adlib API v3.7** (CID collections management) | `resources/adlib.py` — all functions (`get`, `post`, `retrieve_record`, `create_record_data`, etc.) | Collection catalogue lookups, media record creation, data appends | Replace `adlib.py` with your own catalogue API client. Consumer files: `file_assessment.py` (item priref lookup, file-type matching), `verification.py` (media record creation), `source_deletion.py` (media data append), `cid_metadata_update.py` (87-field FIELDS metadata enrichment), `utils.py` (`fetch_item_priref`, `check_file_has_media_rec`, `cid_media_append`) |
| **Spectra Logic Black Pearl** (ds3 SDK) | `resources/bp_utils.py` — 16 functions backed by `ds3.createClientFromEnv()` | Tape archival, object verification, checksum comparison, deletion | Replace `bp_utils.py` with your own storage backend. Modul-level `CLIENT` and `HELPER` objects initialise on import, so this module must be present (or stubbed) even if unused. Consumer files: `file_assessment.py` (bucket mapping), `verification.py` (tape confirmation/checksum/deletion) |

### Infrastructure services

The pipeline expects these services to be reachable at startup. Without them, Dagster will fail to initialise.

| Service | Purpose | Env vars | Config file |
|---|---|---|---|
| **PostgreSQL** (workflow database) | Pipeline tracking — file status, metadata, checksums | `WORKFLOW_PG_HOST`, `WORKFLOW_PG_PORT`, `WORKFLOW_PG_USERNAME`, `WORKFLOW_PG_PASSWORD`, `WORKFLOW_PG_DB` | `resources/database.py` |
| **PostgreSQL** (Dagster instance) | Dagster run history, event log, schedules | `DAGSTER_PG_HOST`, `DAGSTER_PG_PORT`, `DAGSTER_PG_USERNAME`, `DAGSTER_PG_PASSWORD`, `DAGSTER_PG_DB` | `dagster.yaml` |
| **Redis** | Celery task queue broker and result backend | `CELERY_BROKER_URL` (default `redis://localhost:6379/0`), `CELERY_RESULT_BACKEND` (default `redis://localhost:6379/1`) | `celery_config.py` |

> **Network note:** If PostgreSQL or Redis run on a separate server, you must:
> 1. Bind them to all interfaces — Redis: `bind 0.0.0.0` in `redis.conf`; PostgreSQL: `listen_addresses = '*'` in `postgresql.conf`
> 2. Allow the client subnet in authentication — Redis: `requirepass`; PostgreSQL: add `host all all <subnet> md5` to `pg_hba.conf`
> 3. Open TCP 6379 (Redis) and TCP 5432 (PostgreSQL) through any firewalls

### System binaries (must be installed on the control server and every worker)

These are called directly via `subprocess`. Python pip packages alone will **not** suffice.

| Binary | Called from | Default path env var | Notes |
|---|---|---|---|
| `ffmpeg` | `proxy_utils.py` (encoding, JPEG extraction) | `FFMPEG_PATH=/usr/bin/ffmpeg` | Required for all proxy encoding |
| `ffprobe` | `utils.py`, `proxy_utils.py` (stream probing) | `FFPROBE_PATH=/usr/bin/ffprobe` | Ships with FFmpeg |
| `mediainfo` | `utils.py`, `proxy_utils.py` (metadata extraction) | hardcoded on PATH | Called in ~20 places across the codebase |
| `mediaconch` | `utils.py` (`get_mediaconch`) | hardcoded on PATH | Optional — only if `MP4_POLICY` is set |
| `gm` (GraphicsMagick) | `proxy_utils.py` (`make_jpg`) | hardcoded on PATH | Image resizing / thumbnail creation |
| `exiftool` | `utils.py` (`exif_data`), `extract_metadata.py` (autoingest pipeline) | Legacy — used for image metadata extraction |

Install on Ubuntu:
```bash
sudo apt install ffmpeg mediainfo mediaconch graphicsmagick libimage-exiftool-perl
```

### Python dependencies with native extensions

- `python-magic` / `libmagic` — file-type detection via `file_assessment.py::check_mime_type()`
- `xxhash` — fast non-cryptographic hashing
- `psycopg2-binary` — PostgreSQL driver

### JSON configuration files (by convention)

These files must exist at the paths derived from environment variables. They are **not created by the project** — you must provision them.

| File | Read by | Format | Example |
|---|---|---|---|
| `$LOG_PATH/downtime_control.json` | `utils.py::get_current_api()` (line 534) | `{"current_api": "<ENV_VAR_NAME>"}` — the env var whose value is the Adlib API URL | `{"current_api": "CID_API_URL"}` with `CID_API_URL=http://adlib.example.com/` |
| `$LOG_PATH/storage_control.json` | `utils.py::check_storage()` (line 115) | `{"all_storage_on": true, "/mnt/path": true}` | Controls which ingest paths are active |
| `$DPI_BUCKET` | `utils.py::get_buckets()`, `bp_utils.py::get_buckets()` (line 572) | JSON mapping donor names → Black Pearl bucket names with active flags | `{"preservation0": true, "imagen": true, "netflix0": true}` |

### Complete environment variable reference

Variables marked **Required** will raise a `KeyError` at import time if missing. Variables with defaults are safe to omit.

| Variable | Required? | Default | Used in | Purpose |
|---|---|---|---|---|
| `WORKFLOW_PG_HOST` | **Yes** | — | `resources/database.py` | Workflow DB host |
| `WORKFLOW_PG_PORT` | No | `5432` | `resources/database.py` | Workflow DB port |
| `WORKFLOW_PG_USERNAME` | **Yes** | — | `resources/database.py` | Workflow DB user |
| `WORKFLOW_PG_PASSWORD` | **Yes** | — | `resources/database.py` | Workflow DB password |
| `WORKFLOW_PG_DB` | **Yes** | — | `resources/database.py` | Workflow DB name |
| `DAGSTER_PG_HOST` | **Yes** | — | `dagster.yaml` | Dagster instance DB host |
| `DAGSTER_PG_PORT` | **Yes** | — | `dagster.yaml` | Dagster instance DB port |
| `DAGSTER_PG_USERNAME` | **Yes** | — | `dagster.yaml` | Dagster instance DB user |
| `DAGSTER_PG_PASSWORD` | **Yes** | — | `dagster.yaml` | Dagster instance DB password |
| `DAGSTER_PG_DB` | **Yes** | — | `dagster.yaml` | Dagster instance DB name |
| `DAGSTER_COMPUTE_LOG_DIR` | **Yes** | — | `dagster.yaml` | Dagster compute log directory |
| `CELERY_BROKER_URL` | No | `redis://localhost:6379/0` | `celery_config.py` | Redis broker URL |
| `CELERY_RESULT_BACKEND` | No | `redis://localhost:6379/1` | `celery_config.py` | Redis result backend |
| `FFMPEG_PATH` | No | `/usr/bin/ffmpeg` | `resources/encoding.py` | FFmpeg binary path |
| `FFPROBE_PATH` | No | `/usr/bin/ffprobe` | `resources/encoding.py` | ffprobe binary path |
| `ENCODING_THREAD_COUNT` | No | `0` (auto) | `resources/encoding.py` | FFmpeg thread count |
| `PROXY_OUTPUT_PATH` | No | `/mnt/proxy` | `resources/encoding.py` | Proxy file output directory |
| `WATCH_FOLDER_PATHS` | No | `""` | `sensors/watch_folder.py` | Comma-separated ingest watch directories |
| `VALIDATION_FOLDER_PATHS` | No | `""` | `sensors/validation_folder.py` | Comma-separated validation watch directories |
| `MP4_POLICY` | No | `None` | `ops/celery/proxy_video.py` | MediaConch XML policy file path |
| `VIEWER_PORT` | No | `5050` | `app/__main__.py` | Flask viewer + KLC viewer port |
| `CONFLUENCE_URL` | No | `""` | `app/__init__.py` | Existing viewer Help button URL |
| `SERVICE_DESK_URL` | No | `""` | `app/__init__.py` | Service Desk button URL (viewer + KLC) |
| `KLC_HELP_URL` | No | `""` | `app/__init__.py` | KLC error guidance links base URL |
| `LOG_PATH` | No | `""` | `utils.py`, `verification.py` | Base directory for control JSON and Black Pearl log files |
| `DPI_BUCKET` | **Yes** (bp_utils) | — | `utils.py`, `bp_utils.py` | Path to Black Pearl bucket-config JSON file |
| `JSON_END_POINT` | **Yes** (bp_utils) | — | `bp_utils.py` | Black Pearl notification endpoint URL |
| `CID_API3` | **Yes** | — | `verification.py`, `file_assessment.py`, `cid_metadata_update.py`, `utils.py` | Adlib REST API base URL |
| `CID_API_URL` | **Yes** (indirect) | — | `utils.py` (via `downtime_control.json`) — **deprecated** | Legacy API URL ref; prefer `CID_API3` |
| `DS3_ENDPOINT` `DS3_ACCESS_KEY` `DS3_SECRET_KEY` | **Yes** (ds3 SDK) | — | `bp_utils.py` (implicit via `ds3.createClientFromEnv()`) | Black Pearl S3 credentials |
| `DASHBOARD_REFRESH` | No | `60` | `dashboard/config.py` | Streamlit dashboard auto-refresh interval (seconds) |
| `DASHBOARD_MAX_ROWS` | No | `500` | `dashboard/config.py` | Max rows in dashboard data tables |

### What to strip or replace when forking

1. **`resources/adlib.py`** — Replace with your own catalogue API module. Provides record lookups, media-record creation, and XML payload building that three ops depend on.
2. **`resources/bp_utils.py`** — Replace with your own archival backend. Covers bucket mapping, tape confirmation, checksum verification, object deletion. The module-level `CLIENT` init also requires stubbing if the ds3 SDK is absent.
3. **`resources/utils.py`** — Trim or stub functions that reach out to the above: `fetch_item_priref`, `check_file_has_media_rec`, `cid_media_append`, `get_buckets`, `get_buckets_blob`. The remaining utility functions (filename parsing, checksums, mediainfo wrappers, `exif_data`) are broadly reusable.
4. **`ops/local/file_assessment.py`** — Lines referencing `bp.get_buckets()`, `adlib.retrieve_record()`, `adlib.retrieve_field_name()`, `utils.fetch_item_priref()`, `utils.check_file_has_media_rec()` will need porting to your replacement services.
5. **`ops/local/verification.py`** — Black Pearl tape verification and CID media-record creation will need to map to your archival + catalog system.
6. **`ops/local/source_deletion.py`** — The `utils.cid_media_append()` call updates the catalogue record. Replace with your own catalogue update.
7. **`ops/local/cid_metadata_update.py`** — Builds XML payloads from the FIELDS dictionary (87 fields across container/video/audio/other/text/image) and POSTs to CID. Replace with your own metadata enrichment pipeline. Uses `adlib.post()` and `adlib.create_record_data()`.
7. **`setup_database.sql`** — The schema is specific to this pipeline's tracking tables (`file_catalogue`, `file_tracking`, etc.). Reuse if you want the same status-tracking model, or replace entirely.
8. **JSON config files** — `downtime_control.json`, `storage_control.json`, and the `DPI_BUCKET` config file all need to exist at configured paths. Either remove the code paths that read them or provision the files.

## Learn more

To learn more about this project and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
