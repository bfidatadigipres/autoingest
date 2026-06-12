# autoingest

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

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

Navigate terminal into your autoingest/ folder. Start the Dagster UI web server:

```bash
dagster dev -h 0.0.0.0 -p 3000
```

Open http://localhost:3000 in your browser to see the project. Local host and port can be adjusted as needed.

### Starting Celery Workers (for distributed transcoding)

On each worker server, install the project and run:

```bash
uv sync
source .venv/bin/activate
dagster-celery worker -A dagster_celery.app
```

Workers need the same environment variables as the control server to connect to Redis and PostgreSQL.

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
| **Adlib API v3.7** (CID collections management) | `resources/adlib.py` — all functions (`get`, `post`, `retrieve_record`, `create_record_data`, etc.) | Collection catalogue lookups, media record creation, data appends | Replace `adlib.py` with your own catalogue API client. Consumer files: `file_assessment.py` (item priref lookup, file-type matching), `verification.py` (media record creation), `cleanup/source_deletion.py` (media data append), `utils.py` (`fetch_item_priref`, `check_file_has_media_rec`, `get_media_input_date`, `cid_media_append`) |
| **Spectra Logic Black Pearl** (ds3 SDK) | `resources/bp_utils.py` — 16 functions backed by `ds3.createClientFromEnv()` | Tape archival, object verification, checksum comparison, deletion | Replace `bp_utils.py` with your own storage backend. Modul-level `CLIENT` and `HELPER` objects initialise on import, so this module must be present (or stubbed) even if unused. Consumer files: `file_assessment.py` (bucket mapping), `verification.py` (tape confirmation/checksum/deletion) |

### Infrastructure services

The pipeline expects these services to be reachable at startup. Without them, Dagster will fail to initialise.

| Service | Purpose | Env vars | Config file |
|---|---|---|---|
| **PostgreSQL** (workflow database) | Pipeline tracking — file status, metadata, checksums | `WORKFLOW_PG_HOST`, `WORKFLOW_PG_PORT`, `WORKFLOW_PG_USERNAME`, `WORKFLOW_PG_PASSWORD`, `WORKFLOW_PG_DB` | `resources/database.py` |
| **PostgreSQL** (Dagster instance) | Dagster run history, event log, schedules | `DAGSTER_PG_HOST`, `DAGSTER_PG_PORT`, `DAGSTER_PG_USERNAME`, `DAGSTER_PG_PASSWORD`, `DAGSTER_PG_DB` | `dagster.yaml` |
| **Redis** | Celery task queue broker and result backend | `CELERY_BROKER_URL` (default `redis://localhost:6379/0`), `CELERY_RESULT_BACKEND` (default `redis://localhost:6379/1`) | `celery_config.py` |

### System binaries (must be installed on the control server and every worker)

These are called directly via `subprocess`. Python pip packages alone will **not** suffice.

| Binary | Called from | Default path env var | Notes |
|---|---|---|---|
| `ffmpeg` | `proxy_utils.py` (encoding, JPEG extraction) | `FFMPEG_PATH=/usr/bin/ffmpeg` | Required for all proxy encoding |
| `ffprobe` | `utils.py`, `proxy_utils.py` (stream probing) | `FFPROBE_PATH=/usr/bin/ffprobe` | Ships with FFmpeg |
| `mediainfo` | `utils.py`, `proxy_utils.py` (metadata extraction) | hardcoded on PATH | Called in ~20 places across the codebase |
| `mediaconch` | `utils.py` (`get_mediaconch`) | hardcoded on PATH | Optional — only if `MP4_POLICY` is set |
| `gm` (GraphicsMagick) | `proxy_utils.py` (`make_jpg`) | hardcoded on PATH | Image resizing / thumbnail creation |
| `exiftool` | `utils.py` (`exif_data`) | hardcoded on PATH | Legacy — used in one code path |

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
| `MP4_POLICY` | No | `None` | `ops/encoding/proxy_video.py` | MediaConch XML policy file path |
| `LOG_PATH` | No | `""` | `utils.py`, `verification.py` | Base directory for control JSON and Black Pearl log files |
| `DPI_BUCKET` | **Yes** (bp_utils) | — | `utils.py`, `bp_utils.py` | Path to Black Pearl bucket-config JSON file |
| `JSON_END_POINT` | **Yes** (bp_utils) | — | `bp_utils.py` | Black Pearl notification endpoint URL |
| `CID_API_URL` | **Yes** (indirect) | — | `utils.py` (via `downtime_control.json`) | Adlib REST API base URL — the exact name is set inside `downtime_control.json` |
| `DS3_ENDPOINT` `DS3_ACCESS_KEY` `DS3_SECRET_KEY` | **Yes** (ds3 SDK) | — | `bp_utils.py` (implicit via `ds3.createClientFromEnv()`) | Black Pearl S3 credentials |

### What to strip or replace when forking

1. **`resources/adlib.py`** — Replace with your own catalogue API module. Provides record lookups, media-record creation, and XML payload building that three ops depend on.
2. **`resources/bp_utils.py`** — Replace with your own archival backend. Covers bucket mapping, tape confirmation, checksum verification, object deletion. The module-level `CLIENT` init also requires stubbing if the ds3 SDK is absent.
3. **`resources/utils.py`** — Trim or stub functions that reach out to the above: `fetch_item_priref`, `check_file_has_media_rec`, `get_media_input_date`, `cid_media_append`, `get_current_api`, `get_buckets`, `get_buckets_blob`. The remaining utility functions (filename parsing, checksums, mediainfo wrappers) are broadly reusable.
4. **`ops/ingest/file_assessment.py`** — Lines referencing `bp.get_buckets()`, `adlib.retrieve_record()`, `adlib.retrieve_field_name()`, `utils.fetch_item_priref()`, `utils.check_file_has_media_rec()`, `bp.check_no_bp_status()` will need porting to your replacement services.
5. **`ops/archive/verification.py`** — Black Pearl tape verification and CID media-record creation will need to map to your archival + catalog system.
6. **`ops/cleanup/source_deletion.py`** — The `utils.cid_media_append()` call at the end updates the catalogue record. Replace with your own catalogue update.
7. **`setup_database.sql`** — The schema is specific to this pipeline's tracking tables (`file_catalogue`, `file_tracking`, etc.). Reuse if you want the same status-tracking model, or replace entirely.
8. **JSON config files** — `downtime_control.json`, `storage_control.json`, and the `DPI_BUCKET` config file all need to exist at configured paths. Either remove the code paths that read them or provision the files.

## Learn more

To learn more about this project and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
