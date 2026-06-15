# autoingest — Dagster-based media ingest pipeline

## Tech Stack
- **Python 3.13** — project language
- **Dagster** — orchestration (ops/jobs/sensors/resources)
- **Celery + Redis** — task queue for heavy ops (encoding)
- **PostgreSQL** — workflow tracking & Dagster instance storage
- **Adlib API v3.7** — CID collections management system
- **SpectraLogic Black Pearl** — tape archival via `ds3` SDK
- **FFmpeg/ffprobe, MediaInfo, MediaConch, GraphicsMagick, ExifTool** — media processing
- **pip** — dependency management (pyproject.toml for manifest)

## Project Structure
```
autoingest/
├── pyproject.toml           # Dependencies & Dagster entry point
├── dagster.yaml             # Dagster instance config (Postgres, QueuedRunCoordinator)
├── workspace.yaml           # Loads autoingest.definitions
├── celery_config.py         # Celery broker/backend (Redis, JSON serialization)
├── setup_database.sql       # Full DB schema (pipeline_events, file_tracking, file_type_config)
├── autoingest/
│   ├── definitions.py       # Dagster Definitions (jobs + sensors + resources)
│   ├── sensors/
│   │   ├── watch_folder.py          # Polls directories every 30s, triggers ingest job
│   │   └── validation_folder.py     # Polls validation dirs, triggers validation job
│   ├── jobs/
│   │   ├── single_file_ingest.py    # assess → extract → checksum → catalogue
│   │   ├── validation_job.py        # verify tape → encode proxy → images → cleanup
│   │   └── cleanup_job.py           # sweep & delete completed files
│   ├── ops/
│   │   ├── ingest/
│   │   │   ├── file_assessment.py       # Filename validation, CID lookup, donor detection
│   │   │   └── metadata_extraction.py   # MediaInfo, MD5, XXHash32
│   │   ├── catalogue/
│   │   │   └── db_documentation.py      # DB insert, file move to processing
│   │   ├── archive/
│   │   │   └── verification.py          # BP tape verification, CID media record creation
│   │   ├── encoding/
│   │   │   ├── proxy_video.py           # FFmpeg H.264 proxy, MediaConch policy check
│   │   │   └── proxy_images.py          # JPEG + thumbnail via GraphicsMagick
│   │   └── cleanup/
│   │       └── source_deletion.py       # CID update, source file deletion
│   └── resources/
│       ├── database.py          # WorkflowDatabase (psycopg2)
│       ├── encoding.py          # FFmpeg paths, thread count
│       ├── utils.py             # ~30 shared utilities (checksums, MIME, MediaInfo, CID helpers)
│       ├── adlib.py             # Adlib v3.7 REST client (tenacity retries)
│       ├── bp_utils.py          # Black Pearl (ds3 SDK) client
│       ├── celery_client.py     # Dagster Celery executor config
│       └── proxy_utils.py       # FFmpeg filter chains, image processing
```

## Key Conventions

### Two-Phase Pipeline
1. **Ingest** (`single_file_ingest_job`): assess → extract metadata → checksum → catalogue (DB + file move)
2. **Validation** (`validation_job`): verify BP tape → encode proxy → generate images → cleanup source

Jobs triggered by cursor-based sensors polling watch folders every 30s.

### Donor Model
Files classified by donor (BFI, Netflix, Amazon, Disney). Non-BFI skip proxy encoding. Path prefix drives classification (e.g., `/ingest/netflix/`).

### Part/Whole Multipart Files
Convention: `<OBJECT_NUMBER>_<PART>of<WHOLE>.<ext>`. Pipeline validates ordering and ensures prior parts ingested first.

### Database-Centric Tracking
Each file tracked in `file_catalogue` with status through pipeline stages. Statuses: `"No Status"`, `"Failed assessment"`, `"File cleared for ingest"`, `"complete"`.

### Error Handling
Ops return empty dict `{}` on non-fatal failures instead of raising. Callers check for expected keys. Encoding failures raise `RuntimeError`.

### Celery Routing
Heavy ops tagged `{"dagster-celery/queue": "encoding"}` for dedicated workers. Validation job uses Celery executor.

### Environment Variables
Nearly all config via env vars (prefixed `WORKFLOW_PG_`, `DAGSTER_PG_`, `CELERY_`, `FFMPEG_PATH`, `FFPROBE_PATH`, `WATCH_FOLDER_PATHS`, `VALIDATION_FOLDER_PATHS`, `PROXY_OUTPUT_PATH`, `LOG_PATH`, etc.)

### Legacy Code
`database_old.py` and `celery_client_old.py` kept for reference. Use the non-old versions.

## Running

**Development** (single process, all in one):
```bash
dagster dev -w workflow -h 0.0.0.0 -p 3000
```

**Production** (two services, persistent):
```bash
# Terminal 1: daemon (sensors + schedules + run queue)
DAGSTER_HOME=/opt/dagster/home dagster-daemon run

# Terminal 2: webserver (Dagit UI)
DAGSTER_HOME=/opt/dagster/home dagster-webserver -h 0.0.0.0 -p 3000
```

Or as systemd services — see `dagster-daemon.service` / `dagster-webserver.service` examples above.

## Testing
```bash
pytest tests/ -v
```

## License
MIT — BFI National Archive, 2026.
