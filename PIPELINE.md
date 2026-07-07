# Autoingest Pipeline Reference

## Autoingest folder structure

```
autoingest/                                             Top level folder
│
├── ingest/                                             Single ingest point for colleagues
│   ├── autodetect/                                     Majority of files placed here for all paths
│   ├── amazon/                                         DMS option only
│   ├── disney/                                         DMS option only
│   ├── netflix/                                        DMS option only
│   ├── incomplete_scans/                               Film Operations option only
│   └── archive/                                        Screencraft option only
│
├── processing/                                         Automation PUT path
│   ├── bfi/                                            Majority of files placed in here
│   │   ├── ingest_2026-06-24_13-30-10/                 PUT scripts sort into 1TB batches
│   │   └── blob/                                       Any over 1TB isolated and PUT separately
│   ├── amazon/                                         DMS requirement only
│   │   └── blob/                                       Unlikely to be needed
│   ├── disney/                                         DMS requirement only
│   │   └── blob/                                       Unlikely to be needed
│   └── netflix/                                        DMS requirement only
│       └── blob/                                       Unlikely to be needed
│
└── validation/                                         Automation validation / transcode / deletion path
    └── 8571113e-d23a-4f59-9fd4-91ea60e47ace/           PUT batches completed (grouped to 1TB or single > 1TB)
```


## Project Structure

```
autoingest/
├── celery_config.py              Celery broker/backend, timeouts, worker lifecycle
├── dagster.yaml                  Dagster instance (Postgres, QueuedRunCoordinator)
├── workspace.yaml                Loads autoingest.definitions
├── pyproject.toml                Dependencies, Dagster entry point
├── setup_database.sql            Full DB schema (3 tables)
│
├── autoingest/
│   ├── definitions.py            Dagster Definitions — all jobs + sensors + resources
│   │
│   ├── sensors/                  Folder watchers & DB status-driven chain sensors
│   │   ├── watch_folder.py       → triggers ingest_local_job (30s poll)
│   │   ├── validation_folder.py  → triggers verify_local_job (30s poll)
│   │   └── chain_sensors.py      → 5 status-driven sensors (ingest→checksum→catalogue→
│   │                                  encoding→cleanup→metadata_update)
│   │
│   ├── jobs/                     Assembled from graphs — wired to executors
│   │   ├── ingest_jobs.py        ingest_local_job, ingest_celery_job, catalogue_local_job
│   │   ├── validation_jobs.py    verify_local_job, encoding_celery_job, cleanup_local_job,
│   │   │                         metadata_update_local_job
│   │   └── cleanup_job.py        Standalone sweep (periodic file deletion)
│   │
│   ├── graphs/                   Dagster @graph compositions (ops wired together)
│   │   ├── ingest_graphs.py      assess_filename → extract_metadata
│   │   │                         generate_checksum (standalone)
│   │   │                         create_catalogue_record (standalone)
│   │   ├── validation_graphs.py  verify_tape_copy (standalone)
│   │   │                         encode_proxy_mp4 → generate_images
│   │   │                         check_and_delete_source (standalone)
│   │   │                         update_cid_metadata (standalone)
│   │   └── cleanup_graph.py      sweep_completed_files (standalone)
│   │
│   ├── ops/
│   │   ├── local/                Ops that run on the control server (DATA15)
│   │   │   ├── file_assessment.py     assess_filename — validate filename, CID lookup
│   │   │   ├── extract_metadata.py    extract_metadata — MediaInfo + ExifTool extraction
│   │   │   ├── db_documentation.py    create_catalogue_record — DB insert, file move
│   │   │   ├── verification.py        verify_tape_copy — BP tape check, CID media create
│   │   │   ├── source_deletion.py     check_and_delete_source — CID append, source delete
│   │   │   ├── cid_metadata_update.py update_cid_metadata — MediaInfo/Exif enrichment
│   │   │   └── cleanup_sweep.py       sweep_completed_files — periodic deletion sweep
│   │   │
│   │   └── celery/               Ops tagged for Celery encoding workers
│   │       ├── checksum.py            generate_checksum — MD5 + XXHash32
│   │       ├── proxy_video.py         encode_proxy_mp4 — FFmpeg H.264 proxy + JPEG
│   │       └── proxy_images.py        generate_images — largeimage + thumbnail via GM
│   │
│   ├── resources/                Shared utilities and external service clients
│   │   ├── database.py           WorkflowDatabase — psycopg2, ALLOWED_FIELDS
│   │   ├── encoding.py           EncodingConfig — FFmpeg path, thread count, proxy output
│   │   ├── utils.py              ~30 shared functions — checksums, MIME, Mediainfo, CID
│   │   ├── adlib.py              Adlib v3.7 REST client — CID CRUD (tenacity retries)
│   │   ├── bp_utils.py           Black Pearl ds3 SDK — tape archive client
│   │   ├── proxy_utils.py        FFmpeg filter chains, audio detection, JPEG/GM operations
│   │   └── celery_client.py      Celery executor config
│   │
│   ├── app/                      Flask apps (viewer + KLC dashboard)
│   │   ├── __init__.py           App factory — registers both Blueprints
│   │   ├── __main__.py           Entry point (python -m autoingest.app, port 5050)
│   │   ├── routes.py             Existing viewer — /api/files, /api/stats, /api/refresh, /api/delete
│   │   ├── static/
│   │   │   ├── style.css         Existing viewer styles
│   │   │   └── klc.css           KLC viewer — dark theme, tooltips, filter bar
│   │   ├── templates/
│   │   │   └── index.html        Existing viewer template
│   │   └── klc/                  KLC File Progress Viewer (read-only, port 5050)
│   │       ├── __init__.py       Blueprint factory
│   │       ├── routes.py         /klc, /api/klc/files, /api/klc/stats, /api/klc/guidance
│   │       ├── templates/
│   │       │   └── klc.html              9-column table, search, filters, error tooltips
│   │       └── static/
│   │           └── klc.css       (in main static/ folder)
│   │
│   └── dashboard/                Streamlit Pipeline Monitor (port 8501)
│       ├── __init__.py
│       ├── config.py             DB connection, refresh interval env vars
│       ├── queries.py            16 SQL query functions for all tabs
│       ├── charts.py             7 Plotly chart builders
│       ├── file_view.py          File Lookup tab — search, runs, timings, raw events
│       └── app.py                Entry point — 5-tab layout, sidebar summaries
│
└── tests/                        pytest test suite
    ├── conftest.py
    ├── test_utils.py
    ├── test_proxy_utils.py
    ├── test_adlib.py
    ├── test_file_assessment.py
    └── test_verification.py
```


## File Processing Pipeline

```
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                           INGEST PHASE                                      │
 ├─────────────────────────────────────────────────────────────────────────────┤
 │                                                                             │
 │  ┌──────────────────┐     watch_folder_sensor (30s poll)                    │
 │  │  /ingest/<donor>/ │────────────────┐                                     │
 │  │   (watch folders) │                ▼                                     │
 │  └──────────────────┘     ┌──────────────────────┐                          │
 │                           │   ingest_local_job   │  [DATA15 — local]        │
 │                           │                      │                          │
 │                           │  assess_filename ────│──►  No Status → assessed │
 │                           │    · filename check  │       (or Failed assess) │
 │                           │    · CID item lookup │                          │
 │                           │    · donor/BP bucket │                          │
 │                           │    · part/whole      │                          │
 │                           │         │            │                          │
 │                           │         ▼            │                          │
 │                           │  extract_metadata ───│──►  assessed → assessed  │
 │                           │    · Mediainfo JSON  │    (metadata in DB)      │
 │                           │    · ExifTool TIF    │                          │
 │                           └──────────────────────┘                          │
 │                                     │                                       │
 │                            ingest_chain_sensor                              │
 │                            (watches: assessed)                              │
 │                                     │                                       │
 │                                     ▼                                       │
 │                      ┌────────────────────────────┐                         │
 │                      │    ingest_celery_job       │  [CELERY — checksum q]  │
 │                      │                            │                         │
 │                      │  generate_checksum ────────│──►  assessed → checksum │
 │                      │    · Guard: skip if        │    (or generating_      │
 │                      │      generating_checksum   │     checksum on retry)  │
 │                      │    · Claim: assessed →     │                         │
 │                      │      generating_checksum   │                         │
 │                      │    · MD5 + XXHash32        │                         │
 │                      │    · Rollback: generating_ │                         │
 │                      │      checksum → assessed   │                         │
 │                      │      on failure            │                         │
 │                      └────────────────────────────┘                         │
 │                                     │                                       │
 │                            catalogue_chain_sensor                           │
 │                            (watches: checksummed)                           │
 │                                     │                                       │
 │                                     ▼                                       │
 │     ┌──────────────────────────────────────────────────┐                    │
 │     │              catalogue_local_job                 │  [DATA15]          │
 │     │                                                  │                    │
 │     │  create_catalogue_record ────────────────────────│──► File cleared    │
 │     │    · Guard: skip if cataloguing or               │    for ingest      │
 │     │      status ≠ checksummed                        │                    │
 │     │    · Claim: checksummed → cataloguing            │                    │
 │     │    · DB upsert (all metadata committed)          │                    │
 │     │    · File moved: /ingest/ → /processing/<donor>/ │                    │
 │     │    · Rollback: cataloguing → checksummed         │                    │
 │     │      on upsert failure                           │                    │
 │     └──────────────────────────────────────────────────┘                    │
 │                                     │                                       │
 └─────────────────────────────────────┼───────────────────────────────────────┘
                                       │
                    [ Black Pearl PUT — external ]
                    File moved to /validation/<bp_job_id>/
                                       │
 ┌─────────────────────────────────────┼───────────────────────────────────────┐
 │                          VALIDATION PHASE                                   │
 ├─────────────────────────────────────────────────────────────────────────────┤
 │                                                                             │
 │                      validation_folder_sensor (30s poll)                    │
 │                      (watches /validate/<bp_job_id>/)                       │
 │                                     │                                       │
 │                                     ▼                                       │
 │                     ┌──────────────────────────────┐                        │
 │                     │     verify_local_job         │  [DATA15 — local]      │
 │                     │                              │                        │
 │                     │  verify_tape_copy ───────────│──► validating          │
 │                     │    · BP tapeList check       │   → verified           │
 │                     │    · Fixity (MD5 + length)   │   (or reingest)        │
 │                     │    · CID media record create │                        │
 │                     │    · Sets ingest_month       │                        │
 │                     │    · Sets verify_time_sec    │                        │
 │                     └──────────────────────────────┘                        │
 │                                     │                                       │
 │                            encoding_chain_sensor                            │
 │                            (watches: verified)                              │
 │                                     │                                       │
 │                                     ▼                                       │
 │        ┌──────────────────────────────────────────────────────────┐         │
 │        │                 encoding_celery_job                      │  [CEL—  │
 │        │                                                          │  ERY]   │
 │        │  encode_proxy_mp4 ───────────────────────────────────────│         │
 │        │    · Guard: skip if status != verified                   │         │
 │        │    · Guard: skip if already encoding (stale tasks)       │         │
 │        │    · Claim: verified → encoding                          │         │
 │        │    · Navigate to /validation/<bp_job_id>/ source file    │         │
 │        │    · SKIP: non-video → encoding_complete                 │         │
 │        │    · SKIP: non-BFI → encoding_complete                   │         │
 │        │    · MediaInfo probes (height, width, DAR, PAR, audio)   │         │
 │        │    · FFmpeg H.264 proxy → /access_renditions/<YYYYMM>/   │         │
 │        │    · JPEG extraction from proxy                          │         │
 │        │    · Mediaconch policy check                             │         │
 │        │    · Sets encode_time_sec, ffmpeg_command                │         │
 │        │    · Success → encoded                                   │         │
 │        │         │                                                │         │
 │        │         ▼   [file_info: proxy_video_path, mime_type, ...]│         │
 │        │                                                          │         │
 │        │  generate_images ────────────────────────────────────────│         │
 │        │    · Guard: skip if status != encoded                    │         │
 │        │    · Guard: skip if already generating_images            │         │
 │        │    · Claim: encoded → generating_images                  │         │
 │        │    · GM largeimage + thumbnail from JPEG                 │         │
 │        │    · Strip .jpg extension (HLS NGINX requirement)        │         │
 │        │    · Rename proxy: strip .mp4 extension                  │         │
 │        │    · Sets image_time_sec                                 │         │
 │        │    · Success → encoding_complete                         │         │
 │        │                                                          │         │
 │        │              HELPERS (encode_proxy_mp4):                 │         │
 │        │              · _set_encoding_status(db, file_id)         │         │
 │        │              · _rollback_encoding_status(db, file_id)    │         │
 │        │              · _cleanup_partial_files(paths, ctx)        │         │
 │        │                                                          │         │
 │        │              HELPERS (generate_images):                  │         │
 │        │              · _set_images_status(db, file_id)           │         │
 │        │              · _rollback_images_status(db, file_id)      │         │
 │        │              · _cleanup_partial_images(paths, ctx)       │         │
 │        │                                                          │         │
 │        │              On failure at either step:                  │         │
 │        │              • Rollback status → verified                │         │
 │        │              • Clean up partial proxy/JPEG/images        │         │
 │        └──────────────────────────────────────────────────────────┘         │
 │                                     │                                       │
 │                            cleanup_chain_sensor                             │
 │                            (watches: encoding_complete)                     │
 │                                     │                                       │
 │                                     ▼                                       │
 │                  ┌─────────────────────────────────────┐                    │
 │                  │       cleanup_local_job             │  [DATA15]          │
 │                  │                                     │                    │
 │                  │  check_and_delete_source ───────────│                    │
 │                  │    · Guard: skip if deleting_source │                    │
 │                  │      or status ≠ encoding_complete  │                    │
 │                  │    · Claim: encoding_complete →     │                    │
 │                  │      deleting_source                │                    │
 │                  │    · CID append: proxy filenames    │                    │
 │                  │      (access_rendition.mp4,         │                    │
 │                  │       access_rendition.largeimage,  │                    │
 │                  │       access_rendition.thumbnail)   │                    │
 │                  │    · Delete source from             │                    │
 │                  │      /validation/<bp_job_id>/       │                    │
 │                  │    · Remove empty bp_job_id folder  │                    │
 │                  │    · Sets proxy_created = TRUE      │                    │
 │                  │    · Rollback: deleting_source →    │                    │
 │                  │      encoding_complete on failure   │                    │
 │                  │    · Success → complete             │                    │
 │                  └─────────────────────────────────────┘                    │
 │                                     │                                       │
 │                       metadata_update_chain_sensor                          │
 │                       (watches: complete)                                   │
 │                                     │                                       │
 │                                     ▼                                       │
 │             ┌───────────────────────────────────────────────┐               │
 │             │         metadata_update_local_job             │  [DATA15]     │
 │             │                                               │               │
 │             │  update_cid_metadata ─────────────────────────│               │
 │             │    · Guard: skip if updating_cid              │               │
 │             │      or status ≠ complete                     │               │
 │             │    · Claim: complete → updating_cid           │               │
 │             │    · Read mdata_full_json / mdata_exif from DB│               │
 │             │    · Build XML payload from FIELDS dictionary │               │
 │             │    · POST to CID media record (updaterecord)  │               │
 │             │    · Sets updated_to_cid = TRUE               │               │
 │             │    · Sets total_ingest_time_sec               │               │
 │             │    · Rollback: updating_cid → complete        │               │
 │             │      on failure                               │               │
 │             │    · Success → metadata_updated               │               │
 │             │                                               │               │
 │             │              On failure:                      │               │
 │             │              • error_message in DB            │               │
 │             │              • XML payload + CID response     │               │
 │             │                logged to pipeline_events      │               │
 │             └───────────────────────────────────────────────┘               │
 │                                                                             │
 └─────────────────────────────────────────────────────────────────────────────┘
```


## Status Lifecycle (per file)

```
 No Status ─► assessed ─► checksummed ─► File cleared for ingest
                                                 │
                                    [ Black Pearl PUT — external ]
                                                 │
                    ┌────────────────────────────┘
                    ▼
               validating ─► verified ─► encoding ─► encoded
                                                     │
                                                     ▼
                                            generating_images
                                                     │
                                                     ▼
                                           encoding_complete
                                                     │
                                                     ▼
                                               complete
                                                     │
                                                     ▼
                                           metadata_updated
```


## DB Timing Fields

| Field | Populated by | Measures |
|---|---|---|
| `checksum_time_sec` | `generate_checksum` | Full op wall-clock (MD5 + XXHash + DB reads) |
| `encode_time_sec` | `encode_proxy_mp4` | FFmpeg transcode time only |
| `image_time_sec` | `generate_images` | GM JPEG generation time |
| `verify_time_sec` | `verify_tape_copy` | Full verification including BP checks + CID create |
| `total_ingest_time_sec` | `update_cid_metadata` | `EXTRACT(EPOCH FROM (NOW() - created_at))` — cumulative since file record creation |
| `ingest_month` | `verify_tape_copy` | `datetime.now().strftime("%Y%m")` — used by encode_proxy_mp4 for proxy output directory naming |
| `ffmpeg_command` | `encode_proxy_mp4` | Complete FFmpeg CLI string, persisted to DB for debugging |


## CID Updates (per file)

| Stage | Field(s) written | Method |
|---|---|---|
| `verify_tape_copy` | `original_filename`, `file_size`, `object_number`, `part`, `total`, `preservation_bucket`, `input.*` | `insertrecord` — new media record |
| `check_and_delete_source` | `access_rendition.mp4`, `.largeimage`, `.thumbnail` | `updaterecord` — append proxy filenames |
| `update_cid_metadata` | 87 FIELDS across `container`, `video`, `audio`, `other`, `text`, `image` | `updaterecord` — MediaInfo/ExifTool metadata blocks |


## Queue Architecture

```
┌─────────────────┐     ┌──────────────────┐
│   checksum q    │     │   encoding q     │
│                 │     │                  │
│ generate_       │     │ encode_proxy_mp4 │
│ checksum        │     │                  │
│                 │     │ generate_images  │
│ (fast, seconds) │     │                  │
│                 │     │ (slow, minutes)  │
│ high concurrency│     │ low concurrency  │
│ (~20/worker)    │     │ (~40/worker)     │
└─────────────────┘     └──────────────────┘
         │                       │
         └───────┬───────────────┘
                 │
          ┌──────┴───────┐
          │    Redis     │
          │  broker +    │
          │   backend    │
          └──────────────┘
```


## Celery Config

| Setting | Value | Purpose |
|---|---|---|
| `task_acks_late` | `True` | Redeliver on worker crash |
| `worker_prefetch_multiplier` | `1` | No task hoarding |
| `task_time_limit` | 86400 | 24h hard limit for long encodes |
| `worker_max_tasks_per_child` | 50 | Restart worker after 50 tasks to prevent leaks |
| `worker_disable_rate_limits` | `True` | Skip rate limit overhead |
| `result_expires` | 3600 | Discard results after 1 hour |
| `task_ignore_result` | `True` | Skip result persistence |
