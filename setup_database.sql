-- setup_database.sql
-- Run as postgres superuser: psql -U postgres -f setup_database.sql

-- Create the dagster user
-- CHANGE THIS PASSWORD before running in production
CREATE USER dagster_user WITH PASSWORD 'I<ZHnN?sghQr/8!J;HVI';

-- Create the database
CREATE DATABASE dagster_instance OWNER dagster_user;

-- Connect to the new database
\c dagster_instance;

-- Grant schema privileges for Dagster's auto-created tables
GRANT CREATE ON SCHEMA public TO dagster_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dagster_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dagster_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON TABLES TO dagster_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON SEQUENCES TO dagster_user;

-- ============================================================
-- Custom application schema
-- ============================================================
CREATE SCHEMA IF NOT EXISTS app;
GRANT USAGE ON SCHEMA app TO dagster_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA app TO dagster_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA app TO dagster_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA app
    GRANT ALL PRIVILEGES ON TABLES TO dagster_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA app
    GRANT ALL PRIVILEGES ON SEQUENCES TO dagster_user;

-- ============================================================
-- TABLE 1: pipeline_events
-- Tracks success/failure events from your pipeline runs
-- at a granularity you control (per-op, per-job, custom)
-- ============================================================
CREATE TABLE IF NOT EXISTS app.pipeline_events (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(255),
    job_name        VARCHAR(255),
    op_name         VARCHAR(255),
    event_type      VARCHAR(100) NOT NULL,
    status          VARCHAR(50) NOT NULL,
    message         TEXT,
    worker_node     VARCHAR(255),
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pe_run_id     ON app.pipeline_events(run_id);
CREATE INDEX idx_pe_job_name   ON app.pipeline_events(job_name);
CREATE INDEX idx_pe_status     ON app.pipeline_events(status);
CREATE INDEX idx_pe_created_at ON app.pipeline_events(created_at);
CREATE INDEX idx_pe_event_type ON app.pipeline_events(event_type);

-- ============================================================
-- TABLE 2: file_catalogue
-- Tracks every file moving through the pipeline with its
-- metadata, checksums, status, and proxy paths
-- ============================================================
CREATE TABLE IF NOT EXISTS app.file_catalogue (
    id                  SERIAL PRIMARY KEY, -- 0
    file_name           VARCHAR(512) NOT NULL, -- 1
    file_status         VARCHAR(50) NOT NULL DEFAULT 'No Status',
    file_path           TEXT,
    error_message       TEXT,
    source              TEXT,
    do_ingest           VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN',
    incomplete_scan     VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN',
    screencraft_arch    VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN',
    part                INT,
    whole               INT,  -- 10
    extension           VARCHAR(10),
    ffprobe_exit        INT,
    mime_type           TEXT,
    cid_item_priref     BIGINT,
    cid_file_type       VARCHAR(10),
    cid_ob_num          VARCHAR(20),
    cid_media_priref    VARCHAR(20),
    bp_bucket           TEXT,
    bucket_list         TEXT,
    file_size           BIGINT,  -- 20
    checksum_xxh        VARCHAR(36),
    checksum_md5        VARCHAR(32),
    checksum_date       TEXT,
    ingest_month        TEXT,
    mdata_text          TEXT,
    mdata_full_text     TEXT,
    mdata_full_xml      TEXT,
    mdata_ebucore       TEXT,
    mdata_pbcore        TEXT,
    mdata_full_json     JSONB DEFAULT '{}', -- 30
    file_fmt	        TEXT,
    video_codec         TEXT,
    audio_codec         TEXT,
    writing_library     TEXT,
    audio_format        TEXT,
    framerate           TEXT,
    audio_ch_total      TEXT,
    audio_count         TEXT,
    video_count         TEXT,
    height              TEXT, -- 40
    width               TEXT,
    colorpsace          TEXT,
    bitdepth            TEXT,
    video_duration      TEXT,
    autoingest_path     TEXT,  -- Eg from autoingest/ingest/autodetect or autoingest/ingest/incomplete_scans or autoingest/ingest/bfi/blob
    bp_job_id           TEXT,
    put_type            TEXT,  -- Eg, Blob / Group
    persisted_ok        TEXT,  -- Bool
    bp_etag             VARCHAR(32),  -- Whole file checksum
    bp_length           BIGINT, -- 50  Total file size in BP
    bp_version_id       VARCHAR(64),  -- Version ID
    validated           TEXT,
    reference_num       TEXT,
    ffmpeg_command      TEXT,
    proxy_video_path    TEXT,
    proxy_size          TEXT,
    proxy_image_path    TEXT,
    proxy_thumb_path    TEXT,
    updated_to_cid      TEXT,
    source_deletion     TEXT, -- 60
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tape_verified       BOOLEAN,
    proxy_created       BOOLEAN,
    checksum_time_sec   DOUBLE PRECISION,
    encode_time_sec     DOUBLE PRECISION,
    image_time_sec      DOUBLE PRECISION,
    verify_time_sec     DOUBLE PRECISION,
    total_ingest_time_sec DOUBLE PRECISION,
    mdata_exif          TEXT
);

CREATE INDEX idx_ft_status      ON app.file_catalogue(file_status);
CREATE INDEX idx_ft_file_name   ON app.file_catalogue(file_name);
CREATE INDEX idx_ft_checksum    ON app.file_catalogue(checksum_md5);
CREATE INDEX idx_ft_created_at  ON app.file_catalogue(created_at);
CREATE INDEX idx_ft_source      ON app.file_catalogue(source);

-- Auto-update updated_at on row modification
CREATE OR REPLACE FUNCTION app.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_file_tracking_updated_at ON app.file_catalogue;
CREATE TRIGGER trg_file_tracking_updated_at
    BEFORE UPDATE ON app.file_catalogue
    FOR EACH ROW
    EXECUTE FUNCTION app.set_updated_at();

-- ============================================================
-- TABLE 3: file_type_config
-- Lookup table for processing profiles per file type
-- Pre-populate with your known file types
-- ============================================================
CREATE TABLE app.file_type_config (
    id                  SERIAL PRIMARY KEY,
    file_extension      VARCHAR(20) NOT NULL UNIQUE,
    file_type_label     VARCHAR(100),
    processing_profile  JSONB DEFAULT '{}',
    proxy_settings      JSONB DEFAULT '{}',
    validation_rules    JSONB DEFAULT '{}',
    active              BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed with some common video types
INSERT INTO app.file_type_config (file_extension, file_type_label, processing_profile, proxy_settings, validation_rules) VALUES
    ('.mp4',  'MPEG-4',       '{"codec": "h264", "container": "mp4"}',
        '{"create_proxy": true, "proxy_scale": "640:-2", "create_thumbnail": true}',
        '{"min_file_size": 1024, "check_streams": true}'),
    ('.mov',  'QuickTime',    '{"codec": "prores", "container": "mov"}',
        '{"create_proxy": true, "proxy_scale": "640:-2", "create_thumbnail": true}',
        '{"min_file_size": 1024, "check_streams": true}'),
    ('.mxf',  'MXF',          '{"codec": "xdcam", "container": "mxf"}',
        '{"create_proxy": true, "proxy_scale": "640:-2", "create_thumbnail": true}',
        '{"min_file_size": 1024, "check_streams": true}'),
    ('.avi',  'AVI',          '{"codec": "various", "container": "avi"}',
        '{"create_proxy": true, "proxy_scale": "640:-2", "create_thumbnail": true}',
        '{"min_file_size": 512, "check_streams": true}'),
    ('.mkv',  'Matroska',     '{"codec": "various", "container": "mkv"}',
        '{"create_proxy": true, "proxy_scale": "640:-2", "create_thumbnail": true}',
        '{"min_file_size": 1024, "check_streams": true}'),
    ('.ts',   'MPEG-TS',      '{"codec": "h264", "container": "ts"}',
        '{"create_proxy": true, "proxy_scale": "640:-2", "create_thumbnail": true}',
        '{"min_file_size": 1024, "check_streams": true}')
ON CONFLICT (file_extension) DO NOTHING;

-- ============================================================
-- TABLE 4: io_manager_store
-- Persistent intermediate op-output storage for Celery workers.
-- Replaces the default fs_io_manager (local-disk-based) so that
-- every encoding worker can read / write outputs via shared
-- PostgreSQL, regardless of which machine the step lands on.
-- Data is pickled BYTEA; cleanup after 4 months.
-- ============================================================
CREATE TABLE IF NOT EXISTS app.io_manager_store (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(255) NOT NULL,
    step_key        VARCHAR(255) NOT NULL,
    output_name     VARCHAR(255) NOT NULL DEFAULT 'result',
    value           BYTEA NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, step_key, output_name)
);

CREATE INDEX idx_io_run_step   ON app.io_manager_store(run_id, step_key);
CREATE INDEX idx_io_created_at ON app.io_manager_store(created_at);
