from autoingest.dashboard.config import get_db


_STORAGE_SQL = "SUBSTRING(file_path FROM '/mnt/(.+?)/autoingest/')"


def fetch_status_counts():
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_status, COUNT(*) FROM app.file_catalogue "
                "GROUP BY file_status ORDER BY COUNT(*) DESC"
            )
            return cur.fetchall()


def fetch_storage_status_24h():
    """
    Returns (storage, file_status, count) for files updated in the last 24 hours.
    Storages with zero matching files are inferred by the absence of rows.
    """
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT {_STORAGE_SQL} AS storage,
                       file_status,
                       COUNT(*) AS count
                FROM app.file_catalogue
                WHERE updated_at >= NOW() - INTERVAL '24 hours'
                  AND file_path LIKE '/mnt/%/autoingest/%'
                GROUP BY storage, file_status
                ORDER BY storage, file_status
            """)
            return cur.fetchall()


def fetch_today_totals():
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS files_today,
                    COUNT(*) FILTER (WHERE file_status = 'metadata_updated') AS completed,
                    COUNT(*) FILTER (WHERE error_message IS NOT NULL AND error_message != '') AS errored,
                    COALESCE(SUM(file_size), 0) AS bytes_processed,
                    AVG(encode_time_sec) AS avg_encode_sec,
                    AVG(total_ingest_time_sec) AS avg_total_sec
                FROM app.file_catalogue
                WHERE updated_at >= CURRENT_DATE
            """)
            return cur.fetchone()


def fetch_source_counts():
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source, COUNT(*) FROM app.file_catalogue "
                "GROUP BY source ORDER BY COUNT(*) DESC"
            )
            return cur.fetchall()


def fetch_mime_counts():
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT mime_type, COUNT(*) FROM app.file_catalogue "
                "GROUP BY mime_type ORDER BY COUNT(*) DESC"
            )
            return cur.fetchall()


def fetch_recent_events(limit: int = 50):
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT op_name, job_name, status, "
                "metadata->>'preview' AS preview, "
                "metadata->>'duration_sec' AS duration, "
                "created_at "
                "FROM app.pipeline_events "
                "ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_encode_performance():
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT file_name, encode_time_sec, ffmpeg_command,
                       file_size, proxy_size,
                       height, width, mime_type, source,
                       {_STORAGE_SQL} AS storage
                FROM app.file_catalogue
                WHERE encode_time_sec IS NOT NULL
                ORDER BY encode_time_sec DESC
                LIMIT 200
            """)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_stage_timings():
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT op_name,
                       COUNT(*) AS runs,
                       AVG((metadata->>'duration_sec')::float) AS avg_sec,
                       MIN((metadata->>'duration_sec')::float) AS min_sec,
                       MAX((metadata->>'duration_sec')::float) AS max_sec
                FROM app.pipeline_events
                WHERE event_type = 'op_completed'
                  AND status = 'success'
                  AND metadata->>'duration_sec' IS NOT NULL
                GROUP BY op_name
                ORDER BY avg_sec DESC
            """)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_throughput_by_hour():
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT DATE_TRUNC('hour', updated_at) AS hour,
                       COUNT(*) AS files,
                       COALESCE(SUM(file_size), 0) AS bytes_processed,
                       {_STORAGE_SQL} AS storage
                FROM app.file_catalogue
                WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY hour, storage
                ORDER BY hour, storage
            """)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_throughput_by_day():
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DATE(updated_at) AS day,
                       COUNT(*) AS files,
                       COALESCE(SUM(file_size), 0) AS bytes_processed
                FROM app.file_catalogue
                WHERE updated_at >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY day
                ORDER BY day
            """)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_error_distribution():
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(error_message, 'None') AS error_message,
                       COUNT(*) AS count
                FROM app.file_catalogue
                WHERE error_message IS NOT NULL AND error_message != ''
                GROUP BY error_message
                ORDER BY count DESC
                LIMIT 30
            """)
            return cur.fetchall()


def fetch_files_with_errors(limit: int = 100):
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT file_name, file_status, error_message,
                       mime_type, source, file_size,
                       updated_at
                FROM app.file_catalogue
                WHERE error_message IS NOT NULL AND error_message != ''
                ORDER BY updated_at DESC
                LIMIT %s
            """, (limit,))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_latency_distribution():
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT total_ingest_time_sec
                FROM app.file_catalogue
                WHERE total_ingest_time_sec IS NOT NULL
                ORDER BY total_ingest_time_sec
            """)
            return [row[0] for row in cur.fetchall()]


def search_file(search_term: str):
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            like = f"%{search_term}%"
            cur.execute(
                "SELECT * FROM app.file_catalogue "
                "WHERE file_name ILIKE %s "
                "ORDER BY updated_at DESC LIMIT 20",
                (like,),
            )
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_file_events(file_name: str):
    db = get_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            like = f"%{file_name}%"
            cur.execute("""
                SELECT run_id, job_name, op_name, status,
                       metadata->>'duration_sec' AS duration,
                       metadata->>'ffmpeg_time_sec' AS ffmpeg_time,
                       metadata->>'image_time_sec' AS image_time,
                       metadata->>'cid_update_time_sec' AS cid_time,
                       metadata->>'bp_check_time_sec' AS bp_time,
                       metadata->>'checksum_md5' AS md5,
                       metadata->>'preview' AS preview,
                       metadata,
                       message,
                       created_at
                FROM app.pipeline_events
                WHERE metadata->>'file_name' = %s
                   OR message ILIKE %s
                ORDER BY created_at ASC
            """, (file_name, like))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
