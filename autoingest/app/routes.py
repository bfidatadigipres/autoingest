import os
import shutil
from pathlib import Path
from datetime import datetime
from flask import Blueprint, render_template, jsonify, request, current_app

bp = Blueprint("viewer", __name__)


def _db():
    return current_app.config["db"]


def _format_size(size_bytes):
    if not size_bytes:
        return ""
    size = int(size_bytes)
    if size >= 1073741824:
        return f"{size / 1073741824:.1f} GB"
    if size >= 1048576:
        return f"{size / 1048576:.1f} MB"
    if size >= 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size} B"


_ONE_STEP_EARLIER = {
    "assessed": "No Status",
    "generating_checksum": "assessed",
    "checksummed": "assessed",
    "cataloguing": "checksummed",
    "File cleared for ingest": "checksummed",
    "bp_json_pending": "File cleared for ingest",
    "validating": "File cleared for ingest",
    "verified": "File cleared for ingest",
    "encoding": "verified",
    "encoded": "verified",
    "generating_images": "verified",
    "encoding_complete": "verified",
    "deleting_source": "encoding_complete",
    "complete": "encoding_complete",
    "updating_cid": "complete",
    "All stages complete": "complete",
}


@bp.route("/")
def index():
    return render_template(
        "index.html",
        confluence_url=current_app.config.get("CONFLUENCE_URL", ""),
        service_desk_url=current_app.config.get("SERVICE_DESK_URL", ""),
    )


@bp.route("/api/files")
def api_files():
    """
    Returns files that need operator attention:
      - Stuck files:  status != 'All stages complete' AND no update in 24+ hours
      - Error files:  any error_message present, regardless of age
    """
    db = _db()
    search = request.args.get("search", "").strip()

    conditions = [
        "("
        "(file_status != 'All stages complete' AND updated_at < NOW() - INTERVAL '0 hours')"
        " OR"
        " (error_message IS NOT NULL AND error_message != '')"
        ")"
    ]
    params = []

    if search:
        like = f"%{search}%"
        conditions.append(
            "(file_name ILIKE %s OR file_status ILIKE %s OR error_message ILIKE %s)"
        )
        params.extend([like, like, like])

    where_clause = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT id, file_name, file_status, source, file_size,
               mime_type, error_message, tape_verified, proxy_created,
               created_at, updated_at
        FROM app.file_catalogue
        {where_clause}
        ORDER BY updated_at DESC
        LIMIT %s
    """
    params.append(1000)

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    for r in rows:
        r["file_size_fmt"] = _format_size(r.get("file_size"))
        if r.get("created_at"):
            r["created_at"] = r["created_at"].strftime("%Y-%m-%d %H:%M")
        if r.get("updated_at"):
            r["updated_at"] = r["updated_at"].strftime("%Y-%m-%d %H:%M")
        r["tape_verified"] = bool(r.get("tape_verified"))
        r["proxy_created"] = bool(r.get("proxy_created"))

    return jsonify(rows)


@bp.route("/api/refresh/<int:file_id>", methods=["POST"])
def api_refresh(file_id):
    db = _db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_name, file_path, autoingest_path, proxy_video_path "
                "FROM app.file_catalogue WHERE id = %s",
                (file_id,),
            )
            row = cur.fetchone()

    if not row:
        return jsonify({"success": False, "error": "File not found"}), 404

    file_name, file_path, autoingest_path, proxy_video_path = row

    found_path = None
    candidates = [file_path]
    if autoingest_path and file_path:
        base_dir = Path(file_path).parent.parent.parent.parent
        candidates.append(str(base_dir / autoingest_path / file_name))
    for candidate in candidates:
        if candidate and os.path.isfile(candidate):
            found_path = candidate
            break

    if not found_path and proxy_video_path:
        proxy_dir = os.path.dirname(proxy_video_path)
        candidate = os.path.join(proxy_dir, file_name)
        if os.path.isfile(candidate):
            found_path = candidate

    move_attempted = False
    move_success = False
    move_error = ""
    old_path = found_path

    if found_path and file_path and found_path != file_path:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        try:
            shutil.move(found_path, file_path)
            move_attempted = True
            move_success = True
        except OSError as exc:
            move_attempted = True
            move_success = False
            move_error = str(exc)
    elif found_path and found_path == file_path:
        move_attempted = False
    elif not found_path:
        return jsonify({
            "success": False,
            "error": f"File '{file_name}' not found on disk. Cannot retry ingest.",
        }), 404

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE app.file_catalogue
                SET file_status = 'No Status',
                    do_ingest = 'UNKNOWN',
                    incomplete_scan = 'UNKNOWN',
                    screencraft_arch = 'UNKNOWN',
                    part = NULL,
                    whole = NULL,
                    error_message = NULL,
                    tape_verified = NULL,
                    proxy_created = NULL,
                    source_deletion = NULL,
                    validated = NULL,
                    checksum_xxh = NULL,
                    checksum_md5 = NULL,
                    checksum_date = NULL,
                    file_fmt = NULL,
                    video_codec = NULL,
                    audio_codec = NULL,
                    writing_library = NULL,
                    audio_format = NULL,
                    framerate = NULL,
                    audio_ch_total = NULL,
                    audio_count = NULL,
                    video_count = NULL,
                    height = NULL,
                    width = NULL,
                    colorspace = NULL,
                    bitdepth = NULL,
                    video_duration = NULL,
                    ffmpeg_command = NULL,
                    proxy_video_path = NULL,
                    proxy_size = NULL,
                    proxy_image_path = NULL,
                    proxy_thumb_path = NULL,
                    mdata_text = NULL,
                    mdata_full_text = NULL,
                    mdata_full_xml = NULL,
                    mdata_ebucore = NULL,
                    mdata_pbcore = NULL,
                    mdata_full_json = '{}',
                    mdata_exif = NULL,
                    cid_item_priref = NULL,
                    cid_file_type = NULL,
                    cid_ob_num = NULL,
                    cid_media_priref = NULL,
                    bp_bucket = NULL,
                    bp_job_id = NULL,
                    put_type = NULL,
                    persisted_ok = NULL,
                    bp_etag = NULL,
                    bp_length = NULL,
                    bp_version_id = NULL,
                    reference_num = NULL,
                    updated_to_cid = NULL,
                    encode_time_sec = NULL,
                    image_time_sec = NULL,
                    checksum_time_sec = NULL,
                    verify_time_sec = NULL,
                    total_ingest_time_sec = NULL,
                    updated_at = NOW()
                WHERE id = %s
            """, (file_id,))

    try:
        db.record_pipeline_event(
            run_id="manual",
            job_name="viewer",
            op_name="refresh_request",
            event_type="manual_refresh",
            status="success" if (move_success or not move_attempted) else "failure",
            metadata={
                "file_id": file_id,
                "file_name": file_name,
                "old_path": old_path,
                "destination": file_path,
                "move_attempted": move_attempted,
                "move_success": move_success,
                "timestamp": str(datetime.now())[:19],
            },
            message=(
                f"Manual refresh: '{file_name}' moved from {old_path} to {file_path}"
                if move_success
                else (
                    f"Manual refresh: '{file_name}' already in watch folder"
                    if not move_attempted
                    else f"Manual refresh: move failed — {move_error}"
                )
            ),
        )
    except Exception:
        pass

    if move_attempted and not move_success:
        return jsonify({
            "success": False,
            "error": f"DB reset done, but file move failed: {move_error}",
        }), 500

    return jsonify({
        "success": True,
        "message": (
            f"'{file_name}' moved back to watch folder and queued for re-ingest."
            if move_success
            else f"'{file_name}' already in watch folder — queued for re-ingest."
        ),
        "move_attempted": move_attempted,
        "move_success": move_success,
    })


@bp.route("/api/validator-reset/<int:file_id>", methods=["POST"])
def api_validator_reset(file_id):
    db = _db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_name FROM app.file_catalogue WHERE id = %s",
                (file_id,),
            )
            row = cur.fetchone()

    if not row:
        return jsonify({"success": False, "error": "File not found"}), 404

    file_name = row[0]

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE app.file_catalogue
                SET file_status     = 'File cleared for ingest',
                    error_message   = NULL,
                    tape_verified   = NULL,
                    validated       = NULL,
                    cid_media_priref = NULL,
                    proxy_video_path = NULL,
                    proxy_image_path = NULL,
                    proxy_thumb_path = NULL,
                    proxy_size      = NULL,
                    bp_version_id   = NULL,
                    bp_etag         = NULL,
                    bp_length       = NULL,
                    ffmpeg_command  = NULL,
                    proxy_created   = NULL,
                    source_deletion = NULL,
                    updated_at      = NOW()
                WHERE id = %s
            """, (file_id,))

    try:
        db.record_pipeline_event(
            run_id="manual",
            job_name="viewer",
            op_name="validator_reset",
            event_type="manual_validator_reset",
            status="success",
            metadata={
                "file_id": file_id,
                "file_name": file_name,
                "timestamp": str(datetime.now())[:19],
            },
            message=f"Manual validator reset: '{file_name}' (id={file_id}) returned to pre-verification state",
        )
    except Exception:
        pass

    return jsonify({
        "success": True,
        "message": f"'{file_name}' reset to pre-verification state. bp_job_id retained.",
    })


@bp.route("/api/status-reset/<int:file_id>", methods=["POST"])
def api_status_reset(file_id):
    db = _db()
    data = request.get_json()
    if not data or "new_status" not in data:
        return jsonify({"success": False, "error": "Missing 'new_status' field"}), 400

    new_status = data["new_status"].strip()
    if new_status not in _ONE_STEP_EARLIER.values():
        return jsonify({"success": False, "error": f"Invalid status: {new_status}"}), 400

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_name, file_status FROM app.file_catalogue WHERE id = %s",
                (file_id,),
            )
            row = cur.fetchone()

    if not row:
        return jsonify({"success": False, "error": "File not found"}), 404

    file_name, current_status = row

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE app.file_catalogue
                SET file_status = %s,
                    error_message = NULL,
                    updated_at = NOW()
                WHERE id = %s
            """, (new_status, file_id))

    try:
        db.record_pipeline_event(
            run_id="manual",
            job_name="viewer",
            op_name="status_reset",
            event_type="manual_status_reset",
            status="success",
            metadata={
                "file_id": file_id,
                "file_name": file_name,
                "old_status": current_status,
                "new_status": new_status,
                "timestamp": str(datetime.now())[:19],
            },
            message=f"Manual status reset: '{file_name}' (id={file_id}) from '{current_status}' to '{new_status}'",
        )
    except Exception:
        pass

    return jsonify({
        "success": True,
        "message": f"'{file_name}' status reset from '{current_status}' to '{new_status}'.",
    })


@bp.route("/api/stats")
def api_stats():
    db = _db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_status, COUNT(*) FROM app.file_catalogue "
                "GROUP BY file_status ORDER BY COUNT(*) DESC"
            )
            rows = cur.fetchall()
    return jsonify([{"status": r[0], "count": r[1]} for r in rows])


@bp.route("/api/delete/<int:file_id>", methods=["DELETE"])
def api_delete(file_id):
    db = _db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_name FROM app.file_catalogue WHERE id = %s",
                (file_id,),
            )
            row = cur.fetchone()

    if not row:
        return jsonify({"success": False, "error": "File not found"}), 404

    file_name = row[0]
    db.delete_file_record(file_id)

    try:
        db.record_pipeline_event(
            run_id="manual",
            job_name="viewer",
            op_name="delete_record",
            event_type="manual_delete",
            status="success",
            metadata={
                "file_id": file_id,
                "file_name": file_name,
                "timestamp": str(datetime.now())[:19],
            },
            message=f"Manual delete: '{file_name}' (id={file_id}) removed from file_catalogue",
        )
    except Exception:
        pass

    return jsonify({
        "success": True,
        "message": f"Row for '{file_name}' deleted.",
    })
