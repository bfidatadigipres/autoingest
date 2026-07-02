import re
from flask import Blueprint, render_template, jsonify, request, current_app

klc_bp = Blueprint(
    "klc_routes",
    __name__,
    template_folder="templates",
    static_folder="static",
)

STORAGE_OPTIONS = [
    "qnap_01/Public/F47",
    "qnap_03",
    "qnap_04",
    "qnap_05/Public",
    "qnap_06",
    "qnap_08",
    "qnap_09",
    "qnap_10",
    "qnap_11/digital_operations",
]

GUIDANCE = {
    "Cannot find record with <object number>": {
        "text": "The object number extracted from the filename does not match any record in the CID collections database. Verify the filename follows BFI naming conventions (PREFIX_OBJECTNUMBER_PARTofWHOLE.ext) and check that the object exists in CID Collect.",
        "link": "/display/KB/Object+Number+Errors",
    },
    "Filename formatted incorrectly": {
        "text": "The filename does not conform to BFI naming conventions. Accepted prefixes: N, C, PD, SPD, PBS, PBM, PBL, SCR, CA. Filename must follow the pattern PREFIX_OBJECTNUMBER_PARTofWHOLE.ext with no special characters.",
        "link": "/display/KB/Filename+Errors",
    },
    "CID API unreachable": {
        "text": "The CID collections management system is not responding. This may be due to scheduled maintenance or a temporary outage. The file will be retried automatically on the next ingest cycle, or you can manually refresh from the autoingest viewer.",
        "link": "/display/KB/CID+API+Errors",
    },
    "FFprobe failed to read file": {
        "text": "FFprobe could not read the file, which may indicate file corruption, an unsupported codec, or a partial/corrupt transfer. Verify the file plays correctly in VLC or another media player, and check the source storage for transfer errors.",
        "link": "/display/KB/FFprobe+Errors",
    },
    "Extension does not match": {
        "text": "The file extension does not match the expected file_type in the CID Item record. Check that the file extension is correct (e.g., .mxf for MXF files) and that the CID Item record's file_type field is accurate.",
        "link": "/display/KB/File+Type+Errors",
    },
    "MIMEtype is not permitted": {
        "text": "The file's MIME type is not in the list of accepted formats. The pipeline currently processes video, audio, image, and application files. If the file should be accepted, check that its extension is registered in the accepted file types list.",
        "link": "/display/KB/MIME+Type+Errors",
    },
    "Filename already has a CID Media record": {
        "text": "A CID Media record already exists with this filename. This could indicate a duplicate ingest attempt. Check whether the file has already been processed, or whether a previous record needs to be cleaned up.",
        "link": "/display/KB/Duplicate+Errors",
    },
    "Skip object as previous part not yet ingested": {
        "text": "This is part of a multi-part file set. The pipeline requires parts to be ingested in sequential order. The earlier part has not yet been ingested or is still in progress. Wait for the earlier part to complete processing.",
        "link": "/display/KB/Multipart+Errors",
    },
    "checksums do not match": {
        "text": "The MD5 checksum of the file on disk does not match the checksum stored in Black Pearl (the tape archive). This indicates data corruption during transfer. The file will be automatically re-ingested. If the error persists, check the source file integrity.",
        "link": "/display/KB/Checksum+Errors",
    },
    "Failed validation": {
        "text": "The Black Pearl tape verification stage failed. This could be due to network issues between the ingest server and the tape archive, or a problem with data persistence. The file will be retried automatically.",
        "link": "/display/KB/Validation+Errors",
    },
    "FFmpeg encoding failed": {
        "text": "The FFmpeg proxy encoding step failed. This usually indicates an unsupported codec, a damaged source file, or the file requires special handling (e.g., unusual resolution, frame rate, or colour space). Check the file in a media player and contact DP team if it is a known format.",
        "link": "/display/KB/Encoding+Errors",
    },
    "JPEG image not found": {
        "text": "The JPEG extraction from the proxy video failed because the source JPEG image was not created. This typically cascades from an earlier encoding failure. Check the encoding logs for the root cause.",
        "link": "/display/KB/Encoding+Errors",
    },
    "CID POST failed": {
        "text": "The metadata update could not be written to the CID Media record. This is usually a temporary API error. The file should be retried automatically. If it persists, the payload may contain data that CID cannot accept (check the pipeline_events table for the XML payload).",
        "link": "/display/KB/CID+API+Errors",
    },
    "_default": {
        "text": "No specific guidance is available for this error. Please contact the Digital Preservation team for assistance.",
        "link": "",
    },
}


def _db():
    return current_app.config["db"]


def _extract_storage(file_path: str) -> str:
    if not file_path:
        return ""
    match = re.search(r"/mnt/(.+?)/autoingest/", file_path)
    if not match:
        return ""
    return match.group(1)


def _match_guidance(error_message: str) -> dict:
    if not error_message:
        return GUIDANCE["_default"]
    for key, guidance in GUIDANCE.items():
        if key == "_default":
            continue
        if key.lower() in error_message.lower():
            return guidance
    return GUIDANCE["_default"]


@klc_bp.route("/")
def index():
    return render_template(
        "klc.html",
        help_url=current_app.config.get("KLC_HELP_URL", ""),
        service_desk_url=current_app.config.get("SERVICE_DESK_URL", ""),
        storage_options=STORAGE_OPTIONS,
    )


@klc_bp.route("/api/files")
def api_files():
    db = _db()
    search = request.args.get("search", "").strip()
    storage = request.args.get("storage", "").strip()
    has_error = request.args.get("has_error", "").strip()
    limit = min(int(request.args.get("limit", "200")), 1000)

    conditions = []
    params = []

    if search:
        like = f"%{search}%"
        conditions.append(
            "(file_name ILIKE %s OR file_status ILIKE %s OR error_message ILIKE %s)"
        )
        params.extend([like, like, like])

    if storage:
        conditions.append("file_path ILIKE %s")
        params.append(f"%{storage}%")

    if has_error == "yes":
        conditions.append("error_message IS NOT NULL AND error_message != ''")
    elif has_error == "no":
        conditions.append("(error_message IS NULL OR error_message = '')")

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT id, file_name, file_status, error_message, file_path,
               file_size, mime_type, checksum_md5,
               file_fmt, updated_at
        FROM app.file_catalogue
        {where_clause}
        ORDER BY updated_at DESC
        LIMIT %s
    """
    params.append(limit)

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            raw_rows = cur.fetchall()

    rows = []
    for row in raw_rows:
        r = dict(zip(columns, row))
        r["storage"] = _extract_storage(r.get("file_path") or "")
        r["file_size_gb"] = (
            f"{int(r['file_size']) / 1_073_741_824:.2f}"
            if r.get("file_size")
            else ""
        )
        r["checksum_md5_short"] = r.get("checksum_md5") or ""
        if r.get("updated_at"):
            r["updated_at"] = r["updated_at"].strftime("%Y-%m-%d %H:%M")
        rows.append(r)

    return jsonify(rows)


@klc_bp.route("/api/stats")
def api_stats():
    db = _db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_status, COUNT(*) FROM app.file_catalogue "
                "GROUP BY file_status ORDER BY COUNT(*) DESC"
            )
            stats_rows = cur.fetchall()
    return jsonify([{"status": r[0], "count": r[1]} for r in stats_rows])


@klc_bp.route("/api/guidance")
def api_guidance():
    return jsonify(GUIDANCE)
