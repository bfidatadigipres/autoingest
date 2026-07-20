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
    "Cannot parse partWhole from filename": {
        "text": "This indicates poorly formed partWhole statement, eg 01of002. Please change filename to use correct partWhole syntax making sure to use an underscore to separate Object Number, not a hyphen or space.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "Filename formatted incorrectly": {
        "text": "This indicates poorly formed filename with incorrect Object Number syntax, eg using hyphens rather than underscores. Correct any obvious Object Number syntax issues and remove and incorrect characters or extensions.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "Extension does not match record file_type": {
        "text": "This indicates a mismatch between the file_type in the CID record and the file extensions. In some cases this can indicate the wrong Object Number use in the filename. Most commonly the file_type in the CID record must be changed.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "FFprobe failed to read file": {
        "text": "This indicates a serious problem with the file's metadata preventing it from being preserved in DPI. In most cases this indicates a badly encoded file, which must be re-encoded or re-acquired, as it cannot be ingested to DPI.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "Skip object as previous part not yet ingested or queued for ingest": {
        "text": "This message indicates that a partWhole is attempting to ingest out of sequence. This issue should clear itself if all reel/image parts are being processed at the same time. Please raise a ticket in Collections Systems Service desk if you need more assistance with this issue.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "Filesize does not match BlackPearl object length": {
        "text": "This indicates a different filesize locally to the filesize being reported by Black Pearl for the ingested object. This requires investigation by someone close to the digitisation or acquisition activity, to establish whether the version previously ingested must be replaced with this version.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "MIMEtype is not permitted": {
        "text": "The file does not have an accepted MIMEtype - video, audio, image or document. In most cases this indicates a file fault, which may need to be re-encoded or re-acquired, as it cannot be ingested to DPI.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "Filename already has a CID Media record:": {
        "text": "This indicates that this filename has already been ingest to DPI, and has received a CID media record. Please review if this file has been ingested already using DPI Browser. Please raise a ticket in Collections Systems Service Desk to request a review of the problem.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "Invalid <file_type> in Collect record": {
        "text": "This indicates either a file type that is not accepted for ingest, or multiple file_type occurrences in the CID record. Please check the file_type and ensure only one is present and is one of our accepted file types.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "Failed fixity check: checksums do not match": {
        "text": "TThis indicates the MD5 checksum created for the file is different to the MD5 checksum that Black Pearl has stored for the file. This could be an ingest failure or a media management issue. This requires investigation by someone close to the digitisation or acquisition activity, to establish whether there is a media management issue, or whether this indicates an ingest failure.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "Cannot find record": {
        "text": "This indicates that the filename uses an Object Number that does not exist in CID. Check in CID to identify possible mistyping of Object Number. If no corresponding CID record can be found, file should be removed from Autoingest until further investigation can assign a correct Object Number.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "Cannot parse <object_number> from filename": {
        "text": "This indicates that the filename does not have the correct formatting, eg prefix does not match accepted prefixes.Change filename to correct these errors. A list of accepted prefixes is available at the Autoingest Service Overview.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "No BlackPearl ObjectList returned from BlackPearl API query": {
        "text": "This indicates a problem has arisen with the Black Pearl ingest scripts. The file does not appear to have persisted to Black Pearl tape libraries. Please raise a ticket in Collections Systems Service Desk to request a review of the problem.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "BlackPearl failed to ingest file": {
        "text": "This indicates that the BlackPearl PUT notification reported this file as failing ingest, or persisted to tape notification is FALSE. The file should attempt a second ingest but plLease raise a ticket in Collections Systems Service Desk if the problem persists.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "Filename has already been ingested to DPI": {
        "text": "TTis indicates a problem has arisen with the Black Pearl ingest scripts. An instance exists in the BlackPearl library that has the same filename. Please raise a ticket in Collections Systems Service Desk to request a review of the problem.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "No CID Media record found for this file": {
        "text": "This indicates that a file that has been qualified as persisted does not have a CID media record. A CID media record may need manually creating. Please raise a ticket in Collections Systems Service Desk to request a review of the problem.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "File failed move into autoingest folder": {
        "text": "This indicates a problem with the autoingest folder permissions. Please raise a ticket in Collections Systems Service Desk to request a review of the problem.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "CID media metadata update failed for file": {
        "text": "An error occurred while augmenting the file metadata into the CID media record that represents it. Please raise a ticket in Collections Systems Service Desk to request a review of the problem.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "BlackPearl JOB ID absent": {
        "text": "The file has been moved into the validated folder, signalling completion of write to BlackPearl. But JOB ID for this PUT is not available for the file to process. If this issue does not clear in 24 hours please raise a ticket in Collections Systems Service Desk to request a review of the problem.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "Failed to delete BlackPearl file": {
        "text": "The clean up of a failed ingest has not been able to delete the file from BlackPearl tape libraries and no further ingest attempts will succeed. Please raise a ticket in Collections Systems Service Desk to request a review of the problem quoting supplied ID number and filename.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
    },
    "_default": {
        "text": "Please visit the Autoingest User Guide for more information or raise a Service Desk ticket for your specific error.",
        "link": "https://bficollectionssystems.atlassian.net/wiki/spaces/UKB/pages/109871111/Autoingest+-+User+Guide#Error-Log",
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

    target_words = error_message.split()
    target_iter = iter(target_words)
    for key, guidance in GUIDANCE.items():
        if key == "_default":
            continue
        search_words = key.split()
        val = all(word in target_iter for word in search_words)
        if val is True:
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
    limit = min(int(request.args.get("limit", "1000")), 1000)

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
        # Show all unresolved errors regardless of age.
    elif has_error == "no":
        conditions.append("(error_message IS NULL OR error_message = '')")
        # Files without errors are only shown if updated within the last 72 hours.
        conditions.append("updated_at >= NOW() - INTERVAL '72 hours'")
    else:
        # Default view: files updated in the last 72 hours.
        conditions.append("updated_at >= NOW() - INTERVAL '72 hours'")

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
