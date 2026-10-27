import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from dagster import DefaultSensorStatus, RunRequest, SensorEvaluationContext, sensor

from autoingest.jobs.ingest_jobs import ingest_local_job
from autoingest.ops.local.file_assessment import check_for_multipart
from autoingest.resources.utils import (
    WAITING_FOR_PREVIOUS_PART_MSG,
    accepted_file_type,
    check_part_whole,
)

MAX_INGEST_DEPTH = 30
MAX_NEW_PER_TICK = 60
MAX_FILES_PER_PATH_PER_TICK = 100
MAX_LAUNCHES_PER_STORAGE_PER_TICK = 3
TICK_DEADLINE_SEC = 55
CURSOR_TIMEOUT_SEC = 900  # 15 minutes
RETRYABLE_STATUSES = {"No Status", "Failed assessment"}
DEFERRED_RETRY_MIN_AGE_SEC = 4 * 60 * 60  # 4 hours


def _storage_key(path: str) -> str:
    for marker in ("/autoingest/", "/ingest/"):
        prefix, sep, _ = path.partition(marker)
        if sep:
            if prefix.startswith("/mnt/"):
                return prefix[len("/mnt/") :]
            return prefix
    if path.startswith("/mnt/"):
        return path.split("/", 3)[2]
    return str(Path(path).parent)


def _prioritise_multipart(
    db, candidates: list[str], existing_states: dict, seen_paths: set[str]
) -> list[str]:
    series: dict[str, list[tuple[int, str, str]]] = {}
    others: list[tuple[int | None, str]] = []

    for path in candidates:
        name = Path(path).name
        if "/incomplete_scan/" in path or "_" not in name or "of" not in name:
            others.append((None, path))
            continue

        part, whole = check_part_whole(name)
        if not part or part == 1:
            others.append((part, path))
            continue

        previous = check_for_multipart(name, part, whole)
        if not isinstance(previous, str):
            others.append((part, path))
            continue

        series.setdefault(name.rsplit("_", 1)[0], []).append((part, previous, path))

    ready: list[tuple[int, str]] = []
    now = datetime.now(timezone.utc)

    for items in series.values():
        items.sort(key=lambda item: item[0])
        part, previous, path = items[0]
        previous_path = os.path.join(os.path.dirname(path), previous)
        details = db.lookup_file_details(previous)

        if details is None:
            if previous_path in seen_paths:
                continue
            state = existing_states.get(path)
            if state is not None:
                updated_at = state[2]
                age = (
                    (now - updated_at).total_seconds()
                    if updated_at is not None
                    else float("inf")
                )
                if age < DEFERRED_RETRY_MIN_AGE_SEC:
                    continue
        else:
            status = details[2]
            do_ingest = details[6] if len(details) > 6 else None
            if status in (
                "No Status",
                "Failed assessment",
                "Error",
                "Duplicate filename",
            ):
                continue
            if do_ingest == "FALSE":
                continue

        ready.append((part, path))

    ready.sort(key=lambda item: item[0])
    others.sort(
        key=lambda item: (
            item[0] is None,
            item[0] or 0,
            Path(item[1]).name,
        )
    )
    return [path for _, path in ready] + [path for _, path in others]


@sensor(
    job=ingest_local_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"workflow_db"},
)
def watch_folder_sensor(context: SensorEvaluationContext) -> list[RunRequest]:
    tick_start = time.perf_counter()

    watch_paths_file = os.environ.get("WATCH_PATHS_FILE", "")
    if watch_paths_file:
        try:
            watch_paths = [
                line.strip()
                for line in Path(watch_paths_file).read_text().splitlines()
                if line.strip() and not line.startswith("#")
            ]
        except Exception as exc:
            context.log.warning(f"Failed to read {watch_paths_file}: {exc}")
            watch_paths = []
    else:
        watch_paths = [
            p.strip() for p in os.environ.get("WATCH_FOLDER_PATHS", "").split(",")
            if p.strip()
        ]

    if not watch_paths:
        context.log.warning("WATCH_FOLDER_PATHS is empty — no folders to watch.")
        return []

    db = context.resources.workflow_db

    # ── Round-robin: rotate which path goes first each tick ──
    tick_number = int(time.time()) // 30
    n = tick_number % len(watch_paths)
    rotated_paths = watch_paths[n:] + watch_paths[:n]

    # ── Phase 1: Scan watch directories ──────────────────────
    # Always scan, regardless of pipeline depth.

    phase1_start = time.perf_counter()
    current_files: dict[str, dict] = {}
    seen_paths: set[str] = set()
    total_scanned = 0
    skipped_extension = 0
    skipped_not_file = 0
    skipped_size = 0
    timed_out = False

    for watch_path in rotated_paths:
        watch_dir = Path(watch_path)
        if not watch_dir.exists():
            context.log.warning(f"Watch folder does not exist: {watch_path}")
            continue

        path_count = 0

        def _scan_recursive(dir_path):
            nonlocal total_scanned, skipped_extension, skipped_not_file, skipped_size, timed_out, path_count
            try:
                with os.scandir(dir_path) as scan_iter:
                    for file_entry in scan_iter:
                        if path_count >= MAX_FILES_PER_PATH_PER_TICK:
                            return

                        total_scanned += 1

                        if time.perf_counter() - tick_start > TICK_DEADLINE_SEC:
                            timed_out = True
                            return

                        if not file_entry.is_file():
                            if file_entry.is_dir():
                                _scan_recursive(file_entry.path)
                            else:
                                skipped_not_file += 1
                            continue
                        if not accepted_file_type(
                            Path(file_entry.name).suffix.lstrip(".")
                        ):
                            skipped_extension += 1
                            continue

                        seen_paths.add(file_entry.path)

                        try:
                            st = file_entry.stat()
                            age_sec = time.time() - st.st_mtime
                            if age_sec < 5 or st.st_size == 0:
                                skipped_size += 1
                                context.log.info(
                                    f"Skipping in-flight file: {file_entry.name} "
                                    f"(modified {age_sec:.1f}s ago, size={st.st_size})"
                                )
                                continue
                        except OSError as exc:
                            context.log.warning(
                                f"OS error checking {file_entry.name}: {exc}"
                            )
                            continue

                        current_files[file_entry.path] = {
                            "size": st.st_size,
                            "mtime": st.st_mtime,
                        }
                        path_count += 1
            except OSError:
                pass

        try:
            with os.scandir(watch_dir) as dir_iter:
                for entry in dir_iter:
                    if not entry.is_dir():
                        continue
                    _scan_recursive(entry.path)
                    if timed_out or path_count >= MAX_FILES_PER_PATH_PER_TICK:
                        break
        except OSError:
            pass

        if path_count >= MAX_FILES_PER_PATH_PER_TICK:
            context.log.info(
                f"Path {watch_path} reached scan budget ({MAX_FILES_PER_PATH_PER_TICK}) — "
                f"remaining files deferred to next tick"
            )

        if timed_out:
            context.log.warning(
                f"Tick time limit ({TICK_DEADLINE_SEC}s) reached — "
                f"{total_scanned} items scanned, remaining deferred to next tick"
            )
            break

    context.log.info(
        f"Scan complete — scanned {total_scanned}, "
        f"skipped: not-file={skipped_not_file} ext={skipped_extension} "
        f"size-unstable={skipped_size}, found={len(current_files)}"
    )
    context.log.info(f"Phase 1 (scan) took {time.perf_counter() - phase1_start:.2f}s")

    # ── Phase 2: Load and prune cursor ───────────────────────
    # Cursor stores paths that have been launched but not yet
    # confirmed by DB — sensor memory between ticks.
    # Format: {"submitted": [...], "timestamps": {path: iso_time}}
    # Old format (list) auto-migrates on first tick.

    phase2_start = time.perf_counter()

    submitted_paths: set[str] = set()
    submitted_timestamps: dict[str, str] = {}
    if context.cursor:
        try:
            raw = json.loads(context.cursor)
            if isinstance(raw, list):
                submitted_paths = set(raw)
            elif isinstance(raw, dict):
                submitted_paths = set(raw.get("submitted", []))
                submitted_timestamps = raw.get("timestamps", {})
        except (json.JSONDecodeError, TypeError):
            submitted_paths = set()
            submitted_timestamps = {}

    cursor_initial = len(submitted_paths)

    # Migrate old-format entries (no timestamp) to current time.
    now = datetime.now(timezone.utc)
    for path in submitted_paths:
        if path not in submitted_timestamps:
            submitted_timestamps[path] = now.isoformat()

    # Prune: remove paths for files no longer on disk (moved away
    # by catalogue after processing).
    stale_on_disk = submitted_paths - set(current_files.keys())
    submitted_paths -= stale_on_disk
    for path in stale_on_disk:
        submitted_timestamps.pop(path, None)
    if stale_on_disk:
        context.log.info(
            f"Cursor: removed {len(stale_on_disk)} paths no longer on disk"
        )

    # Prune: remove paths that have been in the cursor longer than
    # the timeout without being confirmed by the DB. These files
    # will become candidates again on the next tick.
    timed_out_paths = set()
    for path in list(submitted_paths):
        ts_str = submitted_timestamps.get(path)
        if ts_str:
            try:
                ts = datetime.fromisoformat(ts_str)
                if (now - ts).total_seconds() > CURSOR_TIMEOUT_SEC:
                    timed_out_paths.add(path)
                    submitted_timestamps.pop(path, None)
            except ValueError:
                timed_out_paths.add(path)
                submitted_timestamps.pop(path, None)
    submitted_paths -= timed_out_paths
    if timed_out_paths:
        context.log.info(
            f"Cursor: evicted {len(timed_out_paths)} entries "
            f"older than {CURSOR_TIMEOUT_SEC}s — will retry"
        )

    # Clean up orphaned timestamp entries (not in submitted_paths)
    orphaned_timestamps = set(submitted_timestamps.keys()) - submitted_paths
    for path in orphaned_timestamps:
        submitted_timestamps.pop(path, None)
    if orphaned_timestamps:
        context.log.info(
            f"Cursor: cleaned up {len(orphaned_timestamps)} orphaned timestamp entries"
        )

    # ── Phase 3: DB batch lookup ─────────────────────────────
    # Cross-reference both disk files and cursor entries against
    # DB to determine what truly needs processing.

    phase3_start = time.perf_counter()

    all_disk_paths = set(current_files.keys())
    lookup_paths = all_disk_paths | submitted_paths

    existing_states: dict[str, tuple[str, str | None, datetime]] = {}
    try:
        if lookup_paths:
            existing_states = db.batch_lookup_file_states(lookup_paths)
    except Exception as exc:
        context.log.warning(
            f"DB batch lookup failed, proceeding with empty lookup: {exc}"
        )

    # Prune cursor: remove paths that now have DB records.
    # This means the op processed the file between the last launch
    # and this tick — the DB is now authoritative for those files.
    cursor_with_db = {p for p in submitted_paths if p in existing_states}
    submitted_paths -= cursor_with_db
    for path in cursor_with_db:
        submitted_timestamps.pop(path, None)
    if cursor_with_db:
        context.log.info(
            f"Cursor: removed {len(cursor_with_db)} paths now in DB"
        )

    context.log.info(
        f"Cursor state: {len(submitted_paths)} pending "
        f"(initial={cursor_initial}, pruned_disk={len(stale_on_disk)}, "
        f"pruned_db={len(cursor_with_db)})"
    )
    context.log.info(f"Phase 3 (DB lookup) took {time.perf_counter() - phase3_start:.2f}s")
    context.log.info(f"Phase 2 (cursor) took {time.perf_counter() - phase2_start:.2f}s")

    # ── Phase 4: Candidate filter ─────────────────────────────
    # A file needs processing iff:
    #   · NOT already launched (not in submitted_paths), AND
    #   · No DB row, OR DB row with retryable status

    phase4_start = time.perf_counter()

    candidates = []
    skipped_cursor = 0
    skipped_db = 0

    for file_key in current_files:
        if file_key in submitted_paths:
            skipped_cursor += 1
            continue

        state = existing_states.get(file_key)
        if state is None:
            candidates.append(file_key)
            continue

        db_status = state[0]
        if db_status in RETRYABLE_STATUSES:
            candidates.append(file_key)
            context.log.debug(
                f"  Retry candidate: {Path(file_key).name} (status={db_status})"
            )
        else:
            skipped_db += 1

    context.log.info(
        f"Dedup: {len(candidates)} candidates, "
        f"skipped: cursor={skipped_cursor} db={skipped_db}"
    )
    context.log.info(f"Phase 4 (filter) took {time.perf_counter() - phase4_start:.2f}s")

    # ── Phase 5: Gate check (ingest stages only) ─────────────
    # Only count Files actively in the pre-BP-PUT ingest pipeline.
    # 'File cleared for ingest' is excluded — it means catalogue
    # is done and the file is waiting for external BP PUT.

    phase5_start = time.perf_counter()

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM app.file_catalogue "
                    "WHERE file_status IN ("
                    "'No Status', 'assessed', 'checksummed', "
                    "'cataloguing'"
                    ") AND NOT (file_status = 'No Status' AND error_message = %s)",
                    (WAITING_FOR_PREVIOUS_PART_MSG,),
                )
                ingest_count = cur.fetchone()[0]
    except Exception as exc:
        context.log.warning(
            f"Ingest-depth check failed, skipping new launches: {exc}"
        )
        ingest_count = MAX_INGEST_DEPTH + 1

    if ingest_count > MAX_INGEST_DEPTH:
        context.log.info(
            f"watch_folder_sensor: drain mode — "
            f"{ingest_count} files in ingest pipeline, "
            f"launching at reduced rate"
        )

    context.log.info(
        f"watch_folder_sensor: ingest depth — "
        f"{ingest_count} files (limit {MAX_INGEST_DEPTH})"
    )
    context.log.info(f"Phase 5 (gate check) took {time.perf_counter() - phase5_start:.2f}s")

    # ── Phase 6: Launch RunRequests ───────────────────────────

    phase6_start = time.perf_counter()

    retry_candidates = []
    new_candidates = []

    for file_key in candidates:
        state = existing_states.get(file_key)
        if state is not None and state[0] in RETRYABLE_STATUSES:
            retry_candidates.append(file_key)
        else:
            new_candidates.append(file_key)

    new_candidates.sort(key=lambda p: Path(p).name)
    new_candidates = _prioritise_multipart(
        db, new_candidates, existing_states, seen_paths
    )
    retry_candidates = _prioritise_multipart(
        db, retry_candidates, existing_states, seen_paths
    )

    storage_groups: dict[str, dict[str, list[str]]] = {}
    for file_key in new_candidates:
        storage = _storage_key(file_key)
        group = storage_groups.setdefault(storage, {"new": [], "retry": []})
        group["new"].append(file_key)
    for file_key in retry_candidates:
        storage = _storage_key(file_key)
        group = storage_groups.setdefault(storage, {"new": [], "retry": []})
        group["retry"].append(file_key)

    ordered = []
    storage_order = sorted(storage_groups)
    if storage_order:
        offset = tick_number % len(storage_order)
        storage_order = storage_order[offset:] + storage_order[:offset]
    for storage in storage_order:
        group = storage_groups[storage]
        ordered += group["new"][:1] + group["retry"] + group["new"][1:]

    retry_paths = set(retry_candidates)

    run_requests = []
    launched = 0
    storage_launch_counts: dict[str, int] = {}
    for file_key in ordered:
        if launched >= MAX_NEW_PER_TICK:
            context.log.info(
                f"Per-tick launch cap ({MAX_NEW_PER_TICK}) reached — "
                f"{len(ordered) - launched} candidates deferred"
            )
            break

        storage = _storage_key(file_key)
        if storage_launch_counts.get(storage, 0) >= MAX_LAUNCHES_PER_STORAGE_PER_TICK:
            continue

        fname = Path(file_key).name
        base_run_key = f"ingest-{fname}-{int(current_files[file_key]['mtime'])}"
        if file_key in retry_paths:
            retry_token = int(time.time())
            run_key = f"{base_run_key}-retry-{retry_token}"
            context.log.info(
                f"Retrying deferred file: {fname} (retry ts={retry_token})"
            )
        else:
            run_key = base_run_key
            context.log.info(f"New file detected: {fname}")
        run_requests.append(
            RunRequest(
                run_key=run_key,
                run_config={
                    "ops": {
                        "assess_filename": {
                            "config": {"file_path": file_key}
                        }
                    }
                },
            )
        )
        submitted_paths.add(file_key)
        submitted_timestamps[file_key] = datetime.now(timezone.utc).isoformat()
        launched += 1
        storage_launch_counts[storage] = storage_launch_counts.get(storage, 0) + 1

    # ── Phase 7: Update cursor ────────────────────────────────

    cursor_data = {
        "submitted": sorted(submitted_paths),
        "timestamps": submitted_timestamps,
    }
    context.update_cursor(json.dumps(cursor_data))
    context.log.info(
        f"Cursor updated — {launched} launched, "
        f"{len(submitted_paths)} pending, "
        f"{len(current_files)} files on disk"
    )
    context.log.info(f"Phase 6 (launch) took {time.perf_counter() - phase6_start:.2f}s")
    context.log.info(f"Total tick time: {time.perf_counter() - tick_start:.2f}s")
    return run_requests
