#!/usr/bin/env python3

"""
RETRIEVE PATH NAME SYS.ARGV[1] FROM CRON LAUNCH

Script to manage ingest of items over 1TB in size

Script PUT actions:
1. Identify supply path and collection for blobbing bucket selection.
2. Adds item found second level in ingest/(bfi/amazon...)/blob/.
3. The script iterates individual files and PUT to Black Pearl using ds3Helper
   client, and using the blobbing command for items over 1TB.
4. Once complete request that a notification JSON is issued to validate PUT success.
5. Use received job_id to create new folder in validation/job_id path then move file in.
6. Update Autoingest Dagster PostgreSQL table with job id.

2026
"""


import logging
import os
import shutil
import sys
import time

# Local import
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from autoingest.resources.database import WorkflowDatabase
import autoingest.resources.bp_utils as bp
import autoingest.resources.utils as utils

# Global vars
LOG_PATH = os.environ["LOG_PATH"]
CONTROL_JSON = os.environ["CONTROL_JSON"]
INGEST_CONFIG = os.environ["INGEST_SIZE"]

# Setup logging
LOGGER = logging.getLogger(
    f'black_pearl_move_put_blobbing_{sys.argv[1].replace("/", "_")}'
)
HDLR = logging.FileHandler(
    os.path.join(
        LOG_PATH, f'black_pearl_move_put_blobbing_{sys.argv[1].replace("/", "_")}.log'
    )
)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def main():
    """
    Access Black Pearl ingest folders, move items into subfolder
    If subfolder size exceeds 'upload_size', trigger put_dir to send
    the contents to BP bucket in one block. If subfolder doesn't exceed
    'upload_size' (specified by INGEST_CONFIG) leave for next pass.
    """
    if not sys.argv[1]:
        sys.exit("Missing launch path, script exiting")
    if not utils.check_control("black_pearl_put"):
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.check_storage(sys.argv[1]):
        sys.exit("Script run prevented by storage_control.json. Script exiting.")

    if "/netflix/" in str(sys.argv[1]):
        fullpath = os.environ["PLATFORM_INGEST_PTH"]
        autoingest = os.path.join(
            fullpath, f"{os.environ['BP_INGEST_NETFLIX']}/blob/"
        )
        bucket_collection = "netflix"
    elif "/amazon/" in str(sys.argv[1]):
        fullpath = os.environ["PLATFORM_INGEST_PTH"]
        autoingest = os.path.join(
            fullpath, f"{os.environ['BP_INGEST_AMAZON']}/blob/"
        )
        bucket_collection = "amazon"
    elif "/disney/" in str(sys.argv[1]):
        fullpath = os.environ["PLATFORM_INGEST_PTH"]
        autoingest = os.path.join(
            fullpath, f"{os.environ['BP_INGEST_DISNEY']}/blob/"
        )
        bucket_collection = "disney"
    else:
        # Just configuring for BFI ingests >1TB at this time
        data_sizes = utils.read_yaml(INGEST_CONFIG)
        hosts = data_sizes["Host_size"]
        for host in hosts:
            for key, _ in host.items():
                if str(sys.argv[1]) in key:
                    fullpath = key
        autoingest = os.path.join(fullpath, f"{os.environ['BP_INGEST']}/blob")
        bucket_collection = "bfi"
        print(f"*** Bucket collection: {bucket_collection}")
        print(f"Fullpath: {fullpath} {autoingest}")

    if not os.path.exists(autoingest):
        LOGGER.warning("Complication with autoingest path: %s", autoingest)
        sys.exit("Supplied argument did not match path")

    # Get current bucket name for bucket_collection type 
    # _ bucket_list response will be needed later in script life
    bucket, _ = bp.get_buckets(bucket_collection, True)

    # Get initial files as list, exit if none
    files = [
        f for f in os.listdir(autoingest) if os.path.isfile(os.path.join(autoingest, f))
    ]
    files.sort()
    if files:
        LOGGER.info(
            "======== START Black Pearl blob ingest %s START ========",
            sys.argv[1],
        )

        for fname in files:
            if not utils.check_control("black_pearl_put") or not utils.check_control(
                "pause_scripts"
            ):
                sys.exit(
                    "Script run prevented by downtime_control.json. Script exiting."
                )
            if ".DS_Store" in fname:
                continue
            if fname.startswith("."):
                continue
            if fname.endswith((".md5", ".log", ".mhl", ".ini", ".json")):
                continue
            fpath = os.path.join(autoingest, fname)

            status = bp.check_no_bp_status(fname, [bucket])
            if status is False:
                print(f"bp.check_no_bp_status: {status}")
                LOGGER.warning(
                    "Skipping. File already found in Black Pearl: %s",
                    fname,
                )
                continue

            # Begin blobbed PUT (bool argument for checksum validation off/on in ds3Helpers)
            put_job_id = ""
            tic = time.perf_counter()
            LOGGER.info("Beginning PUT of blobbing file %s", fname)
            put_job_id = bp.put_single_file(fpath, fname, bucket, check=True)
            toc = time.perf_counter()
            checksum_put_time = (toc - tic) // 60
            LOGGER.info(
                "** Total time in minutes for Blobbed PUT with internal hash validation: %s",
                checksum_put_time,
            )

            # Confirm job list exists / send notification / write to postgreSQL
            if put_job_id:
                confirmation = bp.put_notification(put_job_id)
                LOGGER.info(
                    "Job %s registered for completion notification at %s", put_job_id, confirmation
                )
                validate_path = os.path.join(fullpath, os.environ.get("VALIDATION"), f"{put_job_id}/")
                LOGGER.info("Moving file into new folder: %s", validate_path)
                try:
                    os.makedirs(validate_path, exist_ok=False)
                    shutil.move(fpath, os.path.join(validate_path, fname))
                    os.chmod(os.path.join(validate_path, fname), 0o777)
                except OSError as err:
                    LOGGER.warning("Error with making validate path and moving file in: %s", validate_path)
                    LOGGER.warning("Manual move of file %s required, and manual update of BP JOB ID: %s", fname, put_job_id)
                    LOGGER.warning("Error: %s", err)

                # Write job id to each file name in folder
                if os.path.isfile(os.path.join(validate_path, fname)):
                    fail_list = update_job_id_postgres(put_job_id, fname)
                    if len(fail_list) > 0:
                        for fail in fail_list:
                            LOGGER.warning("%s - PostgreSQL row for filename was not updated with job ID: %s", fail, put_job_id)
                    else:
                        LOGGER.info("PostgreSQL file row %s updated with job_id %s", fname, put_job_id)
                else:
                    LOGGER.warning("File %s failed to move file to validate path: Manual move and update of BP Job ID needed:\n%s\nJoB ID:%s", fname, validate_path, put_job_id)

            else:
                LOGGER.warning(
                    "JOB list retrieved for file is not correct. %s: %s",
                    fname,
                    put_job_id,
                )

    LOGGER.info(f"======== END Black Pearl blob ingest {sys.argv[1]} END ========")


def _get_db() -> WorkflowDatabase:
    return WorkflowDatabase(
        host=os.environ["WORKFLOW_PG_HOST"],
        port=int(os.environ.get("WORKFLOW_PG_PORT", "5432")),
        username=os.environ["WORKFLOW_PG_USERNAME"],
        password=os.environ["WORKFLOW_PG_PASSWORD"],
        db_name=os.environ["WORKFLOW_PG_DB"],
    )


def update_job_id_postgres(job_id: str, file: str) -> list[str]:
    """
    Read new renamed folder contents
    and update to postgreSQL database
    """

    failed_write: list[str] = []

    db = _get_db()
    if not db.update_bp_job_id(file, job_id):
        failed_write.append(file)

    return failed_write


if __name__ == "__main__":
    main()
