import os
from pathlib import Path
from dagster import op, Config, Out, Output


class FileAssessmentConfig(Config):
    file_path: str


@op(
    out={"file_info": Out(dict)},
    tags={"dagster-celery/queue": "default"},
)
def assess_filename(context, config: FileAssessmentConfig, workflow_db) -> dict:
    file_path = Path(config.file_path)
    filename = file_path.name
    filetype = file_path.suffix.lower().lstrip(".")
    filesize = file_path.stat().st_size

    context.log.info(f"Assessing file: {filename} ({filetype}, {filesize} bytes)")

    field_details = workflow_db.lookup_file_details(filename, filetype)
    if field_details is None:
        context.log.warning(
            f"No field details found for filetype '{filetype}', using defaults"
        )

    return {
        "file_path": str(file_path),
        "filename": filename,
        "filetype": filetype,
        "filesize": filesize,
        "field_details": field_details,
    }
