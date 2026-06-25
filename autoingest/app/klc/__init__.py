import os
from flask import Blueprint
from autoingest.resources.database import WorkflowDatabase


def create_klc_blueprint():
    bp = Blueprint(
        "klc",
        __name__,
        template_folder="templates",
        static_folder="static",
    )

    bp.config = {}
    bp.config["db"] = WorkflowDatabase(
        host=os.environ["WORKFLOW_PG_HOST"],
        port=int(os.environ.get("WORKFLOW_PG_PORT", "5432")),
        username=os.environ["WORKFLOW_PG_USERNAME"],
        password=os.environ["WORKFLOW_PG_PASSWORD"],
        db_name=os.environ["WORKFLOW_PG_DB"],
    )
    bp.config["KLC_HELP_URL"] = os.environ.get("KLC_HELP_URL", "")

    return bp
