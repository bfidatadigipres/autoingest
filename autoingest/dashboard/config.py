import os
from autoingest.resources.database import WorkflowDatabase


def get_db():
    return WorkflowDatabase(
        host=os.environ["WORKFLOW_PG_HOST"],
        port=int(os.environ.get("WORKFLOW_PG_PORT", "5432")),
        username=os.environ["WORKFLOW_PG_USERNAME"],
        password=os.environ["WORKFLOW_PG_PASSWORD"],
        db_name=os.environ["WORKFLOW_PG_DB"],
    )


REFRESH_SECONDS = int(os.environ.get("DASHBOARD_REFRESH", "60"))
MAX_ROWS = int(os.environ.get("DASHBOARD_MAX_ROWS", "500"))
