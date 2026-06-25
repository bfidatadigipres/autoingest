import os
from flask import Flask
from autoingest.resources.database import WorkflowDatabase


def create_app():
    app = Flask(__name__)

    app.config["db"] = WorkflowDatabase(
        host=os.environ["WORKFLOW_PG_HOST"],
        port=int(os.environ.get("WORKFLOW_PG_PORT", "5432")),
        username=os.environ["WORKFLOW_PG_USERNAME"],
        password=os.environ["WORKFLOW_PG_PASSWORD"],
        db_name=os.environ["WORKFLOW_PG_DB"],
    )

    app.config["CONFLUENCE_URL"] = os.environ.get("CONFLUENCE_URL", "")
    app.config["SERVICE_DESK_URL"] = os.environ.get("SERVICE_DESK_URL", "")
    app.config["KLC_HELP_URL"] = os.environ.get("KLC_HELP_URL", "")

    from autoingest.app.routes import bp
    app.register_blueprint(bp)

    from autoingest.app.klc.routes import klc_bp
    app.register_blueprint(klc_bp, url_prefix="/klc")

    return app


if __name__ == "__main__":
    app = create_app()
    port = int(os.environ.get("VIEWER_PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
