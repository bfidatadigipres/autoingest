# New added 21/4/2026 - validity not knownyet
# resources/celery_client.py
from dagster import ConfigurableResource
from celery import Celery
from typing import Any
import time


class CeleryClientResource(ConfigurableResource):
    broker_url: str
    backend_url: str

    def _get_app(self) -> Celery:
        # Lazy init so the resource serialises cleanly
        if not hasattr(self, "_app"):
            self._app = Celery(
                "transcode_workers",
                broker=self.broker_url,
                backend=self.backend_url,
            )
            self._app.conf.update(
                result_expires=86400,
                task_serializer="json",
                result_serializer="json",
                accept_content=["json"],
            )
        return self._app

    def submit_transcode(self, source_path: str, output_path: str, preset: dict) -> Any:
        """Send a single transcode task to the worker pool."""
        app = self._get_app()
        return app.send_task(
            "tasks.transcode_file",
            args=[source_path, output_path, preset],
            queue="transcode",
        )

    def submit_batch(self, task_list: list[dict]) -> list[tuple[dict, Any]]:
        """Submit many transcode tasks, return list of (task_info, async_result)."""
        results = []
        for task in task_list:
            async_result = self.submit_transcode(
                task["source_path"],
                task["output_path"],
                task["preset"],
            )
            results.append((task, async_result))
        return results

    def wait_for_results(self, pending: list[tuple[dict, Any]], timeout: int = 7200, poll_interval: int = 10) -> list[dict]:
        """Poll until all tasks complete or timeout."""
        completed = []
        deadline = time.time() + timeout

        while pending and time.time() < deadline:
            still_pending = []
            for task_info, async_result in pending:
                if async_result.ready():
                    completed.append({
                        **task_info,
                        "success": async_result.successful(),
                        "result": async_result.result if async_result.successful() else str(async_result.result),
                    })
                else:
                    still_pending.append((task_info, async_result))
            pending = still_pending
            if pending:
                time.sleep(poll_interval)

        # Anything still pending after timeout is marked failed
        for task_info, async_result in pending:
            completed.append({
                **task_info,
                "success": False,
                "result": "Timed out waiting for worker",
            })
        return completed