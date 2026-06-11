# autoingest

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Navigate terminal into your autoingest/ folder. Start the Dagster UI web server:

```bash
dagster dev -h 0.0.0.0 -p 3000
```

Open http://localhost:3000 in your browser to see the project. Local host and port can be adjusted as needed.

### Starting Celery Workers (for distributed transcoding)

On each worker server, install the project and run:

```bash
uv sync
source .venv/bin/activate
dagster-celery worker -A dagster_celery.app
```

Workers need the same environment variables as the control server to connect to Redis and PostgreSQL.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
