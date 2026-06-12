# autoingest Tests

## Prerequisites

Ensure you have the project dependencies installed:

```bash
cd autoingest
uv sync           # or: pip install -e ".[dev]"
source .venv/bin/activate
```

## Running Tests

```bash
# Activate the virtual environment first
source .venv/bin/activate

# Run all tests
pytest

# Run with verbose output (recommended)
pytest -v

# Run tests for a specific module
pytest tests/test_utils.py -v
pytest tests/test_proxy_utils.py -v
pytest tests/test_adlib.py -v
pytest tests/test_file_assessment.py -v
pytest tests/test_verification.py -v

# Run with coverage report
pytest --cov=autoingest tests/

# Run tests matching a keyword
pytest -k "check_filename or check_part_whole"
```

## Test Structure

| File | Tier | What it tests |
|---|---|---|
| `test_utils.py` | 1 + 2 | Pure functions (`accepted_file_type`, `check_filename`, `check_part_whole`, `get_object_number`, `sort_ext`) and filesystem functions (`check_storage`, `move_file`, `create_md5_65536`, `create_xxhash_65536`, `get_buckets`, `get_buckets_blob`, `get_current_api`, `probe_metadata`) |
| `test_proxy_utils.py` | 1 + 2 | Pure functions (`build_audio_args`, `select_video_filter`, `_safe_int`, `adjust_seconds`, `_retrieve_blackspaces`, `_check_seconds`) and filesystem (`check_mod_time`) |
| `test_adlib.py` | 1 | Pure Adlib record-processing functions (`retrieve_field_name`, `retrieve_facet_list`, `group_check`, `escape_xml`, `create_grouped_data`) |
| `test_file_assessment.py` | 1 | Pure filename/path parsing functions (`get_data_from_path`, `process_image_archive`, `check_for_multipart`) |
| `test_verification.py` | 2 | Filesystem-dependent verification functions (`retrieve_json_data`, `json_check`, `check_for_failed_file`) |

## Tier Descriptions

- **Tier 1** — Pure functions with no side effects. No mocking or file I/O required.
- **Tier 2** — Functions that depend on the filesystem (read/write files). Uses `tmp_path` fixtures.
