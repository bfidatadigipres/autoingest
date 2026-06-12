import json
import os
import hashlib
import pytest


@pytest.fixture
def tmp_bucket_json(tmp_path):
    data = {
        "preservation0": True,
        "imagen": True,
        "preservationblobbing0": True,
        "netflix0": True,
        "netflixblobbing": True,
        "amazon0": True,
        "amazonblobbing": True,
    }
    p = tmp_path / "buckets.json"
    p.write_text(json.dumps(data))
    return str(p)


@pytest.fixture
def tmp_control_json(tmp_path):
    data = {"current_api": "CID_API_URL"}
    p = tmp_path / "downtime_control.json"
    p.write_text(json.dumps(data))
    return str(p)


@pytest.fixture
def env_setup(tmp_path, tmp_bucket_json, tmp_control_json):
    import os
    os.environ.setdefault("LOG_PATH", str(tmp_path))
    os.environ.setdefault("DPI_BUCKET", tmp_bucket_json)
    os.environ.setdefault("CID_API_URL", "http://fake-api.test")
    yield


@pytest.fixture
def known_content_file(tmp_path):
    content = b"Hello, World!" * 65536
    f = tmp_path / "test.bin"
    f.write_bytes(content)
    md5 = hashlib.md5(content).hexdigest()
    return str(f), md5, content
