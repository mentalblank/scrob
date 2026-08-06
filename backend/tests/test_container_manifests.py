import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _read(name: str) -> str:
    path = os.path.join(REPO_ROOT, name)
    assert os.path.exists(path), f"{name} is missing"
    with open(path, "r") as f:
        return f.read()


def test_standard_dockerfile_manifest():
    content = _read("Dockerfile")

    assert "AS frontend-builder" in content
    assert "uv sync --frozen --no-dev --no-cache" in content
    assert "COPY entrypoint.sh /entrypoint.sh" in content
    assert "COPY supervisord.conf /etc/supervisor/supervisord.conf" in content
    assert "EXPOSE 7330" in content
    assert 'ENTRYPOINT ["/entrypoint.sh"]' in content


def test_omnibus_dockerfile_manifest():
    content = _read("Dockerfile.omnibus")

    assert "AS frontend-builder" in content
    assert "postgresql" in content
    # entrypoint.omnibus.sh execs this exact path in external-database mode
    assert "COPY supervisord.conf /etc/supervisor/supervisord.conf" in content
    assert "EXPOSE 7330" in content
    assert 'ENTRYPOINT ["/entrypoint.omnibus.sh"]' in content


def test_supervisord_runs_backend_from_backend_dir():
    content = _read("supervisord.conf")

    assert "directory=/app/backend" in content
    assert "/app/backend/.venv/bin/uvicorn main:app" in content


def test_compose_manifest():
    content = _read("docker-compose.yaml")

    assert "scrob-db" in content
    assert "7330:7330" in content
    assert "DATABASE_URL" in content
    assert "SECRET_KEY" in content


def test_omnibus_compose_manifest():
    content = _read("docker-compose.omnibus.yml")

    assert "7330:7330" in content
    assert "SECRET_KEY" in content
