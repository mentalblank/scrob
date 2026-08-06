"""Every file Scrob hands out must be one the matching upload accepts."""
import inspect
import os
import pathlib
import re

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from routers import admin, auth, export, trakt

FRONTEND = pathlib.Path(__file__).resolve().parents[2] / "frontend" / "src" / "pages"


def _accepts(fn) -> set[str]:
    """Extensions an upload endpoint tests the filename against."""
    return set(re.findall(r'endswith\("(\.[a-z]+)"\)', inspect.getsource(fn)))


def _produces(fn) -> set[str]:
    """Extensions an endpoint puts in the download filename."""
    return set(re.findall(r'filename = f"[^"]*(\.[a-z]+)"', inspect.getsource(fn)))


def test_data_export_downloads_a_file_its_import_accepts():
    produced = _produces(export.export_data)
    assert produced == {".zip"}
    assert produced <= _accepts(export.import_data)


def test_database_backup_downloads_a_file_both_restores_accept():
    produced = _produces(admin.backup_database)
    assert produced == {".pgdump"}
    assert produced <= _accepts(admin.restore_database)
    assert produced <= _accepts(auth.bootstrap_restore)


def test_trakt_export_import_accepts_the_same_archive_type():
    assert _accepts(trakt.trakt_import_upload) == {".zip"}


def test_extension_checks_are_case_insensitive():
    """A file picker can hand back UPPERCASE.PGDUMP; the check must still pass."""
    for fn in (export.import_data, trakt.trakt_import_upload,
               admin.restore_database, auth.bootstrap_restore):
        source = inspect.getsource(fn)
        assert ".lower()" in source, fn.__name__


def test_file_pickers_offer_every_extension_the_endpoint_accepts():
    """An accept="" filter narrower than the endpoint hides valid backups."""
    expected = {
        "admin.astro": ".pgdump,.bak",
        "login.astro": ".pgdump,.bak",
        "register.astro": ".pgdump,.bak",
    }
    for page, accept in expected.items():
        source = (FRONTEND / page).read_text()
        assert f'accept="{accept}"' in source, page

    connections = (FRONTEND / "connections.astro").read_text()
    assert connections.count('accept=".zip"') == 2
