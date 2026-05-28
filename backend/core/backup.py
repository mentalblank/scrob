"""Database backup and restore via pg_dump / pg_restore."""
import asyncio
import os
from urllib.parse import urlparse

from core.config import settings


def _parse_dsn() -> tuple[str, dict, list[str]]:
    dsn = settings.db_url.replace("postgresql+asyncpg://", "postgresql://")
    parsed = urlparse(dsn)
    dbname = parsed.path.lstrip("/")

    env = os.environ.copy()
    if parsed.password:
        env["PGPASSWORD"] = parsed.password

    conn_args: list[str] = []
    if parsed.hostname:
        conn_args += ["-h", parsed.hostname]
    if parsed.port:
        conn_args += ["-p", str(parsed.port)]
    if parsed.username:
        conn_args += ["-U", parsed.username]

    return dbname, env, conn_args


async def pg_dump() -> bytes:
    """Run pg_dump and return the custom-format backup as bytes."""
    dbname, env, conn_args = _parse_dsn()

    cmd = [
        "pg_dump",
        "--format=custom",
        "--no-owner",
        "--no-acl",
        "--compress=6",
        *conn_args,
        dbname,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
    except FileNotFoundError:
        raise RuntimeError("pg_dump not found. Add postgresql-client to the server environment.")

    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        err = stderr.decode(errors="replace").strip()
        raise RuntimeError(f"pg_dump failed (exit {proc.returncode}): {err}")

    return stdout


async def pg_restore(data: bytes) -> None:
    """Run pg_restore to load a pg_dump custom-format backup.

    Uses --clean --if-exists so existing objects are dropped first, then
    recreated. Wrapped in --single-transaction so a failure rolls back fully.
    """
    dbname, env, conn_args = _parse_dsn()

    cmd = [
        "pg_restore",
        "--format=custom",
        "--no-owner",
        "--no-acl",
        "--clean",
        "--if-exists",
        "--single-transaction",
        "-d", dbname,
        *conn_args,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
    except FileNotFoundError:
        raise RuntimeError("pg_restore not found. Add postgresql-client to the server environment.")

    stdout, stderr = await proc.communicate(input=data)

    if proc.returncode != 0:
        err = stderr.decode(errors="replace").strip()
        raise RuntimeError(f"pg_restore failed (exit {proc.returncode}): {err}")
