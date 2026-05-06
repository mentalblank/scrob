import gzip
import io
import json
import struct

import asyncpg

from core.config import settings


def _topological_sort(tables: list[str], edges: list[tuple[str, str]]) -> list[str]:
    deps: dict[str, set[str]] = {t: set() for t in tables}
    for child, parent in edges:
        if child in deps and parent in deps:
            deps[child].add(parent)
    result: list[str] = []
    seen: set[str] = set()

    def visit(t: str) -> None:
        if t in seen:
            return
        seen.add(t)
        for p in deps.get(t, set()):
            visit(p)
        result.append(t)

    for t in tables:
        visit(t)
    return result


async def asyncpg_conn() -> asyncpg.Connection:
    dsn = settings.db_url.replace("postgresql+asyncpg://", "postgresql://")
    return await asyncpg.connect(dsn)


async def restore_backup(content: bytes) -> None:
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(content), mode="rb") as gz:
            raw = gz.read()
    except Exception:
        raise ValueError("Invalid backup file.")

    offset = 0
    header_len = struct.unpack_from(">I", raw, offset)[0]
    offset += 4
    header = json.loads(raw[offset:offset + header_len])
    offset += header_len

    table_data: dict[str, bytes] = {}
    while offset < len(raw):
        name_len = struct.unpack_from(">H", raw, offset)[0]
        offset += 2
        name = raw[offset:offset + name_len].decode()
        offset += name_len
        data_len = struct.unpack_from(">Q", raw, offset)[0]
        offset += 8
        table_data[name] = raw[offset:offset + data_len]
        offset += data_len

    conn = await asyncpg_conn()
    try:
        # Validate table names against the real schema before any SQL interpolation.
        # Backup headers are untrusted input; names containing '"' could break the
        # TRUNCATE f-string. Only names returned by pg_tables are safe identifiers.
        schema_rows = await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )
        valid_tables = {r["tablename"] for r in schema_rows}
        tables = [t for t in header["tables"] if t in table_data and t in valid_tables]

        fk_rows = await conn.fetch(
            """
            SELECT c.relname AS child, p.relname AS parent
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_class p ON p.oid = con.confrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE con.contype = 'f' AND n.nspname = 'public'
            """
        )
        edges = [(r["child"], r["parent"]) for r in fk_rows]
        ordered = _topological_sort(tables, edges)

        async with conn.transaction():
            table_list = ", ".join(f'"{t}"' for t in ordered)
            await conn.execute(f"TRUNCATE TABLE {table_list} RESTART IDENTITY CASCADE")
            for table in ordered:
                await conn.copy_to_table(table, source=io.BytesIO(table_data[table]), format="binary")

        seq_rows = await conn.fetch(
            """
            SELECT s.relname AS seq, a.attname AS col, c.relname AS tbl
            FROM pg_class s
            JOIN pg_depend d ON d.objid = s.oid
            JOIN pg_class c ON c.oid = d.refobjid
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.refobjsubid
            JOIN pg_namespace n ON n.oid = s.relnamespace
            WHERE s.relkind = 'S' AND n.nspname = 'public'
            """
        )
        for row in seq_rows:
            await conn.execute(
                f'SELECT setval($1, COALESCE((SELECT MAX("{row["col"]}") FROM "{row["tbl"]}"), 1), true)',
                row["seq"],
            )
    finally:
        await conn.close()
