"""Alembic environment: async engine, URL from settings, models as the autogenerate target."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from orchestrator.config import get_settings
from orchestrator.persistence.models import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _database_url() -> str:
    override = config.get_main_option("sqlalchemy.url")
    return override or get_settings().database_url


def run_migrations_offline() -> None:
    context.configure(url=_database_url(), target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def _run_sync(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def _run_async() -> None:
    engine = create_async_engine(_database_url())
    try:
        async with engine.connect() as connection:
            await connection.run_sync(_run_sync)
    finally:
        await engine.dispose()


def run_migrations_online() -> None:
    """Run migrations whether or not an event loop is already running.

    The CLI has no loop. Tests and in-process callers often invoke Alembic from inside a running
    loop, where ``asyncio.run`` is illegal, so the migration then runs on a helper thread that
    owns its own loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(_run_async())
        return
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, _run_async()).result()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
