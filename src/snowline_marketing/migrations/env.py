from alembic import context
from sqlalchemy import engine_from_config, pool

from snowline_marketing.config import database_url
from snowline_marketing.models import Base

config = context.config
# Respect a caller-provided URL (the shared `db.alembic_config()` sets one);
# only default to the env-derived URL when absent (the bare alembic CLI).
# Overwriting unconditionally would silently redirect a caller who pointed a
# Config at a different database.
if not config.get_main_option("sqlalchemy.url"):
    config.set_main_option("sqlalchemy.url", database_url())

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
