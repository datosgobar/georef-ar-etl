from __future__ import with_statement

from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

###################################
#    Agregado por georef-ar-etl   #
###################################

from georef_ar_etl import models, read_config
target_metadata = models.Base.metadata

georef_config = read_config()
db_url = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
    **georef_config['db'])
config.set_main_option('sqlalchemy.url', db_url)


def include_object(obj, name, type_, reflected, compare_to):
    # NO incluir en migraciones:
    # - La tabla especial 'spatial_ref_sys' generada por la extensión PostGIS
    # - Tablas tmp_
    # - Índices
    if not name:
        return True

    return name != 'spatial_ref_sys' and \
        not name.startswith('tmp') and \
        type_ != 'index'


def process_revision_directives(context, revision, directives):
    # No generar migraciones vacías
    if config.cmd_opts.autogenerate:
        script = directives[0]
        if script.upgrade_ops.is_empty():
            directives[:] = []

###################################
#              Fin                #
###################################


# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url, target_metadata=target_metadata, literal_binds=True
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata,
            include_object=include_object,
            process_revision_directives=process_revision_directives
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
