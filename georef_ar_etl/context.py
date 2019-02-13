from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base


class Context:
    def __init__(self, config, data_fs, cache_fs, engine, logger,
                 interactive=False):
        self._config = config
        self._data = data_fs
        self._cache = cache_fs
        self._engine = engine
        self._logger = logger
        self._interactive = interactive
        self._session_maker = sessionmaker(bind=engine)
        self._session = None

    def automap_table(self, table_name):
        metadata = MetaData()
        metadata.reflect(self._engine, only=[table_name])
        base = automap_base(metadata=metadata)
        base.prepare()

        return getattr(base.classes, table_name)

    @property
    def config(self):
        return self._config

    @property
    def data(self):
        return self._data

    @property
    def cache(self):
        return self._cache

    @property
    def logger(self):
        return self._logger

    @property
    def interactive(self):
        return self._interactive

    @property
    def session(self):
        if not self._session:
            self._session = self._session_maker()

        return self._session
