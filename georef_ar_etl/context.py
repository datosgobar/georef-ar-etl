from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

RUN_MODES = frozenset(['normal', 'interactive', 'testing'])


class Context:
    def __init__(self, config, data_fs, cache_fs, engine, logger,
                 mode='normal'):
        self._config = config
        self._data = data_fs
        self._cache = cache_fs
        self._engine = engine
        self._logger = logger

        if mode not in RUN_MODES:
            raise ValueError('Invalid run mode')

        self._mode = mode
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
    def mode(self):
        return self._mode

    @property
    def session(self):
        if not self._session:
            self._session = self._session_maker()

        return self._session

    def query(self, entity_class):
        return self.session.query(entity_class)
