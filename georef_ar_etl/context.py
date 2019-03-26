from sqlalchemy.orm import sessionmaker

RUN_MODES = ['normal', 'interactive', 'testing']


class CachedQuery:
    def __init__(self, query):
        self._cache = {}
        self._query = query

    def __getattr__(self, name):
        return getattr(self._query, name)

    def get(self, key):
        if key not in self._cache:
            self._cache[key] = self._query.get(key)

        return self._cache[key]


class CachedSession:
    def __init__(self, session):
        self._queries = {}
        self._session = session

    def __getattr__(self, name):
        return getattr(self._session, name)

    def query(self, query_class):
        if query_class not in self._queries:
            self._queries[query_class] = CachedQuery(
                self._session.query(query_class))

        return self._queries[query_class]


class Report:
    def __init__(self, logger):
        self._logger = logger
        self.reset()

    def add_data(self, creator, key, data):
        self.info('Dato "%s" agregado al reporte.', key)
        creator_data = self._data.setdefault(creator, {})
        creator_data[key] = data

    def info(self, *args, **kwargs):
        self._logger.info(*args, **kwargs)

    def warn(self, *args, **kwargs):
        self._logger.warning(*args, **kwargs)

    def error(self, *args, **kwargs):
        self._logger.error(*args, **kwargs)

    def exception(self, *args, **kwargs):
        self._logger.exception(*args, **kwargs)

    def reset(self):
        self._data = {}

    def get_data(self, creator):
        return self._data[creator]

    @property
    def logger(self):
        return self._logger


class Context:
    def __init__(self, config, fs, engine, logger, mode='normal'):
        if mode not in RUN_MODES:
            raise ValueError('Invalid run mode.')

        self._config = config
        self._fs = fs
        self._engine = engine
        self._report = Report(logger)
        self._mode = mode
        self._session_maker = sessionmaker(bind=engine)
        self._session = None

    @property
    def config(self):
        return self._config

    @property
    def fs(self):
        return self._fs

    @property
    def engine(self):
        return self._engine

    @property
    def report(self):
        return self._report

    @property
    def mode(self):
        return self._mode

    @property
    def session(self):
        if not self._session:
            self._session = self._session_maker()

        return self._session

    def cached_session(self):
        return CachedSession(self.session)
