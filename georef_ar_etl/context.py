from sqlalchemy.orm import sessionmaker


class Context:
    def __init__(self, config, filesystem, engine, logger):
        self._config = config
        self._filesystem = filesystem
        self._logger = logger
        self._session_maker = sessionmaker(bind=engine)
        self._session = None

    @property
    def config(self):
        return self._config

    @property
    def fs(self):
        return self._filesystem

    @property
    def logger(self):
        return self._logger

    @property
    def session(self):
        if not self._session:
            self._session = self._session_maker()

        return self._session
