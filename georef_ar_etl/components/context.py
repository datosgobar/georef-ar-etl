class Context:
    def __init__(self, config, filesystem, logger):
        self._config = config
        self._filesystem = filesystem
        self._logger = logger

    @property
    def fs(self):
        return self._filesystem

    @property
    def config(self):
        return self._config

    @property
    def logger(self):
        return self._logger
