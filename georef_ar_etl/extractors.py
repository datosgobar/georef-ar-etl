import requests
from .process import Step, ProcessException


class DownloadURLStep(Step):
    def __init__(self, filename, url, params=None):
        super().__init__('download_url', reads_input=False)
        self._filename = filename
        self._url = url
        self._params = params

    def _run_internal(self, data, ctx):
        if ctx.fs.isfile(self._filename) and ctx.mode == 'interactive':
            ctx.report.info('Salteando descarga: %s', self._url)
            return self._filename

        ctx.report.info('Descargando: %s', self._url)
        with requests.get(self._url, stream=True, params=self._params) as req:
            if req.status_code != 200:
                raise ProcessException(
                    'La petición HTTP retornó código {}.'.format(
                        req.status_code))

            chunk_size = ctx.config.getint('etl', 'chunk_size')
            with ctx.fs.open(self._filename, 'wb') as f:
                for chunk in req.iter_content(chunk_size=chunk_size):
                    f.write(chunk)

        return self._filename
