import shutil
import requests
from .process import Step, ProcessException


class DownloadURLStep(Step):
    def __init__(self, filename, url):
        super().__init__('download_url', reads_input=False)
        self._filename = filename
        self._url = url

    def _run_internal(self, data, ctx):
        if ctx.fs.isfile(self._filename) and ctx.mode == 'interactive':
            ctx.logger.info('Salteando descarga: %s', self._url)
            return self._filename

        ctx.logger.info('Descargando: %s', self._url)
        with requests.get(self._url, stream=True) as req:
            if req.status_code != 200:
                raise ProcessException(
                    'La petición HTTP devolvió código {}.'.format(
                        req.status_code))

            with ctx.fs.open(self._filename, 'wb') as f:
                shutil.copyfileobj(req.raw, f)

        return self._filename
