import hashlib
import requests
from .process import Step, ProcessException


class DownloadURLStep(Step):
    def __init__(self, filename, url, process_name, params=None):
        super().__init__('download_url', reads_input=False)
        self._filename = filename
        self._url = url
        self._params = params
        self._process_name = process_name

    def _run_internal(self, data, ctx):
        if ctx.fs.isfile(self._filename) and ctx.mode == 'interactive':
            ctx.report.info('Salteando descarga: %s', self._url)
            return self._filename

        report_data = ctx.report.get_data(self.name)

        with requests.get(self._url, stream=True, params=self._params) as req:
            # req.url contiene la url con el querystring final
            ctx.report.info('Descargando: %s', req.url)
            chunk_size = ctx.config.getint('etl', 'chunk_size')

            if req.status_code != 200:
                raise ProcessException(
                    'La petición HTTP retornó código {}.'.format(
                        req.status_code))

            md5 = hashlib.md5()
            with ctx.fs.open(self._filename, 'wb') as f:
                for chunk in req.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
                    md5.update(chunk)

            ctx.report.info('Archivo descargado. Hash MD5: {}'.format(
                md5.hexdigest()))

            report_data.setdefault(self._process_name, {})[req.url] = md5.hexdigest()

        return self._filename
