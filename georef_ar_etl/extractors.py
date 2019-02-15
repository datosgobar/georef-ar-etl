import shutil
import requests
from .etl import ProcessException


def download_url(filename, url, ctx):
    if ctx.cache.isfile(filename) and ctx.mode == 'interactive':
        ctx.logger.info('Salteando descarga: %s', url)
        return filename

    ctx.logger.info('Descargando: %s', url)
    with requests.get(url, stream=True) as req:
        if req.status_code != 200:
            raise ProcessException(
                'La petición HTTP devolvió código {}.'.format(req.status_code))

        with ctx.cache.open(filename, 'wb') as f:
            shutil.copyfileobj(req.raw, f)

    return filename
