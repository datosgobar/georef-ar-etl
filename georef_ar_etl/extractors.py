import shutil
import requests


def download_url(filename, url, context):
    if context.cache.isfile(filename):
        context.logger.info('Salteando descarga: %s', url)
        return filename

    context.logger.info('Descargando: %s', url)
    with requests.get(url, stream=True) as req:
        if req.status_code != 200:
            # TODO: Cambiar tipo de error
            raise RuntimeError('HTTP status != 200')

        with context.cache.open(filename, 'wb') as f:
            shutil.copyfileobj(req.raw, f)

    return filename
