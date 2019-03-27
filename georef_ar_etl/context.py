import os
import smtplib
import json
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from sqlalchemy.orm import sessionmaker
from . import constants

RUN_MODES = ['normal', 'interactive', 'testing']
SMTP_TIMEOUT = 10


def send_email(host, user, password, subject, message, recipients,
               attachments=None, timeout=SMTP_TIMEOUT):
    """Envía un mail a un listado de destinatarios.

    Args:
        host (str): Hostname de servidor SMTP.
        user (str): Usuario del servidor SMTP.
        password (str): Contraseña para el usuario.
        subject (str): Asunto a utilizar en el mail enviado.
        message (str): Contenido del mail a enviar.
        recipients (list): Lista de destinatarios.
        attachments (dict): Diccionario de contenidos <str, str> a adjuntar en
            el mail. Las claves representan los nombres de los contenidos y los
            valores representan los contenidos en sí.
        timeout (int): Tiempo máximo a esperar en segundos para establecer la
            conexión al servidor SMTP.

    """
    with smtplib.SMTP_SSL(host, timeout=timeout) as smtp:
        smtp.login(user, password)

        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg["From"] = user
        msg["To"] = ",".join(recipients)
        msg.attach(MIMEText(message))

        for name, contents in (attachments or {}).items():
            attachment = MIMEText(contents)
            attachment['Content-Disposition'] = \
                'attachment; filename="{}"'.format(name)
            msg.attach(attachment)

        smtp.send_message(msg)


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
    def __init__(self, logger, logger_stream=None):
        self._logger = logger
        self._logger_stream = logger_stream
        self.reset()

    def get_data(self, creator):
        if creator not in self._data:
            self._data[creator] = {}

        return self._data[creator]

    def info(self, *args, **kwargs):
        self._logger.info(*args, **kwargs)

    def warn(self, *args, **kwargs):
        self._warnings += 1
        self._logger.warning(*args, **kwargs)

    def error(self, *args, **kwargs):
        self._errors += 1
        self._logger.error(*args, **kwargs)

    def exception(self, *args, **kwargs):
        self._errors += 1
        self._logger.exception(*args, **kwargs)

    def reset(self):
        self._errors = 0
        self._warnings = 0
        self._filename_base = time.strftime('georef-etl-%Y.%m.%d-%H.%M.%S.{}')
        self._data = {}

    def write(self, dirname):
        os.makedirs(dirname, exist_ok=True, mode=constants.DIR_PERMS)
        filename_json = self._filename_base.format('json')
        filename_txt = self._filename_base.format('txt')

        if self._logger_stream:
            with open(os.path.join(dirname, filename_txt), 'w') as f:
                f.write(self._logger_stream.getvalue())

        with open(os.path.join(dirname, filename_json), 'w') as f:
            json.dump(self._data, f, ensure_ascii=False, indent=4)

    def email(self, host, user, password, recipients, environment):
        if not self._logger_stream:
            raise RuntimeError('Cannot send email: no logger stream defined.')

        subject = 'Georef ETL [{}] - Errores: {} - Warnings: {}'.format(
            environment,
            self._errors,
            self._warnings
        )
        msg = 'Reporte de entidades de Georef ETL.'
        report_txt = self._logger_stream.getvalue()
        send_email(host, user, password, subject, msg, recipients, {
            self._filename_base.format('txt'): report_txt
        })

    @property
    def logger(self):
        return self._logger


class Context:
    def __init__(self, config, fs, engine, report, mode='normal'):
        if mode not in RUN_MODES:
            raise ValueError('Invalid run mode.')

        self._config = config
        self._fs = fs
        self._engine = engine
        self._report = report
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
