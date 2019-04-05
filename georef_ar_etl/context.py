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
    """Imita un objeto Query de SQLAlchemy, pero cacheando los resultados de
    llamadas a 'get()' para lograr mayor performance.

    Attributes:
        _cache (dict): Cache de resultados.
        _query (sqlalchemy.orm.query.Query): Objeto 'Query' de SQLAlchemy.

    """

    def __init__(self, query):
        """Inicializa un objeto de tipo 'CachedQuery'.

        Args:
            _query (sqlalchemy.orm.query.Query): Objeto 'Query' a cachear.

        """
        self._cache = {}
        self._query = query

    def __getattr__(self, name):
        # Delegar cualquier método que no sea get() al Query interno
        return getattr(self._query, name)

    def get(self, key):
        """Busca una entidad a partir de su Primary Key (cacheado).

        Args:
            key (object): Primary Key de la entidad a buscar.

        Returns:
            object, None: La entidad si se encontró, o None si no fue así.

        """
        if key not in self._cache:
            self._cache[key] = self._query.get(key)

        return self._cache[key]


class CachedSession:
    """Imita un objeto Session de SQLAlchemy, devolviendo CachedQuery en lugar
    de Query para el método 'query()'.

    Attributes:
        _queries (dict): Objetos 'CachedQuery' creados anteriormente en
            llamados a 'query()'.
        _session (sqlalchemy.orm.session.Session): Objeto 'Session' de
            SQLAlchemy.

    """

    def __init__(self, session):
        """Inicializa un objeto de tipo 'CachedSession'.

        Args:
            session (sqlalchemy.orm.session.Session): Objeto Session a cachear.

        """
        self._queries = {}
        self._session = session

    def __getattr__(self, name):
        # Delegar cualquier método que no sea query() al Session interno
        return getattr(self._session, name)

    def query(self, query_class):
        """Crea un objeto 'CachedQuery' en base a un objeto 'Query' y lo
        almacena internamente. El 'CachedQuery' devuelto también cachea todos
        sus resultados.

        Args:
            query_class (DeclarativeMeta): Clase de modelo declarativo a
                consultar.

        Returns:
            CachedQuery: Objeto Query-like a utilizar para consultas.

        """
        if query_class not in self._queries:
            self._queries[query_class] = CachedQuery(
                self._session.query(query_class))

        return self._queries[query_class]


class Report:
    """Representa un reporte (texto y datos) sobre la ejecución de un proceso.
    El reporte contiene logs, con fechas, de los eventos sucedidos durante la
    ejecución del proceso, así también como datos recolectados a la vez.

    Attributes:
        _logger (logging.Logger): Logger interno a utilizar.
        _logger_stream (io.StringIO): Contenidos del logger en forma de str.
        _errors (int): Cantidad de errores registrados.
        _warnings (int): Cantidad de advertencias registradas.
        _filename_base (str): Nombre base para el archivo de reporte de texto y
            el archivo de datos.
        _data (dict): Datos varios de la ejecución del proceso.

    """

    def __init__(self, logger, logger_stream=None):
        """Inicializa un objeto de tipo 'Report'.

        Args:
            logger (logging.Logger): Ver atributo '_logger'.
            logger_stream (io.StringIO): Ver atributo '_logger_stream'.

        """
        self._logger = logger
        self._logger_stream = logger_stream
        self.reset()

    def get_data(self, creator):
        """Accede a datos del reporte, almacenados bajo una key específica. Si
        no existen datos, se crea un diccionario nuevo para almacenar datos
        futuros.

        Args:
            creator (object): Key bajo la cual buscar los datos del reporte.

        Returns:
            dict: Datos del reporte bajo la key especificada.

        """
        if creator not in self._data:
            self._data[creator] = {}

        return self._data[creator]

    def info(self, *args, **kwargs):
        """Agrega un registro 'info' al logger del reporte.

        Args:
            *args, **kwargs: Valores del registro.

        """
        self._logger.info(*args, **kwargs)

    def warn(self, *args, **kwargs):
        """Agrega un registro 'warning' al logger del reporte.

        Args:
            *args, **kwargs: Valores del registro.

        """
        self._warnings += 1
        self._logger.warning(*args, **kwargs)

    def error(self, *args, **kwargs):
        """Agrega un registro 'error' al logger del reporte.

        Args:
            *args, **kwargs: Valores del registro.

        """
        self._errors += 1
        self._logger.error(*args, **kwargs)

    def exception(self, *args, **kwargs):
        """Agrega un registro 'error' al logger del reporte, a partir de una
        excepción generada.

        Args:
            *args, **kwargs: Valores del registro.

        """
        self._errors += 1
        self._logger.exception(*args, **kwargs)

    def reset(self):
        """Reestablece el estado interno del reporte."""
        self._errors = 0
        self._warnings = 0
        self._filename_base = time.strftime('georef-etl-%Y.%m.%d-%H.%M.%S.{}')
        self._data = {}

    def write(self, dirname):
        """Escribe los contenidos del reporte (texto y datos) a dos archivos
        dentro de un directorio específico.

        Args:
            dirname (str): Directorio donde almacenar los archivos.

        """
        os.makedirs(dirname, exist_ok=True, mode=constants.DIR_PERMS)
        filename_json = self._filename_base.format('json')
        filename_txt = self._filename_base.format('txt')

        if self._logger_stream:
            with open(os.path.join(dirname, filename_txt), 'w') as f:
                f.write(self._logger_stream.getvalue())

        with open(os.path.join(dirname, filename_json), 'w') as f:
            json.dump(self._data, f, ensure_ascii=False, indent=4)

    def email(self, host, user, password, recipients, environment):
        """Envía el contenido (texto) del reporte por mail.

        Args:
            host (str): Host del servidor SMTP.
            user (str): Usuario del servidor SMTP.
            password (str): Contraseña del usuario.
            recipients (list): Lista de direcciones de mail a donde envíar el
                mensaje.
            environment (str): Entorno de ejecución (prod/stg/dev).

        """
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
    """Representa un contexto de ejecución que incluye sistema de archivos,
    base de datos, logging y más.

    La clase Context es utilizada en muchas clases y funciones del código del
    georef-ar-etl para asegurar que cualquier acceso a recursos 'externos' al
    ETL sean siempre a través de un punto en común. Queda bajo responsabilidad
    del programador asegurarse de que nunca se accedan a éstos recursos
    externos de forma directa. Por ejemplo, nunca se debe usar open(), pero sí
    ctx.fs.open().

    El uso de la clase Context permite luego realizar tests fácilmente, ya que
    simplemente se puede crear un nuevo Context de prueba, utilizarlo y luego
    descartar todo lo que se creó dentro suyo.

    Attributes:
        _config (configparser.ConfigParser): Configuración del ETL.
        _fs (fs.FS): Sistema de archivos.
        _engine (sqlalchemy.engine.Engine): Conexión a base de datos.
        _report (Report): Reporte actual del ETL.
        _mode (str): Modo de ejecución.
        _session_maker (sqlalchemy.orgm.session.sessionmaker): Creador de
            sesiones de SQLAlchemy.
        _session (sqlalchemy.orm.session.Session): Sesión actual de base de
            datos.

    """

    def __init__(self, config, fs, engine, report, mode='normal'):
        """Inicializa un objeto de tipo 'Context'.

        Args:
            config (configparser.ConfigParser): Ver atributo '_config'.
            fs (fs.FS): Ver atributo '_fs'.
            engine (sqlalchemy.engine.Engine):  Ver atributo '_engine'.
            report (Report):  Ver atributo '_report'.
            mode (str):  Ver atributo '_mode'.

        """
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
        # Crear una nueva sesión si no existe una
        if not self._session:
            self._session = self._session_maker()

        return self._session

    def cached_session(self):
        """Crea una nueva sessión cacheada a partir de self.session.

        Se recomienda utilizar esta sesión cacheada solo para realizar lecturas
        a la base de datos. También se debería evitar consultar tablas que
        están siendo modificadas, ya que potencialmente se podrían retener
        versiones antiguas de entidades que ya fueron borradas/modificadas en
        el objeto Session interno.

        Returns:
            CachedSession: Sesión cacheada.

        """
        return CachedSession(self.session)
