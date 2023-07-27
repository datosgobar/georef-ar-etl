"""Módulo 'context' de georef-ar-etl.

Define las clases 'Context' y 'Report', y otras utilidades relacionadas.

"""
import logging
import os
import smtplib
import json
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import StringIO
from logging import LogRecord
from sqlalchemy.orm import sessionmaker
from . import constants

RUN_MODES = ['normal', 'interactive', 'testing']
SMTP_TIMEOUT = 20


class ProcessGroupHandler(logging.StreamHandler):
    """
    Representa un gestor de registros para poder almacenar los datos relevantes a cada grupo de procesos. La intención
    es que almacene un stream de los logs generados por los procesos que pertenecen al grupo y descarte los de otros
    procesos para poder suministrar un reporte por mail personalizado.
    """

    def __init__(self, stream, processes_group) -> None:
        """

        @param stream: Un objeto Stream que almacene los registros
        @param processes_group: Una lista o conjunto de nombres de procesos que conforman el grupo
        """
        super().__init__(stream)
        self.emit_enabled = True
        self.warning_count = 0
        self.error_count = 0
        self._processes_group = processes_group

    def emit(self, record: LogRecord) -> None:
        if self.emit_enabled:
            super().emit(record)
            if record.levelno == logging.ERROR:
                self.error_count += 1
            elif record.levelno == logging.WARNING:
                self.warning_count += 1

    def set_process(self, process):
        """
        Define si el gestor estará registrando o no en función del proceso activo

        @param process: El proceso activo
        """
        self.emit_enabled = not process or process.name in self._processes_group

    def belong_group(self, processes_group):
        """
        Define si el gestor pertenece al mismo grupo

        @param processes_group: Una lista o conjunto de nombres de procesos
        @return: True si pertenecen al mismo grupo.
        """
        return set(processes_group) == set(self._processes_group)

    def getvalue(self):
        return self.stream.getvalue()


def send_email(host, user, password, subject, message, recipients,
               attachments=None, ssl=True, port=0, timeout=SMTP_TIMEOUT):
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
        ssl (bool): Verdadero si la conexión inicial debería utilizar SSL/TLS.
        port (int): Puerto a utilizar (0 para utilizar el default).
        timeout (int): Tiempo máximo a esperar en segundos para establecer la
            conexión al servidor SMTP.

    """
    client_class = smtplib.SMTP_SSL if ssl else smtplib.SMTP
    with client_class(host, timeout=timeout, port=port) as smtp:
        if not ssl:
            smtp.starttls()
            smtp.ehlo()

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


def get_mail_groups(processes, ctx):
    recipient_processes_dict = {}
    for process in processes:
        recipients = [
            r.strip()
            for r in ctx.config['mailer'].get(constants.RECIPIENTS_PREFIX + process.name, fallback='').split(',')
        ]
        for recipient in recipients:
            recipient_processes_dict.setdefault(recipient, set()).add(process.name)

    recipients_group_list = []
    for recipient, processes in recipient_processes_dict.items():
        if recipient == '':
            continue

        grouped = False
        for group in recipients_group_list:
            if group['processes'] == processes:
                group['recipients'].add(recipient)
                grouped = True
                break

        if not grouped:
            recipients_group_list.append({
                'processes': processes,
                'recipients': {recipient}
            })

    return recipients_group_list


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


class Report:  # pylint: disable=attribute-defined-outside-init
    """Representa un reporte (texto y datos) sobre la ejecución de un proceso.
    El reporte contiene logs, con fechas, de los eventos sucedidos durante la
    ejecución del proceso, así también como datos recolectados a la vez.

    Attributes:
        _logger (logging.Logger): Logger interno a utilizar.
        _logger_stream (io.StringIO): Contenidos del logger en forma de str.
        _errors (int): Cantidad de errores registrados.
        _warnings (int): Cantidad de advertencias registradas.
        _indent (int): Nivel de indentación actual para registros del logger.
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

    def init_process_logs(self, processes_groups):
        self._process_logs = []
        formatter = logging.Formatter('{asctime} - {levelname:^7s} - {message}',
                                      '%Y-%m-%d %H:%M:%S', style='{')
        for process_group in processes_groups:
            process_handler = ProcessGroupHandler(StringIO(), process_group)
            process_handler.setLevel(logging.INFO)
            process_handler.setFormatter(formatter)
            logging.getLogger().addHandler(process_handler)
            self._process_logs.append(process_handler)

    def set_process(self, process):
        for process_log in self._process_logs:
            process_log.set_process(process)

    def _get_process_group_handler(self, process_group):
        for process_log in self._process_logs:
            if process_log.belong_group(process_group):
                return process_log
        raise RuntimeError('Cannot get report: no logger stream defined for group.')

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

    def increase_indent(self):
        """Aumenta el nivel de indentación.

        """
        self._indent += 1

    def decrease_indent(self):
        """Reduce el nivel de indentación.

        """
        if not self._indent:
            raise RuntimeError('Indent is already 0')
        self._indent -= 1

    def reset_indent(self):
        """Resetea el nivel de indentación.

        """
        self._indent = 0

    def _indent_message(self, args):
        """Indenta un mensaje a incluir en el reporte/logger.

        Args:
            args (list): Argumentos recibidos en info, warn, etc.

        """
        return ('| ' * self._indent + args[0],) + args[1:]

    def info(self, *args):
        """Agrega un registro 'info' al logger del reporte.

        Args:
            *args: Valores del registro.

        """
        args = self._indent_message(args)
        self._logger.info(*args)

    def warn(self, *args):
        """Agrega un registro 'warning' al logger del reporte.

        Args:
            *args: Valores del registro.

        """
        self._warnings += 1
        args = self._indent_message(args)
        self._logger.warning(*args)

    def error(self, *args):
        """Agrega un registro 'error' al logger del reporte.

        Args:
            *args: Valores del registro.

        """
        self._errors += 1
        args = self._indent_message(args)
        self._logger.error(*args)

    def exception(self, *args):
        """Agrega un registro 'error' al logger del reporte, a partir de una
        excepción generada.

        Args:
            *args: Valores del registro.

        """
        self._errors += 1
        args = self._indent_message(args)
        self._logger.exception(*args)

    def reset(self):
        """Reestablece el estado interno del reporte."""
        self._errors = 0
        self._warnings = 0
        self._indent = 0
        self._filename_base = time.strftime('georef-etl-%Y.%m.%d-%H.%M.%S.{}')
        self._data = {}
        self._process_registry = {}

    def _get_report_txt(self, processes=None):

        if not processes:
            return self._logger_stream.getvalue()

        return self._get_process_group_handler(processes).getvalue()

    def _get_report_json(self, processes=None):

        if not processes:
            return self._data

        processes_map = {
            constants.PROVINCES: 'provinces_extraction',
            constants.DEPARTMENTS: 'departments_extraction',
            constants.MUNICIPALITIES: 'municipalities_extraction',
            constants.CENSUS_LOCALITIES: 'census_localities_extraction',
            constants.SETTLEMENTS: 'settlements_extraction',
            constants.LOCALITIES: 'localities_extraction',
            constants.STREETS: 'street_extraction'
        }
        report = {'download_url': {}}
        for process in processes:
            if process in processes_map.keys():
                key = processes_map[process]
                if process in self._data.get('download_url', {}).keys():
                    report.setdefault('download_url', {}).update({process: self._data['download_url'][process]})
                report.update({key: self._data.get(key, None)})

        return report

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
                f.write(self._get_report_txt())

        with open(os.path.join(dirname, filename_json), 'w') as f:
            json.dump(self._get_report_json(), f, ensure_ascii=False, indent=4)

    def email(self, host, user, password, recipients, environment, ssl=True,
              port=0, include_json=False, processes=None):
        """Envía el contenido (texto) del reporte por mail.

        Args:
            host (str): Host del servidor SMTP.
            user (str): Usuario del servidor SMTP.
            password (str): Contraseña del usuario.
            recipients (list): Lista de direcciones de mail a donde envíar el
                mensaje.
            environment (str): Entorno de ejecución (prod/stg/dev).
            ssl (bool): Verdadero si la conexión inicial debería utilizar
                SSL/TLS.
            port (int): Puerto a utilizar (0 para utilizar el default)
            include_json (bool): Especifica si hay que incluir el reporte en json en los adjuntos
            processes (list): Una lista con los procesos a filtrar en el reporte que será enviado a los destinatarios.

        """
        if not self._logger_stream:
            raise RuntimeError('Cannot send email: no logger stream defined.')

        subject = 'Georef ETL [{}] - Errores: {} - Warnings: {}'.format(
            environment,
            self._get_process_group_handler(processes).error_count if processes else self._errors,
            self._get_process_group_handler(processes).warning_count if processes else self._warnings
        )
        msg = 'Reporte de entidades de Georef ETL.'
        if processes:
            msg = msg + f'\n\tEntidades filtradas: {", ".join(processes)}'
        attachments = {
            self._filename_base.format('txt'): self._get_report_txt(processes)
        }
        if include_json:
            attachments.update({
                self._filename_base.format('json'): json.dumps(
                    self._get_report_json(processes), ensure_ascii=False, indent=4
                )
            })
        send_email(host, user, password, subject, msg, recipients, attachments, ssl=ssl, port=port)

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
