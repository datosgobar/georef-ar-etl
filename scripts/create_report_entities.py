# -*- coding: utf-8 -*-

"""Módulo 'create_report_entities' de georef-etl

Contiene funciones para la generación, impresión y posterior envío de un reporte
en formato JSON sobre métricas de las entidades políticas (asentamientos,
municipios, departamentos y provincias). Dicho datos son obtenidos desde sus
correspondientes portales, el reporte se realiza antes de procesar o modificar
estos datos mediante geore-etl.
"""

import logging
import json
import psycopg2
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate
from datetime import datetime
from georef.settings import *

report = []
entities = ['provincias', 'departamentos', 'municipios', 'bahra']

MESSAGES = {
    'report_info': '-- Generando reporte de la entidad %s',
    'report_success': 'Se genero el reporte de la entidad %s correctamente.',
    'report_error': 'Se produjo un error al crear el reporte de entidades.',
    'script_info': '-- Cargando funciones SQL',
    'script_success': 'El script "%s" fue cargado exitosamente.',
    'script_error': 'Ocurrió un error al cargar las funciones SQL.',
    'send_report_info': '-- Enviando reporte de entidades',
    'send_report_file_error': 'El archivo "%s" no existe.',
    'send_report_success': 'Se envió exitosamente un reporte a ',
    'send_report_error': 'No se pudo enviar el reporte',
    'recipients_empty': 'La lista de destinarios está vacía.'
}

logging.basicConfig(
    filename='logs/etl_{:%Y%m%d}.log'.format(datetime.now()),
    level=logging.DEBUG, datefmt='%H:%M:%S',
    format='%(asctime)s | %(levelname)s | %(name)s | %(module)s | %(message)s')


def run():
    """Contiene las funciones a llamar cuando se ejecuta el script.

    Returns:
        None
    """
    try:
        load_functions()
        for entity in entities:
            create_report_by_entity(entity)
        create_file_report()
        send_report()
    except (Exception, psycopg2.DatabaseError) as e:
        logging.error("{0}: {1}".format(MESSAGES['report_error'], e))


def get_db_connection():
    """Se conecta a una base de datos especificada en variables de entorno.

    Returns:
        connection: Conexión a base de datos.
    """
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DBNAME'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'))


def run_query(query):
    """Procesa y ejecuta una consulta en la base de datos especificada.

    Args:
        query (str): Consulta a ejecutar.

    Returns:
        list: Resultado de la consulta.
    """
    with get_db_connection().cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()[0][0]


def load_functions():
    """Se conecta a la base datos especificada y realiza la carga de funciones
       requeridas para la creación del reporte de entidades.

    Returns:
        None
    """
    try:
        logging.info(MESSAGES['script_info'])
        file = 'functions_report_entities.sql'
        file_path = BASE_DIR + '/etl_scripts/' + file

        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(open(file_path, 'r').read())
        logging.info(MESSAGES['script_success'] % file)
    except psycopg2.DatabaseError as e:
        logging.error("{0}: {1}".format(MESSAGES['script_error'], e))


def create_report_by_entity(entity):
    """Agrega un objeto entidad a la variable 'report' del tipo lista.

    Args:
        entity (str): Nombre de entidad.

    Return:
        None
    """
    logging.info(MESSAGES['report_info'] % entity.title())
    result = {
        entity: {
            'cantidades': get_count(entity),
            'entidades_nuevas': get_new(entity),
            'codigos_actualizados': get_updates(entity, 'code'),
            'nombres_actualizados': get_updates(entity, 'name'),
            'geometrias_actualizadas': get_updates(entity, 'geom'),
            'codigos_nulos': get_code_nulls(entity),
            'entidades_repetidas': get_duplicates(entity),
            'geometrias_invalidas': get_invalid_geom(entity),
            'provincias_invalidas': get_state_invalid_codes(entity),
            'departamentos_invalidos': get_department_invalid_code(entity)
        }}
    report.append(result)
    logging.info(MESSAGES['report_success'] % entity.title())


def get_count(entity):
    """Obtiene la cantidad de entidades guardadas y las obtenidas de una entidad
       dada tras la actualización de registros.

    Args:
        entity (str): Nombre de la entidad.

    Returns:
        results (dict): Cantidad de entidades guardadas y las obtenidas en la
        actualización.
    """
    query = "SELECT get_quantities('{}')".format(entity)
    results = run_query(query)
    return results


def set_fields(entity):
    """Devuelve los nombres para las columnas de 'código' y 'nombre' de una
       entidad dada, referente a una tabla de la base de datos especificada.

    Args:
        entity (str): Nombre de la entidad.

    Returns:
        column_code (str): Nombre de la columna que contiene el código de la
        entidad.
        column_name (str): Nombre de la columna que contiene el nombre de la
        entidad.
    """
    column_code = 'in1'
    column_name = 'nam'

    if entity is 'bahra':
        column_code = 'cod_bahra'
        column_name = 'nombre_bah'

    return column_code, column_name


def get_new(entity):
    """Obtiene nuevos registros de una entidad dada tras la actualización de
       registros.

    Args:
        entity (str): Nombre de la entidad.

    Returns:
        new_entities (list): Resultado de la consulta.
    """
    column_code, column_name = set_fields(entity)

    query = "SELECT get_new_entities('{}', '{}', '{}')".\
        format(entity, column_code, column_name)
    results = run_query(query)
    if 'result' not in results:
        new_entities = []
        for row in results:
            new_entities.append({
                'codigo': row['code'],
                'nombre': row['name']
            })
        return new_entities


def get_updates(entity, field):
    """Obtiene modificaciones sobre un atributo y entidad dada tras la
       actualización de registros.

    Args:
        entity (str): Nombre de la entidad.
        field (str): Nombre del atributo.

    Returns:
        updates (dict): Resultado de la consulta.
    """
    column_code, column_name = set_fields(entity)

    query = "SELECT get_entities_{}_updates('{}', '{}', '{}')".\
        format(field, entity, column_code, column_name)
    results = run_query(query)
    if 'result' not in results:
        updates = []
        obj_update = {}
        for row in results:
            obj_update.update({
                'codigo': row['code'],
                'nombre': row['name']
            })
            if field is 'name' or field is 'code':
                obj_update.update({'actualizacion': row['update']})
        updates.append(obj_update)
        return updates


def get_code_nulls(entity):
    """Obtiene registros de una entidad dada que contiene el código de la
       entidad con valor nulo.

    Args:
        entity (str): Nombre de la entidad.

    Returns:
        code_nulls (list): Resultado de la consulta.
    """
    column_code, column_name = set_fields(entity)

    query = "SELECT entities_code_null('{}', '{}', '{}')". \
        format(entity, column_code, column_name)
    results = run_query(query)
    if 'result' not in results:
        code_nulls = []
        for row in results:
            code_nulls.append({
                'id': row['id'],
                'nombre': row['name']
            })
        return code_nulls


def get_duplicates(entity):
    """Obtiene registros duplicados de una entidad dada.

    Args:
        entity (str): Nombre de la entidad.

    Returns:
        duplicates (list): Resultado de la consulta.
    """
    column_code, column_name = set_fields(entity)

    query = "SELECT get_entities_duplicates('{}', '{}', '{}')". \
        format(entity, column_code, column_name)
    results = run_query(query)
    if 'result' not in results:
        duplicates = []
        for row in results:
            duplicates.append({
                'id': row['id'],
                'código': row['code'],
                'nombre': row['name']
            })
        return duplicates


def get_invalid_geom(entity):
    """Obtiene registros con geometrías inválidas de una entidad dada.

    Args:
        entity (str): Nombre de la entidad.

    Returns:
        invalid_geometries (list): Resultado de la consulta.
    """
    column_code, column_name = set_fields(entity)

    query = "SELECT get_entities_invalid_geom('{}', '{}', '{}')". \
        format(entity, column_code, column_name)
    results = run_query(query)
    if 'result' not in results:
        invalid_geometries = []
        for row in results:
            invalid_geometries.append({
                'código': row['code'],
                'nombre': row['name']
            })
        return invalid_geometries


def get_state_invalid_codes(entity):
    """Obtiene registros de una entidad dada con código de Provincia inválido.

    Args:
        entity (str): Nombre de la entidad.

    Returns:
        invalid_codes (list): Resultado de la consulta.
    """
    if entity is not 'provincias':
        column_code, column_name = set_fields(entity)
        query = "SELECT get_invalid_states_code('{}', '{}', '{}')".\
            format(entity, column_code, column_name)
    else:
        query = "SELECT get_invalid_states_code()"

    results = run_query(query)
    if 'result' not in results:
        invalid_codes = []
        for row in results:
            invalid_codes.append({
                'código': row['code'],
                'nombre': row['name']
            })
        return invalid_codes


def get_department_invalid_code(entity):
    """Obtiene registros de una entidad dada con código de Departamento inválido.

    Args:
        entity (str): Nombre de la entidad.

    Returns:
        invalid_codes (list): Resultado de la consulta.
    """
    if entity is 'bahra':
        query = "SELECT get_invalid_department_code()"
        results = run_query(query)

        if 'result' not in results:
            invalid_codes = []
            for row in results:
                invalid_codes.append({
                    'código_dpto': row['code'],
                    'nombre': row['name']
                })
            return invalid_codes
    return 'no aplica'


def create_file_report():
    """Imprime un reporte de entidades en formato JSON.

    Returns:
        None
    """
    output = [{
        'fecha_actualizacion': str(datetime.now()),
        'entidades': report
    }]
    filename = 'logs/entities_report.json'

    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    with open(filename, 'w') as file:
        json.dump(output, file, indent=3, ensure_ascii=False)


def send_report():
    """Envía un email adjuntando un reporte de entidades en formato JSON. Los
       parámetros de usuario, contraseña, servidor SMTP, puerto y destinatarios
       son tomados desde variables de entorno.

    Return:
        None
    """
    try:
        logging.info(MESSAGES['send_report_info'])
        msg = MIMEMultipart()
        msg["Subject"] = EMAIL_REPORT_SUBJECT
        msg["From"] = EMAIL_REPORT_FROM
        msg["To"] = ", ".join([EMAIL_REPORT_RECIPIENTS])
        msg["Date"] = formatdate(localtime=True)
        file = PATH_REPORT_FILE
        recipients = EMAIL_REPORT_RECIPIENTS.split(',')

        if not len(EMAIL_REPORT_RECIPIENTS):
            logging.warning(MESSAGES['recipients_empty'])

        if file:
            if os.path.isfile(file):
                with open(file, "rb") as fil:
                    part = MIMEApplication(
                        fil.read(), Name=os.path.basename(file))
                    part['Content-Disposition'] = (
                        'attachment; filename="%s"' % os.path.basename(file)
                    )
                    msg.attach(part)
            else:
                logging.warning(MESSAGES['send_report_file_error'] % file)
        if EMAIL_USE_SSL is 'True':
            s = smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT)
        else:
            s = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
            s.ehlo()
            s.starttls()

        s.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
        s.sendmail(EMAIL_HOST_USER, recipients, msg.as_string())
        s.close()

        logging.info(MESSAGES['send_report_success'] + ","
                     .join([EMAIL_REPORT_RECIPIENTS]))
    except (Exception, smtplib.SMTPException) as e:
        logging.error("{0}: {1}".format(MESSAGES['send_report_error'], e))
