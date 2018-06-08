# -*- coding: utf-8 -*-

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
    'report_info': '-- Generando reporte de la entidad %s.',
    'report_success': 'Se genero el reporte de la entidad %s correctamente.',
    'report_error': 'Se produjo un error al crear el reporte de entidades.',
    'script_info': '-- Cargando funciones SQL.',
    'script_success': 'El script "%s" fue cargado exitosamente.',
    'script_error': 'Ocurrió un error al cargar las funciones SQL.',
    'send_report_info': '-- Enviando reporte de entidades.',
    'send_report_file_error': 'El archivo "%s" no existe',
    'send_report_success': 'Se envió exitosamente un reporte a ',
    'send_report_error': 'No se pudo enviar el reporte',
    'recipients_empty': 'La lista de destinarios está vacía.'
}

logging.basicConfig(
    filename='logs/etl_{:%Y%m%d}.log'.format(datetime.now()),
    level=logging.DEBUG, datefmt='%H:%M:%S',
    format='%(asctime)s | %(levelname)s | %(name)s | %(module)s | %(message)s')


def run():
    try:
        load_functions()
        for entity in entities:
            create_report_by_entity(entity)
        create_report()
        send_report()
    except (Exception, psycopg2.DatabaseError) as e:
        logging.error("{0}: {1}".format(MESSAGES['report_error'], e))


def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DBNAME'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'))


def load_functions():
    try:
        logging.info(MESSAGES['script_info'])
        file = 'functions_report_entities.sql'
        file_path = BASE_DIR + '/etl_scripts/' + file
        with open(file_path, 'r') as f:
            func = f.read()
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(func)
        logging.info(MESSAGES['script_success'] % file)
    except psycopg2.DatabaseError as e:
        logging.error("{0}: {1}".format(MESSAGES['script_error'], e))


def run_query_entities(query):
    with get_db_connection().cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()[0][0]


def create_report():
    output = [{
        'fecha_actualizacion': str(datetime.now()),
        'entidades': report
    }]
    filename = 'logs/entities_report.json'

    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    with open(filename, 'w') as file:
        json.dump(output, file, indent=3, ensure_ascii=False)


def create_report_by_entity(entity):
    logging.info(MESSAGES['report_info'] % entity.title())
    result = {
        entity: {
            'cantidades': get_count(entity),
            'entidades_nuevas': get_new_entities(entity),
            'codigos_actualizados': get_updates(entity, 'code'),
            'nombres_actualizados': get_updates(entity, 'name'),
            'geometrias_actualizadas': get_updates(entity, 'geom'),
            'codigos_nulos': get_code_nulls_by_entities(entity),
            'entidades_repetidas': get_entities_duplicates(entity),
            'geometrias_invalidas': get_entities_invalid_geom(entity),
            'provincias_invalidas': get_state_invalid_codes(entity),
            'departamentos_invalidos': get_department_invalid_code(entity)
        }}
    report.append(result)
    logging.info(MESSAGES['report_success'] % entity.title())


def get_count(entity):
    query = "SELECT get_quantities('{}')".format(entity)
    results = run_query_entities(query)
    return results


def set_fields(entity):
    column_code = 'in1'
    column_name = 'nam'

    if entity is 'bahra':
        column_code = 'cod_bahra'
        column_name = 'nombre_bah'

    return column_code, column_name


def get_new_entities(entity):
    column_code, column_name = set_fields(entity)

    query = "SELECT get_new_entities('{}', '{}', '{}')".\
        format(entity, column_code, column_name)
    results = run_query_entities(query)
    if 'result' not in results:
        new_entities = []
        for row in results:
            new_entities.append({
                'codigo': row['code'],
                'nombre': row['name']
            })
        return new_entities


def get_updates(entity, field):
    column_code, column_name = set_fields(entity)

    query = "SELECT get_entities_{}_updates('{}', '{}', '{}')".\
        format(field, entity, column_code, column_name)
    results = run_query_entities(query)
    if 'result' not in results:
        updates = []
        dict_updates = {}
        for row in results:
            dict_updates.update({
                'codigo': row['code'],
                'nombre': row['name']
            })
            if field is 'name' or field is 'code':
                dict_updates.update({'actualizacion': row['update']})
        updates.append(dict_updates)
        return updates


def get_code_nulls_by_entities(entity):
    column_code, column_name = set_fields(entity)

    query = "SELECT entities_code_null('{}', '{}', '{}')". \
        format(entity, column_code, column_name)
    results = run_query_entities(query)
    if 'result' not in results:
        code_nulls = []
        for row in results:
            code_nulls.append({
                'id': row['id'],
                'nombre': row['name']
            })
        return code_nulls


def get_entities_duplicates(entity):
    column_code, column_name = set_fields(entity)

    query = "SELECT get_entities_duplicates('{}', '{}', '{}')". \
        format(entity, column_code, column_name)
    results = run_query_entities(query)
    if 'result' not in results:
        duplicates = []
        for row in results:
            duplicates.append({
                'id': row['id'],
                'código': row['code'],
                'nombre': row['name']
            })
        return duplicates


def get_entities_invalid_geom(entity):
    column_code, column_name = set_fields(entity)

    query = "SELECT get_entities_invalid_geom('{}', '{}', '{}')". \
        format(entity, column_code, column_name)
    results = run_query_entities(query)
    if 'result' not in results:
        invalid_geometries = []
        for row in results:
            invalid_geometries.append({
                'código': row['code'],
                'nombre': row['name']
            })
        return invalid_geometries


def get_state_invalid_codes(entity):
    if entity is not 'provincias':
        column_code, column_name = set_fields(entity)
        query = "SELECT get_invalid_states_code('{}', '{}', '{}')".\
            format(entity, column_code, column_name)
    else:
        query = "SELECT get_invalid_states_code()"

    results = run_query_entities(query)
    if 'result' not in results:
        invalid_codes = []
        for row in results:
            invalid_codes.append({
                'código': row['code'],
                'nombre': row['name']
            })
        return invalid_codes


def get_department_invalid_code(entity):
    if entity is 'bahra':
        query = "SELECT get_invalid_department_code()"
        results = run_query_entities(query)

        if 'result' not in results:
            invalid_codes = []
            for row in results:
                invalid_codes.append({
                    'código_dpto': row['code'],
                    'nombre': row['name']
                })
            return invalid_codes
    return 'no aplica'


def send_report():
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
