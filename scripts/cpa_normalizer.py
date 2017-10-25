from requests import Session
from requests.auth import HTTPBasicAuth
from xml.etree.ElementTree import Element, SubElement, tostring
from zeep import Client
from zeep.transports import Transport
import os
import random
import string


def run(args):
    try:
        name, locality, state = args.split(',')
        url = 'https://wsec01.correoargentino.com.ar/domicilios/services/' \
              'CleansingService?wsdl'
        user = os.environ.get('CPA_USER')
        password = os.environ.get('CPA_PASS')
        authkey = os.environ.get('CPA_AUTHKEY')

        cleansing = process_cleansing_xml(name, locality, state)
        session = Session()
        session.auth = HTTPBasicAuth(user, password)
        client = Client(url, transport=Transport(session=session))
        result = client.service.exec(authkey, tostring(
            cleansing, encoding='iso-8859-1',
            method='xml', short_empty_elements=False).decode('utf-8'))

        print(result)
    except Exception as e:
        print('Formato inválido: %s ' % e)


def process_cleansing_xml(name, locality, state):
    cleansing = Element('CLEANSING')
    addresses = SubElement(cleansing, 'ADDRESSES')
    id = SubElement(addresses, 'ID')
    province = SubElement(addresses, 'PROVINCE')
    district = SubElement(addresses, 'DISTRICT')
    city = SubElement(addresses, 'CITY')
    neighbourhood = SubElement(addresses, 'NEIGHBOURHOOD')
    zip = SubElement(addresses, 'ZIP')
    door = SubElement(addresses, 'DOOR')
    address = SubElement(addresses, 'ADDRESS')
    street = SubElement(addresses, 'STREET')
    number = SubElement(addresses, 'NUMBER')

    id.text = create_id()
    province.text = state.strip()
    district.text = ''
    city.text = locality.strip()
    neighbourhood.text = ''
    zip.text = ''
    door.text = ''
    address.text = name.strip()
    street.text = ''
    number.text = ''

    return cleansing


def create_id():
    uid = ''
    for i in range(20):
        uid += random.choice(string.ascii_uppercase + string.digits)
    return uid
