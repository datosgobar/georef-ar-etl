from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.transports import Transport
import os


def run():
    try:
        url = 'https://wsec01.correoargentino.com.ar/domicilios/services/' \
              'CleansingService?wsdl'
        user = os.environ.get('CPA_USER')
        password = os.environ.get('CPA_PASS')
        authkey = os.environ.get('CPA_AUTHKEY')

        xml = ''
        xml += '<?xml version="1.0" encoding="iso-8859-1"?>'
        xml += '<CLEANSING>'
        xml += '<ADDRESSES>'
        xml += '<ID>A366EWRV-876</ID>'
        xml += '<PROVINCE>BUENOS AIRES</PROVINCE>'
        xml += '<DISTRICT></DISTRICT>'
        xml += '<CITY>GREGORIO DE LAFERRERE</CITY>'
        xml += '<NEIGHBOURHOOD></NEIGHBOURHOOD>'
        xml += '<ZIP>1757</ZIP>'
        xml += '<DOOR></DOOR>'
        xml += '<ADDRESS>ECHEVERRIA 4497</ADDRESS>'
        xml += '<STREET></STREET>'
        xml += '<NUMBER></NUMBER>'
        xml += '</ADDRESSES>'
        xml += '</CLEANSING>'

        session = Session()
        session.auth = HTTPBasicAuth(user, password)
        client = Client(url, transport=Transport(session=session))
        result = client.service.exec(authkey, xml)
        print(result)
    except Exception as e:
        print(e)
