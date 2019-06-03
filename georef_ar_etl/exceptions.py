"""Módulo 'exceptions' de georef-ar-etl.

Define errores comunes (excepciones) utilizados durante el ETL.

"""

class ValidationException(Exception):
    """Representa un error ocurrido durante la creación de una nueva entidad.

    """


class ProcessException(Exception):
    """Representa un error genérico ocurrido durante la ejecución de un
    proceso. Si se lanza esta excepción, se cancela la ejecución del proceso
    actual y se pasa al siguiente (si existe).

    """
