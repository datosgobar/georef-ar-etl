import os
from .process import Process
from . import constants, utils


def create_process(config):
    filename = 'terminos-excluyentes-nombres.txt'

    def get_excluding_terms_file_path(_):
        return os.path.abspath(os.path.join(constants.DATA_DIR, filename))

    return Process(constants.EXCLUDING_TERMS, [
        utils.FunctionStep(fn=get_excluding_terms_file_path,
                           name='get_excluding_terms_file_path',
                           reads_input=False),
        utils.CopyFileStep(config.get('etl', 'excluding_terms_dest_path'),
                           filename)
    ])
