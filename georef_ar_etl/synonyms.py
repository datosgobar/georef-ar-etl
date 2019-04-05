import os
from .process import Process
from . import constants, utils


def create_process(config):
    filename = 'sinonimos-nombres.txt'

    def get_synonyms_file_path(_):
        return os.path.abspath(os.path.join(constants.DATA_DIR, filename))

    return Process(constants.SYNONYMS, [
        utils.FunctionStep(fn=get_synonyms_file_path,
                           name='get_synonyms_file_path', reads_input=False),
        utils.CopyFileStep(config.get('etl', 'output_dest_path'), filename)
    ])
