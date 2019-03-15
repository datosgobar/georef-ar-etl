from .exceptions import ValidationException
from .process import Process, CompositeStep
from .models import Province, Department, Street
from . import extractors, transformers, loaders, utils, constants, patch


def create_process(config):
    return Process(constants.STREETS, [
        utils.CheckDependenciesStep([Province, Department]),
        extractors.DownloadURLStep(constants.STREETS + '.zip',
                                   config.get('etl', 'streets_url')),
        transformers.ExtractZipStep(),
        loaders.Ogr2ogrStep(table_name=constants.STREETS_RAW_TABLE,
                            geom_type='MultiLineString', encoding='latin1'),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'nomencla': 'varchar',
            'codigo': 'double',
            'tipo': 'varchar',
            'nombre': 'varchar',
            'desdei': 'double',
            'desded': 'double',
            'hastai': 'double',
            'hastad': 'double',
            'codloc': 'varchar',
            'codaglo': 'varchar',
            'link': 'varchar',
            'geom': 'geometry'
        }),
        CompositeStep([
            StreetsExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(size=151000, tolerance=500),
        loaders.CreateJSONFileStep(Street, constants.STREETS + '.json')
    ])


class StreetsExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('streets_extraction_step', Street,
                         entity_class_pkey='id',
                         raw_entity_class_pkey='nomencla')

    def _patch_raw_entities(self, raw_streets, ctx):
        def update_ushuaia(row):
            row.nomencla = '94015' + row.nomencla[constants.DEPARTMENT_ID_LEN:]
            row.codloc = '94015' + row.codloc[constants.DEPARTMENT_ID_LEN:]

        # Actualizar calles de Ushuaia (agregado en ETL2)
        patch.apply_fn(raw_streets, update_ushuaia, ctx,
                       raw_streets.nomencla.like('94014%'))

        def update_rio_grande(row):
            row.nomencla = '94008' + row.nomencla[constants.DEPARTMENT_ID_LEN:]
            row.codloc = '94008' + row.codloc[constants.DEPARTMENT_ID_LEN:]

        # Actualizar calles de Río Grande (agregado en ETL2)
        patch.apply_fn(raw_streets, update_rio_grande, ctx,
                       raw_streets.nomencla.like('94007%'))

    def _build_entities_query(self, raw_entities, ctx):
        return ctx.session.query(raw_entities).filter(
            raw_entities.tipo != 'OTRO')

    def _process_entity(self, raw_street, cached_session, ctx):
        street_id = raw_street.nomencla
        prov_id = street_id[:constants.PROVINCE_ID_LEN]
        dept_id = street_id[:constants.DEPARTMENT_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        department = cached_session.query(Department).get(dept_id)
        if not department:
            raise ValidationException(
                'No existe el departamento con ID {}'.format(dept_id))

        return Street(
            id=street_id,
            nombre=utils.clean_string(raw_street.nombre),
            categoria=utils.clean_string(raw_street.tipo),
            fuente=constants.STREETS_SOURCE,
            inicio_derecha=raw_street.desded or 0,
            fin_derecha=raw_street.hastad or 0,
            inicio_izquierda=raw_street.desdei or 0,
            fin_izquierda=raw_street.hastai or 0,
            geometria=raw_street.geom,
            provincia_id=province.id,
            departamento_id=department.id
        )
