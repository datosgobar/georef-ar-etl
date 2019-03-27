from .exceptions import ValidationException, ProcessException
from .process import Process, CompositeStep
from .models import Province, Department, Street
from . import extractors, transformers, loaders, utils, constants, patch


def create_process(config):
    return Process(constants.STREETS, [
        utils.CheckDependenciesStep([Province, Department]),
        extractors.DownloadURLStep(constants.STREETS + '.zip',
                                   config.get('etl', 'streets_url')),
        transformers.ExtractZipStep(),
        loaders.Ogr2ogrStep(table_name=constants.STREETS_TMP_TABLE,
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
        super().__init__('streets_extraction', Street,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='nomencla')

    def _patch_tmp_entities(self, tmp_streets, ctx):
        def update_commune_data(row):
            # De XX014XXXXXXXX pasar a XX002XXXXXXXX
            prov_id_part = row.nomencla[:constants.PROVINCE_ID_LEN]
            dept_id_part = row.nomencla[constants.PROVINCE_ID_LEN:
                                        constants.DEPARTMENT_ID_LEN]
            id_rest = row.nomencla[constants.DEPARTMENT_ID_LEN:]

            dept_id_int = int(dept_id_part)
            if dept_id_int % constants.CABA_DIV_FACTOR:
                # Alguno de los IDs no es divisible por el factor de división
                raise ProcessException(
                    'El ID de comuna {} no es divisible por {}.'.format(
                        dept_id_part,
                        constants.CABA_DIV_FACTOR))

            dept_new_id_int = dept_id_int // constants.CABA_DIV_FACTOR
            row.nomencla = prov_id_part + str(dept_new_id_int).rjust(
                len(dept_id_part), '0') + id_rest

        patch.apply_fn(tmp_streets, update_commune_data, ctx,
                       tmp_streets.nomencla.like('02%'))

        def update_ushuaia(row):
            row.nomencla = '94015' + row.nomencla[constants.DEPARTMENT_ID_LEN:]
            row.codloc = '94015' + row.codloc[constants.DEPARTMENT_ID_LEN:]

        # Actualizar calles de Ushuaia (agregado en ETL2)
        patch.apply_fn(tmp_streets, update_ushuaia, ctx,
                       tmp_streets.nomencla.like('94014%'))

        def update_rio_grande(row):
            row.nomencla = '94008' + row.nomencla[constants.DEPARTMENT_ID_LEN:]
            row.codloc = '94008' + row.codloc[constants.DEPARTMENT_ID_LEN:]

        # Actualizar calles de Río Grande (agregado en ETL2)
        patch.apply_fn(tmp_streets, update_rio_grande, ctx,
                       tmp_streets.nomencla.like('94007%'))

    def _build_entities_query(self, tmp_entities, ctx):
        return ctx.session.query(tmp_entities).filter(
            tmp_entities.tipo != 'OTRO')

    def _street_valid_num(self, tmp_street):
        start = min(tmp_street.desdei or 0, tmp_street.desded or 0)
        end = max(tmp_street.hastai or 0, tmp_street.hastad or 0)
        return end - start > 0

    def _process_entity(self, tmp_street, cached_session, ctx):
        street_id = tmp_street.nomencla
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

        if not self._street_valid_num(tmp_street):
            report_data = ctx.report.get_data(self.name)
            invalid_num_streets_ids = report_data.setdefault(
                'invalid_num_streets_ids', [])

            invalid_num_streets_ids.append(street_id)

        return Street(
            id=street_id,
            nombre=utils.clean_string(tmp_street.nombre),
            categoria=utils.clean_string(tmp_street.tipo),
            fuente=constants.STREETS_SOURCE,
            inicio_derecha=tmp_street.desded or 0,
            fin_derecha=tmp_street.hastad or 0,
            inicio_izquierda=tmp_street.desdei or 0,
            fin_izquierda=tmp_street.hastai or 0,
            geometria=tmp_street.geom,
            provincia_id=province.id,
            departamento_id=department.id
        )
