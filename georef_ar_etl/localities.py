from .constants import BAHRAType
from .exceptions import ValidationException
from .loaders import CompositeStepCopyFile, CompositeStepCreateFile
from .process import Process, CompositeStep, StepSequence
from .models import Province, Department, LocalGovernment, CensusLocality,\
    Locality
from .settlements import SettlementsExtractionStep
from . import loaders, geometry, utils, constants, extractors


def create_process(config):

    def fetch_tmp_settlements_table(_, ctx):
        return utils.automap_table(constants.SETTLEMENTS_TMP_TABLE, ctx)

    return Process(constants.LOCALITIES, [
        utils.CheckDependenciesStep([constants.SETTLEMENTS_TMP_TABLE]),
        CompositeStep([
            utils.FunctionStep(ctx_fn=fetch_tmp_settlements_table,
                               name='fetch_tmp_settlements_table',
                               reads_input=False),
            StepSequence([
                extractors.DownloadURLStep('localidades.csv',
                                           config.get('etl', 'localities_url'), constants.LOCALITIES),
                loaders.Ogr2ogrStep(
                    table_name=constants.LOCALITIES_TMP_TABLE, geom_type='Geometry', source_epsg='EPSG:4326'
                ),
                utils.ValidateTableSchemaStep({
                    'ogc_fid': 'integer',
                    'field_1': 'varchar',
                    'id': 'varchar',
                    'cod_pcia': 'varchar',
                    'nom_pcia': 'varchar',
                    'cod_depto': 'varchar',
                    'nom_depto': 'varchar',
                    'cod_ase': 'varchar',
                    'nombre': 'varchar',
                    'tipo': 'varchar',
                    'cod_aglo': 'varchar',
                    'nom_aglo': 'varchar',
                    'cod_agl': 'varchar',
                    'nom_agl': 'varchar',
                    'lat_gd': 'varchar',
                    'long_gd': 'varchar',
                    'lat_gs': 'varchar',
                    'long_gs': 'varchar',
                    'fuente': 'varchar',
                    'centroid': 'varchar',
                    'geom': 'geometry',
                    'is_within': 'varchar'
                })
            ], name='load_tmp_localities')
        ]),
        utils.FunctionStep(fn=lambda results: tuple(results)),
        CompositeStep([
            StepSequence([
                utils.FunctionStep(fn=lambda results: list(results)),
                LocalitiesExtractionStep(),
            ]),
            StepSequence([
                utils.FunctionStep(fn=lambda results: list(results)),
                CompositeStep([utils.DropTableStep()] * 2),
            ]),
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'localities_target_size'),
            op='ge'),
        CompositeStepCreateFile(Locality, 'localities', config),
        CompositeStepCopyFile('localities', config),
    ])


class LocalitiesExtractionStep(SettlementsExtractionStep):
    def __init__(self):
        super().__init__('localities_extraction', Locality)

    def _patch_tmp_entities(self, tmp_settlements, ctx):
        # No parchear la tabla tmp_localidades de nuevo.
        pass

    def _build_entities_query(self, tmp_entities, ctx):
        bulk_size = ctx.config.getint('etl', 'bulk_size')
        return ctx.session.query(tmp_entities).\
            filter(tmp_entities.tipo_asent.in_(constants.LOCALITY_TYPES)).\
            yield_per(bulk_size)

    def _change_geom(self, tmp_settlements, tmp_localities, ctx):
        for locality in ctx.session.query(tmp_localities):
            settlement = ctx.session.query(tmp_settlements).filter_by(
                codigo_ase=locality.cod_ase
            ).first()
            if settlement:
                settlement.geom = locality.geom
        ctx.session.commit()

    def _run_internal(self, tmp_entities, ctx):
        tmp_settlements, tmp_localities = tmp_entities

        self._change_geom(tmp_settlements, tmp_localities, ctx)

        return super()._run_internal(tmp_settlements, ctx)

    def _process_entity(self, tmp_locality, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_locality.geom,
                                                     ctx)
        loc_id = tmp_locality.codigo_ase
        prov_id = loc_id[:constants.PROVINCE_ID_LEN]
        dept_id = loc_id[:constants.DEPARTMENT_ID_LEN]
        census_loc_id = loc_id[:constants.CENSUS_LOCALITY_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        # El departamento '02000' tiene un significado especial; ver comentario
        # en constants.py.
        department = cached_session.query(Department).get(dept_id)
        if not department and dept_id != constants.CABA_VIRTUAL_DEPARTMENT_ID:
            raise ValidationException(
                'No existe el departamento con ID {}'.format(dept_id))

        local_government = geometry.get_entity_at_point(LocalGovernment,
                                                    tmp_locality.geom, ctx)

        if prov_id == constants.CABA_PROV_ID:
            # Las calles de CABA pertenecen a la localidad censal 02000010,
            # pero sus IDs *no* comienzan con ese código.
            census_loc_id = constants.CABA_CENSUS_LOCALITY

        census_loc = cached_session.query(CensusLocality).get(
            census_loc_id)
        if not census_loc:
            raise ValidationException(
                'No existe la localidad censal con ID {}'.format(
                    census_loc_id))

        return Locality(
            id=loc_id,
            nombre=utils.clean_string(tmp_locality.nombre_geo),
            categoria=utils.clean_string(tmp_locality.tipo_asent),
            lon=lon, lat=lat,
            provincia_id=prov_id,
            departamento_id=dept_id,
            gobierno_local_id=local_government.id if local_government else None,
            localidad_censal_id=census_loc.id if census_loc else None,
            fuente=utils.clean_string(tmp_locality.fuente_de_),
            geometria=tmp_locality.geom
        )
