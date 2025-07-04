from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape
from shapely.geometry import Point
from sqlalchemy import insert, MetaData, Table, Column, String, inspect, func

from .exceptions import ValidationException, ProcessException
from .loaders import CompositeStepCopyFile, CompositeStepCreateFile
from .process import Process, StepSequence, CompositeStep
from .models import Province, Department, LocalGovernment, CensusLocality,\
    Settlement
from . import extractors, transformers, loaders, geometry, utils, constants, patch


def create_process(config):

    antarctic_bases_sstep = StepSequence([
        extractors.DownloadURLStep(
            constants.ANTARCTIC_BASES + '.zip',
            config.get('etl', 'settlements_antarctic_bases_url'),
            constants.ANTARCTIC_BASES
        ),
        transformers.ExtractZipStep(internal_path=""),
        loaders.Ogr2ogrStep(
            table_name=constants.ANTARCTIC_BASES_TMP_TABLE, geom_type='Point', env={'SHAPE_ENCODING': 'ISO-8859-1'}
        ),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'gid': 'numeric',
            'fna': 'varchar',
            'fdc': 'varchar',
            'cod_pcia': 'varchar',
            'nom_pcia': 'varchar',
            'cod_depto': 'varchar',
            'nom_depto': 'varchar',
            'cod_ase': 'varchar',
            'tipo_asent': 'varchar',
            'cod_aglo': 'varchar',
            'nom_aglo': 'varchar',
            'cod_agl': 'varchar',
            'nom_agl': 'varchar',
            'lat_gd': 'varchar',
            'long_gd': 'varchar',
            'lat_gs': 'varchar',
            'long_gs': 'varchar',
            'geom': 'geometry',
        })
    ])

    hamlets_sstep = StepSequence([
        extractors.DownloadURLStep(
            constants.HAMLETS + '.zip',
            config.get('etl', 'settlements_hamlets_url'),
            constants.HAMLETS
        ),
        transformers.ExtractZipStep(internal_path=""),
        loaders.Ogr2ogrStep(
            table_name=constants.HAMLETS_TMP_TABLE, geom_type='Point', env={'SHAPE_ENCODING': 'ISO-8859-1'}
        ),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'gid': 'numeric',
            'fna': 'varchar',
            'fdc': 'varchar',
            'cod_pcia': 'varchar',
            'nom_pcia': 'varchar',
            'cod_depto': 'varchar',
            'nom_depto': 'varchar',
            'cod_ase': 'varchar',
            'tipo_asent': 'varchar',
            'cod_aglo': 'varchar',
            'nom_aglo': 'varchar',
            'cod_agl': 'varchar',
            'nom_agl': 'varchar',
            'lat_gd': 'varchar',
            'long_gd': 'varchar',
            'lat_gs': 'varchar',
            'long_gs': 'varchar',
            'geom': 'geometry',
        })
    ])

    localities_sstep = StepSequence([
        extractors.DownloadURLStep(
            constants.LOCALITIES + '.zip',
            config.get('etl', 'settlements_localities_url'),
            constants.LOCALITIES
        ),
        transformers.ExtractZipStep(internal_path=""),
        loaders.Ogr2ogrStep(
            table_name=constants.LOCALITIES_TMP_TABLE, geom_type='Point', env={'SHAPE_ENCODING': 'ISO-8859-1'}
        ),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'gid': 'numeric',
            'fna': 'varchar',
            'fdc': 'varchar',
            'cod_pcia': 'varchar',
            'nom_pcia': 'varchar',
            'cod_depto': 'varchar',
            'nom_depto': 'varchar',
            'cod_ase': 'varchar',
            'tipo_asent': 'varchar',
            'cod_aglo': 'varchar',
            'nom_aglo': 'varchar',
            'cod_agl': 'varchar',
            'nom_agl': 'varchar',
            'lat_gd': 'varchar',
            'long_gd': 'varchar',
            'lat_gs': 'varchar',
            'long_gs': 'varchar',
            'geom': 'geometry',
        })
    ])

    entities_sstep = StepSequence([
        extractors.DownloadURLStep(
            constants.ENTITIES + '.zip',
            config.get('etl', 'settlements_entities_url'),
            constants.ENTITIES
        ),
        transformers.ExtractZipStep(internal_path=""),
        loaders.Ogr2ogrStep(
            table_name=constants.ENTITIES_TMP_TABLE, geom_type='Point', env={'SHAPE_ENCODING': 'ISO-8859-1'}
        ),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'gid': 'numeric',
            'tipo_asent': 'varchar',
            'nombre_geo': 'varchar',
            'codigo_ase': 'varchar',
            'nombre_agl': 'varchar',
            'codigo_agl': 'varchar',
            'nombre_dep': 'varchar',
            'codigo_ind': 'varchar',
            'nombre_pro': 'varchar',
            'codigo_in0': 'varchar',
            'latitud_gr': 'varchar',
            'longitud_g': 'varchar',
            'latitud_g0': 'varchar',
            'longitud_0': 'varchar',
            'fuente_de_': 'varchar',
            'geom': 'geometry',
        })
    ])

    return Process(constants.SETTLEMENTS, [
        utils.CheckDependenciesStep([Province, Department, LocalGovernment,
                                     CensusLocality]),
        CompositeStep([
            localities_sstep,
            hamlets_sstep,
            antarctic_bases_sstep,
            entities_sstep
        ]),
        SettlementsExtractionStep(),
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'settlements_target_size'),
            op='ge'),
        CompositeStepCreateFile(Settlement, 'settlements', config),
        CompositeStepCopyFile('settlements', config),
    ])


def update_commune_id(row):
    # Multiplicar por 7 los últimos tres dígitos del ID del departamento
    # Ver comentario en constants.py para más detalles.
    prov_id_part = row.codigo_ase[:constants.PROVINCE_ID_LEN]
    dept_id_part = row.codigo_ase[
        constants.PROVINCE_ID_LEN:constants.DEPARTMENT_ID_LEN]
    id_rest = row.codigo_ase[constants.DEPARTMENT_ID_LEN:]

    dept_id_int = int(dept_id_part)
    if dept_id_int > 15:
        # Alguno de los IDs no cumple con la numeración antigua
        raise ProcessException('El ID de comuna {} no es válido.'.format(
            dept_id_part))

    dept_new_id_int = dept_id_int * constants.CABA_MULT_FACTOR
    row.codigo_ase = prov_id_part + str(dept_new_id_int).rjust(
        len(dept_id_part), '0') + id_rest


class SettlementsExtractionStep(transformers.EntitiesExtractionStep):

    def __init__(self, name='settlements_extraction', entity_class=Settlement):
        super().__init__(name, entity_class, entity_class_pkey='id',
                         tmp_entity_class_pkey='codigo_ase')

    def get_tmp_settlements_table(self, ctx):

        inspector = inspect(ctx.engine)
        if constants.SETTLEMENTS_TMP_TABLE in inspector.get_table_names(schema='public'):
            Table(constants.SETTLEMENTS_TMP_TABLE, MetaData(), schema='public') \
                .drop(bind=ctx.engine)

        Table(
            constants.SETTLEMENTS_TMP_TABLE, MetaData(),
            Column('codigo_ase', String, primary_key=True),
            Column('nombre_geo', String),
            Column('tipo_asent', String),
            Column('fuente_de_', String),
            Column('geom', Geometry('POINT', srid=4326)),
            schema='public'
        ).create(bind=ctx.engine)

        return Table(
            constants.SETTLEMENTS_TMP_TABLE, MetaData(),
            autoload_with=ctx.engine,
            schema='public'
        )

    def _merge_tmp_settlements(self, tmp_settlements, ctx):
        ctx.report.info('Combinando asentamientos...')

        # Descomponemos las clases ORM
        tmp_locality, tmp_hamlet, tmp_antarctic_base, tmp_entity = tmp_settlements

        # Obtenemos todas las filas de cada tabla temporal
        localities = ctx.session.query(tmp_locality).all()
        hamlets = ctx.session.query(tmp_hamlet).all()
        antarctic_bases = ctx.session.query(tmp_antarctic_base).all()
        entities = ctx.session.query(tmp_entity).all()

        tmp_settlements = self.get_tmp_settlements_table(ctx)

        # Insertamos todas las filas en la tabla tmp_asentamientos
        values = []
        for tmp_table in [antarctic_bases, hamlets, localities]:
            for row in tmp_table:
                raw_geom = to_shape(getattr(row, "geom"))  # geometria mal definida: (lat, lon)
                corrected_geom = Point(raw_geom.y, raw_geom.x)  # corregimos a (lon, lat)
                values.append({
                    'codigo_ase': getattr(row, 'cod_ase'),
                    'nombre_geo': getattr(row, 'fna'),
                    'tipo_asent': getattr(row, 'tipo_asent'),
                    'fuente_de_': getattr(row, 'fdc'),
                    'geom': f'SRID=4326;{corrected_geom.wkt}',
                })

        for tmp_table in [entities]:
            for row in tmp_table:
                values.append({
                    'codigo_ase': getattr(row, 'codigo_ase'),
                    'nombre_geo': getattr(row, 'nombre_geo'),
                    'tipo_asent': getattr(row, 'tipo_asent'),
                    'fuente_de_': getattr(row, 'fuente_de_', getattr(row, 'fuente_de_', None)),
                    'geom': f'SRID=4326;{to_shape(getattr(row, "geom")).wkt}'
                })

        if values:
            ctx.session.execute(insert(tmp_settlements), values)

        ctx.session.commit()

        return utils.automap_table(constants.SETTLEMENTS_TMP_TABLE, ctx)

    def _patch_tmp_entities(self, tmp_entities, ctx):

        # Se modifican los códigos de CABA.
        def build_prefixes(codigos):
            return ['02' + str(cod).rjust(3, '0') for cod in codigos]

        expressions = [
            # Primer paso: Liberamos dos llaves para evitar colisiones,
            # ya que 02001 y 02002 pasarán a ser 02007 y 02014 respectivamente.
            func.substr(tmp_entities.codigo_ase, 1, 5).in_(build_prefixes([7, 14])),

            # Segundo paso: Aplicar transformación completa.
            # En este momento 02007 y 02014 mutaron a 02049 y 02098 respectivamente.
            func.substr(tmp_entities.codigo_ase, 1, 5).in_(build_prefixes(list(range(1, 16))))
        ]

        for expression in expressions:
            patch.apply_fn(tmp_entities, update_commune_id, ctx, expression)

        def fix_department(row):
            department = (
                ctx.session.query(Department)
                .filter(
                    Department.provincia_id == '94',
                    func.ST_Intersects(Department.geometria, row.geom)
                )
                .one_or_none()
            )

            if department:
                codigo_ase = department.id + row.codigo_ase[5:]
                row.codigo_ase = codigo_ase

        patch.apply_fn(tmp_entities, fix_department, ctx, tmp_entities.codigo_ase.like("94007%"))
        patch.apply_fn(tmp_entities, fix_department, ctx, tmp_entities.codigo_ase.like("94014%"))

    def _run_internal(self, tmp_settlements, ctx):
        tmp_settlements_merged = self._merge_tmp_settlements(tmp_settlements, ctx)
        return super()._run_internal(tmp_settlements_merged, ctx)

    def _process_entity(self, tmp_settlement, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_settlement.geom,
                                                     ctx)
        settlement_id = tmp_settlement.codigo_ase
        prov_id = settlement_id[:constants.PROVINCE_ID_LEN]
        dept_id = settlement_id[:constants.DEPARTMENT_ID_LEN]
        census_loc_id = settlement_id[:constants.CENSUS_LOCALITY_ID_LEN]

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

        local_governments = geometry.get_entity_at_point(LocalGovernment,
                                                    tmp_settlement.geom, ctx)

        if prov_id == constants.CABA_PROV_ID:
            # Las calles de CABA pertenecen a la localidad censal 02000010,
            # pero sus IDs *no* comienzan con ese código.
            census_loc_id = constants.CABA_CENSUS_LOCALITY

        census_loc = cached_session.query(CensusLocality).get(
            census_loc_id)

        return Settlement(
            id=settlement_id,
            nombre=utils.clean_string(tmp_settlement.nombre_geo),
            categoria=utils.clean_string(tmp_settlement.tipo_asent),
            lon=lon, lat=lat,
            provincia_id=prov_id,
            departamento_id=dept_id,
            gobierno_local_id=local_governments.id if local_governments else None,
            localidad_censal_id=census_loc.id if census_loc else None,
            fuente=utils.clean_string(tmp_settlement.fuente_de_),
            geometria=tmp_settlement.geom
        )
