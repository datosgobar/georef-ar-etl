from .exceptions import ValidationException
from .loaders import CompositeStepCreateFile, CompositeStepCopyFile
from .process import Process, CompositeStep, StepSequence
from .models import Province, Municipality
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(config):

    return Process(constants.MUNICIPALITIES, [
        utils.CheckDependenciesStep([Province]),
        CompositeStep([
            StepSequence([
                extractors.DownloadURLStep(constants.MUNICIPALITIES + '.zip',
                                           config.get('etl', 'municipalities_url'), constants.MUNICIPALITIES),
                transformers.ExtractZipStep(
                    internal_path=""
                ),
                loaders.Ogr2ogrStep(table_name=constants.MUNICIPALITIES_TMP_TABLE,
                                    geom_type='MultiPolygon',
                                    env={'SHAPE_ENCODING': 'utf-8'}),
                utils.ValidateTableSchemaStep({
                    'gid': 'numeric',
                    'ogc_fid': 'integer',
                    'fna': 'varchar',
                    'gna': 'varchar',
                    'nam': 'varchar',
                    'sag': 'varchar',
                    'fdc': 'varchar',
                    'in1': 'varchar',
                    'geom': 'geometry'
                }),
            ], name='load_tmp_municipalities'),
            StepSequence([
                extractors.DownloadURLStep(constants.LOCAL_GOVERNMENTS + '.csv',
                                           config.get('etl', 'local_government_url'), constants.LOCAL_GOVERNMENTS),
                loaders.Ogr2ogrStep(table_name=constants.LOCAL_GOVERNMENTS_TMP_TABLE,
                                    geom_type='Geometry'),
                utils.ValidateTableSchemaStep({
                    'ogc_fid': 'integer',
                    'cod_gl_res144_indec': 'varchar',
                    'nombre_del_gobierno_local': 'varchar',
                    'categoria_de_gobierno': 'varchar',
                    'gobierno_local_con_paso_o_centro_fronterizo': 'varchar',
                    'gobierno_local_que_pertenece_a_un_aglomerado_urbano': 'varchar',
                    'poblacion_censo_2010': 'varchar',
                    'localidades_administradas_por_el_gobierno_local': 'varchar',
                    'distancia_a_la_capital_provincial': 'varchar',

                    'distancia_a_la_capital_alterna': 'varchar',
                    'perfil_economico_de_la_region': 'varchar',
                    'cantidad_de_parques_industriales_renpi': 'varchar',
                    'incendios_reportados_en_el_periodo_1999_2022': 'varchar',

                    'provincia': 'varchar',
                    'departamento': 'varchar',
                    'inundaciones_reportadas_en_el_periodo_1999_2022': 'varchar',
                    'fiestas_locales': 'varchar',

                    'sitio_web': 'varchar',
                    'direccion_postal_de_la_sede_de_gobierno': 'varchar',
                    'telefonos_sede_de_gobierno': 'varchar',
                    'apellido_y_nombre_de_la_maxima_autoridad.': 'varchar',

                    'latitud': 'varchar',
                    'longitud': 'varchar',
                    'geom': 'geometry',
                }),
            ], name='load_tmp_local_governments'),
        ]),
        utils.FunctionStep(fn=lambda results: tuple(results)),
        CompositeStep([
            StepSequence([
                utils.FunctionStep(fn=lambda results: list(results)),
                LocalGovernmentsExtractionStep(),
            ]),
            StepSequence([
                utils.FunctionStep(fn=lambda results: list(results)),
                CompositeStep([utils.DropTableStep()] * 2),
            ]),
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'municipalities_target_size'),
            op='ge'),
        CompositeStepCreateFile(
            Municipality, 'local_governments', config,
            tolerance=config.getfloat("etl", "geojson_tolerance"),
            caba_tolerance=config.getfloat("etl", "geojson_caba_tolerance")
        ),
        CompositeStepCopyFile('local_governments', config),
    ])


class LocalGovernmentsExtractionStep(transformers.EntitiesExtractionStep):

    def __init__(self):
        super().__init__('municipalities_extraction', Municipality,
                         entity_class_pkey='id', tmp_entity_class_pkey='cod_gl_res144_indec')

    def _patch_tmp_entities(self, tmp_entities, ctx):
        # Elasticsearch (georef-ar-api) no procesa correctamente la geometría
        # de algunos municipios, lanza un error "Self-intersection at or near point..."
        # Validar la geometría utilizando ST_MakeValid().
        def make_valid_geom(mun):
            sql_str = """
                            select ST_MakeValid(geom)
                            from {}
                            where cod_gl_res144_indec=:cod_gl
                            limit 1
                            """.format(mun.__table__.name)

            # GeoAlchemy2 no disponibiliza la función ST_MakeValid, utilizar
            # SQL manualmente (como excepción).
            mun.geom = ctx.session.scalar(sql_str, {'cod_gl': mun.cod_gl_res144_indec})

        patch.apply_fn(tmp_entities, make_valid_geom, ctx, cod_gl_res144_indec='180224')
        patch.apply_fn(tmp_entities, make_valid_geom, ctx, cod_gl_res144_indec='180455')
        patch.apply_fn(tmp_entities, make_valid_geom, ctx, cod_gl_res144_indec='060056')

    def _fix_tmp_local_government_id(self, tmp_local_governments, ctx):
        for lg in ctx.session.query(tmp_local_governments):
            lg.cod_gl_res144_indec = str(lg.cod_gl_res144_indec).rjust(6, '0')
        ctx.session.commit()

    def _merge_tmp_municipalities(self, tmp_municipalities, tmp_local_governments, ctx):
        ctx.report.info('Combinando municipios...')

        for local_government in ctx.session.query(tmp_local_governments):
            municipality = ctx.session.query(tmp_municipalities).filter_by(
                in1=local_government.cod_gl_res144_indec
            ).first()
            if municipality:
                local_government.geom = municipality.geom
        ctx.session.commit()

    def _complete_geom(self, tmp_local_governments, ctx):
        for local_government in ctx.session.query(tmp_local_governments).filter_by(geom=None):
            lat = local_government.longitud
            lon = local_government.latitud
            point = tmp_local_governments.geom.ST_MakePoint(lon, lat)
            local_government.geom = tmp_local_governments.geom.ST_SetSRID(
                point,
                4326
            )
        ctx.session.commit()

    def _run_internal(self, tmp_entities, ctx):
        tmp_municipalities, tmp_local_governments = tmp_entities

        self._fix_tmp_local_government_id(tmp_local_governments, ctx)

        if tmp_municipalities:
            self._merge_tmp_municipalities(tmp_municipalities, tmp_local_governments, ctx)

        self._complete_geom(tmp_local_governments, ctx)

        return super()._run_internal(tmp_local_governments, ctx)

    def _process_entity(self, tmp_local_government, cached_session, ctx):
        lon = tmp_local_government.latitud
        lat = tmp_local_government.longitud

        muni_id = tmp_local_government.cod_gl_res144_indec
        prov_id = muni_id[:constants.PROVINCE_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        if ctx.mode == 'normal' and tmp_local_government.geom is not None:
            province_isct = geometry.get_intersection_percentage(
                province.geometria, tmp_local_government.geom, ctx)
        else:
            province_isct = 0  # Saltear operaciones costosas en testing

        categoria = tmp_local_government.categoria_de_gobierno
        nombre = tmp_local_government.nombre_del_gobierno_local
        nombre_completo = "{} {}".format(categoria, nombre)

        return Municipality(
            id=muni_id,
            nombre=utils.clean_string(nombre),
            nombre_completo=utils.clean_string(nombre_completo),
            categoria=utils.clean_string(categoria),
            lon=lon, lat=lat,
            provincia_interseccion=province_isct,
            provincia_id=prov_id,
            fuente='REFEGLO',
            geometria=tmp_local_government.geom
        )
