import re
import unicodedata

from sqlalchemy import select
from sqlalchemy.sql import func
from sqlalchemy.sql.sqltypes import Integer
from tqdm import tqdm

from .exceptions import ValidationException
from .loaders import CompositeStepCreateFile, CompositeStepCopyFile
from .process import Process, CompositeStep, StepSequence
from .models import Province, Department, CensusLocality, Street
from . import extractors, loaders, utils, constants, patch, transformers
from .utils import FunctionStep

INVALID_BLOCKS_CENSUS_LOCALITIES = [
    # '62042450',
    # '74056100',
    # '74056150',
    # '14098230',
    # '14098170',
    # '58042010',
    # '06778020',
    # '42133050'
]

INVALID_BLOCKS_CLC = {
    # '06028028': '06028010',
    # '06035035': '06035010',
    # '06091091': '06091010',
    # '06098098': '06098010',
    # '06105030': '06105040',
    # '06105040': '06105050',
    # '06105050': '06105060',
    # '06105060': '06105070',
    # '06105070': '06105080',
    # '06245245': '06245010',
    # '06252252': '06252010',
    # '06260260': '06260010',
    # '06266020': '06266100',
    # '06270270': '06270010',
    # '06274274': '06274010',
    # '06364364': '06364030',
    # '06371371': '06371010',
    # '06408408': '06408010',
    # '06410410': '06410010',
    # '06412412': '06412010',
    # '06427427': '06427010',
    # '06434434': '06434010',
    # '06441441': '06441030',
    # '06490490': '06490010',
    # '06515515': '06515010',
    # '06525525': '06525020',
    # '06539539': '06539010',
    # '06560560': '06560010',
    # '06568568': '06568010',
    # '06638638': '06638040',
    # '06648648': '06648010',
    # '06658658': '06658010',
    # '06749749': '06749010',
    # '06756756': '06756010',
    # '06760760': '06760010',
    # '06763070': '06763060',
    # '06778778': '06778020',
    # '06805805': '06805010',
    # '06833040': '06833050',
    # '06833050': '06833060',
    # '06833060': '06833070',
    # '06833070': '06833080',
    # '06833080': '06833090',
    # '06833090': '06833100',
    # '06833100': '06833110',
    # '06840840': '06840010',
    # '06847030': '06847020',
    # '06861861': '06861010',
    # '14175103': '14049112',
    # '30021140': '30021125',
    # '30021140': '30021128',
    # '38007093': '38007092',
    # '66105010': '38084055',
    # '42133020': '42133050',
    # '50049250': '50049015',
    # '50063090': '50063070',
    # '50070090': '50070100',
    # '54007010': '54007025',
    # '38084055': '66105010',
    # '70098010': '70063010',
    # '82105240': '82105250',
    # '82119020': '82119150',
    # '86049010': '86049015',
    # '86049120': '86049040',
    # '86049050': '86049060',
    # '86049050': '86049120',
    # '86147050': '86091040',
    # '86091050': '86091060',
    # '86147060': '86147095',
    # '86147130': '86147123',
    # '90098040': '90098035',
}

ThirdResultStep = FunctionStep(fn=lambda results: results[2],
                               name='third_result')

locality_names = {
    "Villa Otuzar": "Villa Ortúzar",
    "El Carmen": "Barrio El Carmen Este",
    "Manuel Belgrano": "Barrio Belgrano",
    "Barrio Santa Rosa": "Santa Rosa",
    "Billinghurts": "Billinghurst",
    "Villa María Irene de Los Remedios de Escalada": "Va.María Irene de los Remedios Escalada",
    "Villa Marques Alejandro María de Aguado": "Va.Marqués Alejandro María de Aguado",
    "Villa Gobernador Udaondo": "Villa Gobernador Udadondo",
}

def create_process(config):

    # CUADRAS
    url_template_street_blocks = config.get('etl', 'street_blocks_url_template')
    download_street_blocks_cstep = CompositeStep([
        extractors.DownloadURLStep(
            '{}_{}.geojson'.format(constants.STREET_BLOCKS, province_id),
            url_template_street_blocks.format(province_id),
            constants.STREET_BLOCKS
        ) for province_id in constants.PROVINCE_IDS
    ], name='download_cstep')
    ogr2ogr_cstep = CompositeStep([
        loaders.Ogr2ogrStep(table_name=constants.STREET_BLOCKS_TMP_TABLE,
                            geom_type='MultiLineString',
                            source_epsg='EPSG:4326')
    ] + [
        loaders.Ogr2ogrStep(table_name=constants.STREET_BLOCKS_TMP_TABLE,
                            geom_type='MultiLineString', overwrite=False,
                            source_epsg='EPSG:4326')
    ] * (len(download_street_blocks_cstep) - 1), name='ogr2ogr_cstep')
    street_blocks_sstep = StepSequence([
                download_street_blocks_cstep,
                ogr2ogr_cstep,
                utils.FirstResultStep,
                utils.ValidateTableSchemaStep({
                    'id': 'integer',
                    'nomencla': 'varchar',
                    'tipo': 'varchar',
                    'nombre': 'varchar',
                    'desdei': 'integer',
                    'desded': 'integer',
                    'hastad': 'integer',
                    'hastai': 'integer',
                    'codloc20': 'varchar',
                    'geom': 'geometry'
                })
            ], name='load_tmp_street_blocks')

    # CALLES
    url_template_street = config.get('etl', 'streets_url_template')
    download_streets_cstep = CompositeStep([
        extractors.DownloadURLStep(
            '{}_{}.geojson'.format(constants.STREETS, province_id),
            url_template_street.format(province_id),
            constants.STREETS
        ) for province_id in constants.PROVINCE_IDS
    ], name='download_cstep_streets')

    ogr2ogr_streets_cstep = CompositeStep([
      loaders.Ogr2ogrStep(table_name=constants.STREETS_TMP_TABLE,
                          geom_type='MultiLineString',
                          source_epsg='EPSG:4326')
    ] + [
      loaders.Ogr2ogrStep(table_name=constants.STREETS_TMP_TABLE,
                          geom_type='MultiLineString', overwrite=False,
                          source_epsg='EPSG:4326')
    ] * (len(download_street_blocks_cstep) - 1), name='ogr2ogr_cstep_streets')
    street_sstep = StepSequence([
                download_streets_cstep,
                ogr2ogr_streets_cstep,
                utils.FirstResultStep,
                utils.ValidateTableSchemaStep({
                    'id': 'integer',
                    'nomencla': 'varchar',
                    'tipo': 'varchar',
                    'nombre': 'varchar',
                    'desdei': 'varchar',
                    'desded': 'varchar',
                    'hastai': 'varchar',
                    'hastad': 'varchar',
                    'codloc': 'varchar',
                    'geom': 'geometry'
                })
            ], name='load_tmp_streets')

    # Pasos para la incorporación de localidades a las calles provisoriamente desde una fuente parcial
    url_template_localities = config.get('etl', 'localities_url')
    download_sstep_localities = StepSequence([
                extractors.DownloadURLStep('entidades_RMBA.zip',
                                           url_template_localities, constants.LOCALITIES),
                transformers.ExtractZipStep(
                    internal_path=""
                ),
            ], name='download_tmp_localities')
    ogr2ogr_sstep_localities = StepSequence([
                loaders.Ogr2ogrStep(
                    table_name=constants.LOCALITIES_TMP_TABLE, geom_type='Geometry', source_epsg='EPSG:3857',
                    precision=False
                ),
            ], name='ogr2ogr_sstep_localities')
    localities_sstep = StepSequence([
                download_sstep_localities,
                ogr2ogr_sstep_localities,
                utils.ValidateTableSchemaStep({
                    'ogc_fid': 'integer',
                    'fid_1': 'bigint',
                    'id': 'varchar',
                    'cpr': 'varchar',
                    'jurisdic': 'varchar',
                    'cde': 'varchar',
                    'depto': 'varchar',
                    'clc': 'varchar',
                    'localidad': 'varchar',
                    'cen': 'varchar',
                    'entidad': 'varchar',
                    'fna': 'varchar',
                    'gna': 'varchar',
                    'nam': 'varchar',
                    'ceu': 'varchar',
                    'link': 'varchar',
                    'shape_leng': 'double',
                    'shape_area': 'double',
                    'nombre': 'varchar',
                    'geom': 'geometry',
                })
            ], name='load_tmp_localities')

    return Process(constants.STREETS, [
        utils.CheckDependenciesStep([Province, Department, CensusLocality]),
        CompositeStep([
            street_blocks_sstep,
            street_sstep,
            localities_sstep,
        ]),
        StreetsExtractionStep(),
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'streets_target_size'),
            op='ge'),
        CompositeStepCreateFile(
            Street, 'streets', config,
            tolerance=config.getfloat("etl", "geojson_tolerance"),
            caba_tolerance=config.getfloat("etl", "geojson_caba_tolerance")
        ),
        CompositeStepCopyFile('streets', config),
    ])


def report_street_block_number_state(tmp_blocks, ctx, name):
    total = ctx.session.query(func.count()).filter(tmp_blocks.tipo == 'CALLE').all()[0][0]
    # Informe de cuadras sin numeración
    sb_no_num_by_loc = ctx.session.query(tmp_blocks.codloc20, func.count()).filter(
        (tmp_blocks.tipo == 'CALLE') &
        (tmp_blocks.desdei == 0) & (tmp_blocks.hastai == 0) & (tmp_blocks.desded == 0) & (tmp_blocks.hastad == 0)
    ).group_by(tmp_blocks.codloc20).all()

    sb_no_num_count = 0
    sb_no_num_warning = []
    for loc in sb_no_num_by_loc:
        sb_no_num_warning.append((loc[0], "Hay {} cuadras de calles sin numeración".format(loc[1])))
        sb_no_num_count += loc[1]

    if sb_no_num_warning:
        message = 'Existen {} cuadras de calles sin numeración de un total de {}'.format(sb_no_num_count, total)
        ctx.report.warn(message)
        ctx.report.get_data(name)['warning'] = sb_no_num_warning

    # informe de cuadras con numeración errónea
    sb_wrong_num_by_loc = ctx.session.query(tmp_blocks.codloc20, func.count()).filter(
        (tmp_blocks.tipo == 'CALLE') &
        ((tmp_blocks.desdei > tmp_blocks.hastai) | (tmp_blocks.desded > tmp_blocks.hastad))
    ).group_by(tmp_blocks.codloc20).all()

    sb_wrong_num_count = 0
    sb_wrong_num_warning = []
    for loc in sb_wrong_num_by_loc:
        sb_wrong_num_warning.append((loc[0], "Hay {} cuadras de calles con numeración errónea".format(loc[1])))
        sb_wrong_num_count += loc[1]

    if sb_wrong_num_warning:
        message = 'Existen {} cuadras de calles con numeración errónea de un total de {}'.format(sb_wrong_num_count, total)
        ctx.report.warn(message)
        ctx.report.get_data(name)['warning'] = sb_wrong_num_warning

class StreetsExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('streets_extraction', Street,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='nomencla')
        self._tmp_localities = None
        self._polygons_errors = []

    def _mix_street_blocks_and_localities(self, ctx):

        Cuadras = utils.automap_table(constants.STREET_BLOCKS_TMP_TABLE, ctx)
        Localidades_tmp = utils.automap_table(constants.LOCALITIES_TMP_TABLE, ctx)
        Localidades = utils.automap_table(constants.LOCALITIES_ETL_TABLE, ctx)

        required_percentage = 0.8  # 80%

        def normalize(text):
            if not text:
                return ""
            text = text.lower()
            text = unicodedata.normalize("NFD", text)
            text = "".join(c for c in text if unicodedata.category(c) != "Mn")  # remove accents
            text = re.sub(r"[^\w\s]", "", text)  # remove punctuation
            text = re.sub(r"\s+", " ", text).strip()  # normalize whitespace
            return text

        clc_list = [
            row[0] for row in ctx.session.query(Localidades_tmp.clc).distinct().order_by(Localidades_tmp.clc).all()
        ]
        loc_warning_msg = []
        loc_error_msg = []
        total_polygons = 0
        from shapely.wkb import loads as load_wkb
        for clc in tqdm(clc_list, desc="Procesando localidades censales con polígonos de localidades..."):

            # Obtenemos todas las localidades que pertenecen a la localidad censal
            localities = ctx.session.query(Localidades).filter(Localidades.localidad_censal_id == clc).all()
            names = [locality.nombre for locality in localities]

            # Obtenemos los polígonos de localidades suministrados por otra fuente
            localities_tmp = ctx.session.query(Localidades_tmp).filter(Localidades_tmp.clc == clc).all()
            total_polygons += len(localities_tmp)

            for locality_tmp in localities_tmp:

                # Buscamos los puntos contenidos en el polígono
                poly = load_wkb(bytes(locality_tmp.geom.data))
                localities_in_polygon = [
                    loc for loc in localities
                    if poly.contains(load_wkb(bytes(loc.geometria.data)))
                ]

                # Si no hay puntos dentro del polígono se excluye la localidad
                if not localities_in_polygon:
                    loc_error_msg.append(
                        (locality_tmp.link,
                         "No se encontraron localidades dentro del polígono de '{}'. Localidades en {}: {}"
                         .format(locality_tmp.nam, clc, names)))
                    continue

                # Si hay un solo punto dentro del polígono se considera a la localidad como válida independientemente del nombre
                if len(localities_in_polygon) == 1:
                    locality = localities_in_polygon[0]
                    if locality.nombre != locality_tmp.nam or locality.id != locality_tmp.link:
                        loc_warning_msg.append(
                            (locality_tmp.link,
                             "Se vinculó el polígono de '{}' a la localidad '{}' (id={})".format(locality_tmp.nam, locality.nombre, locality.id))
                        )

                # Si hay más de un punto...
                else:
                    fixed_name = locality_names.get(locality_tmp.nam, locality_tmp.nam)
                    loc_to_consider = [l for l in localities_in_polygon if normalize(fixed_name) == normalize(l.nombre)]

                    if len(loc_to_consider) == 0:
                        loc_error_msg.append(
                            (locality_tmp.link,
                             "El polígono de '{}' no se pudo vincular a ninguna localidad. Localidades en {}: {}"
                             .format(locality_tmp.nam, clc, names)))
                        continue

                    if len(loc_to_consider) > 1:
                        loc_error_msg.append(
                            (locality_tmp.link,
                             "El polígono de '{}' no se pudo vincular unívocamente a alguna de las siguientes localidades: {}. Localidades en {}: {}"
                             .format(locality_tmp.nam, [l.nombre for l in loc_to_consider], clc, names)))
                        continue

                    locality = loc_to_consider[0]
                    if locality.nombre != locality_tmp.nam or locality.id != locality_tmp.link:
                        loc_warning_msg.append(
                            (locality_tmp.link,
                             "Se viculó el polígono de '{}' a la localidad '{}' (id={})".format(locality_tmp.nam, locality.nombre, locality.id))
                        )

                street_blocks = (
                    ctx.session.query(Cuadras)
                    .filter(Cuadras.codloc20 == clc)
                    .filter(func.ST_Intersects(Cuadras.geom, locality_tmp.geom))
                    .filter((func.ST_Length(func.ST_Intersection(locality_tmp.geom, Cuadras.geom)) / func.ST_Length(
                        Cuadras.geom)) >= required_percentage)
                    .all()
                )

                for street_block in street_blocks:
                    street_block.loc_link = locality.id
                    street_block.loc_nombre = locality.nombre

        ctx.session.commit()

        for street_block in tqdm(ctx.session.query(Cuadras).all(), desc="Cambiando identificador de calles..."):
            subfix = street_block.nomencla[8:]
            if street_block.loc_link:
                prefix = str(street_block.loc_link).ljust(10, '0')
            else:
                prefix = str(street_block.codloc20).ljust(10, '0')
            street_block.nomencla = prefix + subfix

        ctx.session.commit()

        if loc_error_msg:
            message = 'Existen {}/{} polígonos sin identificar.'.format(
                len(loc_error_msg), total_polygons)
            ctx.report.error(message)
            self._polygons_errors = loc_error_msg

        if loc_warning_msg:
            ctx.report.get_data(self.name)['warning'] = loc_warning_msg

    def _patch_tmp_entities(self, tmp_street_blocks, ctx):

        patch.delete(tmp_street_blocks, ctx, tipo='')

        patch.delete(tmp_street_blocks, ctx, nombre='')

        # Una cuadra de la calle "064414417007012" no contiene geometría
        patch.delete(tmp_street_blocks, ctx, geom=None)

        # patch.delete(tmp_street_blocks, ctx, nomencla='4213305000025')
        # patch.delete(tmp_street_blocks, ctx, nomencla='4213305000030')
        # patch.delete(tmp_street_blocks, ctx, nomencla='4213305000075')

        def update_clc(row):
            old_clc = row.nomencla[:constants.CENSUS_LOCALITY_ID_LEN]
            new_clc = INVALID_BLOCKS_CLC.get(old_clc)
            row.nomencla = new_clc + row.nomencla[constants.CENSUS_LOCALITY_ID_LEN:]
            row.codloc20 = new_clc

        for clc in INVALID_BLOCKS_CLC.keys():
            patch.apply_fn(tmp_street_blocks, update_clc, ctx, tmp_street_blocks.nomencla.like('{}%'.format(clc)))

        ctx.session.commit()

        self._mix_street_blocks_and_localities(ctx)

    def _entities_query_count(self, tmp_blocks, ctx):
        return ctx.session.query(tmp_blocks).\
            filter(tmp_blocks.tipo != constants.STREET_TYPE_OTHER).\
            distinct(tmp_blocks.nomencla).\
            count()

    def _build_entities_query(self, tmp_blocks, ctx):
        fields = [
            func.min(tmp_blocks.id).label('id'),
            tmp_blocks.nomencla,
            func.min(tmp_blocks.loc_link).label('loc_link'),
            func.min(tmp_blocks.loc_nombre).label('loc_nombre'),
            func.min(tmp_blocks.nombre).label('nombre'),
            func.min(tmp_blocks.tipo).label('tipo'),
            func.min(tmp_blocks.desdei.cast(Integer)).label('desdei'),
            func.min(tmp_blocks.desded.cast(Integer)).label('desded'),
            func.max(tmp_blocks.hastai.cast(Integer)).label('hastai'),
            func.max(tmp_blocks.hastad.cast(Integer)).label('hastad'),
            func.ST_Multi(tmp_blocks.geom.ST_Union()).label('geom')
        ]

        statement = select(fields).\
            group_by(tmp_blocks.nomencla).\
            where(tmp_blocks.tipo != constants.STREET_TYPE_OTHER)

        return ctx.engine.execute(statement)

    def _copy_tmp_streets(self, tmp_blocks, tmp_streets, ctx):
        bulk_size = ctx.config.getint('etl', 'bulk_size')
        ctx.report.info(
            'Copiando calles de localidades con cuadras inválidas...')

        blocks = []
        for census_locality_id in INVALID_BLOCKS_CENSUS_LOCALITIES:
            ctx.report.info('Localidad Censal ID {}'.format(
                census_locality_id))

            query = ctx.session.query(tmp_streets).\
                filter_by(codloc=census_locality_id).\
                yield_per(bulk_size)

            for tmp_street in query:
                blocks.append(tmp_blocks(
                    geom=tmp_street.geom,
                    codloc20=tmp_street.codloc,
                    nomencla=tmp_street.nomencla,
                    nombre=tmp_street.nombre,
                    tipo=tmp_street.tipo,
                    desded=int(tmp_street.desded),
                    desdei=int(tmp_street.desdei),
                    hastad=int(tmp_street.hastad),
                    hastai=int(tmp_street.hastai)
                ))

        ctx.session.add_all(blocks)
        ctx.session.commit()
        ctx.report.info('Terminado.\n')

    def _run_internal(self, data, ctx):
        tmp_blocks, tmp_streets, self._tmp_localities = data

        # Agregar columnas loc_link y loc_nombre a la tabla si no existen
        with ctx.engine.begin() as connection:
            table_name = tmp_blocks.__table__.name
            connection.execute(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS loc_link VARCHAR')
            connection.execute(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS loc_nombre VARCHAR')
        tmp_blocks = utils.automap_table(constants.STREET_BLOCKS_TMP_TABLE, ctx)

        report_street_block_number_state(tmp_blocks, ctx, self.name)

        if tmp_streets:
            for census_locality_id in INVALID_BLOCKS_CENSUS_LOCALITIES:
                patch.delete(tmp_blocks, ctx, codloc20=census_locality_id)

            self._copy_tmp_streets(tmp_blocks, tmp_streets, ctx)

        result = super()._run_internal(tmp_blocks, ctx)
        ctx.report.get_data(self.name)['errors'].extend(self._polygons_errors)
        return result

    def _process_entity(self, street, cached_session, ctx):
        street_id = street.nomencla
        prov_id = street_id[:constants.PROVINCE_ID_LEN]
        dept_id = street_id[:constants.DEPARTMENT_ID_LEN]
        census_loc_id = street_id[:constants.CENSUS_LOCALITY_ID_LEN]
        loc_id = street.loc_link

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        department = cached_session.query(Department).get(dept_id)
        if not department:
            raise ValidationException(
                'No existe el departamento con ID {}'.format(dept_id))

        census_locality = cached_session.query(CensusLocality).get(
            census_loc_id)
        if not census_locality:
            raise ValidationException(
                'No existe la localidad censal con ID {}'.format(
                    census_loc_id))

        return Street(
            id=street_id,
            nombre=utils.clean_string(street.nombre),
            categoria=utils.clean_string(street.tipo),
            fuente=constants.STREETS_SOURCE,
            inicio_derecha=street.desded or 0,
            fin_derecha=street.hastad or 0,
            inicio_izquierda=street.desdei or 0,
            fin_izquierda=street.hastai or 0,
            geometria=street.geom,
            provincia_id=prov_id,
            departamento_id=dept_id,
            localidad_censal_id=census_loc_id,
            localidad_id=loc_id
        )
