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

    ctx.session.commit()

    return tmp_blocks


class StreetsExtractionStep(transformers.EntitiesExtractionStep):

    def __init__(self):
        super().__init__('streets_extraction', Street,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='nomencla')
        self._tmp_localities = None
        self._polygons_errors = []
        self._loc_error_msg = []
        self._loc_warning_msg = []
        self._total_polygons = 0

    def _add_locality_to_street_blocks(self, tmp_street_blocks, ctx):

        # Agregar columnas loc_link y loc_nombre a la tabla si no existen
        with ctx.engine.begin() as connection:
            table_name = tmp_street_blocks.__table__.name
            connection.execute(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS loc_link VARCHAR')
            connection.execute(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS loc_nombre VARCHAR')

        StreetBlocks_tmp = utils.automap_table(constants.STREET_BLOCKS_TMP_TABLE, ctx)
        Localities_tmp = utils.automap_table(constants.LOCALITIES_TMP_TABLE, ctx)
        Localities = utils.automap_table(constants.LOCALITIES_ETL_TABLE, ctx)

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

        # Genera una lista de códigos de localidades censales para los polígonos de las localidades temporales
        clc_tmp_list = [
            row[0] for row in ctx.session.query(Localities_tmp.clc).distinct().order_by(Localities_tmp.clc).all()
        ]
        from shapely.wkb import loads as load_wkb
        for clc_tmp in tqdm(clc_tmp_list, desc="Procesando localidades censales con polígonos de localidades..."):

            # Obtenemos todas las localidades que pertenecen a la localidad censal
            localities = ctx.session.query(Localities).filter(Localities.localidad_censal_id == clc_tmp).all()
            names = [locality.nombre for locality in localities]

            # Obtenemos los polígonos de localidades temporales suministrados por la fuente externa
            localities_tmp = ctx.session.query(Localities_tmp).filter(Localities_tmp.clc == clc_tmp).all()
            self._total_polygons += len(localities_tmp)

            # Procesamos cada polígono de la localidad censal
            for locality_tmp in localities_tmp:

                # Buscamos los puntos de localidades contenidos en el polígono
                poly = load_wkb(bytes(locality_tmp.geom.data))
                localities_in_polygon = [
                    loc for loc in localities
                    if poly.contains(load_wkb(bytes(loc.geometria.data)))
                ]

                # Si no hay puntos dentro del polígono se excluye la localidad
                if not localities_in_polygon:
                    self._loc_error_msg.append(
                        (locality_tmp.link,
                         "No se encontraron localidades dentro del polígono de '{}'. Localidades en {}: {}"
                         .format(locality_tmp.nam, clc_tmp, names)))
                    continue

                # Si hay un solo punto dentro del polígono se considera a la localidad como válida independientemente del nombre
                if len(localities_in_polygon) == 1:
                    locality = localities_in_polygon[0]
                    if locality.nombre != locality_tmp.nam or locality.id != locality_tmp.link:
                        self._loc_warning_msg.append(
                            (locality_tmp.link,
                             "Se vinculó el polígono de '{}' a la localidad '{}' (id={})".format(locality_tmp.nam, locality.nombre, locality.id))
                        )

                # Si hay más de un punto...
                else:
                    fixed_name = locality_names.get(locality_tmp.nam, locality_tmp.nam)
                    localities_to_consider = [l for l in localities_in_polygon if normalize(fixed_name) == normalize(l.nombre)]

                    if len(localities_to_consider) == 0:
                        self._loc_error_msg.append(
                            (locality_tmp.link,
                             "El polígono de '{}' no se pudo vincular a ninguna localidad. Localidades en {}: {}"
                             .format(locality_tmp.nam, clc_tmp, names)))
                        continue

                    if len(localities_to_consider) > 1:
                        self._loc_error_msg.append(
                            (locality_tmp.link,
                             "El polígono de '{}' no se pudo vincular unívocamente a alguna de las siguientes localidades: {}. Localidades en {}: {}"
                             .format(locality_tmp.nam, [l.nombre for l in localities_to_consider], clc_tmp, names)))
                        continue

                    locality = localities_to_consider[0]
                    if locality.nombre != locality_tmp.nam or locality.id != locality_tmp.link:
                        self._loc_warning_msg.append(
                            (locality_tmp.link,
                             "Se viculó el polígono de '{}' a la localidad '{}' (id={})".format(locality_tmp.nam, locality.nombre, locality.id))
                        )

                # Si se pudo identificar la localidad temporal como válida usamos su polígono para extraer todas las
                # cuadras por intersección
                street_blocks_tmp = (
                    ctx.session.query(StreetBlocks_tmp)
                    .filter(StreetBlocks_tmp.codloc20 == clc_tmp)
                    .filter(func.ST_Intersects(StreetBlocks_tmp.geom, locality_tmp.geom))
                    .filter((func.ST_Length(func.ST_Intersection(locality_tmp.geom, StreetBlocks_tmp.geom)) / func.ST_Length(
                        StreetBlocks_tmp.geom)) >= required_percentage)
                    .all()
                )

                # A cada cuadra le agregamos los datos de la localidad
                for street_block in street_blocks_tmp:
                    street_block.loc_link = locality.id
                    street_block.loc_nombre = locality.nombre

        ctx.session.commit()

        return StreetBlocks_tmp

    def _change_street_block_id(self, tmp_street_blocks, ctx):

        bulk_size = ctx.config.getint('etl', 'bulk_size')
        total = ctx.session.query(tmp_street_blocks).count()
        pbar = tqdm(total=total, desc="Cambiando identificador de calles...")

        last_id = 0

        while True:
            street_blocks = (
                ctx.session.query(tmp_street_blocks)
                .filter(tmp_street_blocks.id > last_id)
                .order_by(tmp_street_blocks.id)
                .limit(bulk_size)
                .all()
            )

            if not street_blocks:
                break

            for street_block in street_blocks:
                nomencla_orig = street_block.nomencla
                street_id = nomencla_orig[8:]

                if street_block.loc_link:
                    loc_str = str(street_block.loc_link).ljust(10, '0')
                else:
                    loc_str = str(street_block.codloc20).ljust(10, '0')

                street_block.nomencla = loc_str + street_id

                last_id = street_block.id
                pbar.update(1)

            ctx.session.commit()

        ctx.session.commit()

        pbar.close()

        if self._loc_error_msg:
            message = 'Existen {}/{} polígonos sin identificar.'.format(
                len(self._loc_error_msg), self._total_polygons)
            ctx.report.error(message)
            self._polygons_errors = self._loc_error_msg

        if self._loc_warning_msg:
            ctx.report.get_data(self.name)['warning'] = self._loc_warning_msg

        return tmp_street_blocks

    def _patch_tmp_entities(self, tmp_street_blocks, ctx):

        patch.delete(tmp_street_blocks, ctx, tipo='')

        patch.delete(tmp_street_blocks, ctx, nombre='')

        # Una cuadra de la calle "064414417007012" no contiene geometría
        patch.delete(tmp_street_blocks, ctx, geom=None)

        ctx.session.commit()

    def _entities_query_count(self, tmp_street_blocks, ctx):
        return ctx.session.query(tmp_street_blocks).\
            filter(tmp_street_blocks.tipo != constants.STREET_TYPE_OTHER).\
            distinct(tmp_street_blocks.nomencla).\
            count()

    def _build_entities_query(self, tmp_street_blocks, ctx):

        fields = [
            func.min(tmp_street_blocks.id).label('id'),
            tmp_street_blocks.nomencla,
            func.min(tmp_street_blocks.loc_link).label('loc_link'),
            func.min(tmp_street_blocks.loc_nombre).label('loc_nombre'),
            func.min(tmp_street_blocks.nombre).label('nombre'),
            func.min(tmp_street_blocks.tipo).label('tipo'),
            func.min(tmp_street_blocks.desdei.cast(Integer)).label('desdei'),
            func.min(tmp_street_blocks.desded.cast(Integer)).label('desded'),
            func.max(tmp_street_blocks.hastai.cast(Integer)).label('hastai'),
            func.max(tmp_street_blocks.hastad.cast(Integer)).label('hastad'),
            func.ST_Multi(tmp_street_blocks.geom.ST_Union()).label('geom')
        ]

        statement = select(fields).\
            group_by(tmp_street_blocks.nomencla).\
            where(tmp_street_blocks.tipo != constants.STREET_TYPE_OTHER)

        return ctx.engine.execute(statement)

    def _run_internal(self, data, ctx):
        tmp_street_blocks, self._tmp_localities = data

        tmp_street_blocks = report_street_block_number_state(tmp_street_blocks, ctx, self.name)
        tmp_street_blocks = self._add_locality_to_street_blocks(tmp_street_blocks, ctx)
        tmp_street_blocks = self._change_street_block_id(tmp_street_blocks, ctx)

        result = super()._run_internal(tmp_street_blocks, ctx)
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
            nombre=utils.clean_string(street.nombre or ''),
            categoria=utils.clean_string(street.tipo or ''),
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
