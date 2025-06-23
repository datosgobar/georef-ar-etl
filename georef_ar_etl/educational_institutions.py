import re
import unicodedata

from geoalchemy2.shape import from_shape
from shapely import Point
from sqlalchemy.sql import func
from tqdm import tqdm
from rapidfuzz import fuzz

from .exceptions import ValidationException
from .loaders import CompositeStepCreateFile, CompositeStepCopyFile
from .process import Process, CompositeStep, StepSequence
from .models import Province, Department, EducationalInstitution, LocalGovernment, Settlement
from . import extractors, loaders, utils, constants, transformers, geometry

def create_process(config):

    educational_institutions = StepSequence([
        extractors.DownloadURLStep(
            constants.EDUCATIONAL_INSTITUTIONS + '.zip',
            config.get('etl', 'educational_institution_url'),
            constants.EDUCATIONAL_INSTITUTIONS
        ),
        transformers.ExtractZipStep(internal_path=""),
        loaders.Ogr2ogrStep(
            table_name=constants.EDUCATIONAL_INSTITUTIONS_TMP_TABLE, geom_type='Point', env={'SHAPE_ENCODING': 'ISO-8859-1'}
        ),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'fna': 'varchar',
            'gna': 'varchar',
            'nam': 'varchar',
            'fun': 'varchar',
            'cue': 'varchar',
            'amg': 'varchar',
            'ges': 'varchar',
            'mde': 'varchar',
            'nen': 'varchar',
            'sag': 'varchar',
            'pre': 'varchar',
            'geom': 'geometry',
        })
    ])

    educational_institutions_mi = StepSequence([
        extractors.DownloadURLStep(
            constants.EDUCATIONAL_INSTITUTIONS + '-mi.zip',
            config.get('etl', 'educational_institution_mi_url'),
            constants.EDUCATIONAL_INSTITUTIONS
        ),
        transformers.ExtractZipStep(internal_path=""),
        loaders.Ogr2ogrStep(
            table_name=constants.EDUCATIONAL_INSTITUTIONS_TMP_TABLE + '_mi', geom_type='Point', env={'SHAPE_ENCODING': 'ISO-8859-1'}
        ),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'gid': 'numeric',
            'cueanexo': 'varchar',
            'provincia': 'varchar',
            'departamen': 'varchar',
            'localidad': 'varchar',
            'nombre': 'varchar',
            'domicilio': 'varchar',
            'cod_postal': 'varchar',
            'telefono': 'varchar',
            'email': 'varchar',
            'geom': 'geometry',
        })
    ])

    return Process(constants.EDUCATIONAL_INSTITUTIONS, [
        utils.CheckDependenciesStep([Province, Department, LocalGovernment, Settlement]),
        CompositeStep([
            educational_institutions,
            educational_institutions_mi,
        ]),
        EducationalInstitutionsExtractionStep(),
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'educational_institutions_target_size'),
            op='ge'),
        CompositeStepCreateFile(EducationalInstitution, 'educational_institutions', config),
        CompositeStepCopyFile('educational_institutions', config),
    ])

def normalize(text):
    if not text:
        return ""
    text = text.lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")  # remover acentos
    text = re.sub(r"[^\w\s]", "", text)  # quitar puntuación
    text = re.sub(r"\s+", " ", text).strip()  # espacios normales
    return text

class EducationalInstitutionsExtractionStep(transformers.EntitiesExtractionStep):

    def __init__(self):
        super().__init__('educational_institutions_extraction', EducationalInstitution,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='cue')

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
                    fixed_name = None
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

        bulk_size = ctx.config.getint('etl', 'bulk_size')
        offset = 0
        total = ctx.session.query(Cuadras).count()
        pbar = tqdm(total=total, desc="Cambiando identificador de calles...")
        while True:
            street_blocks = ctx.session.query(Cuadras).offset(offset).limit(bulk_size).all()
            if not street_blocks:
                break

            for street_block in street_blocks:
                subfix = street_block.nomencla[8:]
                if street_block.loc_link:
                    prefix = str(street_block.loc_link).ljust(10, '0')
                else:
                    prefix = str(street_block.codloc20).ljust(10, '0')
                street_block.nomencla = prefix + subfix
                pbar.update(1)

            ctx.session.commit()
            offset += bulk_size

        pbar.close()

        if loc_error_msg:
            message = 'Existen {}/{} polígonos sin identificar.'.format(
                len(loc_error_msg), total_polygons)
            ctx.report.error(message)
            self._polygons_errors = loc_error_msg

        if loc_warning_msg:
            ctx.report.get_data(self.name)['warning'] = loc_warning_msg

    def mix_sources(self, tmp_educational_institutions, tmp_educational_institutions_mi, ctx):
        # Agregar columnas loc_link y loc_nombre a la tabla si no existen
        with ctx.engine.begin() as connection:
            table_a = tmp_educational_institutions.__table__.name
            table_b = tmp_educational_institutions_mi.__table__.name
            connection.execute(f'ALTER TABLE "{table_a}" ADD COLUMN IF NOT EXISTS dom VARCHAR')
            connection.execute(f'ALTER TABLE "{table_a}" ADD COLUMN IF NOT EXISTS loc_nombre VARCHAR')

            connection.execute(f"""
                        UPDATE "{table_a}" AS a
                        SET dom = b.domicilio,
                            loc_nombre = b.localidad
                        FROM "{table_b}" AS b
                        WHERE a.cue = b.cueanexo
                          AND (b.domicilio IS NOT NULL OR b.localidad IS NOT NULL)
                    """)

    def _run_internal(self, data, ctx):
        tmp_educational_institutions, tmp_educational_institutions_mi = data
        self.mix_sources(tmp_educational_institutions, tmp_educational_institutions_mi, ctx)
        tmp_educational_institutions = utils.automap_table(constants.EDUCATIONAL_INSTITUTIONS_TMP_TABLE, ctx)
        return super()._run_internal(tmp_educational_institutions, ctx)

    def _process_entity(self, institution, cached_session, ctx):

        cue = institution.cue

        categoria = institution.gna
        if not categoria:
            raise ValidationException(
                'No se pudo determinar la categoria de la institución con CUE {}'.format(cue))

        gestion = institution.ges
        if not gestion:
            raise ValidationException(
                'No se pudo determinar la gestion de la institución con CUE {}'.format(cue))

        niveles = institution.nen
        if not niveles:
            raise ValidationException(
                'No se pudo determinar los niveles de la institución con CUE {}'.format(cue))

        domicilio = institution.dom
        if not domicilio:
            raise ValidationException(
                'No se pudo determinar el domicilio de la institución con CUE {}'.format(cue))

        try:
            lat, lon = geometry.get_centroid_coordinates(institution.geom, ctx)  # Corregimos las coordenadas
            geom = from_shape(Point(lon, lat), srid=4326)
        except Exception:
            raise ValidationException(
                'No se pudo obtener la geometría de la institución con CUE {}'.format(cue)
            )

        province = geometry.get_entity_at_point(Province, geom, ctx)
        if not province:
            raise ValidationException(
                'No se pudo determinar la provincia de la institución con CUE {}'.format(cue))

        department = ctx.session.query(Department).filter(
            Department.provincia_id == province.id,
            func.ST_Contains(Department.geometria, geom)
        ).one_or_none()
        if not province:
            raise ValidationException(
                'No se pudo determinar el departamento de la institución con CUE {}'.format(cue))

        local_government = geometry.get_entity_at_point(LocalGovernment, geom, ctx)

        settlements = ctx.session.query(Settlement).filter(
            Settlement.departamento_id == department.id
        ).all()

        target_name = normalize(institution.loc_nombre)

        # Elegimos el mejor match fuzzy
        best_match = None
        best_score = 0
        for s in settlements:
            s_norm = normalize(s.nombre)
            score = fuzz.ratio(target_name, s_norm)
            if score > best_score:
                best_score = score
                best_match = s

        # Definir un umbral para aceptar el match
        if best_score >= 80:
            settlement = best_match
        else:
            settlement = None

        if not settlement:
            raise ValidationException(
                'No se pudo determinar la localidad de la institución con CUE {}'.format(cue))

        return EducationalInstitution(
            id=cue,
            nombre=institution.fna,
            fuente=institution.sag,
            categoria=categoria,
            domicilio=domicilio,
            gestion=gestion,
            niveles=niveles,
            lon=lon,
            lat=lat,
            geometria=geom,
            provincia_id=province.id,
            departamento_id=department.id,
            gobierno_local_id=local_government.id if local_government else None,
            asentamiento_id=settlement.id,
        )