import re
import unicodedata

from geoalchemy2.shape import from_shape
from shapely import Point
from sqlalchemy.sql import func

from .exceptions import ValidationException
from .loaders import CompositeStepCreateFile, CompositeStepCopyFile
from .process import Process, CompositeStep, StepSequence
from .models import Province, Department, EducationalInstitution, LocalGovernment, Settlement
from . import extractors, loaders, utils, constants, transformers, geometry, patch


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

    def _patch_tmp_entities(self, tmp_entities, ctx):

        def fix_source(row):
            row.sag = str(row.sag).replace('? DIE', '- DIEE')

        patch.apply_fn(tmp_entities, fix_source, ctx, tmp_entities.sag.like("%? DIE"))

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
        if not department:
            raise ValidationException(
                'No se pudo determinar el departamento de la institución con CUE {}'.format(cue))

        localidad = institution.loc_nombre

        return EducationalInstitution(
            id=cue,
            nombre=institution.fna,
            fuente=institution.sag,
            categoria=categoria,
            domicilio=domicilio,
            localidad=localidad,
            gestion=gestion,
            niveles=niveles,
            lon=lon,
            lat=lat,
            geometria=geom,
            provincia_id=province.id,
            departamento_id=department.id,
        )