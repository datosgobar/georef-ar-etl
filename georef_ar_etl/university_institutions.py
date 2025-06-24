from geoalchemy2.shape import from_shape
from shapely import Point
from sqlalchemy.sql import func

from .exceptions import ValidationException
from .loaders import CompositeStepCreateFile, CompositeStepCopyFile
from .process import Process
from .models import Province, Department, UniversityInstitution
from . import extractors, loaders, utils, constants, transformers, geometry


def create_process(config):

    return Process(constants.UNIVERSITY_INSTITUTIONS, [
        utils.CheckDependenciesStep([Province, Department]),
        extractors.DownloadURLStep(
            constants.UNIVERSITY_INSTITUTIONS + '.zip',
            config.get('etl', 'university_institution_url'),
            constants.UNIVERSITY_INSTITUTIONS
        ),
        transformers.ExtractZipStep(internal_path=""),
        loaders.Ogr2ogrStep(
            table_name=constants.UNIVERSITY_INSTITUTIONS_TMP_TABLE, geom_type='Point',
            env={'SHAPE_ENCODING': 'ISO-8859-1'}
        ),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'cue': 'varchar',
            'regimen': 'varchar',
            'id_univers': 'numeric',
            'universida': 'varchar',
            'id_ua': 'numeric',
            'unidad_aca': 'varchar',
            'niveles': 'varchar',
            'domicilio': 'varchar',
            'codigo_pos': 'varchar',
            'localidad': 'varchar',
            'provincia': 'varchar',
            'latitud': 'numeric',
            'longitud': 'numeric',
            'web': 'varchar',
            'geom': 'geometry',
        }),
        UniversityInstitutionsExtractionStep(),
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'university_institutions_target_size'),
            op='ge'),
        CompositeStepCreateFile(UniversityInstitution, 'university_institutions', config),
        CompositeStepCopyFile('university_institutions', config),
    ])

class UniversityInstitutionsExtractionStep(transformers.EntitiesExtractionStep):

    def __init__(self):
        super().__init__('university_institutions_extraction', UniversityInstitution,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='cue')

    def _run_internal(self, tmp_entity, ctx):
        return super()._run_internal(tmp_entity, ctx)

    def _process_entity(self, institution, cached_session, ctx):

        cue = institution.cue

        if not institution.unidad_aca:
            raise ValidationException(
                'No se pudo determinar la unidad academica de la institución con CUE {}'.format(cue))
        unidad_academica = str(institution.unidad_aca).strip()

        if not institution.universida:
            raise ValidationException(
                'No se pudo determinar la universidad de la institución con CUE {}'.format(cue))
        universidad = str(institution.universida).strip()

        nomencla = " - ".join([unidad_academica, universidad])

        fuente = ""
        categoria = ""

        gestion = institution.regimen
        if not gestion:
            raise ValidationException(
                'No se pudo determinar la gestion de la institución con CUE {}'.format(cue))

        niveles = institution.niveles
        if not niveles:
            raise ValidationException(
                'No se pudo determinar los niveles de la institución con CUE {}'.format(cue))

        domicilio = institution.domicilio
        if not domicilio:
            raise ValidationException(
                'No se pudo determinar el domicilio de la institución con CUE {}'.format(cue))

        try:
            lat, lon = geometry.get_centroid_coordinates(institution.geom, ctx)
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

        localidad = institution.localidad

        return UniversityInstitution(
            id=cue,
            nombre=nomencla,
            fuente=fuente,
            categoria=categoria,
            domicilio=domicilio,
            localidad=localidad,
            gestion=gestion,
            niveles=niveles,
            universidad=universidad,
            unidad_academica=unidad_academica,
            lon=lon,
            lat=lat,
            geometria=geom,
            provincia_id=province.id,
            departamento_id=department.id,
        )