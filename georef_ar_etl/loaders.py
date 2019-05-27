import os
import json
import subprocess
import shutil
import csv
from abc import abstractmethod
from datetime import datetime
from datetime import timezone
from sqlalchemy import MetaData
import geojson
from . import constants, utils
from .process import Step, ProcessException
from .json_stream_writer import JSONStreamWriter, JSONArrayPlaceholder

OGR2OGR_CMD = 'ogr2ogr'
OUTPUT_EPSG = 'EPSG:4326'
NDJSON_LINE_SEPARATOR = '\n'


class Ogr2ogrStep(Step):
    def __init__(self, table_name, geom_type, env=None, precision=True,
                 overwrite=True, metadata=None, db_config=None,
                 source_epsg=None):
        super().__init__('ogr2ogr')
        if not shutil.which(OGR2OGR_CMD):
            raise RuntimeError('ogr2ogr is not installed.')

        self._table_name = table_name
        self._geom_type = geom_type
        self._env = env or {}
        self._precision = precision
        self._overwrite = overwrite
        self._metadata = metadata or MetaData()
        self._db_config = db_config
        self._source_epsg = source_epsg

    def _new_env(self):
        env = os.environ.copy()
        for key, value in self._env.items():
            env[key] = value

        return env

    def _run_internal(self, filename, ctx):
        filepath = ctx.fs.getsyspath(filename)
        db_config = self._db_config or ctx.config['db']

        ctx.report.info('Ejecutando ogr2ogr sobre %s.', filepath)
        args = [
            OGR2OGR_CMD, '-f', 'PostgreSQL',
            ('PG:host={host} ' +
             'user={user} ' +
             'password={password} ' +
             'dbname={database}').format(**db_config),
            '-nln', self._table_name,
            '-nlt', self._geom_type,
            '-t_srs', OUTPUT_EPSG
        ]

        if self._source_epsg:
            args.extend([
                '-s_srs', self._source_epsg
            ])

        if os.path.splitext(filename)[1] == '.csv':
            args.extend([
                '-oo', 'KEEP_GEOM_COLUMNS=no',
                '-oo', 'GEOM_POSSIBLE_NAMES=geom',
            ])

        if self._overwrite:
            args.extend([
                '-overwrite',
                '-lco', 'GEOMETRY_NAME=geom'
            ])

        if not self._precision:
            args.extend(['-lco', 'PRECISION=NO'])

        args.append(filepath)
        result = subprocess.run(args, env=self._new_env())

        if result.returncode:
            raise ProcessException(
                'El comando ogr2ogr retornó codigo {}.'.format(
                    result.returncode))

        return utils.automap_table(self._table_name, ctx, self._metadata)

    @property
    def overwrite(self):
        return self._overwrite

    @overwrite.setter
    def overwrite(self, val):
        self._overwrite = val


class CreateOutputFileStep(Step):
    def __init__(self, name, table, *filename_parts):
        super().__init__(name, reads_input=False)
        self._table = table
        self._filename = os.path.join(*filename_parts)

    @abstractmethod
    def _write_file(self, query, count, cached_session, ctx):
        pass

    def _run_internal(self, data, ctx):
        bulk_size = ctx.config.getint('etl', 'bulk_size')
        query = ctx.session.query(self._table).yield_per(bulk_size)
        count = query.count()
        cached_session = ctx.cached_session()

        dirname = os.path.dirname(self._filename)
        if dirname:
            utils.ensure_dir(dirname, ctx.fs)

        self._write_file(query, count, cached_session, ctx)
        return self._filename


class CreateJSONFileStep(CreateOutputFileStep):
    def __init__(self, table, *filename_parts):
        super().__init__('create_json_file', table, *filename_parts)

    def _write_file(self, query, count, cached_session, ctx):
        entity_name = os.path.basename(os.path.splitext(self._filename)[0])

        contents = {
            'cantidad': count,
            'total': count,
            'inicio': 0,
            'parametros': {},
            entity_name: JSONArrayPlaceholder()
        }

        ctx.report.info('Transformando entidades a JSON...')

        with ctx.fs.open(self._filename, 'w') as f:
            stream_writer = JSONStreamWriter(f, template=contents,
                                             ensure_ascii=False,
                                             separators=(',', ':'))

            with stream_writer:
                for entity in utils.pbar(query, ctx, total=count):
                    entity_dict = entity.to_dict(cached_session)
                    del entity_dict['geometria']

                    stream_writer.append(entity_dict)


class CreateNDJSONFileStep(CreateOutputFileStep):
    def __init__(self, table, *filename_parts):
        super().__init__('create_ndjson_file', table, *filename_parts)

    def _write_json_line(self, obj, f):
        json.dump(obj, f, ensure_ascii=False, separators=(',', ':'))
        f.write(NDJSON_LINE_SEPARATOR)

    def _write_file(self, query, count, cached_session, ctx):
        now = datetime.now(timezone.utc)
        metadata = {
            'fecha_creacion': str(now),
            'timestamp': int(now.timestamp()),
            'version': constants.ETL_VERSION,
            'cantidad': count
        }

        ctx.report.info('Transformando entidades a NDJSON...')

        with ctx.fs.open(self._filename, 'w') as f:
            self._write_json_line(metadata, f)

            for entity in utils.pbar(query, ctx, total=count):
                self._write_json_line(entity.to_dict(cached_session), f)


class CreateGeoJSONFileStep(CreateOutputFileStep):
    def __init__(self, table, *filename_parts):
        super().__init__('create_geojson_file', table, *filename_parts)

    def _write_file(self, query, count, cached_session, ctx):
        collection = geojson.FeatureCollection(JSONArrayPlaceholder())
        ctx.report.info('Transformando entidades a GeoJSON...')

        with ctx.fs.open(self._filename, 'w') as f:
            stream_writer = JSONStreamWriter(f, template=collection,
                                             ensure_ascii=False)

            with stream_writer as w:
                for entity in utils.pbar(query, ctx, total=count):
                    entity_dict = entity.to_dict(cached_session)
                    del entity_dict['geometria']

                    centroid = entity_dict.pop('centroide')
                    point = geojson.Point((centroid['lon'], centroid['lat']))

                    feature = geojson.Feature(geometry=point,
                                              properties=entity_dict)
                    w.append(feature)


def flatten_dict(d, max_depth=3, sep='_'):
    """Aplana un diccionario recursivamente. Modifica el diccionario original.
    Lanza un RuntimeError si no se pudo aplanar el diccionario
    con el número especificado de profundidad.

    Args:
        d (dict): Diccionario a aplanar.
        max_depth (int): Profundidad máxima a alcanzar.

    Raises:
        RuntimeError: cuando se alcanza la profundidad máxima. Se agrega esta
            medida de seguridad en caso de tener un diccionario demasiado
            profundo, o un diccionario con referencias cíclicas.

    """
    if max_depth <= 0:
        raise RuntimeError("Maximum depth reached")

    for key in list(d.keys()):
        v = d[key]
        if isinstance(v, dict):
            flatten_dict(v, max_depth - 1, sep)

            for subkey, subval in v.items():
                flat_key = sep.join([key, subkey])
                d[flat_key] = subval

            del d[key]


class CreateCSVFileStep(CreateOutputFileStep):
    def __init__(self, table, *filename_parts):
        super().__init__('create_csv_file', table, *filename_parts)

    def _write_file(self, query, count, cached_session, ctx):
        first = ctx.session.query(self._table).first().to_dict(ctx.session)
        del first['geometria']
        flatten_dict(first)
        fields = sorted(first.keys())

        dirname = os.path.dirname(self._filename)
        if dirname:
            utils.ensure_dir(dirname, ctx.fs)

        ctx.report.info('Transformando entidades a CSV...')
        with ctx.fs.open(self._filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fields,
                                    quoting=csv.QUOTE_NONNUMERIC)
            writer.writeheader()

            for entity in utils.pbar(query, ctx, total=count):
                entity_dict = entity.to_dict(cached_session)
                del entity_dict['geometria']
                flatten_dict(entity_dict)

                writer.writerow(entity_dict)
