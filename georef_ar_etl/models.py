# pylint: disable=no-self-argument
import json
from sqlalchemy import Column, String, Float, Integer, ForeignKey
from sqlalchemy.orm import validates, relationship
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from geoalchemy2 import Geometry
from .exceptions import ValidationException
from . import constants

SRID = 4326

Base = declarative_base()


def get_relationship(table, cascade='all, delete', passive_deletes=True,
                     **kwargs):
    return relationship(table, cascade=cascade,
                        passive_deletes=passive_deletes,
                        **kwargs)


class EntityMixin:
    _id_len = None

    id = Column(String, primary_key=True)
    nombre = Column(String, nullable=False)
    fuente = Column(String, nullable=False)

    @validates('id')
    def validate_id(self, _key, value):
        if len(value) != self._id_len:
            raise ValidationException(
                'La longitud del ID debe ser {}.'.format(self._id_len))

        return value

    @validates('nombre')
    def validate_name(self, _key, name):
        if not name:
            raise ValidationException('El nombre no puede ser vacío.')

        return name


class InProvinceMixin:
    @declared_attr
    def provincia_id(cls):
        return Column(
            String, ForeignKey(constants.PROVINCES_ETL_TABLE + '.id',
                               ondelete='cascade'),
            nullable=False
        )

    def provincia_nombre(self, session):
        # Nombre de método en castellano para mantener consistencia con los
        # demás campos de los modelos
        return session.query(Province).get(self.provincia_id).nombre


class InNullableDepartmentMixin:
    @declared_attr
    def departamento_id(cls):
        return Column(
            String,
            ForeignKey(constants.DEPARTMENTS_ETL_TABLE + '.id',
                       ondelete='cascade')
        )

    @validates('departamento_id')
    def validate_department_id(self, _key, value):
        # Si se epecificó el departamento 02000, almacenar NULL (ver comentario
        # en constants.py).
        return value if value != constants.CABA_VIRTUAL_DEPARTMENT_ID else None

    def departamento_nombre(self, session):
        # Nombre de método en castellano para mantener consistencia con los
        # demás campos de los modelos
        if not self.departamento_id:
            return None

        return session.query(Department).get(self.departamento_id).nombre


class InDepartmentMixin:
    @declared_attr
    def departamento_id(cls):
        return Column(
            String,
            ForeignKey(constants.DEPARTMENTS_ETL_TABLE + '.id',
                       ondelete='cascade'),
            nullable=False
        )

    def departamento_nombre(self, session):
        # Nombre de método en castellano para mantener consistencia con los
        # demás campos de los modelos
        return session.query(Department).get(self.departamento_id).nombre


class InNullableMunicipalityMixin:
    @declared_attr
    def municipio_id(cls):
        return Column(
            String,
            ForeignKey(constants.MUNICIPALITIES_ETL_TABLE + '.id',
                       ondelete='cascade'),
            nullable=True
        )

    def municipio_nombre(self, session):
        # Nombre de método en castellano para mantener consistencia con los
        # demás campos de los modelos
        if not self.municipio_id:
            return None

        return session.query(Municipality).get(self.municipio_id).nombre


class Province(Base, EntityMixin):
    __tablename__ = constants.PROVINCES_ETL_TABLE
    _id_len = constants.PROVINCE_ID_LEN

    categoria = Column(String, nullable=False)
    nombre_completo = Column(String, nullable=False)
    iso_id = Column(String, nullable=False)
    iso_nombre = Column(String, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOLYGON', srid=SRID), nullable=False)

    departamentos = get_relationship('Department')
    municipios = get_relationship('Municipality')
    localidades = get_relationship('Locality')
    calles = get_relationship('Street')

    def to_dict(self, session):
        return {
            'id': self.id,
            'nombre': self.nombre,
            'nombre_completo': self.nombre_completo,
            'fuente': self.fuente,
            'categoria': self.categoria,
            'centroide': {
                'lon': self.lon,
                'lat': self.lat
            },
            'iso_id': self.iso_id,
            'iso_nombre': self.iso_nombre,
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }


class Department(Base, EntityMixin, InProvinceMixin):
    __tablename__ = constants.DEPARTMENTS_ETL_TABLE
    _id_len = constants.DEPARTMENT_ID_LEN

    categoria = Column(String, nullable=False)
    nombre_completo = Column(String, nullable=False)
    provincia_interseccion = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOLYGON', srid=SRID), nullable=False)

    localidades = get_relationship('Locality')
    calles = get_relationship('Street')

    def to_dict(self, session):
        return {
            'id': self.id,
            'nombre': self.nombre,
            'nombre_completo': self.nombre_completo,
            'provincia': {
                'id': self.provincia_id,
                'nombre': self.provincia_nombre(session),
                'interseccion': self.provincia_interseccion,
            },
            'fuente': self.fuente,
            'categoria': self.categoria,
            'centroide': {
                'lon': self.lon,
                'lat': self.lat
            },
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }


class Municipality(Base, EntityMixin, InProvinceMixin):
    __tablename__ = constants.MUNICIPALITIES_ETL_TABLE
    _id_len = constants.MUNICIPALITY_ID_LEN

    categoria = Column(String, nullable=False)
    nombre_completo = Column(String, nullable=False)
    provincia_interseccion = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOLYGON', srid=SRID), nullable=False)

    localidades = get_relationship('Locality')

    def to_dict(self, session):
        return {
            'id': self.id,
            'nombre': self.nombre,
            'nombre_completo': self.nombre_completo,
            'provincia': {
                'id': self.provincia_id,
                'nombre': self.provincia_nombre(session),
                'interseccion': self.provincia_interseccion,
            },
            'fuente': self.fuente,
            'categoria': self.categoria,
            'centroide': {
                'lon': self.lon,
                'lat': self.lat
            },
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }


class Locality(Base, EntityMixin, InProvinceMixin, InNullableDepartmentMixin,
               InNullableMunicipalityMixin):
    __tablename__ = constants.LOCALITIES_ETL_TABLE
    _id_len = constants.LOCALITY_ID_LEN

    categoria = Column(String, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOINT', srid=SRID), nullable=False)

    @validates('categoria')
    def validate_category(self, _key, category):
        if category not in constants.LOCALITY_TYPES:
            raise ValidationException(
                'El valor "{}" no es un tipo de localidad válido.'.format(
                    category))

        return category

    def to_dict(self, session):
        return {
            'id': self.id,
            'nombre': self.nombre,
            'fuente': self.fuente,
            'provincia': {
                'id': self.provincia_id,
                'nombre': self.provincia_nombre(session)
            },
            'departamento': {
                'id': self.departamento_id,
                'nombre': self.departamento_nombre(session)
            },
            'municipio': {
                'id': self.municipio_id,
                'nombre': self.municipio_nombre(session)
            },
            'categoria': constants.LOCALITY_TYPES[self.categoria],
            'centroide': {
                'lon': self.lon,
                'lat': self.lat
            },
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }


class CensusLocality(Base, EntityMixin, InProvinceMixin,
                     InNullableDepartmentMixin, InNullableMunicipalityMixin):
    __tablename__ = constants.CENSUS_LOCALITIES_ETL_TABLE
    _id_len = constants.CENSUS_LOCALITY_ID_LEN

    categoria = Column(String, nullable=True)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('POINT', srid=SRID), nullable=False)

    @validates('categoria')
    def validate_category(self, _key, category):
        if category not in constants.CENSUS_LOCALITY_ADMIN_FUNCTIONS.values():
            raise ValidationException(
                'Función de localidad censal inválida: {}'.format(category))

        return category

    def to_dict(self, session):
        return {
            'id': self.id,
            'nombre': self.nombre,
            'fuente': self.fuente,
            'provincia': {
                'id': self.provincia_id,
                'nombre': self.provincia_nombre(session)
            },
            'departamento': {
                'id': self.departamento_id,
                'nombre': self.departamento_nombre(session)
            },
            'municipio': {
                'id': self.municipio_id,
                'nombre': self.municipio_nombre(session)
            },
            'categoria': self.categoria,
            'centroide': {
                'lon': self.lon,
                'lat': self.lat
            },
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }


class DoorNumberedMixin:
    inicio_derecha = Column(Integer, nullable=False)
    fin_derecha = Column(Integer, nullable=False)
    inicio_izquierda = Column(Integer, nullable=False)
    fin_izquierda = Column(Integer, nullable=False)

    def door_numbers_dict(self):
        return {
            'inicio': {
                'derecha': self.inicio_derecha,
                'izquierda': self.inicio_izquierda
            },
            'fin': {
                'derecha': self.fin_derecha,
                'izquierda': self.fin_izquierda
            }
        }


class Street(Base, EntityMixin, InProvinceMixin, InDepartmentMixin,
             DoorNumberedMixin):
    __tablename__ = constants.STREETS_ETL_TABLE
    _id_len = constants.STREET_ID_LEN

    categoria = Column(String, nullable=False)
    geometria = Column(Geometry('MULTILINESTRING', srid=SRID), nullable=False)

    intersecciones_a = get_relationship('Intersection',
                                        foreign_keys='Intersection.calle_a_id')
    intersecciones_b = get_relationship('Intersection',
                                        foreign_keys='Intersection.calle_b_id')
    cuadras = get_relationship('StreetBlock')

    def to_dict_simple(self, session):
        return {
            'id': self.id,
            'nombre': self.nombre,
            'fuente': self.fuente,
            'provincia': {
                'id': self.provincia_id,
                'nombre': self.provincia_nombre(session)
            },
            'departamento': {
                'id': self.departamento_id,
                'nombre': self.departamento_nombre(session)
            },
            'categoria': self.categoria
        }

    def to_dict(self, session):
        base = self.to_dict_simple(session)

        base['altura'] = self.door_numbers_dict()
        base['geometria'] = json.loads(session.scalar(
            self.geometria.ST_AsGeoJSON()))

        return base


class StreetBlock(Base, DoorNumberedMixin):
    __tablename__ = constants.STREET_BLOCKS_ETL_TABLE

    id = Column(String, primary_key=True)
    calle_id = Column(String, ForeignKey(constants.STREETS_ETL_TABLE + '.id',
                                         ondelete='cascade'),
                      nullable=False)
    geometria = Column(Geometry('MULTILINESTRING', srid=SRID), nullable=False)

    def to_dict(self, session):
        street = session.query(Street).get(self.calle_id)

        return {
            'id': self.id,
            'calle': street.to_dict_simple(session),
            'altura': self.door_numbers_dict(),
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }


class Intersection(Base):
    __tablename__ = constants.INTERSECTIONS_ETL_TABLE

    id = Column(String, primary_key=True)
    calle_a_id = Column(String, ForeignKey(constants.STREETS_ETL_TABLE + '.id',
                                           ondelete='cascade'),
                        nullable=False)
    calle_b_id = Column(String, ForeignKey(constants.STREETS_ETL_TABLE + '.id',
                                           ondelete='cascade'),
                        nullable=False)
    geometria = Column(Geometry('POINT', srid=SRID), nullable=False)

    def to_dict(self, session):
        street_a = session.query(Street).get(self.calle_a_id)
        street_b = session.query(Street).get(self.calle_b_id)

        return {
            'id': self.id,
            'calle_a': street_a.to_dict_simple(session),
            'calle_b': street_b.to_dict_simple(session),
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }
