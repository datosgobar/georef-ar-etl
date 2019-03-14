import json
from sqlalchemy import Column, String, Float, Integer, ForeignKey
from sqlalchemy.orm import validates
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from geoalchemy2 import Geometry
from .exceptions import ValidationException
from . import constants

Base = declarative_base()


class EntityMixin:
    _id_len = None

    id = Column(String, primary_key=True)
    nombre = Column(String, nullable=False)
    fuente = Column(String, nullable=False)
    categoria = Column(String, nullable=False)

    @validates('id')
    def validate_id(self, _key, value):
        if len(value) != self._id_len:
            raise ValidationException(
                'La longitud del ID debe ser {}.'.format(self._id_len))

        return value


class InProvinceMixin:
    # TODO: testear si funciona bien el on cascade delete
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


class Province(Base, EntityMixin):
    __tablename__ = constants.PROVINCES_ETL_TABLE
    _id_len = constants.PROVINCE_ID_LEN

    # TODO: Agregar país
    nombre_completo = Column(String, nullable=False)
    iso_id = Column(String, nullable=False)
    iso_nombre = Column(String, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOLYGON'), nullable=False)

    def to_dict(self, session):
        return {
            'id': self.id,
            'nombre': self.nombre,
            'nombre_completo': self.nombre_completo,
            'fuente': self.fuente,
            'categoria': self.categoria,
            'iso_id': self.iso_id,
            'iso_nombre': self.iso_nombre,
            'lat': self.lat,
            'lon': self.lon,
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }


class Department(Base, EntityMixin, InProvinceMixin):
    __tablename__ = constants.DEPARTMENTS_ETL_TABLE
    _id_len = constants.DEPARTMENT_ID_LEN

    nombre_completo = Column(String, nullable=False)
    provincia_interseccion = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOLYGON'), nullable=False)

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
            'lat': self.lat,
            'lon': self.lon,
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }


class Municipality(Base, EntityMixin, InProvinceMixin):
    __tablename__ = constants.MUNICIPALITIES_ETL_TABLE
    _id_len = constants.MUNICIPALITY_ID_LEN

    nombre_completo = Column(String, nullable=False)
    provincia_interseccion = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOLYGON'), nullable=False)

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
            'lat': self.lat,
            'lon': self.lon,
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }


class Locality(Base, EntityMixin, InProvinceMixin, InDepartmentMixin):
    __tablename__ = constants.LOCALITIES_ETL_TABLE
    _id_len = constants.LOCALITY_ID_LEN

    municipio_id = Column(
        String,
        ForeignKey(constants.MUNICIPALITIES_ETL_TABLE + '.id',
                   ondelete='cascade'),
        nullable=True
    )
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOINT'), nullable=False)

    @validates('categoria')
    def validate_category(self, _key, category):
        if category not in constants.BAHRA_TYPES.keys():
            raise ValidationException(
                'El valor "{}" no es un tipo BAHRA.'.format(category))

        return category

    def municipio_nombre(self, session):
        if not self.municipio_id:
            return None

        return session.query(Municipality).get(self.municipio_id).nombre

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
            'lat': self.lat,
            'lon': self.lon,
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }


class Street(Base, EntityMixin, InProvinceMixin, InDepartmentMixin):
    __tablename__ = constants.STREETS_ETL_TABLE
    _id_len = constants.STREET_ID_LEN

    inicio_derecha = Column(Integer, nullable=False)
    fin_derecha = Column(Integer, nullable=False)
    inicio_izquierda = Column(Integer, nullable=False)
    fin_izquierda = Column(Integer, nullable=False)
    geometria = Column(Geometry('MULTILINESTRING'), nullable=False)

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

        base['nomenclatura'] = '{}, {}, {}'.format(
            self.nombre,
            self.departamento_nombre(session),
            self.provincia_nombre(session)
        )
        base['altura'] = {
            'inicio': {
                'derecha': self.inicio_derecha,
                'izquierda': self.inicio_izquierda
            },
            'fin': {
                'derecha': self.fin_derecha,
                'izquierda': self.fin_izquierda
            }
        }
        base['geometria'] = json.loads(session.scalar(
            self.geometria.ST_AsGeoJSON()))

        return base


class Intersection(Base):
    __tablename__ = constants.INTERSECTIONS_ETL_TABLE

    id = Column(String, primary_key=True)
    calle_a_id = Column(String, ForeignKey(constants.STREETS_ETL_TABLE + '.id',
                                           ondelete='cascade'),
                        nullable=False)
    calle_b_id = Column(String, ForeignKey(constants.STREETS_ETL_TABLE + '.id',
                                           ondelete='cascade'),
                        nullable=False)
    geometria = Column(Geometry('POINT'), nullable=False)

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
