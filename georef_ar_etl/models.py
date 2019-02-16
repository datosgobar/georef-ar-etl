from sqlalchemy import Column, String, Float, Integer, ForeignKey
from sqlalchemy.orm import validates
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from geoalchemy2 import Geometry
from . import constants

Base = declarative_base()


class ValidationException(Exception):
    pass


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


class InDepartmentMixin:
    @declared_attr
    def departamento_id(cls):
        return Column(
            String,
            ForeignKey(constants.DEPARTMENTS_ETL_TABLE + '.id',
                       ondelete='cascade'),
            nullable=False
        )


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


class Department(Base, EntityMixin, InProvinceMixin):
    __tablename__ = constants.DEPARTMENTS_ETL_TABLE
    _id_len = constants.DEPARTMENT_ID_LEN

    nombre_completo = Column(String, nullable=False)
    provincia_interseccion = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOLYGON'), nullable=False)


class Municipality(Base, EntityMixin, InProvinceMixin):
    __tablename__ = constants.MUNICIPALITIES_ETL_TABLE
    _id_len = constants.MUNICIPALITY_ID_LEN

    nombre_completo = Column(String, nullable=False)
    provincia_interseccion = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOLYGON'), nullable=False)


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


class Street(Base, EntityMixin, InProvinceMixin, InDepartmentMixin):
    __tablename__ = constants.STREETS_ETL_TABLE
    _id_len = constants.STREET_ID_LEN

    inicio_derecha = Column(Integer, nullable=False)
    fin_derecha = Column(Integer, nullable=False)
    inicio_izquierda = Column(Integer, nullable=False)
    fin_izquierda = Column(Integer, nullable=False)
    geometria = Column(Geometry('MULTILINESTRING'), nullable=False)


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
