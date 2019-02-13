from sqlalchemy import Column, String, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
from . import constants

Base = declarative_base()


class Province(Base):
    __tablename__ = constants.PROVINCES_ETL_TABLE

    id = Column(String, primary_key=True)
    nombre = Column(String, nullable=False)
    nombre_completo = Column(String, nullable=False)
    iso_id = Column(String, nullable=False)
    iso_nombre = Column(String, nullable=False)
    categoria = Column(String, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    fuente = Column(String, nullable=False)
    geometria = Column(Geometry('MULTIPOLYGON'), nullable=False)
    # TODO: Agregar país


class Department(Base):
    __tablename__ = constants.DEPARTMENTS_ETL_TABLE

    id = Column(String, primary_key=True)
    nombre = Column(String, nullable=False)
    nombre_completo = Column(String, nullable=False)
    categoria = Column(String, nullable=False)
    # TODO: Testear si el on delete cascade funciona bien
    provincia_id = Column(String,
                          ForeignKey(constants.PROVINCES_ETL_TABLE + '.id',
                                     ondelete='cascade'),
                          nullable=False)
    provincia_interseccion = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    fuente = Column(String, nullable=False)
    geometria = Column(Geometry('MULTIPOLYGON'), nullable=False)
