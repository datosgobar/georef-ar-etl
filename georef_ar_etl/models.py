from sqlalchemy import Column, String, Float
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
from . import constants

Base = declarative_base()


class Province(Base):
    __tablename__ = constants.ETL_TABLE_NAME.format('provincias')

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
