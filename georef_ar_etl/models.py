from sqlalchemy import Column, String, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

RAW_TABLE_NAME = 'raw_{}'
ETL_TABLE_NAME = 'georef_{}'
Base = declarative_base()


class ProvinceRaw(Base):
    __tablename__ = RAW_TABLE_NAME.format('provincias')

    ogc_fid = Column(Integer, primary_key=True)
    fna = Column(String)
    gna = Column(String)
    nam = Column(String)
    sag = Column(String)
    in1 = Column(String)
    geom = Column(Geometry('MULTIPOLYGON'))


class Province(Base):
    __tablename__ = ETL_TABLE_NAME.format('provincias')

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
