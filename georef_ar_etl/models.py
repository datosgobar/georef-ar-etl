"""Módulo 'models' de georef-ar-etl.

Define los modelos utilizados durante el proceso de ETL, que representan
distintas entidades geográficas.

"""

# pylint: disable=no-self-argument
import json
from sqlalchemy import Column, String, Float, Integer, ForeignKey
from sqlalchemy.orm import validates, relationship
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from geoalchemy2 import Geometry
from .exceptions import ValidationException
from . import constants

SRID = 4326
"""int: Código SRID del sistema de coordenadas WGS84.
"""

Base = declarative_base()


def get_relationship(model_name, cascade='all, delete', passive_deletes=True,
                     **kwargs):
    """Helper para facilitar la creación de relaciones entre tablas de
    SQLAlchemy. Las relaciones permiten acceder a entidades que hagan
    referencia a otras mediante una Foreign Key. Por ejemplo, se puede
    tomar un objeto Province y acceder a su propiedad 'localidades' para
    obtener automáticamente todas las localidades de esa provincia.

    Args:
        model_name (str): Nombre del modelo al cual relacionar.
        cascade (str): Parámetros para CASCADE de SQL.
        passive_deletes (bool): No cargar modelos cuando se elimina el objeto
            al que referencian.
        kwargs: Parámetros extra para relationship().

    Returns:
        sqlalchemy.orm.relationships.RelationshipProperty: Relación.

    """
    return relationship(model_name, cascade=cascade,
                        passive_deletes=passive_deletes,
                        **kwargs)


class EntityMixin:
    """Modelo base para todos los modelos utilizados en el ETL.

    Attributes:
        _id_len (int): Longitud que deberían tener los IDs utilizados por esta
            entidad. Los IDs en sí son de tipo 'str'.
        id (str): ID de la entidad.
        nombre (str): Nombre de la entidad.
        fuente (str): Fuente de datos de la entidad.
        categoria (str): Categoría de la entidad. La definición de categoría
            varía de acuerdo a cada tipo de entidad geográfica.

    """

    _id_len = None

    id = Column(String, primary_key=True)
    nombre = Column(String, nullable=False)
    fuente = Column(String, nullable=False)
    categoria = Column(String, nullable=False)

    @validates('id')
    def validate_id(self, _key, value):
        """Valida la longitud del valor a utilizar como ID de la entidad.

        Args:
            _key (str): Campo de ID ('id').
            value (str): Valor a establecer como ID.

        Returns:
            str: Valor del campo validado.

        """
        if len(value) != self._id_len:
            raise ValidationException(
                'La longitud del ID debe ser {}.'.format(self._id_len))

        return value

    @validates('nombre')
    def validate_name(self, _key, name):
        """Valida el valor a utilizar como nombre de la entidad.

        Args:
            _key (str): Campo de nombre ('nombre').
            value (str): Valor a establecer como nombre.

        Returns:
            str: Valor del campo validado.

        """
        if not name:
            raise ValidationException('El nombre no puede ser vacío.')

        return name

    def to_dict(self, session):
        """Retorna una representación de la entidad como diccionario 'dict'.
        Los campos compuestos (que contienen varios valores) se representan
        también como diccionarios de varios valores. El resultado puede ser
        utilizado para serializar fácilmente la entidad a formatos como JSON.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            dict: Entidad en forma de diccionario.

        """
        raise NotImplementedError()


class InProvinceMixin:
    """Define atributos y funciones de entidades que están contenidas dentro de
    una provincia, o pertenecen a una.

    Attributes:
        provincia_id (str): ID de la provincia referenciada.

    """

    @declared_attr
    def provincia_id(cls):
        return Column(
            String, ForeignKey(constants.PROVINCES_ETL_TABLE + '.id',
                               ondelete='cascade'),
            nullable=False
        )

    def provincia_nombre(self, session):
        """Retorna el nombre de la provincia a la cual pertenece la entidad.
        El nombre de método está en castellano para mantener consistencia con
        los demás campos de los modelos.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            str: Nombre de la provincia.

        """
        return session.query(Province).get(self.provincia_id).nombre


class InNullableDepartmentMixin:
    """Define atributos y funciones de entidades que están opcionalmente
    contenidas dentro de un departamento, o pertenecen a uno (o no).

    Attributes:
        departamento_id (str): ID del departamento referenciado, o 'None'.

    """

    @declared_attr
    def departamento_id(cls):
        return Column(
            String,
            ForeignKey(constants.DEPARTMENTS_ETL_TABLE + '.id',
                       ondelete='cascade')
        )

    @validates('departamento_id')
    def validate_department_id(self, _key, value):
        """Valida el valor a utilizar como ID de departamento.

        Args:
            _key (str): Campo de nombre ('departamento_id').
            value (str): Valor a establecer como ID.

        Returns:
            str: Valor del campo validado.

        """
        # Si se epecificó el departamento 02000, almacenar NULL (ver comentario
        # en constants.py).
        return value if value != constants.CABA_VIRTUAL_DEPARTMENT_ID else None

    def departamento_nombre(self, session):
        """Retorna el nombre del departamento a la cual pertenece la entidad.
        El nombre de método está en castellano para mantener consistencia con
        los demás campos de los modelos.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            str: Nombre del departamento.

        """
        if not self.departamento_id:
            return None

        return session.query(Department).get(self.departamento_id).nombre


class InDepartmentMixin:
    """Define atributos y funciones de entidades que están contenidas dentro de
    un departamento, o pertenecen a uno.

    Attributes:
        departamento_id (str): ID del departamento referenciado.

    """

    @declared_attr
    def departamento_id(cls):
        return Column(
            String,
            ForeignKey(constants.DEPARTMENTS_ETL_TABLE + '.id',
                       ondelete='cascade'),
            nullable=False
        )

    def departamento_nombre(self, session):
        """Retorna el nombre del departamento a la cual pertenece la entidad.
        El nombre de método está en castellano para mantener consistencia con
        los demás campos de los modelos.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            str: Nombre del departamento.

        """
        return session.query(Department).get(self.departamento_id).nombre


class InNullableMunicipalityMixin:
    """Define atributos y funciones de entidades que están opcionalmente
    contenidas dentro de un municipio, o pertenecen a uno (o no).

    Attributes:
        municipio_id (str): ID del municipio referenciado, o 'None'.

    """

    @declared_attr
    def municipio_id(cls):
        return Column(
            String,
            ForeignKey(constants.MUNICIPALITIES_ETL_TABLE + '.id',
                       ondelete='cascade'),
            nullable=True
        )

    def municipio_nombre(self, session):
        """Retorna el nombre del municipio a la cual pertenece la entidad. El
        nombre de método está en castellano para mantener consistencia con los
        demás campos de los modelos.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            str: Nombre del municipio.

        """
        if not self.municipio_id:
            return None

        return session.query(Municipality).get(self.municipio_id).nombre


class InCensusLocalityMixin:
    """Define atributos y funciones de entidades que pertececen a una localidad
    censal.

    Attributes:
        localidad_censal_id (str): ID de la localidad referenciada.

    """

    @declared_attr
    def localidad_censal_id(cls):
        return Column(
            String,
            ForeignKey(constants.CENSUS_LOCALITIES_ETL_TABLE + '.id',
                       ondelete='cascade'),
            # Facilitar la migración de la DB haciendo que el campo sea
            # nullable en la base.
            nullable=True
        )

    @validates('localidad_censal_id')
    def validate_census_locality_id(self, _key, value):
        """Valida el valor a utilizar como ID de localidad censal.

        Args:
            _key (str): Campo de nombre ('localidad_censal_id').
            value (str): Valor a establecer como ID.

        Returns:
            str: Valor del campo validado.

        """
        if not value:
            raise ValidationException('La localidad censal no puede ser nula.')

        return value

    def localidad_censal_nombre(self, session):
        """Retorna el nombre de la localidad censal a la cual pertenece la
        entidad. El nombre de método está en castellano para mantener
        consistencia con los demás campos de los modelos.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            str: Nombre de la localidad censal.

        """
        return session.query(CensusLocality).get(
            self.localidad_censal_id).nombre


class InNullableCensusLocalityMixin:
    """Define atributos y funciones de entidades que pertececen opcionalmente a
    una localidal censal.

    Attributes:
        localidad_censal_id (str): ID de la localidad referenciada, o 'None'.

    """

    @declared_attr
    def localidad_censal_id(cls):
        return Column(
            String,
            ForeignKey(constants.CENSUS_LOCALITIES_ETL_TABLE + '.id',
                       ondelete='cascade'),
            nullable=True
        )

    def localidad_censal_nombre(self, session):
        """Retorna el nombre de la localidad censal a la cual pertenece la
        entidad. El nombre de método está en castellano para mantener
        consistencia con los demás campos de los modelos.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            str: Nombre de la localidad censal.

        """
        if not self.localidad_censal_id:
            return None

        return session.query(CensusLocality).get(
            self.localidad_censal_id).nombre


class Province(Base, EntityMixin):
    """Modelo utilizado para representar provincias.

    Attributes:
        __tablename__ (str): Nombre de la tabla.
        _id_len (int): Longitud de los IDs.
        nombre_completo (str): Nombre completo de la provincia, es decir, la
            categoría de la provincia junto a su nombre: 'Provincia de Jujuy'.
        iso_id (str): Código ISO 3166-2 de la provincia.
        iso_nombre (str): Nombre ISO 3166-2 de la provincia.
        lon (float): Longitud del centroide de la provincia.
        lat (float): Latitud del centroide de la provincia.
        geometría (geoalchemy2.Geometry): Geometría de la provincia.

    """

    __tablename__ = constants.PROVINCES_ETL_TABLE
    _id_len = constants.PROVINCE_ID_LEN

    nombre_completo = Column(String, nullable=False)
    iso_id = Column(String, nullable=False)
    iso_nombre = Column(String, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOLYGON', srid=SRID), nullable=False)

    departamentos = get_relationship('Department')
    municipios = get_relationship('Municipality')
    localidades_censales = get_relationship('CensusLocality')
    asentamientos = get_relationship('Settlement')
    localidades = get_relationship('Locality')
    calles = get_relationship('Street')

    def to_dict(self, session):
        """Retorna una representación de la entidad como diccionario 'dict'.
        Los campos compuestos (que contienen varios valores) se representan
        también como diccionarios de varios valores. El resultado puede ser
        utilizado para serializar fácilmente la entidad a formatos como JSON.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            dict: Entidad en forma de diccionario.

        """
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
    """Modelo utilizado para representar departamentos.

    Attributes:
        __tablename__ (str): Nombre de la tabla.
        _id_len (int): Longitud de los IDs.
        nombre_completo (str): Nombre completo del departamento, es decir, la
            categoría del departamento junto a su nombre: 'Partido de San
            Antonio de Areco'.
        provincia_interseccion (float): Porcentaje del área de la provincia que
            ocupa el departamento (entre 0 y 1).
        lon (float): Longitud del centroide del departamento.
        lat (float): Latitud del centroide del departamento.
        geometría (geoalchemy2.Geometry): Geometría del departamento.

    """

    __tablename__ = constants.DEPARTMENTS_ETL_TABLE
    _id_len = constants.DEPARTMENT_ID_LEN

    nombre_completo = Column(String, nullable=False)
    provincia_interseccion = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOLYGON', srid=SRID), nullable=False)

    localidades_censales = get_relationship('CensusLocality')
    asentamientos = get_relationship('Settlement')
    localidades = get_relationship('Locality')
    calles = get_relationship('Street')

    def to_dict(self, session):
        """Retorna una representación de la entidad como diccionario 'dict'.
        Los campos compuestos (que contienen varios valores) se representan
        también como diccionarios de varios valores. El resultado puede ser
        utilizado para serializar fácilmente la entidad a formatos como JSON.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            dict: Entidad en forma de diccionario.

        """
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
    """Modelo utilizado para representar departamentos.

    Attributes:
        __tablename__ (str): Nombre de la tabla.
        _id_len (int): Longitud de los IDs.
        nombre_completo (str): Nombre completo del municipio, es decir, la
            categoría del municipio junto a su nombre: 'Comuna Salto Grande'.
        provincia_interseccion (float): Porcentaje del área de la provincia que
            ocupa el municipio (entre 0 y 1).
        lon (float): Longitud del centroide del municipio.
        lat (float): Latitud del centroide del municipio.
        geometría (geoalchemy2.Geometry): Geometría del municipio.

    """

    __tablename__ = constants.MUNICIPALITIES_ETL_TABLE
    _id_len = constants.MUNICIPALITY_ID_LEN

    nombre_completo = Column(String, nullable=False)
    provincia_interseccion = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOLYGON', srid=SRID), nullable=False)

    localidades_censales = get_relationship('CensusLocality')
    asentamientos = get_relationship('Settlement')
    localidades = get_relationship('Locality')

    def to_dict(self, session):
        """Retorna una representación de la entidad como diccionario 'dict'.
        Los campos compuestos (que contienen varios valores) se representan
        también como diccionarios de varios valores. El resultado puede ser
        utilizado para serializar fácilmente la entidad a formatos como JSON.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            dict: Entidad en forma de diccionario.

        """
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


class SettlementMixin(EntityMixin, InProvinceMixin, InNullableDepartmentMixin,
                      InNullableMunicipalityMixin):
    """Define atributos y métodos para modelos de entidades tomadas de la base
    BAHRA.

    Attributes:
        lon (float): Longitud del centroide de la entidad.
        lat (float): Latitud del centroide de la entidad.
        geometría (geoalchemy2.Geometry): Geometría de la entidad.

    """

    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    geometria = Column(Geometry('MULTIPOINT', srid=SRID), nullable=False)

    def to_dict(self, session):
        """Retorna una representación de la entidad como diccionario 'dict'.
        Los campos compuestos (que contienen varios valores) se representan
        también como diccionarios de varios valores. El resultado puede ser
        utilizado para serializar fácilmente la entidad a formatos como JSON.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            dict: Entidad en forma de diccionario.

        """
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
            'localidad_censal': {
                'id': self.localidad_censal_id,
                'nombre': self.localidad_censal_nombre(session)
            },
            'categoria': self.categoria,
            'centroide': {
                'lon': self.lon,
                'lat': self.lat
            },
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }

    def validate_id(self, _key, value):
        # TODO: Verificar las consecuencias de sobreescribir esta validación
        return value


class Settlement(Base, SettlementMixin, InNullableCensusLocalityMixin):
    """Modelo utilizado para representar asentamientos. Los asentamientos son
    las entidades tomadas de la base BAHRA.

    Attributes:
        __tablename__ (str): Nombre de la tabla.
        _id_len (int): Longitud de los IDs.

    """

    __tablename__ = constants.SETTLEMENTS_ETL_TABLE
    _id_len = constants.SETTLEMENT_ID_LEN

    @validates('categoria')
    def validate_category(self, _key, category):
        """Valida el valor a utilizar como categoría del asentamiento.

        Args:
            _key (str): Campo de nombre ('categoria').
            value (str): Valor a establecer como categoría.

        Returns:
            str: Valor del campo validado.

        """
        if category not in list(constants.BAHRA_TYPES.values()):
            raise ValidationException(
                'El valor "{}" no es un tipo de asentamiento válido.'.format(
                    category))

        return category


class Locality(Base, SettlementMixin, InCensusLocalityMixin):
    """Modelo utilizado para representar localidades. Las localidades son
    entidades tomadas de la base BAHRA.

    Notar que todas las entidades de BAHRA son asentamientos, pero solo algunas
    son localidades.

    Attributes:
        __tablename__ (str): Nombre de la tabla.
        _id_len (int): Longitud de los IDs.

    """

    __tablename__ = constants.LOCALITIES_ETL_TABLE
    _id_len = constants.LOCALITY_ID_LEN

    @validates('categoria')
    def validate_category(self, _key, category):
        """Valida el valor a utilizar como categoría de la localidad.

        Args:
            _key (str): Campo de nombre ('categoria').
            value (str): Valor a establecer como categoría.

        Returns:
            str: Valor del campo validado.

        """
        if category not in constants.LOCALITY_TYPES:
            raise ValidationException(
                'El valor "{}" no es un tipo de localidad válido.'.format(
                    category))

        return category


class CensusLocality(Base, EntityMixin, InProvinceMixin,
                     InNullableDepartmentMixin, InNullableMunicipalityMixin):
    """Modelo utilizado para representar localidades censales.

    Notar que las localidades censales no son lo mismo que las localidades
    (CensusLocality y Locality). Las localidades censales contienen varias
    localidades (one to many).

    Attributes:
        __tablename__ (str): Nombre de la tabla.
        _id_len (int): Longitud de los IDs.
        lon (float): Longitud del centroide de la localidad censal.
        lat (float): Latitud del centroide de la localidad censal.
        funcion (str): Función administrativa de la localidad censal.
        geometría (geoalchemy2.Geometry): Geometría de la localidad censal.

    """

    __tablename__ = constants.CENSUS_LOCALITIES_ETL_TABLE
    _id_len = constants.CENSUS_LOCALITY_ID_LEN

    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    funcion = Column(String, nullable=True)
    geometria = Column(Geometry('POINT', srid=SRID), nullable=False)

    asentamientos = get_relationship('Settlement')
    localidades = get_relationship('Locality')

    @validates('categoria')
    def validate_category(self, _key, category):
        """Valida el valor a utilizar como categoría de la localidad censal.

        Args:
            _key (str): Campo de nombre ('categoria').
            value (str): Valor a establecer como categoría.

        Returns:
            str: Valor del campo validado.

        """
        if category not in constants.CENSUS_LOCALITY_TYPES.values():
            raise ValidationException(
                'El valor "{}" no es un tipo de loc. censal válido.'.format(
                    category))

        return category

    @validates('funcion')
    def validate_administrative_function(self, _key, function):
        """Valida el valor a utilizar como función administrativa de la
        localidad censal.

        Args:
            _key (str): Campo de nombre ('funcion').
            value (str): Valor a establecer como función administrativa.

        Returns:
            str: Valor del campo validado.

        """
        if function not in constants.CENSUS_LOCALITY_ADMIN_FUNCTIONS.values():
            raise ValidationException(
                'Función de localidad censal inválida: {}'.format(function))

        return function

    def to_dict(self, session):
        """Retorna una representación de la entidad como diccionario 'dict'.
        Los campos compuestos (que contienen varios valores) se representan
        también como diccionarios de varios valores. El resultado puede ser
        utilizado para serializar fácilmente la entidad a formatos como JSON.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            dict: Entidad en forma de diccionario.

        """
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
            'categoria': constants.BAHRA_TYPES[self.categoria],
            'funcion': self.funcion,
            'centroide': {
                'lon': self.lon,
                'lat': self.lat
            },
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }


class DoorNumberedMixin:
    """Define atributos y funciones de entidades que contienen números de
    calle.

    Attributes:
        inicio_derecha (int): Valor del comienzo de alturas del lado derecho.
        fin_derecha (int): Valor del final de alturas del lado derecho.
        inicio_izquierda (int): Valor del comienzo de alturas del lado
            izquierdo.
        fin_izquierda (int): Valor del final de alturas del lado izquierdo.

    """

    inicio_derecha = Column(Integer, nullable=False)
    fin_derecha = Column(Integer, nullable=False)
    inicio_izquierda = Column(Integer, nullable=False)
    fin_izquierda = Column(Integer, nullable=False)

    def door_numbers_dict(self):
        """Retorna una representación de las alturas como diccionario 'dict'.

        Returns:
            dict: Alturas en forma de diccionario.

        """
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
             InCensusLocalityMixin, DoorNumberedMixin):
    """Modelo utilizado para representar calles.

    Attributes:
        __tablename__ (str): Nombre de la tabla.
        _id_len (int): Longitud de los IDs.
        geometría (geoalchemy2.Geometry): Geometría de la calle.

    """

    __tablename__ = constants.STREETS_ETL_TABLE
    _id_len = constants.STREET_ID_LEN

    geometria = Column(Geometry('MULTILINESTRING', srid=SRID), nullable=False)

    intersecciones_a = get_relationship('Intersection',
                                        foreign_keys='Intersection.calle_a_id')
    intersecciones_b = get_relationship('Intersection',
                                        foreign_keys='Intersection.calle_b_id')
    cuadras = get_relationship('StreetBlock')

    def to_dict_simple(self, session):
        """Retorna una representación parcial de la entidad como diccionario
        'dict'. No se incluyen las alturas y la geometría.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            dict: Datos de la entidad en forma de diccionario.

        """
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
            'localidad_censal': {
                'id': self.localidad_censal_id,
                'nombre': self.localidad_censal_nombre(session)
            },
            'categoria': self.categoria
        }

    def to_dict(self, session):
        """Retorna una representación de la entidad como diccionario 'dict'.
        Los campos compuestos (que contienen varios valores) se representan
        también como diccionarios de varios valores. El resultado puede ser
        utilizado para serializar fácilmente la entidad a formatos como JSON.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            dict: Entidad en forma de diccionario.

        """
        base = self.to_dict_simple(session)

        base['altura'] = self.door_numbers_dict()
        base['geometria'] = json.loads(session.scalar(
            self.geometria.ST_AsGeoJSON()))

        return base


class StreetBlock(Base, DoorNumberedMixin):
    """Modelo utilizado para representar cuadras de calles. Todas las cuadras
    forman parte de una calle.

    Attributes:
        __tablename__ (str): Nombre de la tabla.
        id (str): ID de la cuadra.
        calle_id (str): ID de la calle a la cual pertenece la cuadra.
        geometría (geoalchemy2.Geometry): Geometría de la cuadra.

    """

    __tablename__ = constants.STREET_BLOCKS_ETL_TABLE

    id = Column(String, primary_key=True)
    calle_id = Column(String, ForeignKey(constants.STREETS_ETL_TABLE + '.id',
                                         ondelete='cascade'),
                      nullable=False)
    geometria = Column(Geometry('MULTILINESTRING', srid=SRID), nullable=False)

    def to_dict(self, session):
        """Retorna una representación de la entidad como diccionario 'dict'.
        Los campos compuestos (que contienen varios valores) se representan
        también como diccionarios de varios valores. El resultado puede ser
        utilizado para serializar fácilmente la entidad a formatos como JSON.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            dict: Entidad en forma de diccionario.

        """
        street = session.query(Street).get(self.calle_id)

        return {
            'id': self.id,
            'calle': street.to_dict_simple(session),
            'altura': self.door_numbers_dict(),
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }


class Intersection(Base):
    """Modelo utilizado para representar intersecciones de calles.

    Attributes:
        __tablename__ (str): Nombre de la tabla.
        id (str): ID de la intersección.
        calle_a_id (str): ID de la primera calle de la intersección.
        calle_b_id (str): ID de la segunda calle de la intersección.
        geometría (geoalchemy2.Geometry): Geometría de la cuadra.

    """

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
        """Retorna una representación de la entidad como diccionario 'dict'.
        Los campos compuestos (que contienen varios valores) se representan
        también como diccionarios de varios valores. El resultado puede ser
        utilizado para serializar fácilmente la entidad a formatos como JSON.

        Args:
            session (sqlalchemy.orm.session.Session): Sesión de base de datos.

        Returns:
            dict: Entidad en forma de diccionario.

        """
        street_a = session.query(Street).get(self.calle_a_id)
        street_b = session.query(Street).get(self.calle_b_id)

        return {
            'id': self.id,
            'calle_a': street_a.to_dict_simple(session),
            'calle_b': street_b.to_dict_simple(session),
            'geometria': json.loads(session.scalar(
                self.geometria.ST_AsGeoJSON()))
        }
