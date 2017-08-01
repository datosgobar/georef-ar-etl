--
-- Trabajando sobre los datos de la base de IGN
--

/*
  PROVINCIAS
*/

-- Descripción: Agrego la columna provincia_id a red vial
ALTER TABLE geocode.red_vial
  ADD COLUMN provincia_id INTEGER;


-- Descripción: Agrego la columna provincia_nombre a red vial
ALTER TABLE geocode.red_vial
  ADD COLUMN provincia_nombre VARCHAR(100);


-- Descripción: Creando índices sobre la tabla demarcacion.provincias
CREATE INDEX provincias_geom_idx
  ON demarcacion.provincias
  USING GIST (geom);


-- Optimizando los índices
VACUUM ANALYZE geocode.red_vial;
VACUUM ANALYZE demarcacion.provincias;


-- Descripción: Función para actualizar el id de provincia
-- Parámetros: Límite de transiciones (Integer)
-- Retorna: número de filas actualizadas (Integer)
CREATE OR REPLACE FUNCTION agregar_id_provincia(_limit INTEGER) RETURNS INTEGER AS $$
DECLARE
  result INTEGER;
BEGIN
UPDATE geocode.red_vial
  SET provincia_id=sq.id FROM (SELECT r.gid, p.id
                     FROM geocode.red_vial r INNER JOIN demarcacion.provincias p
                         ON ST_Intersects(r.geom, p.geom)
                     WHERE r.provincia_id ISNULL
                     LIMIT $1
  ) AS sq
WHERE geocode.red_vial.gid = sq.gid;
GET DIAGNOSTICS result = ROW_COUNT;
RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Descripción: Ejemplo de uso
-- Parámetros: Límite de transiciones (Integer)
SELECT agregar_id_provincia(10000);


-- Descripción: Contador de la cantidad de calles sin asignarle el id de provincia
-- Nota: El valor es probable que se deba al desfasaje que existe entre las provincias y el callejero
SELECT count(*)
FROM geocode.red_vial
WHERE provincia_id ISNULL;


-- Descripción: Función para setear el nombre de provincia
-- Parámetro: Límite de transiciones (Integer)
-- Retorna: Número de filas actualizadas (Integer)
CREATE OR REPLACE FUNCTION agregar_nombre_provincia(_limit INTEGER) RETURNS INTEGER AS $$
DECLARE
  result INTEGER;
BEGIN
UPDATE geocode.red_vial
    SET provincia_nom = t.nam FROM
    (
        SELECT p.id, p.nam
        FROM geocode.red_vial r INNER JOIN demarcacion.provincias p
        ON r.provincia_id = p.id
        WHERE r.provincia_nom ISNULL
        LIMIT $1
    ) AS t
WHERE provincia_id = t.id;
GET DIAGNOSTICS result = ROW_COUNT;
RETURN result;
END
$$ LANGUAGE plpgsql;


-- Descripción: Ejemplo de uso
-- Parámetros: Límite de transiciones (Integer)
SELECT agregar_nombre_provincia(1000);


/*
  DEPARTAMENTOS
*/


-- La tabla de departamento tiene los nombre de los mismos con acento.
-- Para cruzar luego las tabla, primero se las tengo que quitar
-- Á É Í Ó Ú
UPDATE demarcacion.departamento
  SET fna = replace(fna,'Á','A');


-- Descripción: Agrego la columna departamento_id
ALTER TABLE geocode.red_vial
ADD COLUMN departamento_id integer;


-- Descripción: Agrego la columna provincia_nom a red vial
ALTER TABLE ign.geocode.red_vial
ADD COLUMN provincia_nom VARCHAR(100);


-- Descripción: Agrego la columna departamento nombre a red vial
ALTER TABLE ign.geocode.red_vial
ADD COLUMN departamento_nom VARCHAR(100);


-- Descripción: Agrego un índice a la tabla departamento
CREATE INDEX departamento_geom_idx
  ON demarcacion.departamento
  USING GIST (geom);


-- Descripción: Optimización de índices y estadísticas
VACUUM ANALYZE demarcacion.departamento;


-- Descripción: Función para agregar id de departamento
-- Parámetros: Límite de transiciones (Integer)
-- Retorna: número de filas actualizadas (Integer)
CREATE OR REPLACE FUNCTION agregar_id_departamento(_limit INTEGER) RETURNS INTEGER AS $$
DECLARE
  result INTEGER;
BEGIN
  UPDATE geocode.red_vial
    SET departamento_id=sq.id FROM (SELECT r.gid, d.id
                       FROM geocode.red_vial r INNER JOIN demarcacion.departamento d
                           ON ST_Intersects(r.geom, d.geom)
                       WHERE r.departamento_id ISNULL
                       LIMIT $1
    ) AS sq
  WHERE geocode.red_vial.gid = sq.gid;
  GET DIAGNOSTICS result = ROW_COUNT;
  RETURN result;
END;
$$ LANGUAGE plpgsql;


-- Descripción: Ejemplo de uso
-- Parámetros: Límite de transiciones (Integer)
SELECT agregar_id_departamento(1000);


-- Descripción: Contador de la cantidad de calles sin asignarle el id de departamento
-- Nota: El valor es probable que se deba al desfasaje que existe entre los departamentos y el callejero
SELECT count(*)
FROM geocode.red_vial
WHERE departamento_id ISNULL;


-- Descripción: Función para agregar nombre de departamento
-- Parámetros: Límite de transiciones (Integer)
-- Retorna: número de filas actualizadas (Integer)
CREATE OR REPLACE FUNCTION agregar_nombre_departamento(_limit INTEGER) RETURNS INTEGER AS $$
DECLARE
  result INTEGER;
BEGIN
UPDATE geocode.red_vial
    SET departamento_nom = t.fna FROM
    (
        SELECT d.id, d.fna
        FROM geocode.red_vial r INNER JOIN demarcacion.departamento d
        ON r.departamento_id = d.id
        WHERE departamento_nom ISNULL
        LIMIT $1
    ) AS t
WHERE departamento_id = t.id;
GET DIAGNOSTICS result = ROW_COUNT;
RETURN result;
END
$$ LANGUAGE plpgsql;


-- Descripción: Ejemplo de uso
-- Parámetros: Límite de transiciones (Integer)
SELECT agregar_nombre_departamento(1000);


/*
  LOCALIDAD
*/


-- Descripción: Agrego la columna ciudad_data
ALTER TABLE geocode.red_vial ADD COLUMN ciudad_data jsonb;


-- Descripción: Función que calcula(distancias menores) las ciudades candidatas al la cual puede pertenecer un registro geoespacial de red vial
-- Parámetros: Límite de transiciones (Integer)
-- Retorna: Ciudades candidatas (Jsonb)
-- Nota: Las ciudades son del tipo Punto
CREATE OR REPLACE FUNCTION get_candidatos(_id_red_vial INTEGER) RETURNS jsonb AS $$
DECLARE
  jeyson jsonb;
BEGIN
  SELECT json_agg(DISTINCT t.nombre) into jeyson
  FROM (
    SELECT t2.nombre, st_distance(t1.geom, t2.geom) distancia, t2.nom_depto, t2.nom_prov
    FROM (
          SELECT (st_dumppoints(geom)).geom, departamento_nom, provincia_nom
          FROM geocode.red_vial
          WHERE gid = $1
         ) t1,
         (SELECT gid, geom, nombre, nom_depto, nom_prov
           FROM demarcacion.bahra) t2
    WHERE t1.departamento_nom = t2.nom_depto
      AND t1.provincia_nom = t2.nom_prov
    ORDER BY distancia
    LIMIT 10) t;
  RETURN jeyson;
END;
$$ LANGUAGE plpgsql;


-- Descripción: Función que inserta las ciudades candidatas al la cual puede pertenecer un registro geoespacial de red vial
-- Parámetros: Id de registro de red vial (Integer)
-- Retorna: Filas afectadas (Jsonb)
CREATE OR REPLACE FUNCTION insertar_ciudad_data(_id_red_vial INTEGER) RETURNS VOID AS $$
BEGIN
  UPDATE geocode.red_vial
    SET ciudad_data = (SELECT get_candidatos($1))
  WHERE gid = $1;
END;
$$ LANGUAGE plpgsql;


-- Descripción: Ejemplo de uso
-- Parámetros: Id de registro de red vial (Integer)
-- Notas: Primero pasar a mayúscula el nombre de departamento
UPDATE geocode.red_vial
SET departamento_nom = upper(departamento_nom);

-- Ejmplo de uso
SELECT insertar_ciudad_data(gid)
FROM geocode.red_vial
WHERE red_vial.ciudad_data ISNULL
LIMIT 10000;


/*
  NOMBRE DE CALLES
*/


--
-- Tabla nombre de calles
--

CREATE TABLE public.nombre_calles AS
	SELECT pc.id, pc.nombre_completo, pc.nombre_abreviado, pc.apellido, pc.nombre, tp.nombre tipo_camino, pl.nombre localidad, pp.nombre partido, ppv.nombre provincia
	FROM postal.calles pc INNER JOIN postal.localidades pl
	ON pc.id_localidad = pl.id INNER JOIN postal.tipos_camino tp
	ON pc.id_tipo_camino = tp.id INNER JOIN postal.parajes pp
	ON pl.id_paraje = pp.id INNER JOIN postal.provincias ppv
	ON pp.id_provincia = ppv.id;

  

-- Descripción: Función que procesa si hay coincidencias entre la tabla geocode.red_vial y nombre_calles
-- Nota: se debe importar la tabla nombre_calles
CREATE OR REPLACE FUNCTION get_match_red_vial(_id INTEGER) RETURNS INTEGER AS $$
DECLARE
    id_match INTEGER;
    result INTEGER;
BEGIN
    SELECT rv.gid INTO id_match
    FROM public.nombre_calles nc, geocode.red_vial rv
    WHERE nc.id = $1
    AND nc.nombre_completo = rv.nombre
    AND rv.ciudad_data_text ILIKE '%' || nc.localidad ||'%'
    AND nc.partido = rv.departamento_nom
    AND nc.provincia = rv.provincia_nom;
        IF id_match NOTNULL THEN
            UPDATE public.nombre_calles
                SET id_red_vial = id_match
            WHERE id = $1;
            GET DIAGNOSTICS result = ROW_COUNT;
        ELSE
            UPDATE public.nombre_calles
                SET id_red_vial = -1
            WHERE id = $1;
            GET DIAGNOSTICS result = ROW_COUNT;
        END IF ;
     RETURN result;
END;
$$ LANGUAGE plpgsql;


-- Descripción: Ejemplo de uso
-- Parámetros: Límite de transiciones (Integer)
SELECT get_match_red_vial(c.id)
FROM public.nombre_calles c
WHERE c.id_red_vial ISNULL
LIMIT 100000;
