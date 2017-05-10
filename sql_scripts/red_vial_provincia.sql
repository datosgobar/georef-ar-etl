--
-- Trabajando sobre los datos de la base de IGN
--

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