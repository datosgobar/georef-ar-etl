--
-- Trabajando sobre los datos de la base de IGN
--


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
