--
-- Trabajando sobre los datos de la base de IGN
--


-- Agrego la columna departamento_id
ALTER TABLE geocode.red_vial
  ADD COLUMN departamento_id integer;


-- Agrego un índice a la tabla departamento
CREATE INDEX departamento_geom_idx
    ON demarcacion.departamento
    USING GIST (geom);


-- Optimizacion de indices y estadisticias
VACUUM ANALYZE demarcacion.departamento;


-- Actualizo la columna departamento_id con id del departamento correspondiente
-- Para ello utilizo inner join con la tabla tablas red_vial y departamento
UPDATE geocode.red_vial
    SET departamento_id=sq.id FROM (SELECT r.gid, d.id
                       FROM geocode.red_vial r INNER JOIN demarcacion.departamento d
                           ON ST_Intersects(r.geom, d.geom)
                       WHERE r.departamento_id ISNULL
                       LIMIT 10000
    ) AS sq
WHERE geocode.red_vial.gid = sq.gid;


--
-- Función para agregar id de departamento
--
CREATE OR REPLACE FUNCTION agregar_id_departamento(_limit INTEGER) RETURNS VOID AS $$
BEGIN
  UPDATE geocode.red_vial
    SET departamento_id=sq.id FROM (SELECT r.gid, d.id
                       FROM geocode.red_vial r INNER JOIN demarcacion.departamento d
                           ON ST_Intersects(r.geom, d.geom)
                       WHERE r.departamento_id ISNULL
                       LIMIT $1
    ) AS sq
  WHERE geocode.red_vial.gid = sq.gid;

END;
$$ LANGUAGE plpgsql;


-- Contador de la cantidad de calles sin asignarle el id de departamento
-- El valor es probable que se deba al desfasaje que existe entre los departamentos y el callejero
SELECT count(*)
FROM geocode.red_vial
WHERE departamento_id ISNULL;
