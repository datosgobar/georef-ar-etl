--
-- Trabajando sobre los datos de la base de IGN
--

-- agrego la columna provincia_id a red_vial
ALTER TABLE geocode.red_vial
  ADD COLUMN provincia_id integer;

-- Creando indices sobre la tabla demarcacion.provincias
CREATE INDEX provincias_geom_idx
    ON demarcacion.provincias
    USING GIST (geom);

-- Optimizando los indices
VACUUM ANALYZE geocode.red_vial;
VACUUM ANALYZE demarcacion.provincias;

-- Actualizo la columna provincia_id con el id de provincia correspondiente
-- Para ello utilizo inner join con la tabla demarcacion de provincias
UPDATE geocode.red_vial
    SET provincia_id=sq.id FROM (SELECT r.gid, p.id
                       FROM geocode.red_vial r INNER JOIN demarcacion.provincias p
                           ON ST_Intersects(r.geom, p.geom)
                       WHERE r.provincia_id ISNULL

    ) AS sq
WHERE geocode.red_vial.gid = sq.gid;

--
-- Función para actualizar el id de provincia
-- Se le agrega como parámetro el limite de transiciones
--

CREATE OR REPLACE FUNCTION agregar_id_provincia(_limit INTEGER) RETURNS VOID AS $$
BEGIN
  UPDATE geocode.red_vial
    SET provincia_id=sq.id FROM (SELECT r.gid, p.id
                       FROM geocode.red_vial r INNER JOIN demarcacion.provincias p
                           ON ST_Intersects(r.geom, p.geom)
                       WHERE r.provincia_id ISNULL
                       LIMIT $1
    ) AS sq
  WHERE geocode.red_vial.gid = sq.gid;
END;
$$ LANGUAGE plpgsql;

-- Contador de la cantidad de calles sin asignarle el id de provincia
-- El valor es probable que se deba al desfasaje que existe entre las provincias y el callejero
SELECT count(*)
FROM geocode.red_vial
WHERE provincia_id ISNULL;

