--
-- Trabajando sobre los datos de la base de IGN
--


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



SELECT insertar_ciudad_data(gid)
FROM geocode.red_vial
WHERE red_vial.ciudad_data ISNULL
LIMIT 10000;
