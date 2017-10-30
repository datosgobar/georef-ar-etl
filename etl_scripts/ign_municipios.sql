-- La funciones utiliza la tabla 'demarcaciones.provincias' provista por IGN.


-- Crea la tabla municipios con la provincia incluida por cada registro
-- La provincia se obtiene siempre y cuando el municipio este totalmente
-- dentro de la provincia.
CREATE TABLE municipios_georef AS
      SELECT
            m.objectid,
            upper(m.fna) nombre,
            st_y(st_centroid(m.geom)) lat,
            st_x(st_centroid(m.geom)) lon,
            p.nam provincia,
            m.geom
      FROM public.municipios m LEFT OUTER JOIN demarcacion.provincias p
                  ON st_within(m.geom, p.geom)
      ORDER BY m.objectid;


-- Crea función para obtener la provincia de cada municipio para aquellas que
-- no esten completamente dentro de una provincia sino por área de intersecón compartida
CREATE OR REPLACE FUNCTION  get_state(id INTEGER, OUT result TEXT) AS $$
BEGIN
      SELECT p.nam INTO result
      FROM public.municipios m , demarcacion.provincias p
      WHERE m.objectid = $1
      ORDER BY st_area(st_intersection(m.geom, p.geom)) DESC
      LIMIT 1;
END;
$$ LANGUAGE 'plpgsql';


-- Actualiza los municipios que no tienen provincia
UPDATE municipios_georef
SET provincia = subquery.provincia
FROM (SELECT objectid, get_state(m.objectid) provincia
      FROM municipios_georef m) subquery
WHERE municipios_georef.objectid = subquery.objectid
AND municipios_georef.provincia ISNULL ;
