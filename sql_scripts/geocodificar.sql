CREATE OR REPLACE FUNCTION geocodificar(IN  geom   TEXT, IN altura INTEGER, IN alt_ini_i INTEGER, IN alt_fin_d INTEGER,
                                        OUT result TEXT)
AS $$
DECLARE
  lat TEXT;
  lng TEXT;
BEGIN
  SELECT st_astext(ST_Line_Interpolate_Point(
                       st_geomfromtext(
                           REPLACE(REPLACE(REPLACE(st_astext($1), 'MULTI', ''), '((', '('), '))', ')'), 4326),
                       CASE
                       WHEN (($2 - $3) / ($4 - $3) :: FLOAT) > 1
                         THEN 1
                       WHEN (($2 - $3) / ($4 - $3) :: FLOAT) < 0
                         THEN 0
                       ELSE (($2 - $3) / ($4 - $3) :: FLOAT)
                       END
                   ))
  INTO result;
  SELECT st_y(result)
  INTO lat;
  SELECT st_x(result)
  INTO lng;

  result := lat || ',' || lng;
END;
$$ LANGUAGE 'plpgsql';
