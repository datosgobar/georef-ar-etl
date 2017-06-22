/*
 Función de geocodificación
 Parámetros: Nombre de calle (text), Altura (integer)
 Retorna: Latitud, Logitud (text)
*/
CREATE OR REPLACE FUNCTION geocodificar(IN calle TEXT, IN altura INTEGER, OUT result TEXT)
AS $$
DECLARE
  lat TEXT;
  lng TEXT;
BEGIN
  IF (calle = '') IS NOT FALSE
  THEN
    RAISE EXCEPTION 'El nombre de la calle es requerido.';
  ELSE
    SELECT st_astext(ST_Line_Interpolate_Point(
                         st_geomfromtext(
                             REPLACE(REPLACE(REPLACE(st_astext(geom), 'MULTI', ''), '((', '('), '))', ')'), 4326),
                         CASE
                         WHEN ((272 - alt_ini_d) / (alt_fin_i - alt_ini_d) :: FLOAT) > 1
                           THEN 1
                         WHEN ((272 - alt_ini_d) / (alt_fin_i - alt_ini_d) :: FLOAT) < 0
                           THEN 0
                         ELSE ((272 - alt_ini_d) / (alt_fin_i - alt_ini_d) :: FLOAT)
                         END
                     ))
    INTO result
    FROM san_luis.justo_daract_callejero
    WHERE nombre ILIKE '%' || $1 || '%'
          AND ($2 - alt_ini_i) / (alt_fin_i - alt_ini_d) :: FLOAT >= 0
          AND ($2 - alt_ini_i) / (alt_fin_i - alt_ini_d) :: FLOAT <= 1
          AND alt_ini_i <> 0
          AND alt_ini_d <> 0
          AND alt_fin_i <> 0
          AND alt_fin_d <> 0
    ORDER BY alt_fin_i ASC
    LIMIT 1;

    SELECT st_y(result)
    INTO lat;
    SELECT st_x(result)
    INTO lng;

    result := lat || ', ' || lng;
  END IF;
END;
$$ LANGUAGE 'plpgsql';

-- Ejemplo de uso
SELECT geocodificar('JUAN B JUSTO', 452);


/*
  Función que devuelve el punto inicial de una calle
  Parámetros: Nombre de calle (text)
  Retorna: Latitud, Logitud (text)
*/
CREATE OR REPLACE FUNCTION get_punto_inicial(IN nombre TEXT, OUT result TEXT)
AS $$
DECLARE
  _id INTEGER;
  lat TEXT;
  lng TEXT;
BEGIN
  IF (nombre = '') IS NOT FALSE
  THEN
    RAISE EXCEPTION 'El nombre de la calle es requerido.';
  ELSE
    SELECT DISTINCT ON (j.nombre)
      id,
      least(alt_ini_i, alt_ini_d, alt_fin_i, alt_fin_d)
    INTO _id
    FROM san_luis.justo_daract_callejero j
    WHERE j.nombre ILIKE '%' || $1 || '%'
          AND alt_ini_i <> 0
          AND alt_ini_d <> 0
          AND alt_fin_i <> 0
          AND alt_fin_d <> 0;

    SELECT
      st_y(st_startpoint(st_linemerge(geom))),
      st_x(st_startpoint(st_linemerge(geom)))
    INTO lat, lng
    FROM san_luis.justo_daract_callejero j
    WHERE j.id = _id;

    result := lat || ', ' || lng;
  END IF;
END;
$$ LANGUAGE 'plpgsql';

-- Ejemplo de uso
SELECT get_punto_inicial('LOS ANDES');


/*
  Función que devuelve el centroide de una calle
  Parámetros: Nombre de calle (text)
  Retorna: Latitud, Logitud (text)
*/
CREATE OR REPLACE FUNCTION get_centroide(IN calle TEXT, OUT lat_lng TEXT)
AS $$
DECLARE
  lat TEXT;
  lng TEXT;
BEGIN
  IF (calle = '') IS NOT FALSE
  THEN
    RAISE EXCEPTION 'El nombre de la calle es requerido.';
  ELSE
    SELECT
      st_y(st_centroid(st_collect(geom))),
      st_x(st_centroid(st_collect(geom)))
    INTO lat, lng
    FROM san_luis.justo_daract_callejero
    WHERE nombre ILIKE '%' || $1 || '%';
    lat_lng := lat || ', ' || lng;
  END IF;
END;
$$ LANGUAGE 'plpgsql';

-- Ejemplo de uso
SELECT get_centroide('JUAN B JUSTO');