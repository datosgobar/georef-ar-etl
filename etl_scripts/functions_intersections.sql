CREATE OR REPLACE FUNCTION get_department_by_code(code CHARACTER(5), OUT result VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN
  SELECT nam INTO result
  FROM ign_departamentos
  WHERE in1 = code;
  EXCEPTION WHEN OTHERS
    THEN
      result := SQLERRM;
END
$$;


CREATE OR REPLACE FUNCTION get_state_by_code(code CHARACTER(2), OUT result VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN
  SELECT nam INTO result
  FROM ign_provincias
  WHERE in1 = code;
  EXCEPTION WHEN OTHERS
    THEN
      result := SQLERRM;
END
$$;


CREATE OR REPLACE FUNCTION create_intersections(code_state CHAR(2))
  RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
  sql_drop_tbl TEXT;
  sql_create_tbl TEXT;
  sql_insert TEXT;
  sql_delete_not_point TEXT;
  sql_centroide_multipoint TEXT;
  tbl_name    TEXT;
  type_filter TEXT;
  type_filter_multipoint TEXT;
  type_filter_point TEXT;
BEGIN
  tbl_name := 'indec_intersecciones_' || code_state;
  type_filter := 'OTRO';
  type_filter_multipoint := 'ST_MultiPoint';
  type_filter_point := 'ST_Point';

  sql_drop_tbl := 'DROP TABLE IF EXISTS %s';

  sql_create_tbl := 'CREATE TABLE IF NOT EXISTS %s (' ||
                    ' id serial not null primary key,' ||
                    ' a_nomencla varchar(13),' ||
                    ' a_nombre varchar(100),' ||
                    ' a_tipo varchar(10),' ||
                    ' a_dept_id varchar(5),' ||
                    ' a_dept_nombre varchar(100),' ||
                    ' a_prov_id varchar(2),' ||
                    ' a_prov_nombre varchar(100),' ||
                    ' b_nomencla varchar(13),' ||
                    ' b_nombre varchar(100),' ||
                    ' b_tipo varchar(10),' ||
                    ' b_dept_id varchar(5),' ||
                    ' b_dept_nombre varchar(100),' ||
                    ' b_prov_id varchar(2),' ||
                    ' b_prov_nombre varchar(100),' ||
                    ' geom geometry,' ||
                    ' nomencla_interseccion varchar(254)' ||
                    ')';

  sql_insert := 'INSERT INTO %s (' ||
                'a_nomencla, a_nombre, a_tipo, a_dept_id, a_dept_nombre, a_prov_id, a_prov_nombre,' ||
                'b_nomencla, b_nombre, b_tipo, b_dept_id, b_dept_nombre, b_prov_id, b_prov_nombre,' ||
                'geom, nomencla_interseccion)' ||
                ' SELECT' ||
                '   a.nomencla a_nomencla,' ||
                '   a.nombre a_nombre,' ||
                '   a.tipo a_tipo,' ||
                '   substr(a.nomencla, 1, 5) a_dept_id,' ||
                '   get_department_by_code(substr(a.nomencla, 1, 5)) a_dept_nombre,' ||
                '   substr(a.nomencla, 1, 2) a_prov_id,' ||
                '   get_state_by_code(substr(a.nomencla, 1, 2)) a_prov_nombre,' ||
                '   b.nomencla b_nomencla,' ||
                '   b.nombre b_nombre,' ||
                '   b.tipo b_tipo,' ||
                '   substr(b.nomencla, 1, 5) b_dept_id,' ||
                '   get_department_by_code(substr(b.nomencla, 1, 5)) b_dept_nombre,' ||
                '   substr(b.nomencla, 1, 2) b_prov_id,' ||
                '   get_state_by_code(substr(b.nomencla, 1, 2)) b_prov_nombre,' ||
                '   CASE WHEN ST_CoveredBy(a.geom, b.geom)' ||
                '     THEN' ||
                '       a.geom' ||
                '     ELSE' ||
                '       ST_Intersection(a.geom,b.geom)' ||
                '     END AS geom,' ||
                '   CASE WHEN a.nomencla > b.nomencla' ||
                '     THEN' ||
                '       a.nomencla || b.nomencla' ||
                '     ELSE' ||
                '       b.nomencla || a.nomencla' ||
                '     END AS nomencla' ||
                '  FROM indec_vias AS a INNER JOIN indec_vias AS b' ||
                '       ON ST_Intersects(a.geom, b.geom)' ||
                '  WHERE a.nomencla LIKE %L AND b.nomencla LIKE %L' ||
                '  AND a.tipo <> %L AND b.tipo <> %L';

  sql_delete_not_point := 'DELETE FROM %s WHERE ST_GeometryType(geom) <> %L AND ST_GeometryType(geom) <> %L';
  sql_centroide_multipoint := 'UPDATE %s SET geom = ST_Centroid(geom) WHERE St_GeometryType(geom) = %L';

  BEGIN
    RAISE NOTICE '-- Creando intersecciones para la provincia con código "%".', code_state;
    EXECUTE format(sql_drop_tbl, tbl_name);
    EXECUTE format(sql_create_tbl, tbl_name);
    EXECUTE format(sql_insert, tbl_name, concat(code_state, '%'), concat(code_state, '%'), type_filter, type_filter);

    RAISE NOTICE 'Eliminando intersecciones distintas al tipo Point y MultiPoint para la provincia con código "%".', code_state;
    EXECUTE format(sql_delete_not_point, tbl_name, type_filter_point, type_filter_multipoint);

    RAISE NOTICE 'Obteniendo centroide para multiples puntos para la provincia con código "%".', code_state;
    EXECUTE format(sql_centroide_multipoint, tbl_name, type_filter_multipoint);
    RETURN TRUE;
  EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Se produjo el siguiente error: % (%)', SQLERRM, SQLSTATE;
		RETURN FALSE;
  END;
END
$$;


CREATE OR REPLACE FUNCTION create_intersections_states_joined()
  RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
   sql_drop_tbl TEXT;
   sql_create_tbl TEXT;
   sql_alter_tbl TEXT;
   sql_delete_not_point TEXT;
   sql_centroide_multipoint TEXT;
   tbl_name TEXT;
   type_filter TEXT;
   type_filter_multipoint TEXT;
   type_filter_point TEXT;
BEGIN
  type_filter := 'OTRO';
  tbl_name := 'indec_intersecciones_provincias';
  type_filter_multipoint := 'ST_MultiPoint';
  type_filter_point := 'ST_Point';

  sql_drop_tbl := 'DROP TABLE IF EXISTS indec_intersecciones_provincias';
  sql_create_tbl := 'CREATE TABLE indec_intersecciones_provincias AS' ||
                    '  SELECT' ||
                    '    a.nomencla a_nomencla,' ||
                    '    a.nombre   a_nombre,' ||
                    '    a.tipo     a_tipo,' ||
                    '    substr(a.nomencla, 1, 5) a_dept_id,' ||
                    '    get_department_by_code(substr(a.nomencla, 1, 5)) a_dept_nombre,' ||
                    '    substr(a.nomencla, 1, 2) a_prov_id,' ||
                    '    get_state_by_code(substr(a.nomencla, 1, 2)) a_prov_nombre,' ||
                    '    b.nomencla b_nomencla,' ||
                    '    b.nombre   b_nombre,' ||
                    '    b.tipo     b_tipo,' ||
                    '    substr(b.nomencla, 1, 5) b_dept_id,' ||
                    '    get_department_by_code(substr(b.nomencla, 1, 5)) b_dept_nombre,' ||
                    '    substr(b.nomencla, 1, 2) b_prov_id,' ||
                    '    get_state_by_code(substr(b.nomencla, 1, 2)) b_prov_nombre,' ||
                    '    CASE WHEN ST_CoveredBy(a.geom, b.geom)' ||
                    '      THEN' ||
                    '       a.geom' ||
                    '      ELSE' ||
                    '       ST_Intersection(a.geom,b.geom)' ||
                    '     END AS geom,' ||
                    '    CASE WHEN a.nomencla > b.nomencla' ||
                    '      THEN' ||
                    '        a.nomencla || b.nomencla' ||
                    '    ELSE' ||
                    '      b.nomencla || a.nomencla' ||
                    '    END AS nomencla_interseccion' ||
                    '  FROM indec_vias AS a INNER JOIN indec_vias AS b' ||
                    '  ON ST_Intersects(a.geom, b.geom)' ||
                    'WHERE a.tipo <> %L AND b.tipo <> %L' ||
                    'AND get_state_by_code(substr(a.nomencla, 1, 2)) <> get_state_by_code(substr(b.nomencla, 1, 2))';

   sql_alter_tbl := 'ALTER TABLE %s ADD COLUMN id SERIAL PRIMARY KEY';
   sql_delete_not_point := 'DELETE FROM %s WHERE ST_GeometryType(geom) <> %L AND ST_GeometryType(geom) <> %L';
   sql_centroide_multipoint := 'UPDATE %s SET geom = ST_Centroid(geom) WHERE St_GeometryType(geom) = %L';

  BEGIN
    RAISE NOTICE '-- Creando intersecciones de provincias.';
    EXECUTE sql_drop_tbl;
    EXECUTE format(sql_create_tbl, type_filter, type_filter);
    EXECUTE format(sql_alter_tbl, tbl_name);

    RAISE NOTICE 'Eliminando intersecciones distintas al tipo Point y MultiPoint de provincias.';
    EXECUTE format(sql_delete_not_point, tbl_name, type_filter_point, type_filter_multipoint);

    RAISE NOTICE 'Obteniendo centroide para multiples puntos para provincias';
    EXECUTE format(sql_centroide_multipoint, tbl_name, type_filter_multipoint);
    RETURN TRUE;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Se produjo el siguiente error: % (%)', SQLERRM, SQLSTATE;
    RETURN FALSE;
  END;
END
$$;


CREATE OR REPLACE FUNCTION insert_intersections_states_joined(code_state CHAR(2))
  RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
  tbl_name TEXT;
  sql_insert TEXT;
BEGIN
  tbl_name = 'indec_intersecciones_' || code_state;
  sql_insert = 'INSERT INTO %s (a_nombre, a_nomencla, a_tipo, a_dept_id, a_dept_nombre, a_prov_id, a_prov_nombre,' ||
               '                b_nombre, b_nomencla, b_tipo, b_dept_id, b_dept_nombre, b_prov_id, b_prov_nombre, geom)' ||
               ' SELECT ' ||
               '  a_nombre, a_nomencla, a_tipo, a_dept_id, a_dept_nombre, a_prov_id, a_prov_nombre,' ||
               '  b_nombre, b_nomencla, b_tipo, b_dept_id, b_dept_nombre, b_prov_id, b_prov_nombre, geom' ||
               ' FROM indec_intersecciones_provincias' ||
               ' WHERE ' ||
               '  a_nomencla LIKE %L OR' ||
               '  b_nomencla LIKE %L';
  BEGIN
    EXECUTE format(sql_insert, tbl_name, concat(code_state, '%'), concat(code_state, '%'));
    RETURN TRUE;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Se produjo el siguiente error: % (%)', SQLERRM, SQLSTATE;
    RETURN FALSE;
  END;
END
$$;


CREATE OR REPLACE FUNCTION delete_intersections_duplicate(tbl_code VARCHAR(50))
  RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
  sql_delete_duplicates TEXT;
  tbl_name TEXT;
BEGIN
  tbl_name := 'indec_intersecciones_' || tbl_code;
  sql_delete_duplicates := 'DELETE FROM %s' ||
                           ' WHERE id NOT IN (' ||
                           '  SELECT t.id FROM (' ||
                           '     SELECT DISTINCT' ||
                           '       first_value(id) OVER w AS id,' ||
                           '       first_value(a_nomencla) OVER w AS a_nomencla,' ||
                           '       first_value(a_nombre) OVER w AS a_nombre,' ||
                           '       first_value(b_nomencla) OVER w AS b_nomencla,' ||
                           '       first_value(b_nombre) OVER w AS b_nombre,' ||
                           '       first_value(geom) OVER w AS geom,' ||
                           '       nomencla_interseccion' ||
                           '   FROM %s' ||
                           '   WINDOW w AS (' ||
                           '      PARTITION BY nomencla_interseccion' ||
                           '      ORDER BY nomencla_interseccion' ||
                           '      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING' ||
                           '   )' ||
                           ' ) AS t' ||
                           ')';
  BEGIN
    EXECUTE format(sql_delete_duplicates, tbl_name, tbl_name);
    RETURN TRUE;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Se produjo el siguiente error: % (%)', SQLERRM, SQLSTATE;
    RETURN FALSE;
  END;
 END
$$;


CREATE OR REPLACE FUNCTION process_intersections()
  RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
  insert_result BOOLEAN;
  delete_result BOOLEAN;
  create_result_joined BOOLEAN;
  insert_result_joined BOOLEAN;
  delete_result_joined BOOLEAN;
  sql_create_intersections TEXT;
  sql_delete_duplicate TEXT;
  sql_create_intersections_states_joined TEXT;
  sql_insert_intersections_states_joined TEXT;
  sql_delete_duplicate_states_joined TEXT;
BEGIN
  sql_create_intersections = 'SELECT create_intersections(in1) FROM ign_provincias ORDER BY in1 ASC';
  sql_delete_duplicate = 'SELECT delete_intersections_duplicate(in1) FROM ign_provincias ORDER BY in1 ASC';
  sql_create_intersections_states_joined = 'SELECT create_intersections_states_joined()';
  sql_insert_intersections_states_joined = 'SELECT insert_intersections_states_joined(in1) FROM ign_provincias ORDER BY in1 ASC';
  sql_delete_duplicate_states_joined = 'SELECT delete_intersections_duplicate(%L)';

  EXECUTE sql_create_intersections INTO insert_result;
  EXECUTE sql_delete_duplicate INTO delete_result;
  EXECUTE sql_create_intersections_states_joined INTO create_result_joined;
  EXECUTE sql_insert_intersections_states_joined INTO insert_result_joined;
  EXECUTE format(sql_delete_duplicate_states_joined, 'provincias') INTO delete_result_joined;
  IF insert_result AND delete_result AND create_result_joined AND insert_result_joined AND delete_result_joined THEN
    RETURN TRUE ;
  ELSE
    RETURN FALSE ;
  END IF ;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Se produjo el siguiente error: % (%)', SQLERRM, SQLSTATE;
  RETURN FALSE;
END
$$;