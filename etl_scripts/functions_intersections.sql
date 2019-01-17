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
  tbl_name    TEXT;
  nam_filter  TEXT;
  type_filter TEXT;
  sql_drop_tbl TEXT;
  sql_create_tbl TEXT;
  sql_insert TEXT;
  sql_delete_types TEXT;
  sql_delete_duplicates TEXT;
BEGIN
  tbl_name = 'indec_intersecciones_' || code_state;
  nam_filter = 'CALLE S N';
  type_filter = 'OTRO';

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
                '       ST_GeometryN(' ||
                '         ST_Multi(' ||
                '           ST_Intersection(a.geom,b.geom)' ||
                '         ) ,1' ||
                '       )' ||
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
                '  AND a.nombre <> %L AND b.nombre <> %L' ||
                '  AND a.tipo <> %L AND b.tipo <> %L';

  sql_delete_types := 'DELETE' ||
                      ' FROM %s' ||
                      ' WHERE ST_GeometryType(geom) = %L' ||
                      ' OR ST_GeometryType(geom) = %L';

  sql_delete_duplicates := 'SELECT delete_intersections_duplicate(%L)';

  BEGIN
    RAISE NOTICE '-- Creando intersecciones para la provincia con código "%".', code_state;
    EXECUTE format(sql_drop_tbl, tbl_name);
    EXECUTE format(sql_create_tbl, tbl_name);
    EXECUTE format(sql_insert, tbl_name, concat(code_state, '%'), concat(code_state, '%'), nam_filter, nam_filter, type_filter, type_filter);
    EXECUTE format(sql_delete_types, tbl_name, 'ST_MultiLineString', 'ST_LineString');
    EXECUTE format(sql_delete_duplicates, tbl_name);
    RETURN TRUE;
  EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Se produjo el siguiente error: % (%)', SQLERRM, SQLSTATE;
		RETURN FALSE;
  END;
END
$$;


CREATE OR REPLACE FUNCTION delete_intersections_duplicate(tbl_name VARCHAR(50))
  RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
  sql_delete_duplicates TEXT;
BEGIN
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


CREATE OR REPLACE FUNCTION create_intersections_states_joined()
  RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
   sql_drop_tbl TEXT;
   sql_create_tbl TEXT;
   sql_alter_tbl TEXT;
   sql_delete_duplicates TEXT;
   nam_filter  TEXT;
   type_filter TEXT;
   sql_delete_types TEXT;
BEGIN
  nam_filter = 'CALLE S N';
  type_filter = 'OTRO';

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
                    '        a.geom' ||
                    '      ELSE' ||
                    '        ST_GeometryN(' ||
                    '          ST_Multi(' ||
                    '            ST_Intersection(a.geom, b.geom)' ||
                    '        ), 1' ||
                    '      )' ||
                    '    END AS geom,' ||
                    '    CASE WHEN a.nomencla > b.nomencla' ||
                    '      THEN' ||
                    '        a.nomencla || b.nomencla' ||
                    '    ELSE' ||
                    '      b.nomencla || a.nomencla' ||
                    '    END AS nomencla_interseccion' ||
                    '  FROM indec_vias AS a INNER JOIN indec_vias AS b' ||
                    '  ON ST_Intersects(a.geom, b.geom)' ||
                    'WHERE a.nombre <> %L AND b.nombre <> %L' ||
                    'AND a.tipo <> %L AND b.tipo <> %L' ||
                    'AND get_state_by_code(substr(a.nomencla, 1, 2)) <> get_state_by_code(substr(b.nomencla, 1, 2))';

  sql_alter_tbl := 'ALTER TABLE indec_intersecciones_provincias ADD COLUMN id SERIAL PRIMARY KEY';

  sql_delete_types := 'DELETE' ||
                    ' FROM indec_intersecciones_provincias' ||
                    ' WHERE ST_GeometryType(geom) = %L' ||
                    ' OR ST_GeometryType(geom) = %L';

   sql_delete_duplicates = 'SELECT delete_intersections_duplicate(%L)';

  BEGIN
    RAISE NOTICE '-- Creando intersecciones de provincias.';
    EXECUTE sql_drop_tbl;
    EXECUTE format(sql_create_tbl, nam_filter, nam_filter, type_filter, type_filter);
    EXECUTE sql_alter_tbl;
    EXECUTE format(sql_delete_types, 'ST_MultiLineString', 'ST_LineString');
    EXECUTE format(sql_delete_duplicates, 'indec_intersecciones_provincias');
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
  sql_insert = 'INSERT INTO %s (a_nombre, a_nomencla, a_tipo, a_depto_id, a_depto_nombre, a_prov_id, a_prov_nombre,' ||
               '                b_nombre, b_nomencla, b_tipo, b_depto_id, b_depto_nombre, b_prov_id, b_prov_nombre, geom)' ||
               ' SELECT ' ||
               '  a_nombre, a_nomencla, a_tipo, a_depto_id, a_depto_nombre, a_prov_id, a_prov_nombre,' ||
               '  b_nombre, b_nomencla, b_tipo, b_depto_id, b_depto_nombre, b_prov_id, b_prov_nombre, geom' ||
               ' FROM indec_intersecciones_provincias' ||
               ' WHERE ' ||
               '  nomencla_a LIKE %L OR' ||
               '  nomencla_b LIKE %L';
  BEGIN
    EXECUTE format(sql_insert, tbl_name, concat(code_state, '%'), concat(code_state, '%'));
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
  create_result BOOLEAN;
  insert_result_joined BOOLEAN;
BEGIN
  EXECUTE 'SELECT create_intersections(in1) FROM ign_provincias' INTO insert_result;
  EXECUTE 'SELECT create_intersections_states_joined()' INTO create_result;
  EXECUTE 'SELECT insert_intersections_states_joined(in1) FROM ign_provincias' INTO insert_result_joined;
  IF insert_result AND create_result AND insert_result_joined THEN
    RETURN TRUE ;
  ELSE
    RETURN FALSE ;
  END IF ;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Se produjo el siguiente error: % (%)', SQLERRM, SQLSTATE;
  RETURN FALSE;
END
$$;
