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
                      ' nomencla_a varchar(13),' ||
                      ' nombre_a varchar(100),' ||
                      ' tipo_a varchar(10),' ||
                      ' departamento_a varchar(100),' ||
                      ' provincia_a varchar(100),' ||
                      ' nomencla_b varchar(13),' ||
                      ' nombre_b varchar(100),' ||
                      ' tipo_b varchar(10),' ||
                      ' departamento_b varchar(100),' ||
                      ' provincia_b varchar(100),' ||
                      ' geom geometry,' ||
                      ' nomencla_interseccion varchar(254)' ||
                      ')';

    sql_insert := 'INSERT INTO %s (' ||
                  'nomencla_a, nombre_a, tipo_a, departamento_a, provincia_a, ' ||
                  'nomencla_b, nombre_b, tipo_b, departamento_b, provincia_b, ' ||
                  'geom, nomencla_interseccion)' ||
                  ' SELECT' ||
                  '   a.nomencla nomencla_a,' ||
                  '   a.nombre nombre_a,' ||
                  '   a.tipo tipo_a,' ||
                  '   get_department(substr(a.nomencla, 1, 5)) departamento_a,' ||
                  '   get_state(substr(a.nomencla, 1, 2)) provincia_a,' ||
                  '   b.nomencla nomencla_b,' ||
                  '   b.nombre nombre_b,' ||
                  '   b.tipo tipo_b,' ||
                  '   get_department(substr(b.nomencla, 1, 5)) departamento_b,' ||
                  '   get_state(substr(b.nomencla, 1, 2)) provincia_b,' ||
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
		RAISE NOTICE 'Se produjo el siguiente error: % (%)',
       SQLERRM, SQLSTATE;
		RETURN FALSE;
  END;
END $$;


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
                           '       first_value(nomencla_a) OVER w AS nomencla_a,' ||
                           '       first_value(nombre_a) OVER w AS nombre_a,' ||
                           '       first_value(nomencla_b) OVER w AS nomencla_b,' ||
                           '       first_value(nombre_b) OVER w AS nombre_b,' ||
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
			RAISE NOTICE 'Se produjo el siguiente error: % (%)',
        SQLERRM, SQLSTATE;
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
                      '    a.nomencla nomencla_a,' ||
                      '    a.nombre   nombre_a,' ||
                      '    a.tipo     tipo_a,' ||
                      '    get_department(substr(a.nomencla, 1, 5)) departamento_a,' ||
                      '    get_state(substr(a.nomencla, 1, 2)) provincia_a,' ||
                      '    b.nomencla nomencla_b,' ||
                      '    b.nombre   nombre_b,' ||
                      '    b.tipo     tipo_b,' ||
                      '    get_department(substr(b.nomencla, 1, 5)) departamento_b,' ||
                      '    get_state(substr(b.nomencla, 1, 2)) provincia_b,' ||
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
                      'AND get_state(substr(a.nomencla, 1, 2)) <> get_state(substr(b.nomencla, 1, 2))';

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
      RAISE NOTICE 'Se produjo el siguiente error: % (%)',
        SQLERRM, SQLSTATE;
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
    sql_insert = 'INSERT INTO %s (nombre_a, nomencla_a, tipo_a, departamento_a, provincia_a, ' ||
                 '                nombre_b, nomencla_b, tipo_b, departamento_b, provincia_b, geom)' ||
                 ' SELECT ' ||
                 '  nombre_a, nomencla_a, tipo_a, departamento_a, provincia_a,' ||
                 '  nombre_b, nomencla_b, tipo_b, departamento_b, provincia_b, geom' ||
                 ' FROM indec_intersecciones_provincias' ||
                 ' WHERE ' ||
                 '  nomencla_a LIKE %L OR' ||
                 '  nomencla_b LIKE %L';
    BEGIN
      EXECUTE format(sql_insert, tbl_name, concat(code_state, '%'), concat(code_state, '%'));
      RETURN TRUE;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Se produjo el siguiente error: % (%)',
        SQLERRM, SQLSTATE;
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
    RAISE NOTICE 'Se produjo el siguiente error: % (%)',
      SQLERRM, SQLSTATE;
    RETURN FALSE;
  END
$$;
