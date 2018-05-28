CREATE OR REPLACE FUNCTION get_new_entities(entity VARCHAR, colum_code VARCHAR, colum_name VARCHAR)
  RETURNS JSONB
  STRICT
LANGUAGE plpgsql
AS $$
DECLARE
  tbl_name TEXT;
  tbl_name_tmp TEXT;
  sql TEXT;
  result JSONB;
BEGIN
  tbl_name = 'ign_' || lower(entity);
  tbl_name_tmp = tbl_name || '_tmp';
  sql := 'SELECT json_agg(json_build_object(%L, t1.%I, %L, t1.%I))' ||
         ' FROM %s t1' ||
         ' WHERE %I IN ( ' ||
         ' SELECT %I' ||
         ' FROM %s' ||
         ' EXCEPT' ||
         ' SELECT %I' ||
         ' FROM %s' ||
         ' )' ||
				 ' AND geom IN (' ||
				 ' SELECT geom' ||
				 ' FROM %s' ||
				 ' EXCEPT' ||
				 ' SELECT geom' ||
				 ' FROM %s' ||
				 ' )';
  EXECUTE format(sql, 'code', colum_code, 'name', colum_name, tbl_name_tmp,
                   colum_code, colum_code, tbl_name_tmp, colum_code, tbl_name,
								   tbl_name_tmp, tbl_name) INTO result;
  IF result IS NULL THEN
      RETURN json_build_object('result', NULL );
  END IF;

  RETURN result;
END $$;


CREATE OR REPLACE FUNCTION get_entities_code_updates(entity VARCHAR, colum_code VARCHAR, colum_name VARCHAR)
  RETURNS JSON
  STRICT
LANGUAGE plpgsql
AS $$
DECLARE
  tbl_name TEXT;
  tbl_name_tmp TEXT;
  sql TEXT;
  result JSONB;
BEGIN
  tbl_name = 'ign_' || lower(entity);
  tbl_name_tmp = tbl_name || '_tmp';
  sql := 'SELECT json_agg(json_build_object(%L, t1.%I, %L, t1.%I, %L, t2.%I))' ||
         ' FROM %s t1 FULL OUTER JOIN %s t2' ||
         ' ON t1.%I = t2.%I' ||
         ' WHERE t1.%I <> t2.%I' ||
         ' AND st_equals(t1.geom, t2.geom)';
  EXECUTE format(sql, 'name', colum_name, 'code', colum_code, 'update', colum_code,
                 tbl_name, tbl_name_tmp, colum_name, colum_name, colum_code, colum_code) INTO result;

  IF result IS NULL THEN
      RETURN json_build_object('result', NULL );
  END IF;

  RETURN result;
END $$;


CREATE OR REPLACE FUNCTION get_entities_name_updates(entity VARCHAR, colum_code VARCHAR, colum_name VARCHAR)
  RETURNS JSONB
  STRICT
LANGUAGE plpgsql
AS $$
DECLARE
  tbl_name TEXT;
  tbl_name_tmp TEXT;
  sql TEXT;
  result JSONB;
BEGIN
  tbl_name = 'ign_' || entity;
  tbl_name_tmp = tbl_name || '_tmp';
  sql := 'SELECT json_agg(json_build_object(%L, t1.%I, %L, t1.%I , %L, t2.%I))' ||
         ' FROM %s t1 FULL OUTER JOIN %s t2' ||
         ' ON t1.%I = t2.%I' ||
         ' WHERE t1.%I <> t2.%I' ||
         ' AND st_equals(t1.geom, t2.geom)';
  EXECUTE format(sql, 'code',  colum_code, 'name', colum_name, 'update', colum_name,
                 tbl_name, tbl_name_tmp, colum_code, colum_code, colum_name, colum_name) INTO result;

  IF result ISNULL THEN
      RETURN json_build_object('result', NULL );
  END IF;

  RETURN result;
END $$;


CREATE OR REPLACE FUNCTION get_entities_geom_updates(entity VARCHAR, colum_code VARCHAR, colum_name VARCHAR)
  RETURNS JSONB
  STRICT
LANGUAGE plpgsql
AS $$
DECLARE
  tbl_name TEXT;
  tbl_name_tmp TEXT;
  sql TEXT;
  result JSONB;
BEGIN
  tbl_name = 'ign_' || lower(entity);
  tbl_name_tmp = tbl_name || '_tmp';
  sql := 'SELECT json_agg(json_build_object(%L, t2.%I , %L, t2.%I))' ||
         ' FROM %s t1 INNER JOIN %s t2' ||
         ' ON  t1.%I = t2.%I' ||
         ' WHERE t1.%I = t2.%I' ||
         ' AND NOT st_equals(t1.geom, t2.geom)';
  EXECUTE format(sql, 'code',  colum_code, 'name', colum_name,
                 tbl_name, tbl_name_tmp, colum_code, colum_code, colum_name, colum_name) INTO result;

  IF result IS NULL THEN
      RETURN json_build_object('result', NULL );
  END IF;

  RETURN result;
END$$;


CREATE OR REPLACE FUNCTION get_quantities(entity VARCHAR)
  RETURNS JSONB
  STRICT
LANGUAGE plpgsql
AS $$
DECLARE
  tbl_name TEXT;
  tbl_name_tmp TEXT;
  sql TEXT;
  result JSONB;
BEGIN
  tbl_name = 'ign_' || lower(entity);
  tbl_name_tmp = tbl_name || '_tmp';
  sql := 'SELECT json_build_object(%L, t1.guardada, %L, t2.entrada)' ||
         ' FROM (' ||
         ' SELECT count(*) as guardada FROM %s ) t1, (' ||
         ' SELECT count(*) as entrada FROM %s) t2';

  EXECUTE format(sql, 'guardada', 'entrada', tbl_name, tbl_name_tmp) INTO result;
  RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION entities_code_null(entity VARCHAR, column_code VARCHAR, column_name VARCHAR)
  RETURNS JSONB
  STRICT
LANGUAGE plpgsql
AS $$
DECLARE
  tbl_name TEXT;
  sql TEXT;
  result JSONB;
BEGIN
  tbl_name = 'ign_' || lower(entity) || '_tmp';
  sql := 'SELECT json_agg(json_build_object(%L, ogc_fid, %L, %I))' ||
         ' FROM %s' ||
         ' WHERE %I ISNULL';
  RAISE NOTICE '%', sql;
  EXECUTE format(sql, 'id', 'name', column_name, tbl_name, column_code) INTO result;

  IF result ISNULL THEN
      RETURN json_build_object('result', NULL );
  END IF;

  RETURN result;
END;
$$;


CREATE OR REPLACE FUNCTION get_entities_duplicates(entity VARCHAR, column_code VARCHAR, column_name VARCHAR)
  RETURNS JSONB
  STRICT
LANGUAGE plpgsql
AS $$
DECLARE
  tbl_name TEXT;
  sql TEXT;
  result JSONB;
BEGIN
  tbl_name = 'ign_' || lower(entity) || '_tmp';
  sql := 'SELECT json_agg(json_build_object(%L, ogc_fid, %L, %I, %L, %I))' ||
         ' FROM %s' ||
         ' GROUP BY %I' ||
         ' HAVING count(%I) > 1';

  EXECUTE format(sql, 'id', 'code', column_code, 'name', column_name,
                 tbl_name, column_code, column_code) INTO result;

  IF result ISNULL THEN
      RETURN json_build_object('result', NULL );
  END IF;

  RETURN result;
END;
$$;


CREATE OR REPLACE FUNCTION get_entities_invalid_geom(entity VARCHAR, column_code VARCHAR, column_name VARCHAR)
  RETURNS JSONB
  STRICT
LANGUAGE plpgsql
AS $$
DECLARE
  tbl_name TEXT;
  sql TEXT;
  result JSONB;
BEGIN
  tbl_name = 'ign_' || lower(entity) || '_tmp';
  sql := 'SELECT json_agg(json_build_object(%L, %I, %L, %I))' ||
         ' FROM %s' ||
         ' WHERE st_isvalid(geom) = FALSE';
  EXECUTE format(sql, 'code', column_code, 'name', column_name, tbl_name) INTO result;

  IF result ISNULL THEN
      RETURN json_build_object('result', NULL );
  END IF;

  RETURN result;
END;
$$;


CREATE OR REPLACE FUNCTION get_invalid_states_code()
  RETURNS JSONB
  STRICT
LANGUAGE plpgsql
AS $$
DECLARE
  result JSONB;
BEGIN
  SELECT json_agg(json_build_object('code', in1, 'name', nam)) INTO result
  FROM ign_provincias
  WHERE in1 NOT IN (
    '02', '06', '10', '14', '18', '22', '26', '30', '34', '38',
    '42', '46', '50', '54', '58', '62', '66', '70', '74', '78',
    '82', '86', '90', '94'
  );
  IF result ISNULL THEN
      RETURN json_build_object('result', NULL );
  END IF;

  RETURN result;
END;
$$;


CREATE OR REPLACE FUNCTION get_invalid_states_code(entity VARCHAR, column_code VARCHAR, column_name VARCHAR)
  RETURNS JSONB
  STRICT
LANGUAGE plpgsql
AS $$
DECLARE
  tbl_name TEXT;
  tbl_state TEXT;
  sql TEXT;
  result JSONB;
BEGIN
  tbl_name = 'ign_' || lower(entity) || '_tmp';
  tbl_state = 'ign_provincias_tmp';
  sql := 'SELECT json_agg(json_build_object(%L, t2.%I, %L, t2.%I))' ||
         ' FROM %s t1 FULL OUTER JOIN %s t2' ||
         ' ON t1.in1 = substr(t2.%I, 1, 2)' ||
         ' WHERE t1.in1 ISNULL AND t2.%I NOTNULL';

  EXECUTE format(sql, 'code', column_code, 'name', column_name,
                 tbl_state, tbl_name, column_code, column_code) INTO result;

  IF result ISNULL THEN
      RETURN json_build_object('result', NULL );
  END IF;

  RETURN result;
END;
$$;


CREATE OR REPLACE FUNCTION get_invalid_department_code()
  RETURNS JSONB
  STRICT
LANGUAGE plpgsql
AS $$
DECLARE
  result JSONB;
BEGIN
  SELECT json_agg(json_build_object('code', substr(t2.cod_bahra, 1, 5), 'name', t2.nombre_bah)) INTO result
  FROM ign_departamentos_tmp t1 FULL OUTER JOIN ign_bahra_tmp t2
    ON t1.in1 = substr(t2.cod_bahra, 1, 5)
  WHERE t1.in1 ISNULL AND t2.cod_bahra NOTNULL ;

  IF result ISNULL THEN
      RETURN json_build_object('result', NULL );
  END IF;

  RETURN result;
END;
$$;


CREATE OR REPLACE FUNCTION replace_tables()
  RETURNS BOOLEAN
STRICT
LANGUAGE plpgsql

AS $$
BEGIN
  IF exists(
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'public'
            AND table_name IN (
              'ign_provincias_tmp',
              'ign_departamentos_tmp',
              'ign_municipios_tmp',
              'ign_bahra_temp'
      )
  )
  THEN
    DROP TABLE IF EXISTS ign_provincias CASCADE;
    DROP TABLE IF EXISTS ign_departamentos CASCADE;
    DROP TABLE IF EXISTS ign_municipios CASCADE;
    DROP TABLE IF EXISTS ign_bahra CASCADE;

    ALTER TABLE IF EXISTS ign_provincias_tmp
      RENAME TO ign_provincias;
    ALTER TABLE IF EXISTS ign_departamentos_tmp
      RENAME TO ign_departamentos;
    ALTER TABLE IF EXISTS ign_municipios_tmp
      RENAME TO ign_municipios;
    ALTER TABLE IF EXISTS ign_bahra_tmp
      RENAME TO ign_bahra;

    ALTER INDEX IF EXISTS ign_provincias_tmp_geom_geom_idx
    RENAME TO ign_provincias_geom_geom_idx;
    ALTER INDEX IF EXISTS ign_departamentos_tmp_geom_geom_idx
    RENAME TO ign_departamentos_geom_geom_idx;
    ALTER INDEX IF EXISTS ign_municipios_tmp_geom_geom_idx
    RENAME TO ign_municipios_geom_geom_idx;
    ALTER INDEX IF EXISTS ign_bahra_tmp_geom_geom_idx
    RENAME TO ign_bahra_geom_geom_idx;
    RETURN TRUE;
  ELSE
    RETURN FALSE ;
  END IF;
  EXCEPTION WHEN OTHERS
  THEN
    RAISE NOTICE 'Se produjo el siguiente error: % (%)',
    SQLERRM, SQLSTATE;
    RETURN FALSE;
END
$$;