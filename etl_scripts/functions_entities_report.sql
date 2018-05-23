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