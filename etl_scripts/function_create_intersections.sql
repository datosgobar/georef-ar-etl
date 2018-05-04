CREATE OR REPLACE FUNCTION create_intersections(code_state CHAR(2))
  RETURNS BOOLEAN LANGUAGE plpgsql
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
                    ' nomencla_a varchar(254),' ||
                    ' nombre_a varchar(254),' ||
                    ' nomencla_b varchar(254),' ||
                    ' nombre_b varchar(254),' ||
                    ' geom geometry,' ||
                    ' nomencla_interseccion varchar(254)' ||
                    ')';

  sql_insert := 'INSERT INTO %s (nomencla_a, nombre_a, nomencla_b, nombre_b, geom, nomencla_interseccion)' ||
                ' SELECT' ||
                '   a.nomencla nomencla_a,' ||
                '   a.nombre nombre_a,' ||
                '   b.nomencla nomencla_b,' ||
                '   b.nombre nombre_b,' ||
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
                      ' WHERE ST_GeometryType(geom) = %L';

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
    EXECUTE format(sql_drop_tbl, tbl_name);
    EXECUTE format(sql_create_tbl, tbl_name);
    EXECUTE format(sql_insert, tbl_name, concat(code_state, '%'), concat(code_state, '%'), nam_filter, nam_filter, type_filter, type_filter);
    EXECUTE format(sql_delete_types, tbl_name, 'ST_MultiLineString');
    EXECUTE format(sql_delete_duplicates, tbl_name, tbl_name);
    RETURN TRUE;
  EXCEPTION WHEN OTHERS THEN
			RAISE NOTICE 'Se produjo el siguiente error: % (%)',
        SQLERRM, SQLSTATE;
			RETURN FALSE;
  END;
END $$;
