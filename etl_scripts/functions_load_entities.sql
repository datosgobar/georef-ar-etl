CREATE OR REPLACE FUNCTION get_department(code_muncipality TEXT, OUT result TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
  SELECT d.in1
  INTO result
  FROM ign_municipios m INNER JOIN ign_departamentos d
      ON substr(m.in1, 1, 2) = substr(d.in1, 1, 2)
  WHERE m.in1 = code_muncipality
  ORDER BY st_area(st_intersection(m.geom, d.geom)) DESC
  LIMIT 1;
  EXCEPTION WHEN OTHERS
    THEN
      result := SQLERRM;
END;
$$;


CREATE OR REPLACE FUNCTION get_municipality(code_bahra TEXT, OUT result TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
  SELECT m.in1
  INTO result
  FROM ign_municipios m, ign_bahra b
  WHERE st_contains(m.geom, b.geom)
        AND b.cod_bahra = code_bahra;
  EXCEPTION WHEN OTHERS
    THEN
      result := SQLERRM;
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

    ALTER TABLE IF EXISTS ign_provincias_tmp RENAME TO ign_provincias;
    ALTER TABLE IF EXISTS ign_departamentos_tmp RENAME TO ign_departamentos;
    ALTER TABLE IF EXISTS ign_municipios_tmp RENAME TO ign_municipios;
    ALTER TABLE IF EXISTS ign_bahra_tmp RENAME TO ign_bahra;

    ALTER INDEX IF EXISTS ign_provincias_tmp_geom_geom_idx RENAME TO ign_provincias_geom_geom_idx;
    ALTER INDEX IF EXISTS ign_departamentos_tmp_geom_geom_idx RENAME TO ign_departamentos_geom_geom_idx;
    ALTER INDEX IF EXISTS ign_municipios_tmp_geom_geom_idx RENAME TO ign_municipios_geom_geom_idx;
    ALTER INDEX IF EXISTS ign_bahra_tmp_geom_geom_idx RENAME TO ign_bahra_geom_geom_idx;
    RETURN TRUE;
  ELSE
    RETURN FALSE;
  END IF;
  EXCEPTION WHEN OTHERS
  THEN
    RAISE NOTICE 'Se produjo el siguiente error: % (%)', SQLERRM, SQLSTATE;
    RETURN FALSE;
END
$$;