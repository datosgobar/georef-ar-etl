CREATE OR REPLACE FUNCTION get_municipality(code_bahra TEXT, OUT result TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
  SELECT m.in1
  INTO result
  FROM ign_municipios m, ign_bahra b
  WHERE st_contains(m.geom, b.geom)
  AND b.cod_bahra = code_bahra;
  EXCEPTION
  WHEN OTHERS
    THEN
      result := SQLERRM;
END;
$$;