CREATE OR REPLACE FUNCTION get_department(code CHARACTER(5), OUT result VARCHAR)
LANGUAGE plpgsql
AS $$
  BEGIN
    SELECT upper(nam) INTO result
    FROM ign_departamentos
    WHERE in1 = code;
  EXCEPTION
  WHEN OTHERS
    THEN
      result := SQLERRM;
END $$;


CREATE OR REPLACE FUNCTION get_department(code_muncipality TEXT, OUT result TEXT)
LANGUAGE plpgsql
AS $$
  BEGIN
    SELECT d.in1
    INTO result
    FROM ign_municipios m, ign_departamentos d
    WHERE m.in1 = code_muncipality
    ORDER BY st_area(st_intersection(m.geom, d.geom)) DESC
    LIMIT 1;
  EXCEPTION
  WHEN OTHERS
    THEN
      result := SQLERRM;
END;
$$;