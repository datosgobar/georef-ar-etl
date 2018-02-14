CREATE OR REPLACE FUNCTION get_department(code_muncipality TEXT, OUT result TEXT) AS $$
  BEGIN
    SELECT d.code INTO result
    FROM geo_admin_municipality m, geo_admin_department d
    WHERE m.code = code_muncipality
    ORDER BY st_area(st_intersection(m.geom, d.geom)) DESC
    LIMIT 1;
    EXCEPTION
    WHEN OTHERS
      THEN
        result := SQLERRM;
  END;
$$
LANGUAGE 'plpgsql';