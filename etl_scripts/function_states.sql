CREATE OR REPLACE FUNCTION get_state_by_code(code CHARACTER(2), OUT result VARCHAR)
LANGUAGE plpgsql
AS $$
  BEGIN
    SELECT upper(nam) INTO result
    FROM ign_provincias
    WHERE in1 = code;
  EXCEPTION
  WHEN OTHERS
    THEN
      result := SQLERRM;
END $$;