-- Function: tiger.ar_pprint_addy(norm_addy)

-- DROP FUNCTION tiger.ar_pprint_addy(norm_addy);

CREATE OR REPLACE FUNCTION tiger.ar_pprint_addy(input norm_addy)
  RETURNS character varying AS
$BODY$
DECLARE
  result VARCHAR;
BEGIN
  IF NOT input.parsed THEN
    RETURN NULL;
  END IF;

  result := COALESCE(' ' || input.preDirAbbrev, '')
         || CASE WHEN is_pretype(input.streetTypeAbbrev) THEN ' ' || input.streetTypeAbbrev  ELSE '' END
         || CASE WHEN NOT is_pretype(input.streetTypeAbbrev) THEN ' ' || input.streetTypeAbbrev  ELSE '' END
         || COALESCE(' ' || input.streetName, '')
         || COALESCE(' ' || input.address, '')
         || COALESCE(' ' || input.postDirAbbrev, '')
         || CASE WHEN
              input.address IS NOT NULL OR
              input.streetName IS NOT NULL
              THEN ', ' ELSE '' END
         || cull_null(input.internal)
         || CASE WHEN input.internal IS NOT NULL THEN ', ' ELSE '' END
         || COALESCE(input.zip || ' ', '')
         || cull_null(input.location)
         || CASE WHEN input.location IS NOT NULL THEN ', ' ELSE '' END
         || COALESCE(input.stateAbbrev || ' ' , '');

  RETURN trim(result);

END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;
ALTER FUNCTION tiger.ar_pprint_addy(norm_addy)
  OWNER TO postgres;
COMMENT ON FUNCTION tiger.ar_pprint_addy(norm_addy) IS 'args: in_addy - Given a norm_addy composite type object, returns a pretty print representation of it. Usually used in conjunction with normalize_address.';
