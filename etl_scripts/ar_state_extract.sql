-- Function: tiger.ar_state_extract(character varying)

-- DROP FUNCTION tiger.ar_state_extract(character varying);

CREATE OR REPLACE FUNCTION tiger.ar_state_extract(rawinput character varying)
  RETURNS character varying AS
$BODY$
DECLARE
  tempInt INTEGER;
  tempString VARCHAR;
  state VARCHAR;
  stateAbbrev VARCHAR;
  result VARCHAR;
  rec RECORD;
  test BOOLEAN;
  ws VARCHAR;
  var_verbose boolean := false;
BEGIN
  ws := E'[ ,.\t\n\f\r]';

  -- If there is a trailing space or , get rid of it
  -- this is to handle case where people use , instead of space to separate state and zip
  -- such as '2450 N COLORADO ST, PHILADELPHIA, PA, 19132' instead of '2450 N COLORADO ST, PHILADELPHIA, PA 19132'
  
  --tempString := regexp_replace(rawInput, E'(.*)' || ws || '+', E'\\1');
  tempString := btrim(rawInput, ', ');
  -- Separate out the last word of the state, and use it to compare to
  -- the state lookup table to determine the entire name, as well as the
  -- abbreviation associated with it.  The zip code may or may not have
  -- been found.
  tempString := substring(tempString from ws || E'+([^ ,.\t\n\f\r0-9]*?)$');
  IF var_verbose THEN RAISE NOTICE 'state_extract rawInput: % tempString: %', rawInput, tempString; END IF;
  SELECT INTO tempInt count(*) FROM (select distinct abbrev from ar_state_lookup
      WHERE upper(abbrev) = upper(tempString)) as blah;
  IF tempInt = 1 THEN
    state := tempString;
    SELECT INTO stateAbbrev abbrev FROM (select distinct abbrev from
        ar_state_lookup WHERE upper(abbrev) = upper(tempString)) as blah;
  ELSE
    SELECT INTO tempInt count(*) FROM ar_state_lookup WHERE upper(name)
        like upper('%' || tempString);
    IF tempInt >= 1 THEN
      FOR rec IN SELECT name from ar_state_lookup WHERE upper(name)
          like upper('%' || tempString) LOOP
        SELECT INTO test texticregexeq(rawInput, name) FROM ar_state_lookup
            WHERE rec.name = name;
        IF test THEN
          SELECT INTO stateAbbrev abbrev FROM ar_state_lookup
              WHERE rec.name = name;
          state := substring(rawInput, '(?i)' || rec.name);
          EXIT;
        END IF;
      END LOOP;
    ELSE
      -- No direct match for state, so perform fuzzy match.
      SELECT INTO tempInt count(*) FROM ar_state_lookup
          WHERE soundex(tempString) = end_soundex(name);
      IF tempInt >= 1 THEN
        FOR rec IN SELECT name, abbrev FROM ar_state_lookup
            WHERE soundex(tempString) = end_soundex(name) LOOP
          tempInt := count_words(rec.name);
          tempString := get_last_words(rawInput, tempInt);
          test := TRUE;
          FOR i IN 1..tempInt LOOP
            IF soundex(split_part(tempString, ' ', i)) !=
               soundex(split_part(rec.name, ' ', i)) THEN
              test := FALSE;
            END IF;
          END LOOP;
          IF test THEN
            state := tempString;
            stateAbbrev := rec.abbrev;
            EXIT;
          END IF;
        END LOOP;
      END IF;
    END IF;
  END IF;

  IF state IS NOT NULL AND stateAbbrev IS NOT NULL THEN
    result := state || ':' || stateAbbrev;
  END IF;

  RETURN result;
END;
$BODY$
  LANGUAGE plpgsql STABLE
  COST 100;
ALTER FUNCTION tiger.ar_state_extract(character varying)
  OWNER TO postgres;
