--
-- Descripción: Función que procesa si hay coincidencias entre la tabla geocode.red_vial y nombre_calles
-- Nota: se debe importar la tabla nombre_calles

CREATE OR REPLACE FUNCTION get_match_red_vial(_id INTEGER) RETURNS INTEGER AS $$
DECLARE
    id_match INTEGER;
    result INTEGER;
BEGIN
    SELECT rv.gid INTO id_match
    FROM public.nombre_calles nc, geocode.red_vial rv
    WHERE nc.id = $1
    AND nc.nombre_completo = rv.nombre
    AND rv.ciudad_data_text ILIKE '%' || nc.localidad ||'%'
    AND nc.partido = rv.departamento_nom
    AND nc.provincia = rv.provincia_nom;
        IF id_match NOTNULL THEN
            UPDATE public.nombre_calles
                SET id_red_vial = id_match
            WHERE id = $1;
            GET DIAGNOSTICS result = ROW_COUNT;
        ELSE
            UPDATE public.nombre_calles
                SET id_red_vial = -1
            WHERE id = $1;
            GET DIAGNOSTICS result = ROW_COUNT;
        END IF ;
     RETURN result;
END;
$$ LANGUAGE plpgsql;


-- Descripción: Ejemplo de uso
-- Parámetros: Límite de transiciones (Integer)
SELECT get_match_red_vial(c.id)
FROM public.nombre_calles c
WHERE c.id_red_vial ISNULL
LIMIT 100000;
