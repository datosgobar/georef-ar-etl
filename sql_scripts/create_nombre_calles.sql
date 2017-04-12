--
-- Función que elimina duplicados de la tabla postal.localidades
-- Name: delete_duplicates(integer, integer, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION delete_duplicates(_id integer, _id_paraje integer, _nombre character varying) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
	delete from postal.localidades where id = (
		select id
		from postal.localidades
		where id != _id
		and id_paraje = _id_paraje
		and nombre = _nombre
		and id > _id
		order by nombre asc
		limit 1

	);
END;
$$;

--
-- Tabla nombre de calles
--

CREATE TABLE public.nombre_calles AS
	SELECT pc.id, pc.nombre_completo, pc.nombre_abreviado, pc.apellido, pc.nombre, tp.nombre tipo_camino, pl.nombre localidad, pp.nombre partido, ppv.nombre provincia
	FROM postal.calles pc INNER JOIN postal.localidades pl 
	ON pc.id_localidad = pl.id INNER JOIN postal.tipos_camino tp 
	ON pc.id_tipo_camino = tp.id INNER JOIN postal.parajes pp
	ON pl.id_paraje = pp.id INNER JOIN postal.provincias ppv
	ON pp.id_provincia = ppv.id;