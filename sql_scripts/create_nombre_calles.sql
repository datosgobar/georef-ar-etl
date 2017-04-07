CREATE TABLE public.nombre_calles AS 
	SELECT pc.id, pc.nombre_completo, pc.nombre_abreviado, pc.apellido, pc.nombre, tp.nombre tipo_camino, pl.nombre localidad, pp.nombre partido, ppv.nombre provincia
	FROM postal.calles pc INNER JOIN postal.localidades pl 
	ON pc.id_localidad = pl.id INNER JOIN postal.tipos_camino tp 
	ON pc.id_tipo_camino = tp.id INNER JOIN postal.parajes pp
	ON pl.id_paraje = pp.id INNER JOIN postal.provincias ppv
	ON pp.id_provincia = ppv.id;