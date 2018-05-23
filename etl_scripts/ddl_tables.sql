CREATE TABLE IF NOT EXISTS public.indec_cuadras
(
  fid character varying(25),
  geom geometry(MultiLineString),
  fnode_ character varying(25),
  tnode_ character varying(25),
  lpoly_ character varying(25),
  rpoly_ character varying(25),
  length character varying(25),
  codigo10 character varying(25),
  nomencla character varying(25),
  codigo20 character varying(25),
  ancho character varying(25),
  anchomed character varying(25),
  tipo character varying(25),
  nombre character varying(100),
  ladoi character varying(25),
  ladod character varying(25),
  desdei character varying(25),
  desded character varying(25),
  hastai character varying(25),
  hastad character varying(25),
  mzai character varying(25),
  mzad character varying(25),
  codloc20 character varying(25),
  nomencla10 character varying(25),
  nomenclai character varying(25),
  nomenclad character varying(25)
)
WITH (
  OIDS=FALSE
);

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
