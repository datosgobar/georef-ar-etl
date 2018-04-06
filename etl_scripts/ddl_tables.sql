CREATE TABLE public.indec_cuadras
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