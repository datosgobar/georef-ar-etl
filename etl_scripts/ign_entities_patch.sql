/*
** PARCHE ENTIDADES
*/

-- Todas las entidades pasan a ser del tipo MultiPolygon
ALTER TABLE IF EXISTS ign_pais ALTER COLUMN geom TYPE geometry(MultiPolygon, 4326) using ST_Multi(geom);
ALTER TABLE IF EXISTS ign_provincias ALTER COLUMN geom TYPE geometry(MultiPolygon, 4326) using ST_Multi(geom);
ALTER TABLE IF EXISTS ign_departamentos ALTER COLUMN geom TYPE geometry(MultiPolygon, 4326) using ST_Multi(geom);
ALTER TABLE IF EXISTS ign_municipios ALTER COLUMN geom TYPE geometry(MultiPolygon, 4326) using ST_Multi(geom);

-- PAÍS
DELETE FROM ign_pais WHERE fna ISNULL;

-- DEPARTAMENTOS
DELETE FROM ign_departamentos WHERE ogc_fid = 530; -- duplicado

UPDATE ign_departamentos SET in1 = '54084' WHERE in1 = '55084'; -- Error de tipeo
UPDATE ign_departamentos SET in1 = '06217' WHERE in1 = '06218'; -- Chascomús
UPDATE ign_departamentos SET in1 = '94008' WHERE in1 = '94007'; -- Río Grande *
UPDATE ign_departamentos SET in1 = '94015' WHERE in1 = '94014'; -- Usuhaia **
UPDATE ign_departamentos SET in1 = '94011' WHERE fna = 'Departamento Río Grande' AND nam = 'Tolhuin'; -- Nulo

-- MUNICIPIOS
DELETE FROM ign_municipios WHERE in1 ISNULL ;
DELETE FROM ign_municipios WHERE gna ISNULL ;

-- Error de asignación de código
UPDATE ign_municipios SET in1 = '540287' WHERE in1 = '550287';
UPDATE ign_municipios SET in1 = '540343' WHERE in1 = '550343';
UPDATE ign_municipios SET in1 = '820277' WHERE in1 = '800277';
UPDATE ign_municipios SET in1 = '585070' WHERE in1 = '545070';
UPDATE ign_municipios SET in1 = '589999' WHERE in1 = '549999';
UPDATE ign_municipios SET in1 = '629999' WHERE in1 = '829999';


-- BARHA
DELETE FROM ign_bahra WHERE cod_bahra ISNULL;
DELETE FROM ign_bahra WHERE nombre_bah = 'EL FICAL'; -- duplicado

UPDATE ign_bahra SET cod_depto = '007' WHERE nom_depto = 'COMUNA 1';
UPDATE ign_bahra SET cod_depto = '014' WHERE nom_depto = 'COMUNA 2';
UPDATE ign_bahra SET cod_depto = '021' WHERE nom_depto = 'COMUNA 3';
UPDATE ign_bahra SET cod_depto = '028' WHERE nom_depto = 'COMUNA 4';
UPDATE ign_bahra SET cod_depto = '035' WHERE nom_depto = 'COMUNA 5';
UPDATE ign_bahra SET cod_depto = '042' WHERE nom_depto = 'COMUNA 6';
UPDATE ign_bahra SET cod_depto = '049' WHERE nom_depto = 'COMUNA 7';
UPDATE ign_bahra SET cod_depto = '056' WHERE nom_depto = 'COMUNA 8';
UPDATE ign_bahra SET cod_depto = '063' WHERE nom_depto = 'COMUNA 9';
UPDATE ign_bahra SET cod_depto = '070' WHERE nom_depto = 'COMUNA 10';
UPDATE ign_bahra SET cod_depto = '077' WHERE nom_depto = 'COMUNA 11';
UPDATE ign_bahra SET cod_depto = '084' WHERE nom_depto = 'COMUNA 12';
UPDATE ign_bahra SET cod_depto = '091' WHERE nom_depto = 'COMUNA 13';
UPDATE ign_bahra SET cod_depto = '098' WHERE nom_depto = 'COMUNA 14';
UPDATE ign_bahra SET cod_depto = '105' WHERE nom_depto = 'COMUNA 15';

-- * Actualiza códigos para los asentamientos del departamento de Río Grande
UPDATE ign_bahra SET cod_bahra = '94008000010' WHERE cod_bahra = '94007000010';
UPDATE ign_bahra SET cod_bahra = '94008000013' WHERE cod_bahra = '94007000013';
UPDATE ign_bahra SET cod_bahra = '94008000073' WHERE cod_bahra = '94007000073';
UPDATE ign_bahra SET cod_bahra = '94008000146' WHERE cod_bahra = '94007000146';
UPDATE ign_bahra SET cod_bahra = '94008010000' WHERE cod_bahra = '94007010000';
UPDATE ign_bahra SET cod_bahra = '94008020000' WHERE cod_bahra = '94007020000';
UPDATE ign_bahra SET cod_depto = '008' WHERE cod_prov = '94' AND cod_depto = '007';

-- ** Actualiza códigos para los asentamientos del departamento de Usuhaia
UPDATE ign_bahra SET cod_bahra = '94015010000' WHERE cod_bahra = '94014010000';
UPDATE ign_bahra SET cod_bahra = '94015020000' WHERE cod_bahra = '94014020000';
UPDATE ign_bahra SET cod_depto = '015' WHERE cod_prov = '94' AND cod_depto = '014';
