/*
** PARCHE ENTIDADES
*/

-- Todas las entidades pasan a ser del tipo MultiPolygon
ALTER TABLE IF EXISTS ign_provincias ALTER COLUMN geom TYPE geometry(MultiPolygon, 4326) using ST_Multi(geom);
ALTER TABLE IF EXISTS ign_departamentos ALTER COLUMN geom TYPE geometry(MultiPolygon, 4326) using ST_Multi(geom);
ALTER TABLE IF EXISTS ign_municipios ALTER COLUMN geom TYPE geometry(MultiPolygon, 4326) using ST_Multi(geom);

-- DEPARTAMENTOS
DELETE FROM ign_departamentos WHERE ogc_fid = 530; -- duplicado

UPDATE ign_departamentos SET in1 = '54084' WHERE in1 = '55084';
UPDATE ign_departamentos SET in1 = '06217' WHERE in1 = '06218';
UPDATE ign_departamentos SET in1 = '94008' WHERE in1 = '94007';
UPDATE ign_departamentos SET in1 = '94015' WHERE in1 = '94014';
UPDATE ign_departamentos SET in1 = '94011' WHERE fna = 'Departamento Río Grande' AND nam = 'Tolhuin';

-- MUNICIPIOS
DELETE FROM ign_municipios WHERE in1 ISNULL ;

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
