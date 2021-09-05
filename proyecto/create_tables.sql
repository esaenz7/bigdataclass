BEGIN;

DROP TABLE IF EXISTS tb_flights;
CREATE TABLE tb_flights (
id SERIAL PRIMARY KEY,
date DATE NULL,
orig VARCHAR(50) NULL,
dest VARCHAR(50) NULL,
carrier VARCHAR(50) NULL,
sdeptim NUMERIC (32,16) NULL,
depdel NUMERIC (32,16) NULL,
txout NUMERIC (32,16) NULL,
sarrtim NUMERIC (32,16) NULL,
selap NUMERIC (32,16) NULL,
dist NUMERIC (32,16) NULL,
dyofwk NUMERIC (32,16) NULL,
wkofyr NUMERIC (32,16) NULL,
sdephr NUMERIC (32,16) NULL,
sarrhr NUMERIC (32,16) NULL,
label INTEGER NULL
);

DROP TABLE IF EXISTS tb_airports;
CREATE TABLE tb_airports (
id SERIAL PRIMARY KEY,
iata VARCHAR(50) NULL,
icao VARCHAR(50) NULL
);

DROP TABLE IF EXISTS tb_weather;
CREATE TABLE tb_weather (
id SERIAL PRIMARY KEY,
date DATE NULL,
wtyp VARCHAR(50) NULL,
wsev VARCHAR(50) NULL,
icao VARCHAR(50) NULL,
evhr NUMERIC (32,16) NULL,
evtim NUMERIC (32,16) NULL
);

DROP TABLE IF EXISTS tb_proyecto;
CREATE TABLE tb_proyecto (
id SERIAL PRIMARY KEY,
carrier VARCHAR(50) NULL,
sdephr NUMERIC (32,16) NULL,
sarrhr NUMERIC (32,16) NULL,
dyofwk NUMERIC (32,16) NULL,
wkofyr NUMERIC (32,16) NULL,
wtyp VARCHAR(50) NULL,
wsev VARCHAR(50) NULL,
depdel NUMERIC (32,16) NULL,
txout NUMERIC (32,16) NULL,
selap NUMERIC (32,16) NULL,
dist NUMERIC (32,16) NULL,
evtim NUMERIC (32,16) NULL,
label INTEGER NULL
);

DROP TABLE IF EXISTS tb_proyectoml;
CREATE TABLE tb_proyectoml (
id SERIAL PRIMARY KEY
);

DROP TABLE IF EXISTS tb_modelolr;
CREATE TABLE tb_modelolr (
id SERIAL PRIMARY KEY
);

DROP TABLE IF EXISTS tb_modelorf;
CREATE TABLE tb_modelorf (
id SERIAL PRIMARY KEY
);

\echo
SELECT table_name, column_name, data_type, numeric_precision, numeric_scale
FROM information_schema.columns WHERE TABLE_NAME SIMILAR TO ('tb%|')
ORDER BY 1;

\echo
\dt[+] tb_*;

COMMIT;