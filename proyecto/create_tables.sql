BEGIN;

DROP TABLE IF EXISTS tb_flights;
CREATE TABLE tb_flights (
id SERIAL PRIMARY KEY
);

DROP TABLE IF EXISTS tb_airports;
CREATE TABLE tb_airports (
id SERIAL PRIMARY KEY
);

DROP TABLE IF EXISTS tb_weather;
CREATE TABLE tb_weather (
id SERIAL PRIMARY KEY
);

DROP TABLE IF EXISTS tb_proyecto;
CREATE TABLE tb_proyecto (
id SERIAL PRIMARY KEY
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