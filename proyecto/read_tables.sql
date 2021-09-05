BEGIN;
\echo
\echo 'tb_flights'
SELECT count(*) FROM tb_flights;
SELECT * FROM tb_flights LIMIT (5);
\echo
\echo 'tb_airports'
SELECT count(*) FROM tb_airports;
SELECT * FROM tb_airports LIMIT (5);
\echo
\echo 'tb_weather'
SELECT count(*) FROM tb_weather;
SELECT * FROM tb_weather LIMIT (5);
\echo
\echo 'tb_proyecto'
SELECT count(*) FROM tb_proyecto;
SELECT * FROM tb_proyecto LIMIT (5);
\echo
\echo 'tb_proyectoml'
SELECT count(*) FROM tb_proyectoml;
SELECT * FROM tb_proyectoml LIMIT (5);
\echo
\echo 'tb_modelolr'
SELECT count(*) FROM tb_modelolr;
SELECT * FROM tb_modelolr LIMIT (5);
\echo
\echo 'tb_modelorf'
SELECT count(*) FROM tb_modelorf;
SELECT * FROM tb_modelorf LIMIT (5);

\echo
\dt[+] tb_*;

COMMIT;