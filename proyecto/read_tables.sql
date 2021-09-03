BEGIN;
\echo
\echo 'tb_proyecto'
SELECT count(*) FROM tb_proyecto;
SELECT * FROM tb_proyecto LIMIT (5);
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
\echo
\dt[+] tb_proyecto;
\dt[+] tb_flights;
\dt[+] tb_airports;
\dt[+] tb_weather;
\echo
COMMIT;