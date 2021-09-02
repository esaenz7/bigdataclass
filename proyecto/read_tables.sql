BEGIN;

\dt[+] tb_proyecto;
SELECT * FROM tb_proyecto LIMIT (5);
SELECT count(*) FROM tb_proyecto;
\dt[+] tb_flights;
SELECT * FROM tb_flights LIMIT (5);
SELECT count(*) FROM tb_flights;
\dt[+] tb_airports;
SELECT * FROM tb_airports LIMIT (5);
SELECT count(*) FROM tb_airports;
\dt[+] tb_weather;
SELECT * FROM tb_weather LIMIT (5);
SELECT count(*) FROM tb_weather;

COMMIT;