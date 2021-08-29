BEGIN;

\dt[+] tarea3;
SELECT * FROM tarea3 LIMIT (5);
SELECT count(*) FROM tarea3;
\dt[+] modelo1;
SELECT * FROM modelo1 LIMIT (5);
SELECT count(*) FROM modelo1;
\dt[+] modelo2;
SELECT * FROM modelo2 LIMIT (5);
SELECT count(*) FROM modelo2;
\dt[+] modelo3;
SELECT * FROM modelo3 LIMIT (5);
SELECT count(*) FROM modelo3;

COMMIT;