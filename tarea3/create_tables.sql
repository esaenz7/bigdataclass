BEGIN;

DROP TABLE IF EXISTS tarea3;
CREATE TABLE tarea3 (
  id SERIAL PRIMARY KEY,
  vmag NUMERIC (32,16) NULL,
  plx NUMERIC (32,16) NULL,
  bv NUMERIC (32,16) NULL,
  amag NUMERIC (32,16) NULL,
  class INTEGER NULL
);

DROP TABLE IF EXISTS modelo1;
CREATE TABLE modelo1 (
  id SERIAL PRIMARY KEY,
  vmag NUMERIC (32,16) NULL,
  plx NUMERIC (32,16) NULL,
  bv NUMERIC (32,16) NULL,
  amag NUMERIC (32,16) NULL,
  class INTEGER NULL,
  prediccion INTEGER NULL
);

DROP TABLE IF EXISTS modelo2;
CREATE TABLE modelo2 (
  id SERIAL PRIMARY KEY,
  vmag NUMERIC (32,16) NULL,
  plx NUMERIC (32,16) NULL,
  bv NUMERIC (32,16) NULL,
  amag NUMERIC (32,16) NULL,
  class INTEGER NULL,
  prediccion INTEGER NULL
);

DROP TABLE IF EXISTS modelo3;
CREATE TABLE modelo3 (
  id SERIAL PRIMARY KEY,
  vmag NUMERIC (32,16) NULL,
  plx NUMERIC (32,16) NULL,
  bv NUMERIC (32,16) NULL,
  amag NUMERIC (32,16) NULL,
  class INTEGER NULL,
  prediccion INTEGER NULL
);

DROP TABLE IF EXISTS temp;
CREATE TABLE temp (
  id SERIAL PRIMARY KEY,
  vmag NUMERIC (32,16) NULL,
  plx NUMERIC (32,16) NULL,
  bv NUMERIC (32,16) NULL,
  amag NUMERIC (32,16) NULL,
  class INTEGER NULL
);

SELECT table_name, column_name, data_type, numeric_precision, numeric_scale
FROM information_schema.columns WHERE TABLE_NAME SIMILAR TO ('tarea%|modelo%|temp%')
ORDER BY 1;

\dt[+]

COMMIT;