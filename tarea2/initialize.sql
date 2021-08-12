BEGIN;

CREATE TABLE metricas (
  id SERIAL PRIMARY KEY,
  tipo_metrica VARCHAR(50) NOT NULL,
  valor VARCHAR(50) NOT NULL,
  fecha DATE without time zone NOT NULL
);

-- INSERT INTO "metricas" (tipo_metrica, valor, fecha) VALUES
-- (0, 0, '2021-08-15 00:00:00');

COMMIT;
