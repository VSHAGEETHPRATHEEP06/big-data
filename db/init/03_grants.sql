-- Grafana only needs to read. Give it a separate least-privilege account.
GRANT USAGE ON SCHEMA ref, ops, speed, batch, serving TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ref, ops, speed, batch, serving TO grafana_reader;
GRANT EXECUTE ON FUNCTION ops.sim_now() TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA ref, ops, speed, batch, serving GRANT SELECT ON TABLES TO grafana_reader;
