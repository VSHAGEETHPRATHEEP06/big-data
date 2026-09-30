# PostgreSQL 17 with the schema and reference data baked in.
# (No bind mounts: Docker Desktop on macOS blocks on folders under ~/Downloads,
# and baking the files in also makes the stack reproducible anywhere.)
FROM postgres:17-alpine
COPY db/init/ /docker-entrypoint-initdb.d/
COPY config/ /opt/fleet/config/
