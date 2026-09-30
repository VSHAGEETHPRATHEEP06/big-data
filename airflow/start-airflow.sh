#!/usr/bin/env bash
# Runs the Airflow scheduler, DAG processor and API server in ONE container.
# The official compose file uses one container per component plus Celery and
# Redis. That needs more memory than our 8 GB laptop can give, so we use the
# LocalExecutor and a single container. If any component stops, the whole
# container exits and Docker restarts it (restart: unless-stopped).
set -euo pipefail

airflow db migrate

# Password file for the Simple auth manager (user name and password from .env)
python - <<'EOF'
import json, os
path = os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"]
user = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
with open(path, "w") as fh:
    json.dump({user: os.environ.get("AIRFLOW_ADMIN_PASSWORD", "admin")}, fh)
EOF

# --skip-serve-logs: the API server reads task logs from this same container
airflow scheduler --skip-serve-logs &
airflow dag-processor &
airflow api-server --port 8080 &

wait -n || true
echo '{"level": "ERROR", "service": "airflow", "stage": "orchestration", "event": "airflow_component_exited"}'
kill $(jobs -p) 2>/dev/null || true
exit 1
