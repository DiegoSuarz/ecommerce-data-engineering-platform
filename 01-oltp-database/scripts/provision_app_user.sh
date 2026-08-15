#!/usr/bin/env bash

set -euo pipefail

# ------------------------------------------------------------
# E-Commerce Data Engineering Platform
# MySQL Application User Provisioning
# ------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"

CONTAINER_NAME="ecommerce-mysql"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: .env file not found."
    exit 1
fi

set -a
source "$ENV_FILE"
set +a

docker exec \
    -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" \
    -i "$CONTAINER_NAME" \
    mysql \
    -u root <<SQL
CREATE USER IF NOT EXISTS '${MYSQL_USER}'@'%'
    IDENTIFIED BY '${MYSQL_PASSWORD}';

ALTER USER '${MYSQL_USER}'@'%'
    IDENTIFIED BY '${MYSQL_PASSWORD}';

GRANT SELECT, INSERT, UPDATE, DELETE
ON ${MYSQL_DATABASE}.*
TO '${MYSQL_USER}'@'%';


SQL

USER_EXISTS="$(
    docker exec \
        -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" \
        "$CONTAINER_NAME" \
        mysql \
        -u root \
        -N \
        -B \
        -e "
SELECT COUNT(*)
FROM mysql.user
WHERE user = '${MYSQL_USER}'
  AND host = '%';
"
)"

if [[ "$USER_EXISTS" -ne 1 ]]; then
    echo "ERROR: MySQL application user was not provisioned correctly."
    exit 1
fi

echo "Application user '${MYSQL_USER}' provisioned successfully."

docker exec \
    -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" \
    "$CONTAINER_NAME" \
    mysql \
    -u root \
    -e "SHOW GRANTS FOR '${MYSQL_USER}'@'%';"
